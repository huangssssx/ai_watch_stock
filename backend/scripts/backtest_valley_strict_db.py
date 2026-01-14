import argparse
import contextlib
import datetime as _datetime
import io
import os
import random
import sqlite3
import sys
import time
import types
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import akshare as real_ak
import numpy as np
import pandas as pd


def _normalize_symbol(code: str) -> str:
    s = "" if code is None else str(code).strip()
    if s.startswith(("sh", "sz", "bj")) and len(s) >= 8:
        return s[2:]
    return s


def _pick_universe(sample_size: int, seed: int):
    n = int(sample_size)
    if n <= 0:
        return {}
    random.seed(int(seed))

    spot = None
    for _ in range(3):
        try:
            spot = real_ak.stock_zh_a_spot_em()
            if spot is not None and not spot.empty:
                break
        except Exception:
            spot = None
        time.sleep(0.7)

    if spot is not None and not spot.empty:
        spot = spot.copy()
        spot = spot[~spot["名称"].str.contains("ST|退", na=False)]
        spot["流通市值"] = pd.to_numeric(spot["流通市值"], errors="coerce")
        spot = spot.dropna(subset=["代码", "名称", "流通市值"])
        if not spot.empty:
            spot["代码"] = spot["代码"].map(_normalize_symbol)
            spot = spot[spot["代码"].str.len() >= 6]
            spot = spot.sort_values("流通市值").reset_index(drop=True)
            n = min(n, len(spot))
            thirds = np.array_split(spot, 3)
            sizes = [n // 3, n // 3, n - 2 * (n // 3)]
            chosen = []
            for part, k in zip(thirds, sizes):
                if part.empty or k <= 0:
                    continue
                idxs = list(part.index)
                sel = idxs if k >= len(idxs) else random.sample(idxs, k)
                chosen.append(spot.loc[sel])
            uni = pd.concat(chosen, axis=0).drop_duplicates(subset=["代码"]).reset_index(drop=True)
            out = {}
            for _, row in uni.iterrows():
                code = str(row["代码"])
                out[code] = {
                    "code": code,
                    "name": str(row["名称"]),
                    "mkt_cap": float(row["流通市值"]) if pd.notna(row["流通市值"]) else 100e8,
                }
            return out

    info = real_ak.stock_info_a_code_name()
    if info is None or info.empty:
        return {}
    info = info.rename(columns={"code": "代码", "name": "名称"})
    info = info[~info["名称"].str.contains("ST|退", na=False)]
    info["代码"] = info["代码"].map(_normalize_symbol)
    info = info[info["代码"].str.len() >= 6].reset_index(drop=True)
    if info.empty:
        return {}
    n = min(n, len(info))
    idxs = list(info.index)
    sel = idxs if n >= len(idxs) else random.sample(idxs, n)
    uni = info.loc[sel].reset_index(drop=True)
    out = {}
    for _, row in uni.iterrows():
        code = str(row["代码"])
        out[code] = {"code": code, "name": str(row["名称"]), "mkt_cap": 100e8}
    return out


def _load_script_from_db(db_path: str, screener_id: int) -> str:
    if not os.path.exists(db_path):
        raise FileNotFoundError(db_path)
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        cur.execute("SELECT script_content FROM stock_screeners WHERE id = ?", (int(screener_id),))
        row = cur.fetchone()
        if not row or not row[0]:
            raise ValueError(f"stock_screeners.id={screener_id} script_content empty")
        return str(row[0])
    finally:
        con.close()


def _load_script_from_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _parse_yyyymmdd(s: str):
    if not s:
        return None
    try:
        return _datetime.datetime.strptime(str(s), "%Y%m%d").date()
    except Exception:
        return None


@dataclass
class BacktestConfig:
    sample_size: int
    seed: int
    test_days: int
    cooldown_days: int
    entry_delay_days: int
    screener_db_id: int
    db_path: str
    v7_file_path: str


class _FixedDateTime(_datetime.datetime):
    _fixed_now = None

    @classmethod
    def now(cls, tz=None):
        if cls._fixed_now is None:
            return super().now(tz=tz)
        if tz is not None:
            return cls._fixed_now.astimezone(tz)
        return cls._fixed_now


class FakeAkshareModule(types.ModuleType):
    def __init__(
        self,
        name: str,
        universe: dict,
        daily_by_code: dict,
        spot_caps: dict,
        current_date: _datetime.date,
        industry_by_code: dict,
    ):
        super().__init__(name)
        self._universe = universe
        self._daily_by_code = daily_by_code
        self._spot_caps = spot_caps
        self._current_date = current_date
        self._industry_by_code = industry_by_code

    def stock_zh_a_spot_em(self):
        rows = []
        for code, meta in self._universe.items():
            daily = self._daily_by_code.get(code)
            if daily is None or daily.empty:
                continue
            mask = daily["date_dt"] == self._current_date
            idxs = daily.index[mask].tolist()
            if not idxs:
                continue
            i = int(idxs[0])
            close = float(daily.loc[i, "收盘"])
            open_ = float(daily.loc[i, "开盘"])
            high = float(daily.loc[i, "最高"])
            low = float(daily.loc[i, "最低"])
            vol = float(daily.loc[i, "成交量"])
            amt = float(daily.loc[i, "成交额"])
            prev_close = float(daily.loc[i - 1, "收盘"]) if i - 1 >= 0 else close
            pct = ((close / prev_close) - 1.0) * 100.0 if prev_close > 0 else 0.0

            vol_hist = daily.loc[max(0, i - 5) : i - 1, "成交量"]
            vol_mean = float(pd.to_numeric(vol_hist, errors="coerce").mean()) if len(vol_hist) else float("nan")
            vol_ratio = float(vol / vol_mean) if (np.isfinite(vol_mean) and vol_mean > 0) else float("nan")

            cap = float(self._spot_caps.get(code, meta.get("mkt_cap", 100e8)))
            turnover = float((amt / cap) * 100.0) if cap > 0 and np.isfinite(amt) else float("nan")

            rows.append(
                {
                    "代码": code,
                    "名称": meta.get("name", code),
                    "最新价": close,
                    "涨跌幅": pct,
                    "成交量": vol,
                    "成交额": amt,
                    "最高": high,
                    "最低": low,
                    "今开": open_,
                    "昨收": prev_close,
                    "量比": vol_ratio,
                    "换手率": turnover,
                    "流通市值": cap,
                    "总市值": cap,
                }
            )

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)

    def stock_zh_a_hist(self, symbol: str, period="daily", start_date=None, end_date=None, adjust="qfq", **kwargs):
        code = _normalize_symbol(symbol)
        daily = self._daily_by_code.get(code)
        if daily is None or daily.empty:
            return pd.DataFrame()
        sdt = _parse_yyyymmdd(start_date)
        edt = _parse_yyyymmdd(end_date)
        if edt is None:
            edt = self._current_date
        if sdt is None:
            sdt = daily["date_dt"].min()
        out = daily[(daily["date_dt"] >= sdt) & (daily["date_dt"] <= edt)].copy()
        if out.empty:
            return pd.DataFrame()
        keep = ["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率"]
        for c in keep:
            if c not in out.columns:
                out[c] = np.nan
        return out[keep].reset_index(drop=True)

    def stock_individual_info_em(self, symbol: str, timeout=None):
        code = _normalize_symbol(symbol)
        industry = self._industry_by_code.get(code)
        name = self._universe.get(code, {}).get("name", code)
        rows = [
            {"item": "股票代码", "value": code},
            {"item": "股票简称", "value": name},
            {"item": "行业", "value": industry if industry is not None else ""},
        ]
        return pd.DataFrame(rows)

    def stock_sector_fund_flow_rank(self, indicator="5日", sector_type="行业资金流", **kwargs):
        if str(indicator) != "5日" or str(sector_type) != "行业资金流":
            return pd.DataFrame()

        by_ind = {}
        for code, daily in self._daily_by_code.items():
            ind = self._industry_by_code.get(code)
            if not ind:
                continue
            idxs = daily.index[daily["date_dt"] == self._current_date].tolist()
            if not idxs:
                continue
            i = int(idxs[0])
            start = max(1, i - 4)
            window = daily.loc[start:i].copy()
            close = pd.to_numeric(window["收盘"], errors="coerce")
            amt = pd.to_numeric(window["成交额"], errors="coerce")
            rets = close.pct_change().fillna(0.0)
            flow = float(np.nansum(rets.to_numpy(dtype=float) * amt.fillna(0.0).to_numpy(dtype=float)))
            amt_sum = float(np.nansum(amt.fillna(0.0).to_numpy(dtype=float)))
            cur = by_ind.get(ind)
            if cur is None:
                by_ind[ind] = {"flow": flow, "amt": amt_sum}
            else:
                cur["flow"] += flow
                cur["amt"] += amt_sum

        rows = []
        for ind, v in by_ind.items():
            ratio = float((v["flow"] / (v["amt"] + 1e-9)) * 100.0) if v["amt"] > 0 else 0.0
            rows.append({"名称": ind, "5日主力净流入-净占比": ratio})
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df["5日主力净流入-净占比"] = pd.to_numeric(df["5日主力净流入-净占比"], errors="coerce")
        df = df.sort_values("5日主力净流入-净占比", ascending=False).reset_index(drop=True)
        df.insert(0, "序号", range(1, len(df) + 1))
        return df

    def stock_hot_rank_em(self, **kwargs):
        rows = []
        for code, meta in self._universe.items():
            daily = self._daily_by_code.get(code)
            if daily is None or daily.empty:
                continue
            idxs = daily.index[daily["date_dt"] == self._current_date].tolist()
            if not idxs:
                continue
            i = int(idxs[0])
            amt = float(pd.to_numeric(daily.loc[i, "成交额"], errors="coerce"))
            close = float(pd.to_numeric(daily.loc[i, "收盘"], errors="coerce"))
            prev = float(pd.to_numeric(daily.loc[i - 1, "收盘"], errors="coerce")) if i - 1 >= 0 else close
            pct = ((close / prev) - 1.0) * 100.0 if prev > 0 else 0.0
            rows.append({"代码": f"SH{code}", "股票名称": meta.get("name", code), "最新价": close, "涨跌幅": pct, "_amt": amt})
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df["_amt"] = pd.to_numeric(df["_amt"], errors="coerce").fillna(0.0)
        df = df.sort_values("_amt", ascending=False).reset_index(drop=True)
        df.insert(0, "当前排名", range(1, len(df) + 1))
        df["涨跌额"] = np.nan
        return df[["当前排名", "代码", "股票名称", "最新价", "涨跌额", "涨跌幅"]]

    def stock_js_weibo_report(self, time_period="CNHOUR24", **kwargs):
        if str(time_period) != "CNHOUR24":
            return pd.DataFrame()
        rows = []
        for code, meta in self._universe.items():
            daily = self._daily_by_code.get(code)
            if daily is None or daily.empty:
                continue
            idxs = daily.index[daily["date_dt"] == self._current_date].tolist()
            if not idxs:
                continue
            i = int(idxs[0])
            close = float(pd.to_numeric(daily.loc[i, "收盘"], errors="coerce"))
            prev = float(pd.to_numeric(daily.loc[i - 1, "收盘"], errors="coerce")) if i - 1 >= 0 else close
            pct = ((close / prev) - 1.0) * 100.0 if prev > 0 else 0.0
            rows.append({"name": meta.get("name", code), "rate": pct})
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df["rate"] = pd.to_numeric(df["rate"], errors="coerce")
        df = df.sort_values("rate", ascending=False).reset_index(drop=True)
        return df

    def tool_trade_date_hist_sina(self):
        today = self._current_date
        dates = pd.date_range(today - _datetime.timedelta(days=3650), today, freq="B").date
        return pd.DataFrame({"trade_date": [str(d) for d in dates]})


def _prepare_daily_history(code: str) -> Optional[pd.DataFrame]:
    df = None
    for _ in range(2):
        try:
            df = real_ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
            break
        except Exception:
            df = None
            time.sleep(0.5)
    if df is None:
        return None
    if df is None or df.empty:
        return None
    for c in ("开盘", "收盘", "最高", "最低", "成交量", "成交额"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["日期", "开盘", "收盘", "最高", "最低", "成交量"])
    if "成交额" not in df.columns or df["成交额"].isna().all():
        df["成交额"] = df["收盘"] * df["成交量"]
    df["date_dt"] = pd.to_datetime(df["日期"], errors="coerce").dt.date
    df = df[df["date_dt"].notna()].sort_values("date_dt").reset_index(drop=True)
    return df


def _exec_screener(script: str, fake_ak_mod: types.ModuleType, fixed_dt: _datetime.datetime):
    stdout = io.StringIO()

    old_ak = sys.modules.get("akshare")
    sys.modules["akshare"] = fake_ak_mod

    old_dt_cls = _datetime.datetime
    _FixedDateTime._fixed_now = fixed_dt
    _datetime.datetime = _FixedDateTime

    g = {"__name__": "__main__"}
    try:
        with contextlib.redirect_stdout(stdout):
            exec(script, g, g)
    finally:
        _datetime.datetime = old_dt_cls
        _FixedDateTime._fixed_now = None
        if old_ak is None:
            sys.modules.pop("akshare", None)
        else:
            sys.modules["akshare"] = old_ak

    df = g.get("df")
    if isinstance(df, pd.DataFrame):
        return df.copy()
    if "df_res" in g and isinstance(g["df_res"], pd.DataFrame):
        return g["df_res"].copy()
    if "results" in g and isinstance(g["results"], list):
        try:
            return pd.DataFrame(g["results"])
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def _calc_forward_metrics(daily: pd.DataFrame, entry_date: _datetime.date, entry_px: float):
    out = {"entry": float(entry_px)}
    if daily is None or daily.empty or not np.isfinite(entry_px) or entry_px <= 0:
        return out | {"ret_5": np.nan, "ret_10": np.nan, "ret_20": np.nan, "mae_10": np.nan, "mfe_10": np.nan, "recovery_days_60": None}

    idxs = daily.index[daily["date_dt"] == entry_date].tolist()
    if not idxs:
        return out | {"ret_5": np.nan, "ret_10": np.nan, "ret_20": np.nan, "mae_10": np.nan, "mfe_10": np.nan, "recovery_days_60": None}

    i = int(idxs[0])
    close = daily["收盘"].to_numpy(dtype=float)
    n = len(close)

    def _ret(h):
        j = i + int(h)
        if j >= n:
            return np.nan
        return float((close[j] - entry_px) / entry_px)

    out["ret_5"] = _ret(5)
    out["ret_10"] = _ret(10)
    out["ret_20"] = _ret(20)

    if i + 10 < n:
        window = close[i : i + 11]
        out["mae_10"] = float((np.nanmin(window) - entry_px) / entry_px)
        out["mfe_10"] = float((np.nanmax(window) - entry_px) / entry_px)
    else:
        out["mae_10"] = np.nan
        out["mfe_10"] = np.nan

    rec = None
    max_days = 60
    for d in range(1, max_days + 1):
        j = i + d
        if j >= n:
            break
        if close[j] >= entry_px:
            rec = d
            break
    out["recovery_days_60"] = rec
    return out


def _analyze(results: List[Dict[str, Any]], title: str):
    if not results:
        return {
            "title": title,
            "count": 0,
            "uniq": 0,
        }
    df = pd.DataFrame(results).replace([np.inf, -np.inf], np.nan)
    uniq = int(df["code"].nunique()) if "code" in df.columns else 0

    def _win_rate(col):
        x = pd.to_numeric(df[col], errors="coerce").dropna()
        if x.empty:
            return np.nan
        return float((x > 0).mean())

    def _mean(col):
        x = pd.to_numeric(df[col], errors="coerce")
        return float(x.mean()) if x.notna().any() else np.nan

    mae = pd.to_numeric(df.get("mae_10"), errors="coerce")
    false_rate = float((mae <= -0.05).mean()) if mae.notna().any() else np.nan
    worst_mae = float(mae.min()) if mae.notna().any() else np.nan
    rec = pd.to_numeric(df.get("recovery_days_60"), errors="coerce")
    rec_ok = float(rec.notna().mean()) if rec is not None and len(rec) else np.nan
    rec_avg = float(rec.mean()) if rec.notna().any() else np.nan

    return {
        "title": title,
        "count": int(len(df)),
        "uniq": uniq,
        "win_5": _win_rate("ret_5"),
        "win_10": _win_rate("ret_10"),
        "win_20": _win_rate("ret_20"),
        "avg_5": _mean("ret_5"),
        "avg_10": _mean("ret_10"),
        "avg_20": _mean("ret_20"),
        "false_10": false_rate,
        "worst_mae_10": worst_mae,
        "rec_ok_60": rec_ok,
        "rec_avg_60": rec_avg,
        "sample": df.sort_values(["date", "code"]).head(10)[["date", "code", "name", "score", "ret_5", "ret_10", "ret_20", "mae_10"]]
        if all(c in df.columns for c in ["date", "code", "name", "score", "ret_5", "ret_10", "ret_20", "mae_10"])
        else None,
    }


def run_backtest(cfg: BacktestConfig):
    universe = _pick_universe(cfg.sample_size, cfg.seed)
    if not universe:
        print("样本池为空，无法回测")
        return

    spot_caps = {code: meta["mkt_cap"] for code, meta in universe.items()}
    daily_by_code = {}
    for i, code in enumerate(universe.keys(), start=1):
        if i % 20 == 0:
            print(f"⏳ 拉取历史: {i}/{len(universe)}")
        daily = _prepare_daily_history(code)
        if daily is None or daily.empty or len(daily) < 320:
            continue
        daily_by_code[code] = daily

    if not daily_by_code:
        print("历史数据不足，无法回测")
        return

    industry_by_code = {}
    for i, code in enumerate(daily_by_code.keys(), start=1):
        if i % 40 == 0:
            print(f"⏳ 拉取行业: {i}/{len(daily_by_code)}")
        industry = None
        for _ in range(2):
            try:
                info_df = real_ak.stock_individual_info_em(symbol=code)
                if info_df is not None and not info_df.empty and "item" in info_df.columns and "value" in info_df.columns:
                    m = info_df["item"].astype(str) == "行业"
                    if m.any():
                        industry = str(info_df.loc[m, "value"].iloc[0]).strip()
                break
            except Exception:
                time.sleep(0.3)
        if industry:
            industry_by_code[code] = industry

    db_script = _load_script_from_db(cfg.db_path, cfg.screener_db_id)
    v7_script = _load_script_from_file(cfg.v7_file_path)

    trade_df = None
    try:
        trade_df = real_ak.tool_trade_date_hist_sina()
    except Exception:
        trade_df = None
    if trade_df is not None and not trade_df.empty and "trade_date" in trade_df.columns:
        trade_dates = pd.to_datetime(trade_df["trade_date"], errors="coerce").dt.date.dropna().unique().tolist()
        trade_dates = sorted(trade_dates)
    else:
        any_daily = next(iter(daily_by_code.values()))
        trade_dates = sorted(any_daily["date_dt"].unique().tolist())

    today = _datetime.date.today()
    dates = [d for d in trade_dates if d <= today][-int(cfg.test_days) :]
    any_daily = next(iter(daily_by_code.values()))
    available_dates = set(any_daily["date_dt"].tolist())
    dates = [d for d in dates if d in available_dates]

    print("🚀 严格回测开始 (直接执行脚本文本)")
    print(f"🎯 股票样本: {len(daily_by_code)} 只  seed={cfg.seed}")
    print(f"📅 回测天数: {len(dates)} (目标 {cfg.test_days})  冷却期={cfg.cooldown_days}天  入场延迟={cfg.entry_delay_days}天")
    print(f"🧩 对比脚本: FILE({os.path.basename(cfg.v7_file_path)}) vs DB(stock_screeners.id={cfg.screener_db_id})")
    print("-" * 70)

    cooldown_a = {}
    cooldown_b = {}
    res_a = []
    res_b = []

    for di, day in enumerate(dates, start=1):
        if di % 20 == 0:
            print(f"⏳ 进度: {di}/{len(dates)}")
        fixed_dt = _datetime.datetime.combine(day, _datetime.time(hour=15, minute=0, second=0))
        fake = FakeAkshareModule("akshare", universe, daily_by_code, spot_caps, day, industry_by_code)

        df_a = _exec_screener(v7_script, fake, fixed_dt)
        df_b = _exec_screener(db_script, fake, fixed_dt)

        def _ingest(df_out: pd.DataFrame, tag: str):
            if df_out is None or df_out.empty:
                return []
            code_col = "代码" if "代码" in df_out.columns else ("ticker" if "ticker" in df_out.columns else None)
            if code_col is None:
                return []
            score_col = "评分" if "评分" in df_out.columns else ("score" if "score" in df_out.columns else None)
            out = []
            for _, r in df_out.iterrows():
                code = _normalize_symbol(r.get(code_col))
                if not code or code not in daily_by_code:
                    continue
                if tag == "A":
                    left = cooldown_a.get(code, 0)
                else:
                    left = cooldown_b.get(code, 0)
                if left > 0:
                    continue

                daily = daily_by_code[code]
                idxs = daily.index[daily["date_dt"] == day].tolist()
                if not idxs:
                    continue
                i0 = int(idxs[0])
                entry_i = i0 + int(cfg.entry_delay_days)
                if entry_i >= len(daily):
                    continue
                entry_date = daily.loc[entry_i, "date_dt"]
                entry_px = float(daily.loc[entry_i, "开盘"])
                metrics = _calc_forward_metrics(daily, entry_date, entry_px)
                score = int(r.get(score_col)) if score_col is not None and pd.notna(r.get(score_col)) else None
                out.append(
                    {
                        "date": str(entry_date),
                        "signal_date": str(day),
                        "code": code,
                        "name": universe.get(code, {}).get("name", code),
                        "score": score,
                        **metrics,
                    }
                )
                if tag == "A":
                    cooldown_a[code] = int(cfg.cooldown_days)
                else:
                    cooldown_b[code] = int(cfg.cooldown_days)
            return out

        res_a.extend(_ingest(df_a, "A"))
        res_b.extend(_ingest(df_b, "B"))

        for k in list(cooldown_a.keys()):
            cooldown_a[k] = max(0, int(cooldown_a[k]) - 1)
            if cooldown_a[k] == 0:
                cooldown_a.pop(k, None)
        for k in list(cooldown_b.keys()):
            cooldown_b[k] = max(0, int(cooldown_b[k]) - 1)
            if cooldown_b[k] == 0:
                cooldown_b.pop(k, None)

    a = _analyze(res_a, f"🔴 文件脚本({os.path.basename(cfg.v7_file_path)}) 严格回测")
    b = _analyze(res_b, f"🟢 DB脚本(stock_screeners.id={cfg.screener_db_id}) 严格回测")

    def _print(stat):
        print(f"\n{stat['title']}:")
        print(f"  信号总数: {stat.get('count', 0)}")
        print(f"  覆盖股票: {stat.get('uniq', 0)}")
        if stat.get("count", 0) <= 0:
            return
        print(f"  5日胜率:  {stat['win_5']:.1%}")
        print(f"  10日胜率: {stat['win_10']:.1%}")
        print(f"  20日胜率: {stat['win_20']:.1%}")
        print(f"  5日均收:  {stat['avg_5']:.2%}")
        print(f"  10日均收: {stat['avg_10']:.2%}")
        print(f"  20日均收: {stat['avg_20']:.2%}")
        print(f"  假信号率(10日跌破-5%): {stat['false_10']:.1%}")
        print(f"  10日最差回撤(MAE): {stat['worst_mae_10']:.2%}")
        print(f"  60日回本率: {stat['rec_ok_60']:.1%}")
        print(f"  60日平均回本天数: {stat['rec_avg_60']:.1f}")

    print("\n" + "=" * 70)
    print("🏁 严格回测结果汇总")
    print("=" * 70)
    _print(a)
    _print(b)

    if b.get("sample") is not None and isinstance(b["sample"], pd.DataFrame) and not b["sample"].empty:
        print("\n🔍 DB脚本信号样例(前10条):")
        print(b["sample"].to_string(index=False))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--sample-size", type=int, default=120)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--test-days", type=int, default=250)
    p.add_argument("--cooldown-days", type=int, default=10)
    p.add_argument("--entry-delay-days", type=int, default=1)
    p.add_argument("--db-path", type=str, default=os.path.join(os.path.dirname(__file__), "..", "stock_watch.db"))
    p.add_argument("--screener-db-id", type=int, default=5)
    p.add_argument(
        "--v7-file-path",
        type=str,
        default=os.path.join(os.path.dirname(__file__), "选股策略", "山谷狙击选股策略.py"),
    )
    args = p.parse_args()
    cfg = BacktestConfig(
        sample_size=args.sample_size,
        seed=args.seed,
        test_days=args.test_days,
        cooldown_days=args.cooldown_days,
        entry_delay_days=args.entry_delay_days,
        screener_db_id=args.screener_db_id,
        db_path=os.path.abspath(args.db_path),
        v7_file_path=os.path.abspath(args.v7_file_path),
    )
    run_backtest(cfg)


if __name__ == "__main__":
    main()
