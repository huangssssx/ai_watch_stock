#!/usr/bin/env python3

import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd


def _project_root() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(here, "..", "..", ".."))


def _ts_date(d: datetime) -> str:
    return d.strftime("%Y%m%d")


def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _to_market_code(ts_code: str) -> Optional[tuple[int, str]]:
    ts_code = str(ts_code or "").strip()
    if not ts_code:
        return None
    if "." in ts_code:
        code, suf = ts_code.split(".", 1)
        code = code.strip()
        suf = suf.strip().upper()
        if suf == "SZ":
            return 0, code
        if suf == "SH":
            return 1, code
        return None
    if len(ts_code) == 6 and ts_code.startswith("6"):
        return 1, ts_code
    if len(ts_code) == 6:
        return 0, ts_code
    return None


def _chunks(items: list, n: int) -> list[list]:
    n = max(1, int(n))
    return [items[i : i + n] for i in range(0, len(items), n)]


def _rank_pct(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    if s.isna().all():
        return pd.Series([0.5] * len(s), index=s.index)
    return s.rank(pct=True).fillna(0.5)


def _safe_call(fn, max_retries: int = 2, sleep_s: float = 0.6, **kwargs):
    last = None
    for i in range(max(1, int(max_retries) + 1)):
        try:
            return fn(**kwargs)
        except Exception as e:
            last = e
            if i < max(1, int(max_retries) + 1):
                time.sleep(float(sleep_s))
    raise last


def _last_open_trade_date(pro, ref: Optional[datetime] = None, lookback_days: int = 40) -> str:
    ref = ref or datetime.now()
    end = ref.date()
    start = end - timedelta(days=int(lookback_days))
    df = _safe_call(
        pro.trade_cal,
        exchange="SSE",
        start_date=_ts_date(datetime.combine(start, datetime.min.time())),
        end_date=_ts_date(datetime.combine(end, datetime.min.time())),
        fields="cal_date,is_open",
    )
    if df is None or df.empty:
        raise RuntimeError("trade_cal 返回为空，无法确定交易日")
    df = df.copy()
    df["cal_date"] = df["cal_date"].astype(str)
    df = df[df["is_open"].astype(int) == 1]
    df = df[df["cal_date"] <= _ts_date(ref)]
    if df.empty:
        raise RuntimeError("trade_cal 无有效开市日期，无法确定交易日")
    return str(df["cal_date"].max())


def _prev_open_trade_date(pro, trade_date: str, lookback_days: int = 90) -> str:
    trade_date = str(trade_date).strip()
    if not trade_date:
        raise ValueError("trade_date 为空")
    try:
        ref = datetime.strptime(trade_date, "%Y%m%d")
    except Exception as e:
        raise ValueError(f"trade_date 格式错误: {trade_date}") from e

    end = ref.date()
    start = end - timedelta(days=int(lookback_days))
    df = _safe_call(
        pro.trade_cal,
        exchange="SSE",
        start_date=_ts_date(datetime.combine(start, datetime.min.time())),
        end_date=_ts_date(datetime.combine(end, datetime.min.time())),
        fields="cal_date,is_open",
    )
    if df is None or df.empty:
        raise RuntimeError("trade_cal 返回为空，无法回退交易日")
    df = df.copy()
    df["cal_date"] = df["cal_date"].astype(str)
    df = df[df["is_open"].astype(int) == 1]
    df = df[df["cal_date"] < trade_date]
    if df.empty:
        raise RuntimeError(f"trade_cal 找不到 {trade_date} 之前的开市日期")
    return str(df["cal_date"].max())


def _parse_hhmmss_to_minutes(v: str) -> float:
    v = str(v or "").strip()
    if not v:
        return float("nan")
    try:
        parts = v.split(":")
        if len(parts) < 2:
            return float("nan")
        h = int(parts[0])
        m = int(parts[1])
        s = int(parts[2]) if len(parts) >= 3 else 0
        return h * 60 + m + s / 60.0
    except Exception:
        return float("nan")


def _append_intraday_snapshot(df: pd.DataFrame, chunk_size: int = 80, sleep_s: float = 0.2) -> tuple[pd.DataFrame, dict]:
    df = df.copy()
    if df is None or df.empty or "ts_code" not in df.columns:
        return df, {"ok": False, "reason": "empty"}

    try:
        from backend.utils.pytdx_client import tdx
    except Exception as e:
        return df, {"ok": False, "reason": f"import_pytdx_failed:{type(e).__name__}"}

    ts_codes = df["ts_code"].astype(str).dropna().tolist()
    pairs = []
    keep = []
    for c in ts_codes:
        mc = _to_market_code(c)
        if mc is None:
            continue
        keep.append(c)
        pairs.append(mc)
    if not keep:
        return df, {"ok": False, "reason": "no_valid_ts_code"}

    rows = []
    with tdx:
        for part in _chunks(list(zip(keep, pairs)), int(chunk_size)):
            req = [p for _, p in part]
            try:
                ret = tdx.get_security_quotes(req)
            except Exception:
                ret = []
            if not isinstance(ret, list):
                ret = []

            for (ts_code, _), q in zip(part, ret):
                if not isinstance(q, dict):
                    continue
                price = float(q.get("price") or 0.0)
                last_close = float(q.get("last_close") or 0.0)
                bid1 = float(q.get("bid1") or 0.0)
                ask1 = float(q.get("ask1") or 0.0)
                vol_hand = float(q.get("vol") or 0.0)
                amount_yuan = float(q.get("amount") or 0.0)
                cur_vol_hand = float(q.get("cur_vol") or 0.0)
                b_vol_hand = float(q.get("b_vol") or 0.0)
                s_vol_hand = float(q.get("s_vol") or 0.0)

                bid_value = 0.0
                ask_value = 0.0
                bid_vol_total = 0.0
                ask_vol_total = 0.0
                for i in range(1, 6):
                    bp = float(q.get(f"bid{i}") or 0.0)
                    bv = float(q.get(f"bid_vol{i}") or 0.0)
                    ap = float(q.get(f"ask{i}") or 0.0)
                    av = float(q.get(f"ask_vol{i}") or 0.0)
                    bid_value += bp * bv
                    ask_value += ap * av
                    bid_vol_total += bv
                    ask_vol_total += av

                denom = ask_value if ask_value > 0 else 1e-9
                imbalance = bid_value / denom
                spread_bp = ((ask1 - bid1) / price * 10000.0) if price > 0 else float("nan")
                speed_pct = float(q.get("reversed_bytes9") or 0.0) / 100.0
                gap_pct = ((price - last_close) / last_close * 100.0) if last_close > 0 else float("nan")
                vwap = (amount_yuan / (vol_hand * 100.0)) if vol_hand > 0 else float("nan")
                b_ratio = (b_vol_hand / vol_hand) if vol_hand > 0 else float("nan")

                rows.append(
                    {
                        "ts_code": ts_code,
                        "rt_servertime": str(q.get("servertime") or ""),
                        "rt_price": price,
                        "rt_last_close": last_close,
                        "rt_gap_pct": gap_pct,
                        "rt_speed_pct": speed_pct,
                        "rt_spread_bp": spread_bp,
                        "rt_imbalance": imbalance,
                        "rt_bid1": bid1,
                        "rt_ask1": ask1,
                        "rt_bid_value": bid_value,
                        "rt_ask_value": ask_value,
                        "rt_bid_vol_5": bid_vol_total,
                        "rt_ask_vol_5": ask_vol_total,
                        "rt_vol_hand": vol_hand,
                        "rt_amount_yuan": amount_yuan,
                        "rt_vwap": vwap,
                        "rt_cur_vol_hand": cur_vol_hand,
                        "rt_b_vol_hand": b_vol_hand,
                        "rt_s_vol_hand": s_vol_hand,
                        "rt_b_ratio": b_ratio,
                    }
                )

            if sleep_s and sleep_s > 0:
                time.sleep(float(sleep_s))

    snap = pd.DataFrame(rows)
    if snap.empty:
        return df, {"ok": False, "reason": "quotes_empty"}
    snap["ts_code"] = snap["ts_code"].astype(str)
    out = df.merge(snap, on="ts_code", how="left")
    return out, {"ok": True, "rows": int(len(snap))}


def _fetch_stock_filters(pro, trade_date: str) -> tuple[set[str], set[str]]:
    st_codes: set[str] = set()
    susp_codes: set[str] = set()
    try:
        st = _safe_call(pro.stock_st, trade_date=trade_date, fields="ts_code")
        if st is not None and not st.empty:
            st_codes = set(st["ts_code"].astype(str).tolist())
    except Exception:
        st_codes = set()
    try:
        susp = _safe_call(pro.suspend_d, trade_date=trade_date, suspend_type="S", fields="ts_code")
        if susp is not None and not susp.empty:
            susp_codes = set(susp["ts_code"].astype(str).tolist())
    except Exception:
        susp_codes = set()
    return st_codes, susp_codes


def _fetch_limit_list_d(pro, trade_date: str) -> pd.DataFrame:
    fields = (
        "ts_code,trade_date,industry,name,close,pct_chg,amount,float_mv,total_mv,"
        "turnover_ratio,fd_amount,first_time,last_time,open_times,up_stat,limit_times,limit"
    )
    df = _safe_call(pro.limit_list_d, trade_date=trade_date, limit_type="U", fields=fields)
    if df is None:
        return pd.DataFrame()
    return df.copy()


def _fetch_kpl_zt(pro, trade_date: str) -> pd.DataFrame:
    fields = (
        "ts_code,name,trade_date,tag,theme,status,lu_desc,net_change,bid_amount,bid_turnover,"
        "bid_pct_chg,limit_order,amount,turnover_rate,free_float"
    )
    df = _safe_call(pro.kpl_list, trade_date=trade_date, tag="涨停", fields=fields)
    if df is None:
        return pd.DataFrame()
    return df.copy()


def _fetch_daily_basic(pro, trade_date: str) -> pd.DataFrame:
    fields = "ts_code,trade_date,turnover_rate,volume_ratio,total_mv,circ_mv,free_share,float_share"
    df = _safe_call(pro.daily_basic, trade_date=trade_date, fields=fields)
    if df is None:
        return pd.DataFrame()
    return df.copy()


def _fetch_moneyflow_any(pro, trade_date: str, prefer: str) -> tuple[pd.DataFrame, str]:
    prefer = str(prefer or "").strip().lower()
    tries = []
    if prefer:
        tries.append(prefer)
    for t in ["dc", "ths", "moneyflow"]:
        if t not in tries:
            tries.append(t)

    last_err = None
    for t in tries:
        try:
            if t == "dc":
                fields = "ts_code,trade_date,net_amount,net_amount_rate,buy_elg_amount,buy_elg_amount_rate,buy_lg_amount,buy_lg_amount_rate"
                df = _safe_call(pro.moneyflow_dc, trade_date=trade_date, fields=fields)
                if df is None:
                    df = pd.DataFrame()
                out = df.copy()
                out.rename(
                    columns={
                        "net_amount": "mf_net_amount",
                        "net_amount_rate": "mf_net_amount_rate",
                        "buy_elg_amount": "mf_buy_elg_amount",
                        "buy_elg_amount_rate": "mf_buy_elg_amount_rate",
                        "buy_lg_amount": "mf_buy_lg_amount",
                        "buy_lg_amount_rate": "mf_buy_lg_amount_rate",
                    },
                    inplace=True,
                )
                return out, "moneyflow_dc"
            if t == "ths":
                fields = "ts_code,trade_date,net_amount,net_d5_amount,buy_lg_amount,buy_lg_amount_rate"
                df = _safe_call(pro.moneyflow_ths, trade_date=trade_date, fields=fields)
                if df is None:
                    df = pd.DataFrame()
                out = df.copy()
                out.rename(
                    columns={
                        "net_amount": "mf_net_amount",
                        "net_d5_amount": "mf_net_d5_amount",
                        "buy_lg_amount": "mf_buy_lg_amount",
                        "buy_lg_amount_rate": "mf_buy_lg_amount_rate",
                    },
                    inplace=True,
                )
                return out, "moneyflow_ths"
            if t == "moneyflow":
                fields = "ts_code,trade_date,net_mf_amount,buy_lg_amount,buy_elg_amount"
                df = _safe_call(pro.moneyflow, trade_date=trade_date, fields=fields)
                if df is None:
                    df = pd.DataFrame()
                out = df.copy()
                out.rename(
                    columns={
                        "net_mf_amount": "mf_net_amount",
                        "buy_lg_amount": "mf_buy_lg_amount",
                        "buy_elg_amount": "mf_buy_elg_amount",
                    },
                    inplace=True,
                )
                return out, "moneyflow"
        except Exception as e:
            last_err = e
            continue
    if last_err is not None:
        raise last_err
    return pd.DataFrame(), "none"


def build_candidates(
    pro,
    trade_date: str,
    prefer_moneyflow: str,
    min_fd_amount: float,
    max_open_times: int,
    min_turnover_rate: float,
    max_turnover_rate: float,
    topk: int,
    fallback_days: int = 5,
) -> tuple[pd.DataFrame, dict]:
    requested_trade_date = str(trade_date).strip()
    trade_date = requested_trade_date

    limit_df = pd.DataFrame()
    kpl_df = pd.DataFrame()
    daily_basic = pd.DataFrame()
    moneyflow_df = pd.DataFrame()
    moneyflow_source = "none"
    st_codes: set[str] = set()
    susp_codes: set[str] = set()

    base = pd.DataFrame()
    used_trade_date = ""
    for attempt in range(max(0, int(fallback_days)) + 1):
        limit_df = _fetch_limit_list_d(pro, trade_date=trade_date)
        try:
            kpl_df = _fetch_kpl_zt(pro, trade_date=trade_date)
        except Exception:
            kpl_df = pd.DataFrame()

        base = limit_df.copy()
        if base.empty and not kpl_df.empty:
            base = kpl_df[["ts_code", "name", "trade_date"]].drop_duplicates().copy()

        if not base.empty:
            used_trade_date = trade_date
            try:
                daily_basic = _fetch_daily_basic(pro, trade_date=trade_date)
            except Exception:
                daily_basic = pd.DataFrame()
            try:
                moneyflow_df, moneyflow_source = _fetch_moneyflow_any(pro, trade_date=trade_date, prefer=prefer_moneyflow)
            except Exception:
                moneyflow_df, moneyflow_source = pd.DataFrame(), "none"
            st_codes, susp_codes = _fetch_stock_filters(pro, trade_date=trade_date)
            break

        if attempt >= max(0, int(fallback_days)):
            break
        trade_date = _prev_open_trade_date(pro, trade_date)

    if base.empty:
        raise RuntimeError(f"{requested_trade_date} 未拉到候选基础数据（limit_list_d/kpl_list 均为空），可尝试指定上一交易日 --trade-date")
    if not used_trade_date:
        used_trade_date = trade_date

    for col in ["ts_code"]:
        if col in base.columns:
            base[col] = base[col].astype(str)
    if "ts_code" not in base.columns:
        raise RuntimeError("候选数据缺少 ts_code")

    base["is_st"] = base["ts_code"].isin(st_codes)
    base["is_suspended"] = base["ts_code"].isin(susp_codes)
    base = base[(~base["is_st"]) & (~base["is_suspended"])].copy()

    if not kpl_df.empty:
        kpl_df = kpl_df.copy()
        kpl_df["ts_code"] = kpl_df["ts_code"].astype(str)
        base = base.merge(
            kpl_df[
                [
                    "ts_code",
                    "theme",
                    "status",
                    "lu_desc",
                    "net_change",
                    "bid_amount",
                    "bid_turnover",
                    "bid_pct_chg",
                    "limit_order",
                ]
            ],
            on="ts_code",
            how="left",
        )

    if not daily_basic.empty:
        daily_basic = daily_basic.copy()
        daily_basic["ts_code"] = daily_basic["ts_code"].astype(str)
        base = base.merge(
            daily_basic[["ts_code", "turnover_rate", "volume_ratio", "circ_mv", "free_share", "float_share"]],
            on="ts_code",
            how="left",
        )

    if not moneyflow_df.empty:
        moneyflow_df = moneyflow_df.copy()
        moneyflow_df["ts_code"] = moneyflow_df["ts_code"].astype(str)
        keep_cols = ["ts_code"]
        for c in [
            "mf_net_amount",
            "mf_net_amount_rate",
            "mf_net_d5_amount",
            "mf_buy_elg_amount",
            "mf_buy_elg_amount_rate",
            "mf_buy_lg_amount",
            "mf_buy_lg_amount_rate",
        ]:
            if c in moneyflow_df.columns:
                keep_cols.append(c)
        base = base.merge(moneyflow_df[keep_cols], on="ts_code", how="left")

    if "fd_amount" in base.columns:
        base["fd_amount"] = pd.to_numeric(base["fd_amount"], errors="coerce")
        base = base[base["fd_amount"].fillna(0.0) >= float(min_fd_amount)].copy()
    if "open_times" in base.columns:
        base["open_times"] = pd.to_numeric(base["open_times"], errors="coerce")
        base = base[base["open_times"].fillna(0).astype(int) <= int(max_open_times)].copy()

    base["turnover_rate"] = pd.to_numeric(base.get("turnover_rate"), errors="coerce")
    if min_turnover_rate is not None:
        base = base[base["turnover_rate"].fillna(0.0) >= float(min_turnover_rate)].copy()
    if max_turnover_rate is not None and max_turnover_rate > 0:
        base = base[base["turnover_rate"].fillna(0.0) <= float(max_turnover_rate)].copy()

    base["first_time_m"] = base.get("first_time").map(_parse_hhmmss_to_minutes) if "first_time" in base.columns else float("nan")
    base["last_time_m"] = base.get("last_time").map(_parse_hhmmss_to_minutes) if "last_time" in base.columns else float("nan")

    if "theme" in base.columns:
        theme_cnt = base.groupby("theme")["ts_code"].transform("count")
        base["theme_hot"] = theme_cnt
    else:
        base["theme_hot"] = 0

    fd_score = _rank_pct(base.get("fd_amount"))
    open_score = 1.0 - _rank_pct(base.get("open_times"))
    mf_score = _rank_pct(base.get("mf_net_amount"))
    vr_score = _rank_pct(base.get("volume_ratio"))
    th_score = _rank_pct(base.get("theme_hot"))
    tor_score = _rank_pct(base.get("turnover_rate"))
    limit_times = pd.to_numeric(base.get("limit_times"), errors="coerce")
    lb_score = _rank_pct(limit_times)

    first_m = pd.to_numeric(base.get("first_time_m"), errors="coerce")
    first_score = 1.0 - _rank_pct(first_m)
    base["score"] = (
        30.0 * fd_score
        + 18.0 * open_score
        + 14.0 * mf_score
        + 10.0 * vr_score
        + 10.0 * th_score
        + 10.0 * tor_score
        + 6.0 * lb_score
        + 2.0 * first_score
    )

    if "pct_chg" in base.columns:
        base["pct_chg"] = pd.to_numeric(base["pct_chg"], errors="coerce")
    if "close" in base.columns:
        base["close"] = pd.to_numeric(base["close"], errors="coerce")

    base = base.sort_values(["score", "fd_amount", "mf_net_amount"], ascending=[False, False, False], na_position="last")
    base = base.head(max(1, int(topk))).copy()

    cols = [
        "ts_code",
        "name",
        "industry",
        "close",
        "pct_chg",
        "limit_times",
        "open_times",
        "fd_amount",
        "first_time",
        "last_time",
        "theme",
        "status",
        "lu_desc",
        "turnover_rate",
        "volume_ratio",
        "circ_mv",
        "mf_net_amount",
        "mf_net_d5_amount",
        "mf_buy_elg_amount",
        "mf_buy_lg_amount",
        "score",
    ]
    keep = [c for c in cols if c in base.columns]
    base = base[keep].copy()

    meta = {
        "trade_date": used_trade_date,
        "requested_trade_date": requested_trade_date,
        "moneyflow_source": moneyflow_source,
        "rows_limit_list_d": int(len(limit_df)) if limit_df is not None else 0,
        "rows_kpl_list": int(len(kpl_df)) if kpl_df is not None else 0,
        "rows_candidates": int(len(base)),
        "filters": {
            "min_fd_amount": float(min_fd_amount),
            "max_open_times": int(max_open_times),
            "min_turnover_rate": float(min_turnover_rate),
            "max_turnover_rate": float(max_turnover_rate),
        },
    }
    return base, meta


def main() -> None:
    parser = argparse.ArgumentParser(description="隔夜套利：盘后候选池（情绪+资金+题材）")
    parser.add_argument("--trade-date", type=str, default="")
    parser.add_argument("--prefer-moneyflow", type=str, default="dc", choices=["dc", "ths", "moneyflow"])
    parser.add_argument("--min-fd-amount", type=float, default=20_000_000.0)
    parser.add_argument("--max-open-times", type=int, default=0)
    parser.add_argument("--min-turnover-rate", type=float, default=1.0)
    parser.add_argument("--max-turnover-rate", type=float, default=35.0)
    parser.add_argument("--topk", type=int, default=60)
    parser.add_argument("--with-intraday", action="store_true")
    parser.add_argument("--intraday-chunk-size", type=int, default=80)
    parser.add_argument("--intraday-sleep-s", type=float, default=0.2)
    parser.add_argument("--out", type=str, default="")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    root = _project_root()
    if root not in sys.path:
        sys.path.insert(0, root)

    if bool(args.dry_run):
        print("dry_run=1，不请求数据")
        return

    try:
        from backend.utils.tushare_client import pro
    except Exception as e:
        raise SystemExit(f"导入 tushare_client 失败: {type(e).__name__}:{e}")

    if pro is None:
        raise SystemExit("pro=None，无法执行（请检查 tushare token/config）")

    trade_date = str(args.trade_date).strip()
    if not trade_date:
        trade_date = _last_open_trade_date(pro)
        if trade_date == _ts_date(datetime.now()) and datetime.now().strftime("%H%M") < "1610":
            try:
                trade_date = _prev_open_trade_date(pro, trade_date)
            except Exception:
                pass

    df, meta = build_candidates(
        pro=pro,
        trade_date=trade_date,
        prefer_moneyflow=str(args.prefer_moneyflow),
        min_fd_amount=float(args.min_fd_amount),
        max_open_times=int(args.max_open_times),
        min_turnover_rate=float(args.min_turnover_rate),
        max_turnover_rate=float(args.max_turnover_rate),
        topk=int(args.topk),
    )

    if bool(args.with_intraday):
        df, snap_meta = _append_intraday_snapshot(
            df,
            chunk_size=int(args.intraday_chunk_size),
            sleep_s=float(args.intraday_sleep_s),
        )
        meta["intraday_snapshot"] = snap_meta

    out = str(args.out).strip()
    if not out:
        out = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f"隔夜候选_{meta.get('trade_date') or trade_date}_{_now_ts()}.csv",
        )

    os.makedirs(os.path.dirname(out), exist_ok=True)
    df.to_csv(out, index=False, encoding="utf-8-sig")

    used_trade_date = meta.get("trade_date") or trade_date
    requested_trade_date = meta.get("requested_trade_date") or trade_date
    fallback_note = ""
    if str(used_trade_date) != str(requested_trade_date):
        fallback_note = f" fallback_from={requested_trade_date}"
    intraday_note = ""
    if isinstance(meta.get("intraday_snapshot"), dict):
        snap_meta = meta["intraday_snapshot"]
        intraday_note = f" intraday={1 if snap_meta.get('ok') else 0}"
    print(
        f"trade_date={used_trade_date}{fallback_note} moneyflow_source={meta['moneyflow_source']}{intraday_note} candidates={len(df)} out={out}"
    )
    with pd.option_context("display.max_rows", 80, "display.max_columns", 50, "display.width", 240):
        print(df.head(min(30, len(df))).to_string(index=False))


if __name__ == "__main__":
    main()
