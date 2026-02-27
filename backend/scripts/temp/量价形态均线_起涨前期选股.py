import argparse
import os
import sys
import time
from dataclasses import dataclass
from typing import Iterable, Optional

import pandas as pd

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from utils.pytdx_client import tdx

try:
    from utils.tushare_client import pro
except Exception:
    pro = None


@dataclass(frozen=True)
class StockDef:
    market: int
    code: str
    name: str


def _ts_code(market: int, code: str) -> str:
    code = str(code).zfill(6)
    if int(market) == 0:
        return f"{code}.SZ"
    return f"{code}.SH"


def _is_a_share_stock(market: int, code: str) -> bool:
    code = str(code or "").zfill(6)
    if int(market) == 0:
        return code.startswith(("000", "001", "002", "003", "300", "301"))
    if int(market) == 1:
        return code.startswith(("600", "601", "603", "605", "688"))
    return False


def _iter_all_a_share_defs() -> Iterable[StockDef]:
    for market in (0, 1):
        total = tdx.get_security_count(market)
        step = 1000
        for start in range(0, int(total), step):
            rows = tdx.get_security_list(market, start) or []
            for r in rows:
                code = str(r.get("code", "")).zfill(6)
                name = str(r.get("name", "")).strip()
                if code and _is_a_share_stock(market, code):
                    yield StockDef(market=int(market), code=code, name=name)


def _load_active_codes_from_tushare() -> Optional[dict[str, str]]:
    if pro is None:
        return None
    try:
        df = pro.stock_basic(exchange="", list_status="L", fields="ts_code,name")
        if df is None or df.empty:
            return None
        df = df.dropna(subset=["ts_code"]).copy()
        df["ts_code"] = df["ts_code"].astype(str).str.strip()
        df["name"] = df.get("name", "").astype(str).str.strip()
        return dict(zip(df["ts_code"], df["name"]))
    except Exception:
        return None


def _daily_bars(market: int, code: str, count: int) -> pd.DataFrame:
    data = tdx.get_security_bars(9, int(market), str(code).zfill(6), 0, int(count))
    df = tdx.to_df(data) if data else pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    if "datetime" not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"]).sort_values("datetime", ascending=True)
    for c in ("open", "close", "high", "low", "vol", "amount"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["open", "close", "high", "low", "vol"])
    return df.reset_index(drop=True)


def _pct_chg(close: pd.Series) -> pd.Series:
    return close.pct_change().fillna(0.0)


def _rolling_prev_mean(s: pd.Series, window: int) -> pd.Series:
    return s.shift(1).rolling(int(window), min_periods=int(window)).mean()


def _limit_up_threshold(code: str) -> float:
    code = str(code).zfill(6)
    if code.startswith(("300", "301", "688")):
        return 0.195
    return 0.095


def _signal_small_bullish_push(df: pd.DataFrame) -> bool:
    if df is None or df.empty or len(df) < 6:
        return False
    w = df.tail(6).copy()
    w["ret"] = _pct_chg(w["close"])
    up = (w["close"] > w["open"]).sum()
    ok_ret = w["ret"].between(0.01, 0.03, inclusive="both").sum()
    big_spike = (w["ret"] >= 0.08).any()
    return bool(up >= 4 and ok_ret >= 4 and not big_spike)


def _signal_recent_limit_up(df: pd.DataFrame, code: str) -> bool:
    if df is None or df.empty or len(df) < 25:
        return False
    w = df.tail(21).copy()
    w["ret"] = _pct_chg(w["close"])
    return bool((w["ret"] >= _limit_up_threshold(code)).any())


def _signal_xianren_zhilu(df: pd.DataFrame) -> bool:
    if df is None or df.empty or len(df) < 12:
        return False
    w = df.tail(12).copy()
    eps = 1e-9
    w["body"] = (w["close"] - w["open"]).clip(lower=eps)
    w["upper_shadow"] = (w["high"] - w[["close", "open"]].max(axis=1)).clip(lower=0.0)
    w["ratio"] = w["upper_shadow"] / w["body"]
    idx = w.index[-2]
    c1 = bool(w.loc[idx, "close"] > w.loc[idx, "open"] and w.loc[idx, "ratio"] >= 1.2)
    c2 = bool(w.iloc[-1]["low"] >= w.loc[idx, "low"])
    return bool(c1 and c2)


def _ma_trend_ok(df: pd.DataFrame) -> bool:
    if df is None or df.empty or len(df) < 40:
        return False
    w = df.copy()
    w["ma5"] = w["close"].rolling(5, min_periods=5).mean()
    w["ma10"] = w["close"].rolling(10, min_periods=10).mean()
    w["ma20"] = w["close"].rolling(20, min_periods=20).mean()
    last = w.iloc[-1]
    if pd.isna(last["ma20"]) or pd.isna(last["ma10"]) or pd.isna(last["ma5"]):
        return False
    align = bool(last["ma5"] > last["ma10"] > last["ma20"])
    ma20_slope = w["ma20"].diff().tail(3).mean()
    ma10_slope = w["ma10"].diff().tail(3).mean()
    ma5_slope = w["ma5"].diff().tail(3).mean()
    return bool(align and ma20_slope > 0 and ma10_slope > 0 and ma5_slope > 0)


def _find_breakout_with_confirm_pullback(df: pd.DataFrame, recent_days: int) -> Optional[dict]:
    if df is None or df.empty or len(df) < 80:
        return None
    w = df.copy()
    w["ret"] = _pct_chg(w["close"])
    w["prev5_vol_mean"] = _rolling_prev_mean(w["vol"], 5)
    w["prev20_high"] = w["high"].shift(1).rolling(20, min_periods=20).max()
    w = w.dropna(subset=["prev5_vol_mean", "prev20_high"])
    if w.empty:
        return None

    tail = w.tail(max(15, int(recent_days) + 10)).copy()
    candidates = tail[
        (tail["vol"] >= 2.0 * tail["prev5_vol_mean"])
        & (tail["ret"] >= 0.05)
        & (tail["close"] > tail["prev20_high"])
    ].copy()

    if candidates.empty:
        return None

    candidates = candidates.sort_values(["datetime"], ascending=False)
    for idx, row in candidates.iterrows():
        breakout_level = float(row["prev20_high"])
        breakout_vol = float(row["vol"])
        i = df.index[df["datetime"] == row["datetime"]]
        if len(i) != 1:
            continue
        i = int(i[0])
        post = df.iloc[i + 1 : i + 4].copy()
        if len(post) < 3:
            continue
        post_close_min = float(post["close"].min())
        if post_close_min < breakout_level * 0.99:
            continue
        post7 = df.iloc[i + 1 : i + 8].copy()
        if post7.empty:
            continue
        pullback = post7[
            (post7["vol"] <= breakout_vol / 3.0)
            & (post7["low"] >= breakout_level * 0.98)
            & (post7["close"] >= breakout_level * 0.99)
        ]
        if pullback.empty:
            continue
        pullback_day = pullback.sort_values("datetime", ascending=True).iloc[0]
        return {
            "breakout_dt": row["datetime"],
            "breakout_level": breakout_level,
            "breakout_vol": breakout_vol,
            "breakout_vol_ratio": float(row["vol"] / (row["prev5_vol_mean"] + 1e-9)),
            "pullback_dt": pullback_day["datetime"],
            "pullback_vol_ratio": float(pullback_day["vol"] / (breakout_vol + 1e-9)),
        }
    return None


def _volume_base_ok(df: pd.DataFrame) -> bool:
    if df is None or df.empty or len(df) < 80:
        return False
    w = df.tail(70).copy()
    if len(w) < 30:
        return False
    idx_min_vol = int(w["vol"].idxmin())
    min_day = df.loc[idx_min_vol]
    post = df.loc[idx_min_vol:].head(8)
    if len(post) < 5:
        return False
    price_not_new_low = bool((post["low"].min() >= float(min_day["low"]) * 0.98))
    return price_not_new_low


def _warm_pile_up_ok(df: pd.DataFrame) -> bool:
    if df is None or df.empty or len(df) < 60:
        return False
    w = df.copy()
    w["prev5_vol_mean"] = _rolling_prev_mean(w["vol"], 5)
    w = w.dropna(subset=["prev5_vol_mean"])
    if len(w) < 25:
        return False
    seg = w.tail(10).copy()
    if seg.empty:
        return False
    seg["vr"] = seg["vol"] / (seg["prev5_vol_mean"] + 1e-9)
    mild = seg["vr"].between(1.2, 2.2, inclusive="both").sum()
    up = (seg["close"] > seg["open"]).sum()
    return bool(mild >= 4 and up >= 5)


def _build_reason(parts: list[str], max_len: int = 200) -> str:
    s = " | ".join([p for p in parts if p])
    if len(s) <= max_len:
        return s
    return s[: max(0, max_len - 3)] + "..."


def screen_one(stock: StockDef, bars_count: int, recent_breakout_days: int) -> Optional[dict]:
    df = _daily_bars(stock.market, stock.code, bars_count)
    if df.empty or len(df) < 80:
        return None

    ma_ok = _ma_trend_ok(df)
    base_ok = _volume_base_ok(df)
    pile_ok = _warm_pile_up_ok(df)
    b = _find_breakout_with_confirm_pullback(df, recent_days=recent_breakout_days)
    volume_price_ok = bool(b is not None)

    k1 = _signal_small_bullish_push(df)
    k2 = _signal_recent_limit_up(df, stock.code)
    k3 = _signal_xianren_zhilu(df)
    k_pattern_ok = bool(k1 or k2 or k3)

    score = 0
    reason_parts: list[str] = []

    if b is not None:
        score += 4
        reason_parts.append("量价:倍量突破+缩量回踩")
    if base_ok:
        score += 1
        reason_parts.append("量价:地量见底")
    if pile_ok:
        score += 1
        reason_parts.append("量价:温和堆量")

    if ma_ok:
        score += 3
        reason_parts.append("均线:5/10/20多头发散")

    k_parts = []
    if k1:
        score += 2
        k_parts.append("小阳推升")
    if k2:
        score += 2
        k_parts.append("近期涨停")
    if k3:
        score += 1
        k_parts.append("仙人指路")
    if k_parts:
        reason_parts.append("形态:" + "/".join(k_parts))

    last_close = float(df.iloc[-1]["close"])
    last_vol = float(df.iloc[-1]["vol"])
    last_dt = df.iloc[-1]["datetime"]

    out = {
        "code": stock.code,
        "name": stock.name,
        "market": stock.market,
        "trade_date": pd.Timestamp(last_dt).strftime("%Y-%m-%d"),
        "close": round(last_close, 4),
        "vol": round(last_vol, 2),
        "score": int(score),
        "ma_ok": bool(ma_ok),
        "volume_price_ok": bool(volume_price_ok),
        "k_pattern_ok": bool(k_pattern_ok),
        "reason": _build_reason(reason_parts),
    }
    if b is not None:
        out.update(
            {
                "breakout_date": pd.Timestamp(b["breakout_dt"]).strftime("%Y-%m-%d"),
                "breakout_level": round(float(b["breakout_level"]), 4),
                "breakout_vol_ratio": round(float(b["breakout_vol_ratio"]), 3),
                "pullback_date": pd.Timestamp(b["pullback_dt"]).strftime("%Y-%m-%d"),
                "pullback_vol_ratio": round(float(b["pullback_vol_ratio"]), 3),
            }
        )
    else:
        out.update(
            {
                "breakout_date": "",
                "breakout_level": None,
                "breakout_vol_ratio": None,
                "pullback_date": "",
                "pullback_vol_ratio": None,
            }
        )
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-stocks", type=int, default=int(os.getenv("MAX_STOCKS", "800")))
    parser.add_argument("--bars", type=int, default=int(os.getenv("BARS", "140")))
    parser.add_argument("--recent-breakout-days", type=int, default=int(os.getenv("RECENT_BREAKOUT_DAYS", "10")))
    parser.add_argument("--min-score", type=int, default=int(os.getenv("MIN_SCORE", "10")))
    require_three_default = os.getenv("REQUIRE_THREE", "1") not in ("0", "false", "False")
    require_three_group = parser.add_mutually_exclusive_group()
    require_three_group.add_argument("--require-three", dest="require_three", action="store_true", default=require_three_default)
    require_three_group.add_argument("--no-require-three", dest="require_three", action="store_false")
    parser.add_argument("--sleep", type=float, default=float(os.getenv("SLEEP", "0.0")))
    parser.add_argument("--out", type=str, default=os.getenv("OUT", ""))
    args = parser.parse_args()

    t0 = time.perf_counter()
    print("开始：量价+形态+均线 起涨前期选股")
    print(f"参数: max_stocks={args.max_stocks}, bars={args.bars}, recent_breakout_days={args.recent_breakout_days}, min_score={args.min_score}, require_three={args.require_three}")

    active_map = _load_active_codes_from_tushare()
    if active_map is None:
        print("tushare pro 不可用或无数据：仅用 pytdx 股票池")
    else:
        print(f"tushare pro 股票池: {len(active_map)}")

    stocks: list[StockDef] = []
    with tdx:
        for s in _iter_all_a_share_defs():
            if active_map is not None:
                ts_code = _ts_code(s.market, s.code)
                if ts_code not in active_map:
                    continue
                name = active_map.get(ts_code) or s.name
                stocks.append(StockDef(market=s.market, code=s.code, name=name))
            else:
                stocks.append(s)
            if args.max_stocks > 0 and len(stocks) >= int(args.max_stocks):
                break

        print(f"股票池数量: {len(stocks)}")
        rows: list[dict] = []
        stat_total = 0
        stat_breakout = 0
        stat_ma = 0
        stat_pattern = 0
        stat_three = 0
        for i, s in enumerate(stocks, start=1):
            r = None
            try:
                r = screen_one(s, bars_count=args.bars, recent_breakout_days=args.recent_breakout_days)
            except Exception as e:
                print(f"异常: {s.code} {s.name} {e}")
                r = None
            if r is not None:
                stat_total += 1
                if r.get("volume_price_ok"):
                    stat_breakout += 1
                if r.get("ma_ok"):
                    stat_ma += 1
                if r.get("k_pattern_ok"):
                    stat_pattern += 1
                if args.require_three:
                    ok = bool(r["volume_price_ok"] and r["k_pattern_ok"] and r["ma_ok"])
                else:
                    ok = bool(r["score"] >= int(args.min_score))
                if ok:
                    stat_three += 1
                if ok and int(r["score"]) >= int(args.min_score):
                    rows.append(r)
            if args.sleep and args.sleep > 0:
                time.sleep(float(args.sleep))
            if i % 200 == 0:
                print(f"进度: {i}/{len(stocks)}")

    print(f"统计: 有效K线={stat_total}, 量价突破回踩={stat_breakout}, 均线多头={stat_ma}, 形态命中={stat_pattern}, 三维同时命中={stat_three}")

    df = pd.DataFrame(rows)
    if df.empty:
        print("无结果")
        return

    df = df.sort_values(["score", "breakout_vol_ratio"], ascending=[False, False], na_position="last")
    df = df.reset_index(drop=True)

    ts = time.strftime("%Y%m%d_%H%M%S")
    out = args.out.strip()
    if not out:
        out = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"起涨前期_量价形态均线_{ts}.csv")
    df.to_csv(out, index=False, encoding="utf-8-sig")

    print(f"完成: {len(df)} 条")
    print(f"输出: {out}")
    print(df.head(50)[["code", "name", "score", "reason", "trade_date", "close"]])
    print(f"总耗时: {time.perf_counter() - t0:.2f}s")


if __name__ == "__main__":
    main()
