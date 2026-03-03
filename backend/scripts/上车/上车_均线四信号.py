import argparse
import os
import sys
import time
from datetime import datetime, time as dtime
from typing import Dict, List, Optional, Tuple

import pandas as pd


def _backend_dir() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    probe = here
    for _ in range(12):
        if os.path.exists(os.path.join(probe, "utils", "pytdx_client.py")):
            return probe
        parent = os.path.dirname(probe)
        if parent == probe:
            break
        probe = parent
    return os.path.abspath(os.path.join(here, "..", ".."))


_BACKEND_DIR = _backend_dir()
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from utils.pytdx_client import tdx, connected_endpoint
from utils.stock_codes import get_all_a_share_codes


PYTDX_VOL_MULTIPLIER = 100


def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _to_ymd(dt: pd.Timestamp) -> str:
    try:
        return dt.strftime("%Y%m%d")
    except Exception:
        return ""


def _parse_markets(s: str) -> List[int]:
    s = str(s or "").strip().lower()
    if s in {"sz", "0"}:
        return [0]
    if s in {"sh", "1"}:
        return [1]
    return [0, 1]


def _ensure_dir(p: str) -> None:
    if not p:
        return
    os.makedirs(p, exist_ok=True)


def _cache_path(cache_dir: str, market: int, code: str) -> str:
    return os.path.join(str(cache_dir), f"daily_{int(market)}_{str(code).zfill(6)}.csv.gz")


def _clean_daily_df(df: pd.DataFrame, convert_vol_from_hand_to_share: bool) -> pd.DataFrame:
    if df is None or df.empty or "datetime" not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.normalize()
    df = df.dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
    for c in ("open", "close", "high", "low", "vol", "amount"):
        if c not in df.columns:
            df[c] = pd.NA
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["open", "close", "high", "low", "vol"]).reset_index(drop=True)
    if bool(convert_vol_from_hand_to_share):
        df["vol"] = df["vol"].astype(float) * float(PYTDX_VOL_MULTIPLIER)
    return df


def _fetch_daily_bars(market: int, code: str, bars: int) -> pd.DataFrame:
    try:
        data = tdx.get_security_bars(9, int(market), str(code).zfill(6), 0, int(bars))
    except Exception:
        data = []
    df = tdx.to_df(data) if data else pd.DataFrame()
    return _clean_daily_df(df, convert_vol_from_hand_to_share=True)


def load_or_fetch_daily(
    cache_dir: str,
    market: int,
    code: str,
    bars: int,
    refresh: bool,
    cache_read_timeout_s: float,
) -> Tuple[pd.DataFrame, bool]:
    cache_dir = str(cache_dir or "").strip()
    if not cache_dir:
        return _fetch_daily_bars(market, code, bars), False
    _ensure_dir(cache_dir)
    p = _cache_path(cache_dir, market, code)
    if (not refresh) and os.path.exists(p):
        try:
            df = pd.read_csv(p, compression="gzip")
        except Exception:
            df = pd.DataFrame()
        df = _clean_daily_df(df, convert_vol_from_hand_to_share=False)
        if df is not None and not df.empty and len(df) >= max(25, int(bars) // 2):
            return df.tail(int(bars)).reset_index(drop=True), True

    df = _fetch_daily_bars(market, code, bars)
    if df is None or df.empty:
        return pd.DataFrame(), False
    try:
        df.to_csv(p, index=False, encoding="utf-8", compression="gzip")
    except Exception:
        pass
    if cache_read_timeout_s and cache_read_timeout_s > 0:
        time.sleep(0.0)
    return df, False


def _calc_ma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(int(window), min_periods=int(window)).mean()


def _pick_eval_row(df: pd.DataFrame, trade_date: str, allow_intraday: bool) -> Optional[int]:
    if df is None or df.empty or "datetime" not in df.columns:
        return None
    if not str(trade_date or "").strip():
        if bool(allow_intraday):
            return int(len(df) - 1)
        if len(df) < 2:
            return int(len(df) - 1)
        last_dt = pd.to_datetime(df["datetime"].iloc[-1], errors="coerce")
        now = datetime.now()
        if pd.notna(last_dt) and last_dt.normalize() == pd.Timestamp(now.date()):
            if now.time() < dtime(15, 5):
                return int(len(df) - 2)
        return int(len(df) - 1)
    target_dt = pd.to_datetime(str(trade_date).strip(), format="%Y%m%d", errors="coerce")
    if pd.isna(target_dt):
        return int(len(df) - 1)
    target_dt = target_dt.normalize()
    m = df["datetime"] == target_dt
    if not bool(m.any()):
        return None
    idx = df.index[m].tolist()
    return int(idx[-1]) if idx else None


def _is_uptrend(row: pd.Series, row_lb: pd.Series, require_ma10_over_ma20: bool) -> bool:
    close = float(row.get("close") or 0.0)
    ma20 = float(row.get("ma20") or 0.0)
    ma20_lb = float(row_lb.get("ma20") or 0.0)
    ma10 = float(row.get("ma10") or 0.0)
    if close <= 0 or ma20 <= 0 or ma20_lb <= 0:
        return False
    if close < ma20:
        return False
    if ma20 < ma20_lb:
        return False
    if bool(require_ma10_over_ma20) and ma10 > 0 and ma10 < ma20:
        return False
    return True


def _stand_firm(df: pd.DataFrame, eval_i: int, stand_days: int, stand_tol_pct: float) -> bool:
    stand_days = max(1, int(stand_days))
    stand_tol = max(0.0, float(stand_tol_pct or 0.0)) / 100.0
    i0 = max(0, int(eval_i) - stand_days + 1)
    win = df.iloc[i0 : int(eval_i) + 1].copy()
    if win is None or win.empty:
        return False
    ok = (pd.to_numeric(win["close"], errors="coerce") >= pd.to_numeric(win["ma20"], errors="coerce") * (1.0 - stand_tol)).all()
    return bool(ok)


def _signal_flags(
    df: pd.DataFrame,
    eval_i: int,
    ma5_near_pct: float,
    ma5_hold_pct: float,
    ma5_close_min_pct: float,
    ma20_near_pct: float,
    ma20_break_pct: float,
    min_golden_cross_pct: float,
) -> Dict[str, bool]:
    row = df.iloc[int(eval_i)]
    prev = df.iloc[int(eval_i) - 1] if int(eval_i) - 1 >= 0 else row
    close = float(row.get("close") or 0.0)
    low = float(row.get("low") or 0.0)
    ma5 = float(row.get("ma5") or 0.0)
    ma10 = float(row.get("ma10") or 0.0)
    ma20 = float(row.get("ma20") or 0.0)
    ma5_prev = float(prev.get("ma5") or 0.0)
    ma10_prev = float(prev.get("ma10") or 0.0)

    ma5_near = max(0.0, float(ma5_near_pct or 0.0)) / 100.0
    ma5_hold = max(0.0, float(ma5_hold_pct or 0.0)) / 100.0
    ma5_close_min = float(ma5_close_min_pct or 0.0) / 100.0
    ma20_near = max(0.0, float(ma20_near_pct or 0.0)) / 100.0
    ma20_break = max(0.0, float(ma20_break_pct or 0.0)) / 100.0
    min_gc = max(0.0, float(min_golden_cross_pct or 0.0))

    strong_pullback = False
    if ma5 > 0 and close > 0 and low > 0:
        if low <= ma5 * (1.0 + ma5_near) and close >= ma5 * (1.0 - ma5_hold) and close >= ma5 * (1.0 + ma5_close_min):
            strong_pullback = True

    support_confirm = False
    if ma20 > 0 and close > 0 and low > 0:
        if low <= ma20 * (1.0 + ma20_near) and low >= ma20 * (1.0 - ma20_break) and close >= ma20:
            support_confirm = True

    golden_cross = False
    if ma5 > 0 and ma10 > 0 and ma5_prev > 0 and ma10_prev > 0:
        if ma5_prev <= ma10_prev and ma5 > ma10:
            gc_strength = (ma5 / ma10 - 1.0) * 100.0
            if gc_strength >= min_gc:
                golden_cross = True

    return {
        "强势回调买": bool(strong_pullback),
        "支撑确认买": bool(support_confirm),
        "金叉跟进买": bool(golden_cross),
    }


def _score(signals: Dict[str, bool], close: float, ma5: float, ma20: float) -> float:
    s = 0.0
    if signals.get("金叉跟进买"):
        s += 3.0
    if signals.get("强势回调买"):
        s += 2.0
    if signals.get("支撑确认买"):
        s += 2.0
    if close > 0 and ma5 > 0:
        s += max(0.0, 1.2 - abs(close / ma5 - 1.0) * 30.0)
    if close > 0 and ma20 > 0:
        s += max(0.0, 0.8 - abs(close / ma20 - 1.0) * 20.0)
    return float(s)


def main() -> None:
    parser = argparse.ArgumentParser(description="上车：均线四信号（MA5/10/20）扫描（pytdx）")
    parser.add_argument("--markets", type=str, default="both")
    parser.add_argument("--max-stocks", type=int, default=0)
    parser.add_argument("--exclude-st", action="store_true")
    parser.add_argument("--bars", type=int, default=140)
    parser.add_argument("--trade-date", type=str, default="")
    parser.add_argument("--allow-intraday", action="store_true")
    parser.add_argument("--trend-lookback", type=int, default=5)
    parser.add_argument("--require-ma10-over-ma20", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--stand-days", type=int, default=2)
    parser.add_argument("--stand-tol-pct", type=float, default=0.2)
    parser.add_argument("--min-signal-count", type=int, default=2)
    parser.add_argument("--ma5-near-pct", type=float, default=0.6)
    parser.add_argument("--ma5-hold-pct", type=float, default=1.2)
    parser.add_argument("--ma5-close-min-pct", type=float, default=-0.2)
    parser.add_argument("--ma20-near-pct", type=float, default=0.8)
    parser.add_argument("--ma20-break-pct", type=float, default=1.0)
    parser.add_argument("--min-golden-cross-pct", type=float, default=0.2)
    parser.add_argument("--cache-dir", type=str, default="")
    parser.add_argument("--refresh-cache", action="store_true")
    parser.add_argument("--sleep", type=float, default=0.0)
    parser.add_argument("--progress-every", type=int, default=200)
    parser.add_argument("--top", type=int, default=120)
    parser.add_argument("--out", type=str, default="")
    args = parser.parse_args()

    markets = _parse_markets(args.markets)
    df_codes = get_all_a_share_codes()
    if df_codes is None or df_codes.empty:
        raise SystemExit("股票池为空：get_all_a_share_codes() 返回空")

    df_codes = df_codes.copy()
    df_codes["market"] = pd.to_numeric(df_codes["market"], errors="coerce").fillna(-1).astype(int)
    df_codes["code"] = df_codes["code"].astype(str).str.zfill(6)
    if "name" not in df_codes.columns:
        df_codes["name"] = ""
    df_codes["name"] = df_codes["name"].astype(str)
    df_codes = df_codes[df_codes["market"].isin([int(m) for m in markets])].reset_index(drop=True)
    if bool(args.exclude_st):
        name_u = df_codes["name"].astype(str).str.upper()
        df_codes = df_codes[~name_u.str.contains("ST", na=False)]
        df_codes = df_codes[~df_codes["name"].astype(str).str.contains("退", na=False)]
        df_codes = df_codes.reset_index(drop=True)

    if int(args.max_stocks) > 0:
        df_codes = df_codes.head(int(args.max_stocks)).reset_index(drop=True)

    cache_dir = str(args.cache_dir or "").strip()
    if not cache_dir:
        cache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_cache_daily")

    rows: List[Dict] = []
    bars = max(40, int(args.bars))
    trade_date = str(args.trade_date or "").strip()
    lb = max(1, int(args.trend_lookback))
    need_rows = max(bars, 30 + lb)
    ep = None
    try:
        ep = connected_endpoint()
    except Exception:
        ep = None
    print(f"{_now_ts()} 开始扫描：stocks={len(df_codes)} markets={markets} endpoint={ep}", flush=True)

    for i, r in df_codes.iterrows():
        if int(args.progress_every) > 0 and (int(i) + 1) % int(args.progress_every) == 0:
            print(f"{_now_ts()} 进度 {int(i)+1}/{len(df_codes)}", flush=True)
        market = int(r["market"])
        code = str(r["code"]).zfill(6)
        name = str(r.get("name") or "").strip()

        df, _ = load_or_fetch_daily(
            cache_dir=cache_dir,
            market=market,
            code=code,
            bars=need_rows,
            refresh=bool(args.refresh_cache),
            cache_read_timeout_s=0.0,
        )
        if df is None or df.empty or len(df) < 25 + lb:
            if float(args.sleep) > 0:
                time.sleep(float(args.sleep))
            continue

        df = df.copy()
        df["ma5"] = _calc_ma(df["close"], 5)
        df["ma10"] = _calc_ma(df["close"], 10)
        df["ma20"] = _calc_ma(df["close"], 20)
        df["ma20_lb"] = df["ma20"].shift(int(lb))
        df["pct_chg"] = (df["close"] / df["close"].shift(1) - 1.0) * 100.0

        eval_i = _pick_eval_row(df, trade_date, allow_intraday=bool(args.allow_intraday))
        if eval_i is None or int(eval_i) <= 0:
            if float(args.sleep) > 0:
                time.sleep(float(args.sleep))
            continue

        row = df.iloc[int(eval_i)]
        row_lb = df.iloc[int(eval_i) - int(lb)] if int(eval_i) - int(lb) >= 0 else row
        close = float(row.get("close") or 0.0)
        ma5 = float(row.get("ma5") or 0.0)
        ma10 = float(row.get("ma10") or 0.0)
        ma20 = float(row.get("ma20") or 0.0)
        if close <= 0 or ma20 <= 0 or ma10 <= 0 or ma5 <= 0:
            if float(args.sleep) > 0:
                time.sleep(float(args.sleep))
            continue

        if not _is_uptrend(row, row_lb, require_ma10_over_ma20=bool(args.require_ma10_over_ma20)):
            if float(args.sleep) > 0:
                time.sleep(float(args.sleep))
            continue
        if not _stand_firm(df, int(eval_i), int(args.stand_days), float(args.stand_tol_pct)):
            if float(args.sleep) > 0:
                time.sleep(float(args.sleep))
            continue

        signals = _signal_flags(
            df=df,
            eval_i=int(eval_i),
            ma5_near_pct=float(args.ma5_near_pct),
            ma5_hold_pct=float(args.ma5_hold_pct),
            ma5_close_min_pct=float(args.ma5_close_min_pct),
            ma20_near_pct=float(args.ma20_near_pct),
            ma20_break_pct=float(args.ma20_break_pct),
            min_golden_cross_pct=float(args.min_golden_cross_pct),
        )
        if not any(bool(v) for v in signals.values()):
            if float(args.sleep) > 0:
                time.sleep(float(args.sleep))
            continue

        sig_list = [k for k, v in signals.items() if bool(v)]
        if len(sig_list) < max(1, int(args.min_signal_count)):
            if float(args.sleep) > 0:
                time.sleep(float(args.sleep))
            continue
        ma5_ma10_diff_pct = 0.0
        if ma5 > 0 and ma10 > 0:
            ma5_ma10_diff_pct = (ma5 / ma10 - 1.0) * 100.0
        td = row.get("datetime")
        rows.append(
            {
                "symbol": code,
                "name": name,
                "market": market,
                "trade_date": _to_ymd(td) if isinstance(td, pd.Timestamp) else "",
                "close": round(close, 4),
                "pct_chg": round(float(row.get("pct_chg") or 0.0), 3),
                "ma5": round(ma5, 4),
                "ma10": round(ma10, 4),
                "ma20": round(ma20, 4),
                "ma5_ma10_diff_pct": round(float(ma5_ma10_diff_pct), 4),
                "signals": " / ".join(sig_list),
                "score": round(_score(signals, close=close, ma5=ma5, ma20=ma20), 4),
                "reason": "；".join(sig_list),
            }
        )

        if float(args.sleep) > 0:
            time.sleep(float(args.sleep))

    df_out = pd.DataFrame(rows)
    if df_out is None or df_out.empty:
        print(f"{_now_ts()} 完成：0 条命中", flush=True)
        return

    df_out = df_out.sort_values(["score", "pct_chg"], ascending=[False, False]).reset_index(drop=True)
    top_n = max(1, int(args.top))
    df_show = df_out.head(top_n).copy()
    print(f"{_now_ts()} 完成：命中 {len(df_out)} 条，展示 Top{len(df_show)}", flush=True)
    with pd.option_context("display.max_rows", min(80, len(df_show)), "display.max_columns", 30, "display.width", 220):
        print(df_show, flush=True)

    out_path = str(args.out or "").strip()
    if not out_path:
        td_tag = str(df_show.iloc[0].get("trade_date") or "").strip() or "latest"
        out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"上车_均线四信号_{td_tag}.csv")
    try:
        df_out.to_csv(out_path, index=False, encoding="utf-8")
        print(f"{_now_ts()} 已写入: {out_path}", flush=True)
    except Exception as e:
        print(f"{_now_ts()} 写入失败: {type(e).__name__}:{e}", flush=True)


if __name__ == "__main__":
    main()
