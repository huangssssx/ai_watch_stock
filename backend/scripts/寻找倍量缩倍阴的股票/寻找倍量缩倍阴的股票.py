import argparse
import os
import sys
import time
from datetime import datetime, time as dtime
from typing import Dict, Optional, Tuple

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

PYTDX_VOL_MULTIPLIER = 100


def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _fmt_dt(v) -> str:
    dt = pd.to_datetime(v, errors="coerce")
    if pd.isna(dt):
        return ""
    try:
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""



def _ensure_dir(p: str) -> None:
    if not p:
        return


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


def _fetch_daily_bars(tdx, market: int, code: str, bars: int) -> pd.DataFrame:
    try:
        data = tdx.get_security_bars(9, int(market), str(code).zfill(6), 0, int(bars))
    except Exception:
        data = []
    df = tdx.to_df(data) if data else pd.DataFrame()
    return _clean_daily_df(df, convert_vol_from_hand_to_share=True)


def load_or_fetch_daily(
    tdx,
    cache_dir: str,
    market: int,
    code: str,
    bars: int,
    refresh: bool,
) -> Tuple[pd.DataFrame, bool]:
    cache_dir = str(cache_dir or "").strip()
    if not cache_dir:
        return _fetch_daily_bars(tdx, market, code, bars), False
    _ensure_dir(cache_dir)
    p = _cache_path(cache_dir, market, code)
    if (not refresh) and os.path.exists(p):
        try:
            df = pd.read_csv(p, compression="gzip")
        except Exception:
            df = pd.DataFrame()
        df = _clean_daily_df(df, convert_vol_from_hand_to_share=False)
        if df is not None and not df.empty and len(df) >= max(70, int(bars) // 2):
            return df.tail(int(bars)).reset_index(drop=True), True
    df = _fetch_daily_bars(tdx, market, code, bars)
    if df is None or df.empty:
        return pd.DataFrame(), False
    try:
        df.to_csv(p, index=False, encoding="utf-8", compression="gzip")
    except Exception:
        pass
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
            if now.time() < dtime(16, 30):
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


def _match_pattern(
    df: pd.DataFrame,
    end_i: int,
    follow_days: int,
    vol_burst_ratio: float,
    day1_up_pct: float,
    shrink_ratio: float,
    require_above_ma60: bool,
) -> Optional[Dict]:
    follow_days = max(1, int(follow_days))
    end_i = int(end_i)
    day1_i = end_i - follow_days
    day0_i = day1_i - 1
    if day0_i < 0:
        return None
    if len(df) < 70:
        return None
    if end_i >= len(df):
        return None

    w = df.copy()
    w["ma60"] = _calc_ma(w["close"], 60)
    if w["ma60"].isna().all():
        return None

    r0 = w.iloc[day0_i]
    r1 = w.iloc[day1_i]
    close0 = float(r0.get("close") or 0.0)
    close1 = float(r1.get("close") or 0.0)
    vol0 = float(r0.get("vol") or 0.0)
    vol1 = float(r1.get("vol") or 0.0)
    ma60_1 = float(r1.get("ma60") or 0.0)
    if close0 <= 0 or close1 <= 0 or vol0 <= 0 or vol1 <= 0 or ma60_1 <= 0:
        return None

    day1_ret = close1 / close0 - 1.0
    if vol1 < vol0 * float(vol_burst_ratio):
        return None
    if day1_ret < float(day1_up_pct) / 100.0:
        return None
    if bool(require_above_ma60) and close1 <= ma60_1:
        return None

    day1_vol = vol1
    prev_vol = day1_vol
    for k in range(1, follow_days + 1):
        i = day1_i + k
        if i > end_i:
            return None
        rk = w.iloc[i]
        close_k = float(rk.get("close") or 0.0)
        vol_k = float(rk.get("vol") or 0.0)
        ma60_k = float(rk.get("ma60") or 0.0)
        if close_k <= 0 or vol_k <= 0 or ma60_k <= 0:
            return None
        if bool(require_above_ma60) and close_k <= ma60_k:
            return None
        if vol_k > day1_vol * float(shrink_ratio):
            return None
        if k >= 2 and vol_k > prev_vol:
            return None
        prev_vol = vol_k

    day2_i = day1_i + 1
    end_row = w.iloc[end_i]
    day2_row = w.iloc[day2_i] if day2_i <= end_i else end_row
    return {
        "day0_dt": w.iloc[day0_i]["datetime"],
        "day1_dt": w.iloc[day1_i]["datetime"],
        "day2_dt": w.iloc[day2_i]["datetime"] if day2_i <= end_i else end_row["datetime"],
        "end_dt": end_row["datetime"],
        "day1_ret_pct": float(day1_ret * 100.0),
        "day1_vol_ratio": float(vol1 / (vol0 + 1e-9)),
        "day2_vol_ratio": float(float(day2_row.get("vol") or 0.0) / (vol1 + 1e-9)),
        "end_vol_ratio_to_day1": float(float(end_row.get("vol") or 0.0) / (vol1 + 1e-9)),
        "end_close": float(end_row.get("close") or 0.0),
        "end_ma60": float(end_row.get("ma60") or 0.0),
    }


def _inspect_one(
    code: str,
    name: str,
    df: pd.DataFrame,
    end_i: int,
    follow_days: int,
    vol_burst_ratio: float,
    day1_up_pct: float,
    shrink_ratio: float,
    require_above_ma60: bool,
) -> None:
    if df is None or df.empty:
        print(f"{_now_ts()} INSPECT 无数据：{code} {name}".strip(), flush=True)
        return
    w = df.copy()
    w["ma60"] = _calc_ma(w["close"], 60)
    end_i = int(end_i)
    follow_days = max(1, int(follow_days))
    day1_i = end_i - follow_days
    day0_i = day1_i - 1
    if day0_i < 0 or end_i >= len(w):
        print(f"{_now_ts()} INSPECT 数据不足：len={len(w)} end_i={end_i} follow_days={follow_days}", flush=True)
        return

    r0 = w.iloc[day0_i]
    r1 = w.iloc[day1_i]
    close0 = float(r0.get("close") or 0.0)
    close1 = float(r1.get("close") or 0.0)
    vol0 = float(r0.get("vol") or 0.0)
    vol1 = float(r1.get("vol") or 0.0)
    ma1 = float(r1.get("ma60") or 0.0)
    day1_ret = (close1 / close0 - 1.0) if close0 > 0 else 0.0
    day1_burst_ok = bool(vol0 > 0 and vol1 >= vol0 * float(vol_burst_ratio))
    day1_up_ok = bool(day1_ret >= float(day1_up_pct) / 100.0)
    day1_ma_ok = True
    if bool(require_above_ma60):
        day1_ma_ok = bool(close1 > 0 and ma1 > 0 and close1 > ma1)

    print(f"{_now_ts()} INSPECT {code} {name}".strip(), flush=True)
    print(
        f"  day0={_fmt_dt(r0.get('datetime'))} close={close0:.2f} vol={vol0/1e4:.2f}万股({vol0/100.0:.0f}手)",
        flush=True,
    )
    print(
        f"  day1={_fmt_dt(r1.get('datetime'))} close={close1:.2f} vol={vol1/1e4:.2f}万股({vol1/100.0:.0f}手) "
        f"ret={day1_ret*100.0:.2f}% vol_ratio={((vol1/vol0) if vol0>0 else 0.0):.2f} ma60={ma1:.2f}",
        flush=True,
    )
    print(
        f"  day1条件: 倍量({day1_burst_ok}) 涨幅>={float(day1_up_pct):g}%({day1_up_ok}) MA60之上({day1_ma_ok})",
        flush=True,
    )

    prev_vol = vol1
    for k in range(1, follow_days + 1):
        i = day1_i + k
        rk = w.iloc[i]
        close_k = float(rk.get("close") or 0.0)
        vol_k = float(rk.get("vol") or 0.0)
        ma_k = float(rk.get("ma60") or 0.0)
        ma_ok = True
        if bool(require_above_ma60):
            ma_ok = bool(close_k > 0 and ma_k > 0 and close_k > ma_k)
        shrink_to_day1_ok = bool(vol_k > 0 and vol_k <= vol1 * float(shrink_ratio))
        shrink_to_prev_ok = True if k == 1 else bool(vol_k <= prev_vol)
        print(
            f"  day{1+k}={_fmt_dt(rk.get('datetime'))} close={close_k:.2f} vol={vol_k/1e4:.2f}万股({vol_k/100.0:.0f}手) "
            f"vol_ratio_to_day1={((vol_k/vol1) if vol1>0 else 0.0):.3f} ma60={ma_k:.2f}",
            flush=True,
        )
        print(
            f"  day{1+k}条件: 缩量<=day1*{float(shrink_ratio):g}({shrink_to_day1_ok}) "
            f"缩量<=前一日({shrink_to_prev_ok}) MA60之上({ma_ok})",
            flush=True,
        )
        prev_vol = vol_k



def main() -> None:
    from utils.pytdx_client import connected_endpoint, tdx
    from utils.stock_codes import get_all_a_share_codes

    parser = argparse.ArgumentParser(description="寻找：倍量 + 上涨5% + MA60之上 -> 连续缩量且MA60之上（pytdx）")
    parser.add_argument("--max-stocks", type=int, default=0)
    parser.add_argument("--exclude-st", action="store_true")
    parser.add_argument("--bars", type=int, default=200)
    parser.add_argument("--trade-date", type=str, default="")
    parser.add_argument("--allow-intraday", action="store_true")
    parser.add_argument("--symbol", type=str, default="")
    parser.add_argument("--market", type=int, default=-1)
    parser.add_argument("--inspect", action="store_true")
    parser.add_argument("--recent-days", type=int, default=6)
    parser.add_argument("--end-must-be-eval", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--follow-days", type=int, default=2)
    parser.add_argument("--vol-burst-ratio", type=float, default=2.0)
    parser.add_argument("--day1-up-pct", type=float, default=5.0)
    parser.add_argument("--shrink-ratio", type=float, default=0.5)
    parser.add_argument("--require-above-ma60", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--cache-dir", type=str, default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "_cache_daily"))
    parser.add_argument("--refresh-cache", action="store_true")
    parser.add_argument("--sleep-ms", type=int, default=0)
    parser.add_argument("--debug-sample", type=int, default=0)
    parser.add_argument("--out", type=str, default="")
    args = parser.parse_args()

    try:
        _ = tdx.get_security_count(0)
    except Exception as e:
        print(f"{_now_ts()} pytdx 连接失败：{type(e).__name__}:{e}", flush=True)
        return
    try:
        ep = connected_endpoint()
    except Exception:
        ep = None
    print(f"{_now_ts()} 开始扫描：endpoint={ep}", flush=True)

    df_codes = get_all_a_share_codes()
    if df_codes is None or df_codes.empty:
        print(f"{_now_ts()} 无股票列表：get_all_a_share_codes() 返回空", flush=True)
        return
    if bool(args.exclude_st) and "name" in df_codes.columns:
        df_codes = df_codes[~df_codes["name"].astype(str).str.contains("ST", na=False)].copy()
    symbol = str(args.symbol or "").strip()
    if symbol:
        symbol = symbol.zfill(6)
        df_codes = df_codes[df_codes["code"].astype(str).str.zfill(6) == symbol].copy()
        if int(args.market) in (0, 1):
            df_codes["market"] = int(args.market)
    if int(args.max_stocks) and int(args.max_stocks) > 0:
        df_codes = df_codes.head(int(args.max_stocks)).copy()

    rows = []
    bars = max(120, int(args.bars))
    recent_days = max(1, int(args.recent_days))
    follow_days = max(1, int(args.follow_days))
    sleep_s = max(0, int(args.sleep_ms)) / 1000.0
    debug_sample = max(0, int(args.debug_sample))
    end_must = bool(args.end_must_be_eval)

    stats = {
        "pool": 0,
        "empty_df": 0,
        "too_short": 0,
        "no_eval": 0,
        "tested": 0,
        "matched": 0,
        "debug_printed": 0,
    }

    total = len(df_codes)
    stats["pool"] = int(total)
    print(
        f"{_now_ts()} 股票池={total} bars={bars} recent_days={recent_days} follow_days={follow_days} "
        f"vol_burst>={float(args.vol_burst_ratio):g}x day1_up>={float(args.day1_up_pct):g}% "
        f"shrink<={float(args.shrink_ratio):g}x require_above_ma60={bool(args.require_above_ma60)} "
        f"end_must_be_eval={end_must}",
        flush=True,
    )
    for i, r in df_codes.reset_index(drop=True).iterrows():
        market = int(r.get("market"))
        code = str(r.get("code")).zfill(6)
        name = str(r.get("name") or "").strip()
        if sleep_s > 0:
            time.sleep(sleep_s)

        df, cached = load_or_fetch_daily(
            tdx,
            cache_dir=str(args.cache_dir),
            market=market,
            code=code,
            bars=bars,
            refresh=bool(args.refresh_cache),
        )
        if df is None or df.empty:
            stats["empty_df"] += 1
            continue
        if len(df) < (65 + follow_days + 2):
            stats["too_short"] += 1
            continue
        eval_i = _pick_eval_row(df, trade_date=str(args.trade_date), allow_intraday=bool(args.allow_intraday))
        if eval_i is None:
            stats["no_eval"] += 1
            continue
        eval_i = int(eval_i)
        if bool(args.inspect):
            _inspect_one(
                code=code,
                name=name,
                df=df,
                end_i=eval_i,
                follow_days=follow_days,
                vol_burst_ratio=float(args.vol_burst_ratio),
                day1_up_pct=float(args.day1_up_pct),
                shrink_ratio=float(args.shrink_ratio),
                require_above_ma60=bool(args.require_above_ma60),
            )
            return
        start_i = max(0, eval_i - recent_days + 1)
        best = None
        best_end_i = None
        stats["tested"] += 1
        end_range = [eval_i] if end_must else list(range(eval_i, start_i - 1, -1))
        for end_i in end_range:
            m = _match_pattern(
                df=df,
                end_i=end_i,
                follow_days=follow_days,
                vol_burst_ratio=float(args.vol_burst_ratio),
                day1_up_pct=float(args.day1_up_pct),
                shrink_ratio=float(args.shrink_ratio),
                require_above_ma60=bool(args.require_above_ma60),
            )
            if m is None:
                continue
            best = m
            best_end_i = end_i
            break
        if best is None:
            if debug_sample and stats["debug_printed"] < debug_sample:
                end_i = eval_i
                day1_i = end_i - follow_days
                day0_i = day1_i - 1
                dbg_parts = []
                if day0_i >= 0 and end_i < len(df):
                    w = df.copy()
                    w["ma60"] = _calc_ma(w["close"], 60)
                    r0 = w.iloc[day0_i]
                    r1 = w.iloc[day1_i] if day1_i >= 0 else r0
                    re = w.iloc[end_i]
                    close0 = float(r0.get("close") or 0.0)
                    close1 = float(r1.get("close") or 0.0)
                    vol0 = float(r0.get("vol") or 0.0)
                    vol1 = float(r1.get("vol") or 0.0)
                    ma1 = float(r1.get("ma60") or 0.0)
                    ret1 = (close1 / close0 - 1.0) * 100.0 if close0 > 0 else 0.0
                    vr = (vol1 / vol0) if vol0 > 0 else 0.0
                    dbg_parts.append(f"{code} {name}".strip())
                    dbg_parts.append(f"day1_ret={ret1:.2f}% (>= {float(args.day1_up_pct):g}%)")
                    dbg_parts.append(f"day1_vol_ratio={vr:.2f} (>= {float(args.vol_burst_ratio):g})")
                    if bool(args.require_above_ma60):
                        dbg_parts.append(f"day1_close={close1:.2f} ma60={ma1:.2f} (close>ma60)")
                    dbg_parts.append(f"end_close={float(re.get('close') or 0.0):.2f}")
                if dbg_parts:
                    print(f"{_now_ts()} DEBUG 未命中样本： " + " | ".join(dbg_parts), flush=True)
                stats["debug_printed"] += 1
            continue
        stats["matched"] += 1

        end_dt = pd.to_datetime(best.get("end_dt"), errors="coerce")
        day1_dt = pd.to_datetime(best.get("day1_dt"), errors="coerce")
        day2_dt = pd.to_datetime(best.get("day2_dt"), errors="coerce")
        day0_dt = pd.to_datetime(best.get("day0_dt"), errors="coerce")
        reason = f"day1倍量>={float(args.vol_burst_ratio):g}x&涨>={float(args.day1_up_pct):g}% | 缩量<={float(args.shrink_ratio):g}xday1 连续{follow_days}天 | MA60之上"
        rows.append(
            {
                "market": market,
                "symbol": code,
                "name": name,
                "cached": bool(cached),
                "day0": day0_dt.strftime("%Y-%m-%d") if pd.notna(day0_dt) else "",
                "day1": day1_dt.strftime("%Y-%m-%d") if pd.notna(day1_dt) else "",
                "day2": day2_dt.strftime("%Y-%m-%d") if pd.notna(day2_dt) else "",
                "end_day": end_dt.strftime("%Y-%m-%d") if pd.notna(end_dt) else "",
                "day1_ret_pct": float(best.get("day1_ret_pct") or 0.0),
                "day1_vol_ratio": float(best.get("day1_vol_ratio") or 0.0),
                "day2_vol_ratio": float(best.get("day2_vol_ratio") or 0.0),
                "end_vol_ratio_to_day1": float(best.get("end_vol_ratio_to_day1") or 0.0),
                "end_close": float(best.get("end_close") or 0.0),
                "end_ma60": float(best.get("end_ma60") or 0.0),
                "reason": reason,
                "follow_days": int(follow_days),
                "end_i": int(best_end_i) if best_end_i is not None else -1,
            }
        )

        if (i + 1) % 200 == 0:
            print(f"{_now_ts()} 进度 {i+1}/{total} 命中 {len(rows)}", flush=True)

    df_out = pd.DataFrame(rows)
    if df_out.empty:
        print(
            f"{_now_ts()} 完成：0 条结果 | pool={stats['pool']} tested={stats['tested']} "
            f"empty_df={stats['empty_df']} too_short={stats['too_short']} no_eval={stats['no_eval']}",
            flush=True,
        )
        return

    for c in ("day1_ret_pct", "day1_vol_ratio", "end_vol_ratio_to_day1"):
        if c in df_out.columns:
            df_out[c] = pd.to_numeric(df_out[c], errors="coerce")
    df_out = df_out.sort_values(["day1_ret_pct", "day1_vol_ratio"], ascending=[False, False]).reset_index(drop=True)

    print(f"{_now_ts()} 完成：共 {len(df_out)} 条结果（展示前 50 条）", flush=True)
    show_cols = [
        "symbol",
        "name",
        "day1",
        "day2",
        "end_day",
        "day1_ret_pct",
        "day1_vol_ratio",
        "day2_vol_ratio",
        "end_vol_ratio_to_day1",
        "end_close",
        "end_ma60",
    ]
    exist_cols = [c for c in show_cols if c in df_out.columns]
    print(df_out[exist_cols].head(50).to_string(index=False), flush=True)

    out_path = str(args.out or "").strip()
    if out_path:
        try:
            df_out.to_csv(out_path, index=False, encoding="utf-8")
            print(f"{_now_ts()} 已写出：{out_path}", flush=True)
        except Exception as e:
            print(f"{_now_ts()} 写出失败：{e}", flush=True)


if __name__ == "__main__":
    main()
