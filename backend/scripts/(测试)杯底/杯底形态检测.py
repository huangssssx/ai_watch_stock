"""
全市场扫描：杯底/杯柄（Cup with Handle）形态（长信号）

默认行为：
- markets=全市场；lookback_days=520；不剔除 ST；不限制扫描数量
- 形态：杯深<=33%，杯期在 [45, 260] 个交易日；右沿≈左沿；杯底出现在中段；右沿后有 5-25 天杯柄回撤
- 信号模式：
  - setup：临近突破（默认），收盘距离杯柄高点不超过 setup_within_pct
  - breakout：收盘突破杯柄高点（默认 breakout_pct），可选成交量放大确认
- 输出：默认写到脚本同目录 CSV，按 score 排序，输出 Top200

典型用法：
python3 "backend/scripts/(测试)杯底/杯底形态检测.py"
python3 "backend/scripts/(测试)杯底/杯底形态检测.py" --markets sh --max-stocks 2000 --lookback-days 700
python3 "backend/scripts/(测试)杯底/杯底形态检测.py" --target-date 20250115
"""

import argparse
import os
import sys
import time
from datetime import datetime
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


def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _parse_markets(s: str) -> List[int]:
    s = str(s or "").strip().lower()
    if s in {"sz", "0"}:
        return [0]
    if s in {"sh", "1"}:
        return [1]
    return [0, 1]


def _chunks(items: List, n: int):
    n = max(1, int(n))
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _normalize_and_validate_daily_bars_df(
    df: pd.DataFrame,
    market: int,
    code: str,
) -> Tuple[Optional[pd.DataFrame], str]:
    sid = f"{int(market)}-{str(code).zfill(6)}"
    if df is None or df.empty:
        return None, f"{sid}:empty_df"

    df = df.copy()
    lower_cols = {c: str(c).strip().lower() for c in df.columns}
    if len(set(lower_cols.values())) == len(lower_cols):
        df = df.rename(columns=lower_cols)

    rename_map: Dict[str, str] = {}
    if "volume" in df.columns and "vol" not in df.columns:
        rename_map["volume"] = "vol"
    if "trade_date" in df.columns and "datetime" not in df.columns:
        rename_map["trade_date"] = "datetime"
    if "date" in df.columns and "datetime" not in df.columns:
        rename_map["date"] = "datetime"
    if rename_map:
        df = df.rename(columns=rename_map)

    if "datetime" not in df.columns:
        return None, f"{sid}:missing_datetime"

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"])
    if df.empty:
        return None, f"{sid}:datetime_all_nan"

    required_price_cols = ["open", "close", "high", "low"]
    missing_price_cols = [c for c in required_price_cols if c not in df.columns]
    if missing_price_cols:
        return None, f"{sid}:missing_cols:{','.join(missing_price_cols)}"

    for c in ("open", "close", "high", "low", "vol", "amount"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=required_price_cols, how="any")
    if df.empty:
        return None, f"{sid}:price_all_nan"

    df = df.drop_duplicates(subset=["datetime"]).sort_values("datetime", ascending=True).reset_index(drop=True)
    if df.empty:
        return None, f"{sid}:empty_after_dedup"

    o = df["open"]
    c = df["close"]
    h = df["high"]
    l = df["low"]

    non_positive = (o <= 0) | (c <= 0) | (h <= 0) | (l <= 0)
    non_pos_ratio = float(non_positive.mean()) if len(df) > 0 else 1.0
    if non_pos_ratio >= 0.2:
        return None, f"{sid}:non_positive_ohlc_ratio:{non_pos_ratio:.3f}"
    if non_positive.any():
        df = df.loc[~non_positive].reset_index(drop=True)
        o = df["open"]
        c = df["close"]
        h = df["high"]
        l = df["low"]

    bad_price = (h < l) | (o < l) | (o > h) | (c < l) | (c > h)
    bad_ratio = float(bad_price.mean()) if len(df) > 0 else 1.0
    if bad_ratio >= 0.2:
        return None, f"{sid}:bad_ohlc_ratio:{bad_ratio:.3f}"
    if bad_price.any():
        df = df.loc[~bad_price].reset_index(drop=True)

    if "vol" in df.columns:
        df = df[df["vol"].fillna(0) >= 0].reset_index(drop=True)
    if "amount" in df.columns:
        df = df[df["amount"].fillna(0) >= 0].reset_index(drop=True)

    if df.empty:
        return None, f"{sid}:empty_after_filters"

    if not df["datetime"].is_monotonic_increasing:
        return None, f"{sid}:datetime_not_monotonic"

    return df, "ok"


def _fetch_daily_bars(
    tdx_,
    market: int,
    code: str,
    count: int,
) -> Tuple[Optional[pd.DataFrame], str]:
    sid = f"{int(market)}-{str(code).zfill(6)}"
    try:
        bars = tdx_.get_security_bars(9, int(market), str(code).zfill(6), 0, int(count))
    except Exception as e:
        return None, f"{sid}:get_security_bars_exception:{type(e).__name__}"
    if not bars:
        return None, f"{sid}:bars_empty"
    df0 = tdx_.to_df(bars) if bars else pd.DataFrame()
    if df0 is None or df0.empty:
        return None, f"{sid}:to_df_empty"
    df2, status = _normalize_and_validate_daily_bars_df(df0, market=int(market), code=str(code).zfill(6))
    if df2 is None or status != "ok":
        return None, str(status)
    return df2, "ok"


def _slice_asof_date(df: pd.DataFrame, target_date: Optional[str]) -> Tuple[pd.DataFrame, str]:
    if not target_date:
        return df, "ok"
    s = str(target_date).strip()
    try:
        dt = pd.to_datetime(s, format="%Y%m%d", errors="raise")
    except Exception:
        return df, "bad_target_date"
    out = df[df["datetime"] <= dt].copy()
    if out.empty:
        return out, "empty_after_target_date"
    return out.reset_index(drop=True), "ok"


def _mean_or_nan(x: pd.Series) -> float:
    try:
        v = float(x.mean())
        return v
    except Exception:
        return float("nan")


def _cup_handle_signal(
    df: pd.DataFrame,
    *,
    signal_mode: str,
    setup_within_pct: float,
    cup_min_days: int,
    cup_max_days: int,
    min_cup_depth: float,
    max_cup_depth: float,
    rim_tolerance: float,
    bottom_pos_min: float,
    bottom_pos_max: float,
    handle_min_days: int,
    handle_max_days: int,
    handle_pullback_min: float,
    handle_pullback_max: float,
    breakout_pct: float,
    volume_ma_days: int,
    breakout_vol_ratio: float,
) -> Tuple[Optional[Dict], str]:
    if df is None or df.empty or len(df) < (cup_min_days + handle_min_days + 20):
        return None, "bars_too_few"

    end_idx = len(df) - 1
    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    vol = df["vol"].astype(float) if "vol" in df.columns else None

    rim_right_min = max(0, end_idx - int(handle_max_days) - 90)
    rim_right_max = max(0, end_idx - int(handle_min_days))
    if rim_right_max <= rim_right_min:
        return None, "rim_right_window_invalid"

    win_close = close.iloc[rim_right_min : rim_right_max + 1]
    if win_close.empty:
        return None, "rim_right_window_empty"

    peak_candidates = win_close.sort_values(ascending=False).head(12)
    if peak_candidates.empty:
        return None, "no_peak"

    best: Optional[Dict] = None
    best_score = -1e9
    best_reason = "no_match"
    signal_mode = str(signal_mode or "").strip().lower()
    if signal_mode not in {"setup", "breakout"}:
        signal_mode = "setup"

    for idx, rim_right_close in peak_candidates.items():
        rim_right_idx = int(idx)
        handle_len = end_idx - rim_right_idx
        if handle_len < int(handle_min_days) or handle_len > int(handle_max_days):
            continue

        rim_right_high = float(high.iloc[rim_right_idx])
        if not (rim_right_high > 0 and rim_right_close > 0):
            continue

        handle_df = df.iloc[rim_right_idx + 1 : end_idx + 1].copy()
        if handle_df is None or handle_df.empty or len(handle_df) != handle_len:
            continue

        handle_low = float(handle_df["low"].min())
        handle_high = float(handle_df["high"].max())
        if not (handle_low > 0 and handle_high > 0):
            continue

        pullback = (rim_right_high - handle_low) / rim_right_high
        if pullback < float(handle_pullback_min) or pullback > float(handle_pullback_max):
            best_reason = "handle_pullback_out"
            continue

        last_close = float(close.iloc[end_idx])
        breakout_price = float(handle_high) * (1.0 + float(breakout_pct))
        setup_price = float(handle_high) * (1.0 - float(setup_within_pct))
        if signal_mode == "breakout":
            if not (last_close >= breakout_price):
                best_reason = "not_breakout"
                continue
        else:
            if not (last_close >= setup_price):
                best_reason = "not_near_breakout"
                continue

        if vol is not None and int(volume_ma_days) > 0:
            ma = vol.rolling(int(volume_ma_days)).mean()
            ma_last = float(ma.iloc[end_idx]) if len(ma) > 0 else float("nan")
            v_last = float(vol.iloc[end_idx])
            if not (ma_last > 0 and v_last > 0):
                best_reason = "volume_invalid"
                continue
            v_ratio = v_last / ma_last
            if signal_mode == "breakout" and v_ratio < float(breakout_vol_ratio):
                best_reason = "breakout_volume_too_small"
                continue
        else:
            v_ratio = float("nan")

        left_end = rim_right_idx - int(cup_min_days)
        left_start = max(0, rim_right_idx - int(cup_max_days))
        if left_end <= left_start:
            best_reason = "cup_window_invalid"
            continue

        left_win = close.iloc[left_start : left_end + 1]
        if left_win.empty:
            best_reason = "cup_window_empty"
            continue

        rim_left_idx = int(left_win.idxmax())
        rim_left_close = float(close.iloc[rim_left_idx])
        rim_left_high = float(high.iloc[rim_left_idx])
        if not (rim_left_close > 0 and rim_left_high > 0):
            best_reason = "rim_left_invalid"
            continue

        rim_diff = abs(rim_left_close - rim_right_close) / max(rim_left_close, rim_right_close)
        if rim_diff > float(rim_tolerance):
            best_reason = "rim_diff_too_large"
            continue

        cup_len = rim_right_idx - rim_left_idx
        if cup_len < int(cup_min_days) or cup_len > int(cup_max_days):
            best_reason = "cup_len_out"
            continue

        cup_seg = df.iloc[rim_left_idx : rim_right_idx + 1].copy()
        if cup_seg is None or cup_seg.empty or len(cup_seg) < int(cup_min_days):
            best_reason = "cup_seg_empty"
            continue

        bottom_pos = int(cup_seg["low"].astype(float).idxmin())
        bottom_low = float(low.iloc[bottom_pos])
        rim_avg = (rim_left_high + rim_right_high) / 2.0
        if not (rim_avg > 0 and bottom_low > 0):
            best_reason = "cup_depth_invalid"
            continue

        depth = (rim_avg - bottom_low) / rim_avg
        if depth < float(min_cup_depth) or depth > float(max_cup_depth):
            best_reason = "cup_depth_out"
            continue

        frac = (bottom_pos - rim_left_idx) / float(max(1, cup_len))
        if frac < float(bottom_pos_min) or frac > float(bottom_pos_max):
            best_reason = "bottom_pos_out"
            continue

        left_part = close.iloc[rim_left_idx : bottom_pos + 1].diff().dropna()
        right_part = close.iloc[bottom_pos : rim_right_idx + 1].diff().dropna()
        left_neg = float((left_part < 0).mean()) if len(left_part) > 0 else 0.0
        right_pos = float((right_part > 0).mean()) if len(right_part) > 0 else 0.0
        if left_neg < 0.35 or right_pos < 0.35:
            best_reason = "u_shape_weak"
            continue

        if vol is not None:
            pad = 5
            l0 = max(rim_left_idx, bottom_pos - pad)
            r0 = min(rim_right_idx, bottom_pos + pad)
            bottom_vol = _mean_or_nan(vol.iloc[l0 : r0 + 1])
            left_vol = _mean_or_nan(vol.iloc[max(rim_left_idx, rim_left_idx - 3) : min(rim_left_idx + 6, rim_right_idx) + 1])
            if left_vol > 0 and bottom_vol > 0:
                bottom_vol_ratio = bottom_vol / left_vol
            else:
                bottom_vol_ratio = float("nan")
        else:
            bottom_vol_ratio = float("nan")

        breakout_strength = (last_close / breakout_price) - 1.0 if breakout_price > 0 else 0.0
        dist_to_breakout = ((breakout_price / last_close) - 1.0) if last_close > 0 else float("inf")
        score = (
            (1.0 - min(1.0, rim_diff / max(1e-9, float(rim_tolerance)))) * 35.0
            + (1.0 - abs(depth - 0.22) / 0.22) * 20.0
            + min(1.0, max(0.0, (float(handle_pullback_max) - pullback) / max(1e-9, float(handle_pullback_max)))) * 15.0
            + (min(1.0, max(0.0, breakout_strength / 0.03)) * 10.0 if signal_mode == "breakout" else 0.0)
            + min(1.0, max(0.0, (left_neg - 0.35) / 0.35)) * 5.0
            + min(1.0, max(0.0, (right_pos - 0.35) / 0.35)) * 5.0
        )
        if vol is not None and pd.notna(v_ratio):
            if signal_mode == "breakout":
                score += min(1.0, max(0.0, (v_ratio - float(breakout_vol_ratio)) / 1.5)) * 10.0
            else:
                score += min(1.0, max(0.0, (1.2 - v_ratio) / 1.2)) * 6.0
        if vol is not None and pd.notna(bottom_vol_ratio):
            score += min(1.0, max(0.0, (1.0 - bottom_vol_ratio))) * 5.0

        if score > best_score:
            best_score = float(score)
            best = {
                "signal_mode": str(signal_mode),
                "rim_left_date": df.iloc[rim_left_idx]["datetime"].strftime("%Y%m%d"),
                "rim_left_price": float(rim_left_close),
                "rim_right_date": df.iloc[rim_right_idx]["datetime"].strftime("%Y%m%d"),
                "rim_right_price": float(rim_right_close),
                "bottom_date": df.iloc[bottom_pos]["datetime"].strftime("%Y%m%d"),
                "bottom_low": float(bottom_low),
                "cup_days": int(cup_len),
                "cup_depth": float(depth),
                "rim_diff": float(rim_diff),
                "handle_days": int(handle_len),
                "handle_pullback": float(pullback),
                "handle_high": float(handle_high),
                "breakout_date": df.iloc[end_idx]["datetime"].strftime("%Y%m%d"),
                "breakout_close": float(last_close),
                "dist_to_breakout": float(dist_to_breakout),
                "breakout_vol_ratio": float(v_ratio) if pd.notna(v_ratio) else float("nan"),
                "bottom_vol_ratio": float(bottom_vol_ratio) if pd.notna(bottom_vol_ratio) else float("nan"),
                "left_neg_ratio": float(left_neg),
                "right_pos_ratio": float(right_pos),
                "score": float(best_score),
                "reason": ("杯底+杯柄突破" if signal_mode == "breakout" else "杯底+杯柄临近突破"),
            }

    if best is None:
        return None, best_reason
    return best, "ok"


def main() -> None:
    parser = argparse.ArgumentParser(description="全市场扫描杯底/杯柄（Cup with Handle）形态（长信号）")
    parser.add_argument("--markets", type=str, default="all", help="扫描市场：all/sz/sh")
    parser.add_argument("--lookback-days", type=int, default=520, help="回看天数（用于获取日线 bars）")
    parser.add_argument("--chunk-size", type=int, default=60, help="批量处理大小（影响进度展示节奏）")
    parser.add_argument("--max-stocks", type=int, default=0, help="限制扫描股票数量（0 表示不限制）")
    parser.add_argument("--sleep-each", type=float, default=0.0, help="每只股票请求间隔秒数（降压用）")
    parser.add_argument("--target-date", type=str, default=None, help="指定检测日期（YYYYMMDD），不指定则取最新")
    parser.add_argument("--output", type=str, default=None, help="输出 CSV 文件路径（默认输出到脚本同目录）")
    parser.add_argument("--signal-mode", type=str, default="setup", choices=["setup", "breakout"], help="信号模式：setup 临近突破 / breakout 已突破")
    parser.add_argument("--setup-within-pct", type=float, default=0.015, help="setup 模式：距离杯柄高点允许回差比例（如 0.015=1.5%）")
    parser.add_argument("--min-score", type=float, default=0.0, help="最低得分过滤（0 表示不过滤）")
    parser.add_argument("--top-n", type=int, default=200, help="输出/保存 TopN（默认 200）")
    parser.add_argument("--no-progress", action="store_true", help="关闭扫描进度显示")
    parser.add_argument("--self-check", action="store_true", help="只做接口/字段自检，不跑全市场扫描")
    parser.add_argument("--self-check-samples", type=int, default=30, help="自检抽样数量")

    parser.add_argument("--cup-min-days", type=int, default=35)
    parser.add_argument("--cup-max-days", type=int, default=300)
    parser.add_argument("--min-cup-depth", type=float, default=0.08)
    parser.add_argument("--max-cup-depth", type=float, default=0.38)
    parser.add_argument("--rim-tolerance", type=float, default=0.10)
    parser.add_argument("--bottom-pos-min", type=float, default=0.25)
    parser.add_argument("--bottom-pos-max", type=float, default=0.75)

    parser.add_argument("--handle-min-days", type=int, default=3)
    parser.add_argument("--handle-max-days", type=int, default=35)
    parser.add_argument("--handle-pullback-min", type=float, default=0.0)
    parser.add_argument("--handle-pullback-max", type=float, default=0.25)
    parser.add_argument("--breakout-pct", type=float, default=0.0)
    parser.add_argument("--volume-ma-days", type=int, default=20)
    parser.add_argument("--breakout-vol-ratio", type=float, default=1.15)

    args = parser.parse_args()

    if not connected_endpoint():
        print("正在连接 pytdx 服务器...")
        try:
            test_df, status = _fetch_daily_bars(tdx, market=0, code="000001", count=10)
            if status != "ok" or test_df is None or test_df.empty:
                raise RuntimeError("无法获取测试数据")
            print(f"连接成功：{connected_endpoint()}")
        except Exception as e:
            print(f"错误：无法连接到 pytdx 服务器 - {e}")
            sys.exit(1)

    df_codes = get_all_a_share_codes()
    if df_codes is None or df_codes.empty:
        raise SystemExit("无法获取全市场股票列表（get_all_a_share_codes 为空）")

    markets = _parse_markets(str(args.markets))
    df_codes = df_codes[df_codes["market"].isin(markets)].copy()
    if df_codes.empty:
        raise SystemExit(f"markets={markets} 过滤后股票列表为空")

    if int(args.max_stocks or 0) > 0:
        df_codes = df_codes.head(int(args.max_stocks)).copy()

    stocks = [
        {"market": int(r["market"]), "code": str(r["code"]).zfill(6), "name": str(r.get("name") or "").strip()}
        for _, r in df_codes.iterrows()
    ]

    print(f"扫描市场：{['深市' if m == 0 else '沪市' for m in markets]}")
    print(f"股票数量：{len(stocks)}")

    if bool(args.self_check):
        n = max(1, min(int(args.self_check_samples), len(stocks)))
        print(f"开始自检：抽样 {n} 只，验证字段/时间顺序/数值合理性")
        ok = 0
        bad = 0
        for s in stocks[:n]:
            df, status = _fetch_daily_bars(tdx, market=int(s["market"]), code=str(s["code"]).zfill(6), count=int(min(200, args.lookback_days)))
            if status == "ok" and df is not None and not df.empty:
                ok += 1
            else:
                bad += 1
                print(f"  FAIL {s['market']}-{s['code']} status={status}")
        print(f"自检结果：OK {ok} / FAIL {bad}")
        return

    results: List[Dict] = []
    bad_stats: Dict[str, int] = {}
    t0 = time.time()
    done = 0
    total = len(stocks)

    with tdx:
        for batch in _chunks(stocks, int(args.chunk_size)):
            for s in batch:
                done += 1
                if not bool(args.no_progress) and (done % 50 == 0 or done == 1 or done == total):
                    elapsed = time.time() - t0
                    speed = done / max(1e-9, elapsed)
                    sys.stdout.write(f"\r进度 {done}/{total}  speed={speed:.1f} stk/s  found={len(results)}")
                    sys.stdout.flush()

                market = int(s["market"])
                code = str(s["code"]).zfill(6)
                name = str(s.get("name") or "")

                df, status = _fetch_daily_bars(tdx, market=market, code=code, count=int(args.lookback_days))
                if status != "ok" or df is None or df.empty:
                    bad_stats[status] = bad_stats.get(status, 0) + 1
                    if float(args.sleep_each) > 0:
                        time.sleep(float(args.sleep_each))
                    continue

                df2, st2 = _slice_asof_date(df, args.target_date)
                if st2 != "ok" or df2 is None or df2.empty:
                    bad_stats[st2] = bad_stats.get(st2, 0) + 1
                    if float(args.sleep_each) > 0:
                        time.sleep(float(args.sleep_each))
                    continue

                hit, reason = _cup_handle_signal(
                    df2,
                    signal_mode=str(args.signal_mode),
                    setup_within_pct=float(args.setup_within_pct),
                    cup_min_days=int(args.cup_min_days),
                    cup_max_days=int(args.cup_max_days),
                    min_cup_depth=float(args.min_cup_depth),
                    max_cup_depth=float(args.max_cup_depth),
                    rim_tolerance=float(args.rim_tolerance),
                    bottom_pos_min=float(args.bottom_pos_min),
                    bottom_pos_max=float(args.bottom_pos_max),
                    handle_min_days=int(args.handle_min_days),
                    handle_max_days=int(args.handle_max_days),
                    handle_pullback_min=float(args.handle_pullback_min),
                    handle_pullback_max=float(args.handle_pullback_max),
                    breakout_pct=float(args.breakout_pct),
                    volume_ma_days=int(args.volume_ma_days),
                    breakout_vol_ratio=float(args.breakout_vol_ratio),
                )
                if hit is None:
                    bad_stats[reason] = bad_stats.get(reason, 0) + 1
                    if float(args.sleep_each) > 0:
                        time.sleep(float(args.sleep_each))
                    continue

                row = {"market": market, "code": code, "name": name}
                row.update(hit)
                results.append(row)

                if float(args.sleep_each) > 0:
                    time.sleep(float(args.sleep_each))

    if not bool(args.no_progress):
        sys.stdout.write("\n")

    if bad_stats:
        bad_items = sorted(bad_stats.items(), key=lambda x: (-x[1], x[0]))[:12]
        print("无效原因Top：", "；".join([f"{k}={v}" for k, v in bad_items]))

    if not results:
        mode = str(args.signal_mode)
        if mode == "setup":
            print("未检测到符合条件的杯底/杯柄临近突破形态")
            print("建议：")
            print("  - 放宽 --setup-within-pct 或 --rim-tolerance")
            print("  - 放宽 --max-cup-depth 或 --handle-pullback-max")
        else:
            print("未检测到符合条件的杯底/杯柄突破形态")
            print("建议：")
            print("  - 放宽 --rim-tolerance 或 --max-cup-depth")
            print("  - 降低 --breakout-vol-ratio 或 --breakout-pct")
        print("建议：")
        return

    df_results = pd.DataFrame(results)
    if float(args.min_score) > 0 and "score" in df_results.columns:
        df_results = df_results[df_results["score"].astype(float) >= float(args.min_score)].copy()
    if df_results is None or df_results.empty:
        print("命中结果在 min_score 过滤后为空，可降低 --min-score")
        return
    df_results = df_results.sort_values("score", ascending=False).reset_index(drop=True)
    df_results = df_results.head(int(max(1, args.top_n))).copy()

    display_cols = [
        "market",
        "code",
        "name",
        "signal_mode",
        "rim_left_date",
        "rim_left_price",
        "bottom_date",
        "bottom_low",
        "rim_right_date",
        "rim_right_price",
        "handle_days",
        "handle_pullback",
        "handle_high",
        "breakout_date",
        "breakout_close",
        "dist_to_breakout",
        "breakout_vol_ratio",
        "cup_days",
        "cup_depth",
        "rim_diff",
        "score",
        "reason",
    ]
    available_cols = [c for c in display_cols if c in df_results.columns]
    print(f"\n检测结果（Top{int(max(1, args.top_n))}，按得分排序）:")
    print(df_results[available_cols].to_string(index=True, max_rows=60))

    output_path = str(args.output or "").strip() or None
    if not output_path:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(script_dir, f"杯底杯柄_{_now_ts()}.csv")
    df_results.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"\n结果已保存到：{output_path}")


if __name__ == "__main__":
    main()
