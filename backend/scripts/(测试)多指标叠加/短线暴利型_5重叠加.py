"""
短线暴利型（5 重叠加）选股脚本

核心逻辑：龙头回踩 + 量能反转 + 形态止跌 + 支撑确认 + 指标共振
适用：偏短持 1-5 天的“回马枪/洗盘后二次启动”候选筛选。

输出：
- 控制台打印命中结果（按 score 降序）
- 同目录生成 CSV（或用 --output 指定路径）

两套常用命令（宽 / 严）

1) 严格版（更“原教旨”：强调收盘接近涨停，候选更少但更硬）
python3 "backend/scripts/(测试)多指标叠加/短线暴利型_5重叠加.py" \
  --chunk-size 80 --top-n 50 \
  --limitup-mode close \
  --signal-lookback 5 \
  --pullback-days-min 3 --pullback-days-max 5 \
  --pullback-vol-ratio-max 0.30 --ref-vol-window 3 \
  --stop-vol-ratio-min 1.50 \
  --support-mode both --support-break-pct 0.00 --ma20-break-pct 0.00 \
  --rsi-prev-max 40 --rsi-now-min 50 --macd-rise-days 2

2) 宽松版（用于全市场“出候选”：触及涨停也算，便于凑满样本做复盘）
python3 "backend/scripts/(测试)多指标叠加/短线暴利型_5重叠加.py" \
  --chunk-size 80 --top-n 50 \
  --limitup-mode touch \
  --signal-lookback 10 \
  --pullback-days-min 2 --pullback-days-max 10 \
  --pullback-vol-ratio-max 0.90 --ref-vol-window 10 \
  --stop-vol-ratio-min 1.10 \
  --support-mode either --support-break-pct 0.02 --ma20-break-pct 0.02 \
  --rsi-prev-max 55 --rsi-now-min 45 --macd-rise-days 1

参数速查（只列核心项）
- --markets: 扫描市场 all/sz/sh
- --lookback-days: 拉取日线条数（用于指标与形态计算）
- --signal-lookback: 在最近 N 个交易日内寻找“止跌日”（最后满足 5 层的那一天）
- --leader-lookback: 龙头层窗口（止跌日前 N 日内需要出现一次涨停/触板）
- --limitup-mode: 涨停识别方式
  - close：收盘价接近涨停（更严格，偏“真封板”）
  - touch：最高价触及涨停附近（更宽松，包含“冲板/炸板”）
- --limitup-touch-slack / --limitup-close-slack: 涨停识别容差（用于对抗涨停价四舍五入/精度误差；越大越宽松）
- --pullback-days-min/max: 回调天数范围（从涨停/触板后到止跌日）
- --ref-vol-window: 缩量参考窗口（参考量能取“涨停/触板日及前 N-1 日”的最大量）
- --pullback-vol-ratio-max: 回调期最大量 / 参考量 的上限（越小越严格）
- --stop-vol-ratio-min: 止跌日放量倍数下限（相对回调末段均量，越大越严格）
- --support-mode: 支撑判定方式
  - either：不破“启动位”或不破 MA20（二选一）
  - both：两者都不破（更严格）
- --support-break-pct / --ma20-break-pct: 支撑允许下破比例
- --rsi-prev-max / --rsi-now-min: RSI 回调曾弱 + 止跌转强
- --macd-rise-days: MACD 绿柱连续缩短天数
"""

import argparse
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from utils.pytdx_client import tdx, connected_endpoint
from utils.stock_codes import get_all_a_share_codes


def _chunks(items: List, n: int):
    n = max(1, int(n))
    for i in range(0, len(items), n):
        yield items[i : i + n]

def _limit_up_pct(code: str) -> float:
    code = str(code or "").zfill(6)
    if code.startswith(("300", "301", "688")):
        return 0.195
    return 0.095


def _normalize_daily_df(df0: pd.DataFrame, market: int, code: str) -> Tuple[Optional[pd.DataFrame], str]:
    sid = f"{int(market)}-{str(code).zfill(6)}"
    if df0 is None or df0.empty:
        return None, f"{sid}:empty"

    df = df0.copy()
    rename_map = {}
    if "datetime" not in df.columns:
        for c in ("date", "day", "trade_date"):
            if c in df.columns:
                rename_map[c] = "datetime"
                break
    if rename_map:
        df.rename(columns=rename_map, inplace=True)

    required = ["datetime", "open", "close", "high", "low", "vol"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        return None, f"{sid}:missing:{','.join(missing)}"

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    if df["datetime"].isna().all():
        return None, f"{sid}:bad_datetime"

    for c in ["open", "close", "high", "low", "vol"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["datetime", "open", "close", "high", "low"]).copy()
    if df.empty:
        return None, f"{sid}:empty_after_clean"

    df = df.sort_values("datetime", ascending=True).reset_index(drop=True)
    if not df["datetime"].is_monotonic_increasing:
        return None, f"{sid}:datetime_not_monotonic"

    bad_price = (df["high"] < df["low"]) | (df["high"] <= 0) | (df["low"] <= 0)
    if bad_price.any():
        df = df.loc[~bad_price].reset_index(drop=True)
    if df.empty:
        return None, f"{sid}:empty_after_filters"

    df["trade_date"] = df["datetime"].dt.strftime("%Y%m%d")
    return df, "ok"


def _fetch_daily_bars(market: int, code: str, count: int) -> Tuple[Optional[pd.DataFrame], str]:
    sid = f"{int(market)}-{str(code).zfill(6)}"
    try:
        bars = tdx.get_security_bars(9, int(market), str(code).zfill(6), 0, int(count))
    except Exception as e:
        return None, f"{sid}:get_security_bars_exception:{type(e).__name__}"
    if not bars:
        return None, f"{sid}:bars_empty"
    df0 = tdx.to_df(bars) if bars else pd.DataFrame()
    return _normalize_daily_df(df0, market=int(market), code=str(code).zfill(6))


def _ema(series: pd.Series, span: int) -> pd.Series:
    span = max(1, int(span))
    return series.ewm(span=span, adjust=False).mean()


def _macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
    close = pd.to_numeric(close, errors="coerce")
    dif = _ema(close, fast) - _ema(close, slow)
    dea = _ema(dif, signal)
    hist = dif - dea
    return dif, dea, hist


def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    close = pd.to_numeric(close, errors="coerce")
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / max(1, int(period)), adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / max(1, int(period)), adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    return pd.to_numeric(rsi, errors="coerce")


def _is_limit_up_day(
    prev_close: float,
    close: float,
    high: float,
    code: str,
    mode: str,
    touch_slack: float,
    close_slack: float,
) -> bool:
    if prev_close is None or prev_close <= 0:
        return False
    pct = _limit_up_pct(code)
    limit_price = prev_close * (1 + pct)
    touch_ok = bool(high >= limit_price * (1 - float(touch_slack)))
    close_ok = bool(close >= limit_price * (1 - float(close_slack)))
    if str(mode).lower().strip() == "touch":
        return touch_ok
    return close_ok


def _bullish_engulfing(prev_o: float, prev_c: float, cur_o: float, cur_c: float) -> bool:
    if any(pd.isna(x) for x in [prev_o, prev_c, cur_o, cur_c]):
        return False
    if not (prev_c < prev_o and cur_c > cur_o):
        return False
    return bool(cur_o <= prev_c and cur_c >= prev_o)


def _needle_bottom(o: float, c: float, h: float, l: float) -> bool:
    if any(pd.isna(x) for x in [o, c, h, l]):
        return False
    if h <= l:
        return False
    body = abs(c - o)
    upper = h - max(o, c)
    lower = min(o, c) - l
    rng = h - l
    if body <= 0:
        body = rng * 0.01
    return bool(lower >= body * 2.0 and upper <= body * 0.8 and (lower / rng) >= 0.55)


def _eval_one(
    market: int,
    code: str,
    name: str,
    lookback_days: int,
    target_date: Optional[str],
    signal_lookback: int,
    leader_lookback: int,
    limitup_mode: str,
    limitup_touch_slack: float,
    limitup_close_slack: float,
    ref_vol_window: int,
    pullback_days_min: int,
    pullback_days_max: int,
    pullback_vol_ratio_max: float,
    stop_vol_ratio_min: float,
    support_mode: str,
    support_break_pct: float,
    ma20_break_pct: float,
    rsi_prev_max: float,
    rsi_now_min: float,
    macd_rise_days: int,
) -> Tuple[Optional[Dict], str]:
    if "ST" in str(name).upper():
        return None, "skip_st"

    df, status = _fetch_daily_bars(market=market, code=code, count=lookback_days)
    if status != "ok" or df is None or df.empty:
        return None, f"bad_daily:{status}"

    if target_date:
        try:
            dt = pd.to_datetime(str(target_date), format="%Y%m%d", errors="coerce")
            if pd.notna(dt):
                df = df[df["datetime"] <= dt].reset_index(drop=True)
        except Exception:
            pass
    if df is None or df.empty or len(df) < 60:
        return None, "too_short"

    close = pd.to_numeric(df["close"], errors="coerce")
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    open_ = pd.to_numeric(df["open"], errors="coerce")
    vol = pd.to_numeric(df["vol"], errors="coerce").fillna(0.0)
    prev_close = close.shift(1)

    ma20 = close.rolling(20, min_periods=20).mean()
    rsi14 = _rsi(close, period=14)
    _, _, macd_hist = _macd(close, fast=12, slow=26, signal=9)

    def _stage(reason: str) -> int:
        m = {
            "no_limitup_in_window": 1,
            "pullback_len_bad": 2,
            "ref_vol_zero": 2,
            "pullback_not_shrink": 3,
            "break_limit_start": 4,
            "ma20_na": 4,
            "break_ma20": 4,
            "pb_vol_ma_zero": 5,
            "no_stopday_volume": 5,
            "no_prev_day": 5,
            "not_stop_fall": 6,
            "kline_not_match": 7,
            "rsi_na": 8,
            "rsi_not_rebound": 8,
            "rsi_never_oversold": 8,
            "macd_na": 9,
            "macd_not_green": 9,
            "macd_not_shorten": 9,
        }
        return int(m.get(str(reason), 0))

    def _eval_at(signal_idx: int) -> Tuple[Optional[Dict], str]:
        pullback_ok = False
        chosen_pullback_days = None
        limit_idx = None
        for pb in range(int(pullback_days_min), int(pullback_days_max) + 1):
            i = int(signal_idx) - int(pb)
            if i <= 0:
                continue
            lb = max(1, int(leader_lookback))
            if i < int(signal_idx) - lb:
                continue
            if _is_limit_up_day(
                float(prev_close.iloc[i]),
                float(close.iloc[i]),
                float(high.iloc[i]),
                code=code,
                mode=str(limitup_mode),
                touch_slack=float(limitup_touch_slack),
                close_slack=float(limitup_close_slack),
            ):
                pullback_ok = True
                chosen_pullback_days = int(pb)
                limit_idx = int(i)
                break
        if not pullback_ok or limit_idx is None or chosen_pullback_days is None:
            return None, "no_limitup_in_window"

        pb_start = int(limit_idx) + 1
        pb_end = int(signal_idx)
        if pb_end < pb_start:
            return None, "pullback_len_bad"

        pullback_df = df.iloc[pb_start : pb_end + 1].copy()
        if len(pullback_df) != int(chosen_pullback_days):
            return None, "pullback_len_bad"

        w = max(1, int(ref_vol_window))
        ref_start = max(0, int(limit_idx) - (w - 1))
        ref_vol = float(vol.iloc[ref_start : int(limit_idx) + 1].max())
        if ref_vol <= 0:
            return None, "ref_vol_zero"
        pb_max_vol = float(pd.to_numeric(pullback_df["vol"], errors="coerce").fillna(0.0).max())
        pb_max_vol_ratio = pb_max_vol / ref_vol
        if pb_max_vol_ratio > float(pullback_vol_ratio_max):
            return None, "pullback_not_shrink"

        limit_open = float(open_.iloc[int(limit_idx)])
        limit_start_price = float(prev_close.iloc[int(limit_idx)]) if pd.notna(prev_close.iloc[int(limit_idx)]) else None
        if limit_start_price is None or limit_start_price <= 0:
            limit_start_price = limit_open
        low_min = float(low.iloc[pb_start : int(signal_idx) + 1].min())
        ma20_signal = float(ma20.iloc[int(signal_idx)]) if pd.notna(ma20.iloc[int(signal_idx)]) else None
        if ma20_signal is None or ma20_signal <= 0:
            return None, "ma20_na"

        hold_limit = bool(low_min >= limit_start_price * (1 - float(support_break_pct)))
        hold_ma20 = bool(low_min >= ma20_signal * (1 - float(ma20_break_pct)))
        sm = str(support_mode or "either").lower().strip()
        if sm == "both":
            if not (hold_limit and hold_ma20):
                if not hold_limit:
                    return None, "break_limit_start"
                return None, "break_ma20"
        else:
            if not (hold_limit or hold_ma20):
                return None, "break_support"

        pb_vol_ma = float(pd.to_numeric(pullback_df["vol"], errors="coerce").fillna(0.0).tail(5).mean())
        if pb_vol_ma <= 0:
            return None, "pb_vol_ma_zero"
        signal_vol = float(vol.iloc[int(signal_idx)])
        base_vol_region = pd.to_numeric(vol.iloc[max(pb_start, int(signal_idx) - 5) : int(signal_idx)], errors="coerce").dropna()
        base_vol = float(base_vol_region.mean()) if not base_vol_region.empty else pb_vol_ma
        if base_vol <= 0:
            base_vol = pb_vol_ma
        signal_vol_ratio = signal_vol / base_vol
        if signal_vol_ratio < float(stop_vol_ratio_min):
            return None, "no_stopday_volume"

        if int(signal_idx) - 1 < 0:
            return None, "no_prev_day"
        prev_row = df.iloc[int(signal_idx) - 1]
        cur_row = df.iloc[int(signal_idx)]
        prev_o = float(prev_row["open"])
        prev_c = float(prev_row["close"])
        cur_o = float(cur_row["open"])
        cur_c = float(cur_row["close"])
        cur_h = float(cur_row["high"])
        cur_l = float(cur_row["low"])

        if not (cur_c >= prev_c and cur_c >= cur_o):
            return None, "not_stop_fall"

        k1 = _bullish_engulfing(prev_o, prev_c, cur_o, cur_c)
        k2 = _needle_bottom(cur_o, cur_c, cur_h, cur_l)
        if not (k1 or k2):
            return None, "kline_not_match"

        rsi_now = float(rsi14.iloc[int(signal_idx)]) if pd.notna(rsi14.iloc[int(signal_idx)]) else None
        if rsi_now is None:
            return None, "rsi_na"
        if rsi_now < float(rsi_now_min):
            return None, "rsi_not_rebound"
        rsi_pullback_min = pd.to_numeric(rsi14.iloc[pb_start : pb_end + 1], errors="coerce").min()
        if pd.isna(rsi_pullback_min) or float(rsi_pullback_min) > float(rsi_prev_max):
            return None, "rsi_never_oversold"

        macd_tail = pd.to_numeric(
            macd_hist.iloc[int(signal_idx) - max(2, int(macd_rise_days)) : int(signal_idx) + 1],
            errors="coerce",
        )
        macd_tail = macd_tail.dropna()
        if len(macd_tail) < (int(macd_rise_days) + 1):
            return None, "macd_na"
        if float(macd_hist.iloc[int(signal_idx)]) >= 0:
            return None, "macd_not_green"
        rising = True
        vals = macd_tail.values.tolist()
        for i in range(1, len(vals)):
            if not (vals[i] > vals[i - 1]):
                rising = False
                break
        if not rising:
            return None, "macd_not_shorten"

        score = 0.0
        score += min(40.0, max(0.0, (signal_vol_ratio - float(stop_vol_ratio_min)) * 15.0))
        score += min(30.0, max(0.0, (rsi_now - float(rsi_now_min)) * 1.2))
        score += min(
            30.0,
            max(
                0.0,
                (abs(float(macd_hist.iloc[int(signal_idx) - 1])) - abs(float(macd_hist.iloc[int(signal_idx)]))) * 500.0,
            ),
        )

        reason_parts = [
            f"近10日涨停={df.iloc[int(limit_idx)]['trade_date']}",
            f"回调{int(chosen_pullback_days)}天缩量(MaxVol/参考Vol={pb_max_vol_ratio:.2f})",
            f"止跌日放量(Vol/回调均量={signal_vol_ratio:.2f})",
            "形态=阳包阴" if k1 else "形态=单针探底",
            f"MACD绿柱缩短({int(macd_rise_days)}连升)",
            f"RSI {rsi_pullback_min:.1f}->{rsi_now:.1f}",
        ]

        return {
            "symbol": str(code).zfill(6),
            "name": str(name),
            "score": round(float(score), 2),
            "reason": "；".join(reason_parts)[:200],
            "signal_date": str(df.iloc[int(signal_idx)]["trade_date"]),
            "limitup_date": str(df.iloc[int(limit_idx)]["trade_date"]),
            "pullback_days": int(chosen_pullback_days),
            "limitup_open": round(limit_open, 3),
            "limitup_start": round(float(limit_start_price), 3),
            "pullback_low_min": round(low_min, 3),
            "ma20": round(ma20_signal, 3),
            "pullback_max_vol_ratio": round(pb_max_vol_ratio, 3),
            "stopday_vol_ratio": round(signal_vol_ratio, 3),
            "hold_limit_start": int(1 if hold_limit else 0),
            "hold_ma20": int(1 if hold_ma20 else 0),
            "rsi_now": round(rsi_now, 2),
            "rsi_pullback_min": round(float(rsi_pullback_min), 2),
            "macd_hist": round(float(macd_hist.iloc[int(signal_idx)]), 6),
        }, "ok"

    end_idx = int(len(df) - 1)
    if end_idx < 30:
        return None, "too_short"
    lb = max(1, int(signal_lookback))
    start_idx = max(30, end_idx - (lb - 1))

    best_fail = "no_limitup_in_window"
    best_stage = _stage(best_fail)
    for signal_idx in range(end_idx, start_idx - 1, -1):
        out, st = _eval_at(signal_idx)
        if out is not None and st == "ok":
            return out, "ok"
        s = _stage(str(st))
        if s > best_stage:
            best_stage = s
            best_fail = str(st)
    return None, best_fail


def main():
    parser = argparse.ArgumentParser(description="短线暴利型（5重叠加）：龙头回踩 + 量能反转 + 形态止跌 + 支撑 + 指标")
    parser.add_argument("--markets", type=str, default="all", help="扫描市场：all/sz/sh")
    parser.add_argument("--lookback-days", type=int, default=180, help="获取日线条数（默认 180）")
    parser.add_argument("--chunk-size", type=int, default=60, help="批量处理大小（默认 60）")
    parser.add_argument("--max-stocks", type=int, default=0, help="限制扫描股票数量（0 表示不限制）")
    parser.add_argument("--target-date", type=str, default=None, help="指定检测日期（YYYYMMDD）")
    parser.add_argument("--output", type=str, default=None, help="输出 CSV 文件路径（默认输出到脚本同目录）")
    parser.add_argument("--top-n", type=int, default=200, help="输出/保存 TopN（默认 200）")

    parser.add_argument("--signal-lookback", type=int, default=5, help="在最近N个交易日内寻找止跌日（默认 5）")
    parser.add_argument("--leader-lookback", type=int, default=10, help="龙头层：涨停需出现在最近N个交易日内（默认 10）")
    parser.add_argument("--limitup-mode", type=str, default="close", choices=["close", "touch"], help="涨停识别：close=收盘接近涨停；touch=最高触及涨停")
    parser.add_argument("--limitup-touch-slack", type=float, default=0.004, help="touch 模式容差（默认 0.004）")
    parser.add_argument("--limitup-close-slack", type=float, default=0.006, help="close 模式容差（默认 0.006）")
    parser.add_argument("--ref-vol-window", type=int, default=3, help="缩量参考量能窗口（默认 3=涨停日及前2日最大量）")
    parser.add_argument("--pullback-days-min", type=int, default=3, help="回调天数下限（默认 3）")
    parser.add_argument("--pullback-days-max", type=int, default=5, help="回调天数上限（默认 5）")
    parser.add_argument("--pullback-vol-ratio-max", type=float, default=0.30, help="回调期最大量能/涨停日量能（默认 0.30）")
    parser.add_argument("--stop-vol-ratio-min", type=float, default=1.50, help="止跌日放量倍数(相对回调均量，默认 1.50)")
    parser.add_argument("--support-mode", type=str, default="either", choices=["either", "both"], help="支撑判定：either=不破启动位或MA20；both=两者都不破")
    parser.add_argument("--support-break-pct", type=float, default=0.00, help="回踩涨停启动位允许跌破比例（默认 0）")
    parser.add_argument("--ma20-break-pct", type=float, default=0.00, help="回踩 MA20 允许跌破比例（默认 0）")
    parser.add_argument("--rsi-prev-max", type=float, default=40.0, help="回调期 RSI 必须曾<=该值（默认 40）")
    parser.add_argument("--rsi-now-min", type=float, default=50.0, help="止跌日 RSI 必须>=该值（默认 50）")
    parser.add_argument("--macd-rise-days", type=int, default=2, help="止跌日前 MACD 绿柱需连续缩短天数（默认 2）")

    args = parser.parse_args()

    if not connected_endpoint():
        try:
            _ = tdx.get_security_bars(9, 0, "000001", 0, 2)
        except Exception as e:
            raise SystemExit(f"pytdx 连接失败：{e}")

    markets = []
    mk = str(args.markets or "all").lower().strip()
    if mk == "all":
        markets = [0, 1]
    elif mk == "sz":
        markets = [0]
    elif mk == "sh":
        markets = [1]
    else:
        raise SystemExit("markets 仅支持 all/sz/sh")

    df_codes = get_all_a_share_codes()
    if df_codes is None or df_codes.empty:
        raise SystemExit("股票列表为空")
    df_codes = df_codes[df_codes["market"].isin(markets)].copy().reset_index(drop=True)
    if int(args.max_stocks) > 0:
        df_codes = df_codes.head(int(args.max_stocks)).copy()

    stocks = list(df_codes[["market", "code", "name"]].itertuples(index=False, name=None))
    t0 = time.perf_counter()
    results: List[Dict] = []
    stats: Dict[str, int] = {"total": len(stocks), "ok": 0}
    bad: Dict[str, int] = {}

    for chunk in _chunks(stocks, int(args.chunk_size)):
        for market, code, name in chunk:
            out, st = _eval_one(
                market=int(market),
                code=str(code).zfill(6),
                name=str(name or ""),
                lookback_days=int(args.lookback_days),
                target_date=str(args.target_date).strip() if args.target_date else None,
                signal_lookback=int(args.signal_lookback),
                leader_lookback=int(args.leader_lookback),
                limitup_mode=str(args.limitup_mode),
                limitup_touch_slack=float(args.limitup_touch_slack),
                limitup_close_slack=float(args.limitup_close_slack),
                ref_vol_window=int(args.ref_vol_window),
                pullback_days_min=int(args.pullback_days_min),
                pullback_days_max=int(args.pullback_days_max),
                pullback_vol_ratio_max=float(args.pullback_vol_ratio_max),
                stop_vol_ratio_min=float(args.stop_vol_ratio_min),
                support_mode=str(args.support_mode),
                support_break_pct=float(args.support_break_pct),
                ma20_break_pct=float(args.ma20_break_pct),
                rsi_prev_max=float(args.rsi_prev_max),
                rsi_now_min=float(args.rsi_now_min),
                macd_rise_days=int(args.macd_rise_days),
            )
            if out is not None and st == "ok":
                results.append(out)
                stats["ok"] = int(stats.get("ok", 0)) + 1
            else:
                key = str(st)
                bad[key] = int(bad.get(key, 0)) + 1

        done = int(stats.get("ok", 0)) + sum(bad.values())
        if done % 300 == 0 or done == len(stocks):
            elapsed = time.perf_counter() - t0
            print(f"进度 {done}/{len(stocks)} | 命中 {len(results)} | 耗时 {elapsed:.1f}s", end="\r")

    print("")

    df_out = pd.DataFrame(results)
    if not df_out.empty:
        df_out = df_out.sort_values(by=["score", "stopday_vol_ratio", "pullback_max_vol_ratio"], ascending=[False, False, True])
        top_n = max(1, int(args.top_n))
        df_show = df_out.head(top_n).copy()
        print(df_show.to_string(index=False, max_rows=50))
    else:
        print("未命中。失败原因分布 Top10：")
        top = sorted(bad.items(), key=lambda x: x[1], reverse=True)[:10]
        for k, v in top:
            print(f"  {k}: {v}")

    output_path = args.output
    if not output_path:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(script_dir, f"短线暴利型_5重叠加_{ts}.csv")
    df_out.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"结果已保存到：{output_path}")
    print(f"总耗时：{time.perf_counter() - t0:.2f}s | 结果数：{len(df_out)}")


if __name__ == "__main__":
    main()
