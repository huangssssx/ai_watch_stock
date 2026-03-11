"""
涨停回马枪（最近一交易日拐点筛选）

## 脚本逻辑概述

本脚本用于筛选符合"涨停回马枪"形态的股票，核心闭环：**涨停打底 + 缩量洗盘 + 放量重启**

### 筛选流程

1. **找涨停日**：从目标日期往前推，在最近N天内寻找涨停日（支持收盘涨停/触板两种模式）
2. **缩量回调**：涨停后缩量回调N天（默认3-5天），回调期间需满足：
   - 成交量萎缩（回调期最大量能 < 参考量能的35%）
   - 回调期不跌破"涨停启动位"（涨停日前一天收盘价）
   - 回调期间有足够下跌（至少2天下跌，回调幅度 >= 2%）
3. **拐点确认**：最近一个交易日（target_date）出现放量启动：
   - 收盘价 > 开盘价（收阳线）
   - 收盘涨幅 > 0.5%
   - 成交量放大（相对回调均量 >= 1.5倍）
4. **形态识别**：对符合条件的股票识别8种形态分类

### 支持识别的形态

**3种基础核心形态（按回调深度）：**
- 踩头型：回调全程不跌破涨停收盘价（极致强势）
- 踩腰型：回调不跌破涨停实体1/2位置（标准经典）
- 踩底型：回调至涨停开盘价附近（极限稳健）

**5种实战经典形态（按K线特征）：**
- 单阳不破型：回调K线始终在涨停实体内部运行
- 长下影探底型：回调期间出现长下影线探底
- 双阴洗盘型：连续2天下跌且量能持续萎缩
- 缺口不补型：跳空涨停后回调不回补缺口
- 平台整理型：回调振幅 < 5%，横盘整理

### 输出字段

- symbol: 股票代码
- name: 股票名称
- ts_code: 完整代码（600000.SH）
- trade_date: 目标交易日
- limitup_date: 涨停日
- pullback_days: 回调天数
- score: 综合评分
- reason: 形态评语（如"踩腰型+单阳不破型：涨停后缩量回调3天...）
- patterns_core: 基础核心形态
- patterns_practical: 实战经典形态
- 及其他技术指标字段...

形态要点（可通过参数调节）：
1) 先出现涨停（收盘封板或盘中触板）
2) 随后缩量回调 N 天（默认 3-5 天）
3) 回调期不破"涨停启动位"（默认用涨停日的前收作为启动位）
4) 最近一个交易日出现拐点：放量转强、收盘上攻（可要求突破回调期最高价）
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

from utils.pytdx_client import connected_endpoint, tdx
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


def _get_latest_trading_date() -> str:
    today = datetime.now().strftime("%Y%m%d")
    try:
        df, st = _fetch_daily_bars(market=0, code="399001", count=10)
        if st == "ok" and df is not None and not df.empty:
            return str(df["trade_date"].iloc[-1])
    except Exception:
        pass
    return today


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


def _to_ts_code(market: int, code: str) -> str:
    code = str(code or "").zfill(6)
    if int(market) == 1:
        return f"{code}.SH"
    return f"{code}.SZ"


def _recognize_patterns(
    pb_days: int,
    limit_idx: int,
    limit_open: float,
    limit_close: float,
    prev_close: float,
    df: pd.DataFrame,
    pb_start: int,
    pb_end: int,
    pb_close_max: float,
    pb_low_min: float,
    pb_high_max: float,
    pb_vol: pd.Series,
) -> Tuple[List[str], List[str]]:
    patterns_core = []
    patterns实战 = []
    
    if limit_close <= 0 or limit_open <= 0:
        return patterns_core, patterns实战
    
    limit_body = limit_close - limit_open
    limit_mid = limit_open + limit_body * 0.5
    
    prev_c = float(prev_close) if pd.notna(prev_close) else 0.0
    has_gap = limit_open > prev_c
    
    pb_lows = df["low"].iloc[pb_start : pb_end + 1]
    pb_highs = df["high"].iloc[pb_start : pb_end + 1]
    pb_closes = df["close"].iloc[pb_start : pb_end + 1]
    pb_opens = df["open"].iloc[pb_start : pb_end + 1]
    
    if pb_low_min >= limit_close * 0.998:
        patterns_core.append("踩头型")
    elif pb_low_min >= limit_mid:
        patterns_core.append("踩腰型")
    elif pb_low_min >= limit_open * 0.998:
        patterns_core.append("踩底型")
    
    if pb_close_max <= limit_close * 1.01 and pb_low_min >= limit_mid:
        count_in_body = 0
        for i in range(len(pb_closes)):
            c = float(pb_closes.iloc[i])
            o = float(pb_opens.iloc[i])
            if o >= limit_open and c <= limit_close:
                count_in_body += 1
        if count_in_body >= pb_days - 1:
            patterns实战.append("单阳不破型")
    
    if has_gap:
        gap_held = True
        for i in range(len(pb_lows)):
            if float(pb_lows.iloc[i]) <= prev_c:
                gap_held = False
                break
        if gap_held:
            patterns实战.append("缺口不补型")
    
    max_shadow = 0.0
    for i in range(len(pb_lows)):
        c = float(pb_closes.iloc[i])
        o = float(pb_opens.iloc[i])
        l = float(pb_lows.iloc[i])
        h = float(pb_highs.iloc[i])
        body = abs(c - o)
        shadow_low = min(c, o) - l
        if shadow_low > max_shadow:
            max_shadow = shadow_low
    if max_shadow > limit_body * 0.5 and max_shadow > limit_close * 0.015:
        patterns实战.append("长下影探底型")
    
    if pb_days >= 2:
        down_count = 0
        for i in range(1, len(pb_closes)):
            if float(pb_closes.iloc[i]) < float(pb_closes.iloc[i-1]):
                down_count += 1
        if down_count >= 2:
            vol_shrinking = True
            for i in range(1, len(pb_vol)):
                if float(pb_vol.iloc[i]) >= float(pb_vol.iloc[i-1]):
                    vol_shrinking = False
                    break
            if vol_shrinking:
                patterns实战.append("双阴洗盘型")
    
    range_pct = (pb_high_max - pb_low_min) / limit_close
    if range_pct < 0.05:
        patterns实战.append("平台整理型")
    
    return patterns_core, patterns实战


def _eval_one(
    market: int,
    code: str,
    name: str,
    target_date: str,
    lookback_days: int,
    leader_lookback: int,
    pullback_days_min: int,
    pullback_days_max: int,
    limitup_mode: str,
    limitup_touch_slack: float,
    limitup_close_slack: float,
    ref_vol_window: int,
    pullback_vol_ratio_max: float,
    pullback_close_max_ratio: float,
    pullback_high_max_ratio: float,
    pullback_down_days_min: int,
    pullback_drop_pct_min: float,
    support_break_pct: float,
    pivot_vol_ratio_min: float,
    pivot_ref_vol_ratio_min: float,
    pivot_breakout_pct: float,
    pivot_close_rise_pct_min: float,
) -> Tuple[Optional[Dict], str]:
    if not bool(name):
        name = ""
    if "ST" in str(name).upper():
        return None, "skip_st"

    df, st = _fetch_daily_bars(market=int(market), code=str(code).zfill(6), count=int(lookback_days))
    if st != "ok" or df is None or df.empty:
        return None, f"bad_daily:{st}"

    target_date = str(target_date).strip()
    df = df[df["trade_date"] <= target_date].reset_index(drop=True)
    if df is None or df.empty:
        return None, "no_bars_before_target"
    if str(df["trade_date"].iloc[-1]) != target_date:
        return None, "target_not_latest_bar"
    if len(df) < 30:
        return None, "too_short"

    close = pd.to_numeric(df["close"], errors="coerce")
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    open_ = pd.to_numeric(df["open"], errors="coerce")
    vol = pd.to_numeric(df["vol"], errors="coerce").fillna(0.0)
    prev_close = close.shift(1)

    is_limit_any = pd.Series(False, index=df.index)
    for i in range(1, len(df)):
        pc = float(prev_close.iloc[i]) if pd.notna(prev_close.iloc[i]) else 0.0
        c = float(close.iloc[i]) if pd.notna(close.iloc[i]) else 0.0
        h = float(high.iloc[i]) if pd.notna(high.iloc[i]) else 0.0
        if _is_limit_up_day(pc, c, h, code=str(code), mode="touch", touch_slack=float(limitup_touch_slack), close_slack=float(limitup_close_slack)):
            is_limit_any.iloc[i] = True
            continue
        if _is_limit_up_day(pc, c, h, code=str(code), mode="close", touch_slack=float(limitup_touch_slack), close_slack=float(limitup_close_slack)):
            is_limit_any.iloc[i] = True

    pivot_idx = int(len(df) - 1)
    prev_idx = pivot_idx - 1
    if prev_idx < 0:
        return None, "no_prev_day"

    best: Optional[Dict] = None
    best_score = -1e18
    best_reason = "no_match"

    for pb_days in range(int(pullback_days_min), int(pullback_days_max) + 1):
        limit_idx = pivot_idx - (int(pb_days) + 1)
        if limit_idx <= 0:
            continue
        if (pivot_idx - limit_idx) > int(leader_lookback) + int(pb_days) + 1:
            continue

        if not _is_limit_up_day(
            float(prev_close.iloc[limit_idx]),
            float(close.iloc[limit_idx]),
            float(high.iloc[limit_idx]),
            code=str(code),
            mode=str(limitup_mode),
            touch_slack=float(limitup_touch_slack),
            close_slack=float(limitup_close_slack),
        ):
            continue

        pb_start = int(limit_idx) + 1
        pb_end = int(pivot_idx) - 1
        if pb_end < pb_start:
            continue
        if int(pb_end - pb_start + 1) != int(pb_days):
            continue
        if pb_start >= len(is_limit_any):
            continue
        if bool(is_limit_any.iloc[pb_start]):
            best_reason = "pullback_starts_with_limitup"
            continue
        if bool(is_limit_any.iloc[pb_start : pb_end + 1].any()):
            best_reason = "pullback_has_limitup"
            continue

        w = max(1, int(ref_vol_window))
        ref_start = max(0, int(limit_idx) - (w - 1))
        ref_vol = float(vol.iloc[ref_start : int(limit_idx) + 1].max())
        if ref_vol <= 0:
            best_reason = "ref_vol_zero"
            continue

        pb_vol = vol.iloc[pb_start : pb_end + 1]
        pb_max_vol = float(pb_vol.max()) if not pb_vol.empty else 0.0
        pb_vol_ma = float(pb_vol.mean()) if not pb_vol.empty else 0.0
        if pb_max_vol / ref_vol > float(pullback_vol_ratio_max):
            best_reason = "pullback_not_shrink"
            continue
        if pb_vol_ma <= 0:
            best_reason = "pullback_vol_zero"
            continue

        limit_open = float(open_.iloc[int(limit_idx)])
        limit_start_price = float(prev_close.iloc[int(limit_idx)]) if pd.notna(prev_close.iloc[int(limit_idx)]) else None
        if limit_start_price is None or limit_start_price <= 0:
            limit_start_price = limit_open

        limit_close = float(close.iloc[int(limit_idx)])
        if not (limit_close > 0):
            best_reason = "limit_close_bad"
            continue

        pb_close = close.iloc[pb_start : pb_end + 1]
        pb_high = high.iloc[pb_start : pb_end + 1]
        pb_low = low.iloc[pb_start : pb_end + 1]
        if pb_close.isna().any() or pb_high.isna().any() or pb_low.isna().any():
            best_reason = "pullback_na"
            continue
        pb_close_max = float(pb_close.max()) if not pb_close.empty else 0.0
        pb_high_max = float(pb_high.max()) if not pb_high.empty else 0.0
        pb_low_min = float(pb_low.min()) if not pb_low.empty else 0.0
        if pb_close_max > limit_close * float(pullback_close_max_ratio):
            best_reason = "pullback_price_too_high"
            continue
        if pb_high_max > limit_close * float(pullback_high_max_ratio):
            best_reason = "pullback_high_too_high"
            continue
        if limit_close > 0:
            drop_pct = (limit_close - pb_low_min) / limit_close
        else:
            drop_pct = 0.0
        if drop_pct < float(pullback_drop_pct_min):
            best_reason = "pullback_drop_too_small"
            continue
        down_days = int((pb_close.diff() < 0).sum())
        if down_days < int(pullback_down_days_min):
            best_reason = "pullback_not_down_enough"
            continue

        low_min_total = float(low.iloc[pb_start : pivot_idx + 1].min())
        if low_min_total < float(limit_start_price) * (1 - float(support_break_pct)):
            best_reason = "break_limit_start"
            continue

        pivot_o = float(open_.iloc[pivot_idx])
        pivot_c = float(close.iloc[pivot_idx])
        pivot_h = float(high.iloc[pivot_idx])
        pivot_l = float(low.iloc[pivot_idx])
        pivot_v = float(vol.iloc[pivot_idx])

        prev_c = float(close.iloc[prev_idx])
        if pivot_c <= pivot_o:
            best_reason = "pivot_not_bullish"
            continue
        if pivot_c <= prev_c * (1 + float(pivot_close_rise_pct_min)):
            best_reason = "pivot_close_not_rise"
            continue

        if pivot_c < pb_high_max * (1 + float(pivot_breakout_pct)):
            best_reason = "pivot_not_breakout"
            continue

        pivot_vol_ratio_pb = pivot_v / pb_vol_ma
        if pivot_vol_ratio_pb < float(pivot_vol_ratio_min):
            best_reason = "pivot_not_expand"
            continue
        pivot_vol_ratio_ref = pivot_v / ref_vol
        if pivot_vol_ratio_ref < float(pivot_ref_vol_ratio_min):
            best_reason = "pivot_vs_ref_too_small"
            continue

        pivot_close_pct = (pivot_c / prev_c - 1) if prev_c > 0 else 0.0
        breakout_pct = (pivot_c / pb_high_max - 1) if pb_high_max > 0 else 0.0
        hold_depth_pct = (low_min_total / limit_start_price - 1) if limit_start_price > 0 else 0.0

        score = 0.0
        score += float(pivot_vol_ratio_pb) * 25.0
        score += float(pivot_vol_ratio_ref) * 10.0
        score += float(pivot_close_pct) * 1000.0
        score += float(breakout_pct) * 800.0
        score += float(hold_depth_pct) * 400.0
        score += max(0.0, 6.0 - float(pb_days)) * 5.0

        limit_idx_for_pattern = int(limit_idx)
        patterns_core, patterns实战 = _recognize_patterns(
            pb_days=int(pb_days),
            limit_idx=limit_idx_for_pattern,
            limit_open=limit_open,
            limit_close=limit_close,
            prev_close=prev_close.iloc[limit_idx_for_pattern],
            df=df,
            pb_start=pb_start,
            pb_end=pb_end,
            pb_close_max=pb_close_max,
            pb_low_min=pb_low_min,
            pb_high_max=pb_high_max,
            pb_vol=pb_vol,
        )

        all_patterns = patterns_core + patterns实战
        if all_patterns:
            pattern_str = "、".join(all_patterns)
            reason = f"{pattern_str}：涨停后缩量回调{pb_days}天，{target_date}放量转强"
        else:
            reason = f"涨停后缩量回调{pb_days}天，{target_date}放量转强"

        out = {
            "symbol": str(code).zfill(6),
            "name": str(name),
            "ts_code": _to_ts_code(market=int(market), code=str(code)),
            "trade_date": str(target_date),
            "limitup_date": str(df['trade_date'].iloc[int(limit_idx)]),
            "pullback_days": int(pb_days),
            "score": round(float(score), 3),
            "reason": reason,
            "patterns_core": "、".join(patterns_core) if patterns_core else "",
            "patterns_practical": "、".join(patterns实战) if patterns实战 else "",
            "close": round(float(pivot_c), 3),
            "open": round(float(pivot_o), 3),
            "high": round(float(pivot_h), 3),
            "low": round(float(pivot_l), 3),
            "vol": round(float(pivot_v), 3),
            "limit_start_price": round(float(limit_start_price), 3),
            "limit_close": round(float(limit_close), 3),
            "pullback_low_min": round(float(low_min_total), 3),
            "pullback_max_vol_ratio": round(float(pb_max_vol / ref_vol), 3),
            "pullback_close_max_ratio": round(float(pb_close_max / limit_close), 4) if limit_close > 0 else None,
            "pullback_high_max_ratio": round(float(pb_high_max / limit_close), 4) if limit_close > 0 else None,
            "pullback_drop_pct": round(float(drop_pct), 4),
            "pullback_down_days": int(down_days),
            "pivot_vol_ratio_pullback": round(float(pivot_vol_ratio_pb), 3),
            "pivot_vol_ratio_ref": round(float(pivot_vol_ratio_ref), 3),
            "pivot_close_pct": round(float(pivot_close_pct), 4),
            "pivot_breakout_pct": round(float(breakout_pct), 4),
        }
        if float(score) > float(best_score):
            best = out
            best_score = float(score)
            best_reason = "ok"

    return best, best_reason


def main():
    parser = argparse.ArgumentParser(description="涨停回马枪：筛选最近一交易日出现拐点的股票")
    parser.add_argument("--markets", type=str, default="all", help="扫描市场：all/sz/sh")
    parser.add_argument("--lookback-days", type=int, default=160, help="获取日线条数（默认 160）")
    parser.add_argument("--chunk-size", type=int, default=60, help="批量处理大小（默认 60）")
    parser.add_argument("--max-stocks", type=int, default=0, help="限制扫描股票数量（0 表示不限制）")
    parser.add_argument("--target-date", type=str, default=None, help="指定检测日期（YYYYMMDD，默认最近交易日）")
    parser.add_argument("--output", type=str, default=None, help="输出 CSV 文件路径（默认输出到脚本同目录）")
    parser.add_argument("--top-n", type=int, default=200, help="输出/保存 TopN（默认 200）")

    parser.add_argument("--leader-lookback", type=int, default=12, help="涨停必须出现在最近N个交易日内（默认 12）")
    parser.add_argument("--pullback-days-min", type=int, default=3, help="缩量回调天数下限（默认 3）")
    parser.add_argument("--pullback-days-max", type=int, default=5, help="缩量回调天数上限（默认 5）")
    parser.add_argument("--limitup-mode", type=str, default="close", choices=["close", "touch"], help="涨停识别：close/touch")
    parser.add_argument("--limitup-touch-slack", type=float, default=0.004, help="touch 模式容差（默认 0.004）")
    parser.add_argument("--limitup-close-slack", type=float, default=0.006, help="close 模式容差（默认 0.006）")
    parser.add_argument("--ref-vol-window", type=int, default=3, help="缩量参考量能窗口（默认 3）")
    parser.add_argument("--pullback-vol-ratio-max", type=float, default=0.35, help="回调期最大量能/参考量能（默认 0.35）")
    parser.add_argument("--pullback-close-max-ratio", type=float, default=1.03, help="回调期最高收盘/涨停收盘上限（默认 1.03）")
    parser.add_argument("--pullback-high-max-ratio", type=float, default=1.06, help="回调期最高最高价/涨停收盘上限（默认 1.06）")
    parser.add_argument("--pullback-down-days-min", type=int, default=2, help="回调期下跌天数下限（默认 2）")
    parser.add_argument("--pullback-drop-pct-min", type=float, default=0.02, help="回调期相对涨停收盘最低回撤比例（默认 2%）")
    parser.add_argument("--support-break-pct", type=float, default=0.0, help="回调期跌破启动位允许比例（默认 0）")

    parser.add_argument("--pivot-vol-ratio-min", type=float, default=1.50, help="拐点日放量倍数（相对回调均量，默认 1.50）")
    parser.add_argument("--pivot-ref-vol-ratio-min", type=float, default=0.80, help="拐点日量能/参考量能下限（默认 0.80）")
    parser.add_argument("--pivot-breakout-pct", type=float, default=0.0, help="拐点日收盘需突破回调高点比例（默认 0）")
    parser.add_argument("--pivot-close-rise-pct-min", type=float, default=0.005, help="拐点日收盘涨幅下限（默认 0.5%）")
    parser.add_argument("--include-st", action="store_true", help="包含 ST（默认剔除）")

    args = parser.parse_args()

    if not connected_endpoint():
        try:
            _ = tdx.get_security_bars(9, 0, "000001", 0, 2)
        except Exception as e:
            raise SystemExit(f"pytdx 连接失败：{e}")

    markets = _parse_markets(args.markets)
    df_codes = get_all_a_share_codes()
    if df_codes is None or getattr(df_codes, "empty", True):
        raise SystemExit("股票列表为空")
    df_codes = df_codes[df_codes["market"].isin(markets)].copy().reset_index(drop=True)
    if not bool(args.include_st) and "name" in df_codes.columns:
        df_codes = df_codes[~df_codes["name"].astype(str).str.upper().str.contains("ST", na=False)].copy()
    if int(args.max_stocks) > 0:
        df_codes = df_codes.head(int(args.max_stocks)).copy()

    target_date = str(args.target_date).strip() if args.target_date else _get_latest_trading_date()
    print(f"{_now_ts()} 目标交易日: {target_date}", flush=True)
    print(f"{_now_ts()} 股票池: {len(df_codes)} | markets={markets}", flush=True)

    stocks = list(df_codes[["market", "code", "name"]].itertuples(index=False, name=None))
    t0 = time.perf_counter()
    results: List[Dict] = []
    bad: Dict[str, int] = {}

    for chunk in _chunks(stocks, int(args.chunk_size)):
        for market, code, name in chunk:
            out, st = _eval_one(
                market=int(market),
                code=str(code).zfill(6),
                name=str(name or ""),
                target_date=str(target_date),
                lookback_days=int(args.lookback_days),
                leader_lookback=int(args.leader_lookback),
                pullback_days_min=int(args.pullback_days_min),
                pullback_days_max=int(args.pullback_days_max),
                limitup_mode=str(args.limitup_mode),
                limitup_touch_slack=float(args.limitup_touch_slack),
                limitup_close_slack=float(args.limitup_close_slack),
                ref_vol_window=int(args.ref_vol_window),
                pullback_vol_ratio_max=float(args.pullback_vol_ratio_max),
                pullback_close_max_ratio=float(args.pullback_close_max_ratio),
                pullback_high_max_ratio=float(args.pullback_high_max_ratio),
                pullback_down_days_min=int(args.pullback_down_days_min),
                pullback_drop_pct_min=float(args.pullback_drop_pct_min),
                support_break_pct=float(args.support_break_pct),
                pivot_vol_ratio_min=float(args.pivot_vol_ratio_min),
                pivot_ref_vol_ratio_min=float(args.pivot_ref_vol_ratio_min),
                pivot_breakout_pct=float(args.pivot_breakout_pct),
                pivot_close_rise_pct_min=float(args.pivot_close_rise_pct_min),
            )
            if out is not None and st == "ok":
                results.append(out)
            else:
                bad[str(st)] = int(bad.get(str(st), 0)) + 1

        done = len(results) + sum(bad.values())
        if done % 300 == 0 or done == len(stocks):
            elapsed = time.perf_counter() - t0
            print(f"进度 {done}/{len(stocks)} | 命中 {len(results)} | 耗时 {elapsed:.1f}s", end="\r")

    print("")
    df_out = pd.DataFrame(results)
    if not df_out.empty:
        df_out = df_out.sort_values(
            by=["score", "pivot_vol_ratio_pullback", "pullback_max_vol_ratio"],
            ascending=[False, False, True],
        )
        df_show = df_out.head(max(1, int(args.top_n))).copy()
        print(df_show.to_string(index=False, max_rows=60))
    else:
        print("未命中。失败原因分布 Top10：")
        top = sorted(bad.items(), key=lambda x: x[1], reverse=True)[:10]
        for k, v in top:
            print(f"  {k}: {v}")

    output_path = args.output
    if not output_path:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        ts = _now_ts()
        output_path = os.path.join(script_dir, f"涨停回马枪_{target_date}_{ts}.csv")
    df_out.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"结果已保存到：{output_path}")
    print(f"总耗时：{time.perf_counter() - t0:.2f}s | 结果数：{len(df_out)}")


if __name__ == "__main__":
    main()
