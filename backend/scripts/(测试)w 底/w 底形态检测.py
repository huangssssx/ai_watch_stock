"""
双底（W 底）形态检测脚本
========================

【功能概述】
扫描全市场 A 股股票，自动识别经典的双底（W 底）反转形态，帮助投资者发现潜在底部反转机会。

【形态定义】
双底（W 底）是经典的底部反转形态，由两个相近的低点和中间的反弹高点构成，形似字母"W"。
- 左底（L1）：下跌趋势中的第一个局部低点
- 右底（L2）：回调形成的第二个低点，与左底价差≤3%（默认），且不创新低
- 颈线（N）：两底之间反弹高点的水平连线，是关键压力位
- 突破：股价放量突破颈线，确认形态完成

【检测逻辑】
1. 识别局部低点：使用滑动窗口算法找出显著的底部（pivot_window=20）
2. 双底结构验证：
   - 右底与左底价差≤3%（可配置）
   - 两底间隔≥20 个交易日（约 4 周，可配置）
   - 右底不显著低于左底（在容差范围内）
3. 颈线确认：找出两底之间的反弹高点
4. 突破检测：收盘价突破颈线>3%（可配置）
5. 成交量验证：突破时成交量≥10 日均量的 1.5 倍（可配置）
6. 成交量结构：右底成交量通常应小于左底（缩量企稳）

【输出信息】
每个检测到的双底形态包含以下详细信息：
- 左底信息：日期、价格、索引位置
- 右底信息：日期、价格、索引位置
- 颈线信息：日期、价格
- 突破信息：日期、价格、成交量、成交量比率（相对均量）
- 形态高度：H = 颈线价 - 底部价
- 目标位测算：
  * 第一目标位：N + H（颈线 + 形态高度）
  * 第二目标位：N + 2H
- 止损位：右底下方 3%（L2 * 0.97）
- 成交量结构：右底区域均量 / 左底区域均量（理想情况≤0.8）
- 两底间隔：交易日天数
- 综合得分：基于形态标准度、成交量配合、时间间隔计算

【参数说明】
--markets: 扫描市场（all/sz/sh，默认 all）
--lookback-days: 回看天数，用于获取历史数据（默认 365）
--pivot-window: 识别枢轴点的窗口大小（默认 20，越大越严格）
--max-price-diff: 两底最大价格差异（默认 0.03 即 3%）
--min-days-between-bottoms: 两底之间最小交易日数（默认 20）
--breakout-threshold: 突破颈线的最小幅度（默认 0.03 即 3%）
--volume-ratio-threshold: 突破时成交量放大倍数（默认 1.5）
--chunk-size: 批量处理大小（默认 50）
--target-date: 指定检测日期（YYYYMMDD 格式），不指定则自动使用最近交易日
--output: 输出 CSV 文件路径（默认输出到脚本同目录）

【典型用法】

# 1. 默认扫描（全市场，最近交易日，标准参数）
python3 "backend/scripts/(测试)w 底/w 底形态检测.py"

# 2. 指定检测日期（回测历史某一天）
python3 "backend/scripts/(测试)w 底/w 底形态检测.py" --target-date 20250115

# 3. 更严格的形态要求（两底价差≤2%，间隔≥30 天，突破≥5%）
python3 "backend/scripts/(测试)w 底/w 底形态检测.py" --max-price-diff 0.02 --min-days-between-bottoms 30 --breakout-threshold 0.05

# 4. 放宽条件（适合捕捉早期信号或熊市）
python3 "backend/scripts/(测试)w 底/w 底形态检测.py" --max-price-diff 0.05 --min-days-between-bottoms 15 --breakout-threshold 0.02 --volume-ratio-threshold 1.3

# 5. 只扫描深市
python3 "backend/scripts/(测试)w 底/w 底形态检测.py" --markets sz

# 6. 指定输出文件
python3 "backend/scripts/(测试)w 底/w 底形态检测.py" --output /path/to/output.csv

# 7. 增加回看天数（检测更长期形态）
python3 "backend/scripts/(测试)w 底/w 底形态检测.py" --lookback-days 730 --pivot-window 30

【结果解读】
- volume_ratio（突破量比）：突破日成交量/前 10 日均量，越大越好（建议≥1.5）
- L2_L1_vol_ratio（右底/左底量比）：右底区域均量/左底区域均量，越小越好（建议≤0.8，表示缩量企稳）
- price_diff_ratio（两底价差）：越小表示形态越标准（建议≤3%）
- days_between_bottoms（间隔天数）：越长表示形态越可靠（建议≥20 天）
- score（综合得分）：综合考虑形态标准度、成交量、时间间隔，越高越好

【注意事项】
1. 双底形态需要突破颈线后才确认完成，未突破的仅为潜在形态
2. 突破后可能有回踩颈线的动作，这是二次入场机会
3. 在熊市中双底形态可靠性会降低，需结合大盘趋势判断
4. 右底明显低于左底（跌幅>5%）时，可能是下跌中继而非反转
5. 建议结合其他技术指标（如 MACD、RSI）和基本面综合判断
6. 本脚本仅作为辅助工具，不构成投资建议

【数据来源】
使用 pytdx 接口获取日线行情数据，扫描全市场 A 股股票。

【作者备注】
脚本基于经典技术分析理论实现，参数可根据实际市场环境调整优化。
"""

import argparse
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd


def _backend_dir() -> str:
    """自动定位 backend 目录"""
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


def _parse_markets(s: str) -> List[int]:
    """解析市场参数"""
    s = str(s or "").strip().lower()
    if s in {"sz", "0"}:
        return [0]
    if s in {"sh", "1"}:
        return [1]
    return [0, 1]


def _chunks(items: List, n: int):
    """分块处理"""
    n = max(1, int(n))
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _get_latest_trading_date() -> str:
    """获取最新的交易日日期"""
    today = datetime.now()
    try:
        df, status = _fetch_daily_bars(tdx, market=0, code="399001", count=5)
        if status == "ok" and df is not None and not df.empty:
            return df["datetime"].max().strftime("%Y%m%d")
    except Exception:
        pass
    return today.strftime("%Y%m%d")


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
    count: int = 500,
) -> Tuple[Optional[pd.DataFrame], str]:
    """获取日线数据"""
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


def _find_local_extrema(
    df: pd.DataFrame,
    price_col: str = "low",
    window: int = 20,
    extrema_type: str = "low",
) -> List[int]:
    """
    寻找局部极值点索引
    
    Args:
        df: 包含价格数据的 DataFrame
        price_col: 价格列名
        window: 判断局部极值的窗口大小
        extrema_type: 'low' 或 'high'
    
    Returns:
        局部极值点的索引列表
    """
    if df is None or df.empty:
        return []
    
    window = max(3, min(int(window), 60))
    prices = df[price_col].values
    
    extrema_indices = []
    for i in range(window // 2, len(prices) - window // 2):
        current = prices[i]
        left_window = prices[max(0, i - window // 2) : i]
        right_window = prices[i + 1 : min(len(prices), i + window // 2 + 1)]
        
        if extrema_type == "low":
            if len(left_window) > 0 and len(right_window) > 0:
                if current <= left_window.min() and current <= right_window.min():
                    extrema_indices.append(i)
        elif extrema_type == "high":
            if len(left_window) > 0 and len(right_window) > 0:
                if current >= left_window.max() and current >= right_window.max():
                    extrema_indices.append(i)
    
    return extrema_indices


def _detect_double_bottom(
    df: pd.DataFrame,
    pivot_window: int = 20,
    max_price_diff: float = 0.03,
    min_days_between_bottoms: int = 20,
    breakout_threshold: float = 0.03,
    volume_ratio_threshold: float = 1.5,
    volume_ma_window: int = 10,
    target_date: Optional[str] = None,
) -> Optional[Dict]:
    """
    检测双底形态
    
    Args:
        df: 包含日线数据的 DataFrame
        pivot_window: 识别枢轴点的窗口大小
        max_price_diff: 两底最大价格差异（默认 3%）
        min_days_between_bottoms: 两底之间最小交易日数
        breakout_threshold: 突破颈线的最小幅度（默认 3%）
        volume_ratio_threshold: 突破时成交量放大倍数（默认 1.5 倍）
        volume_ma_window: 成交量均线窗口
        target_date: 指定检测日期（YYYYMMDD 格式），如果为 None 则使用数据最新日期
    
    Returns:
        包含形态信息的字典，如果未检测到则返回 None
    """
    if df is None or df.empty or len(df) < 60:
        return None
    
    required_cols = ["datetime", "open", "close", "high", "low", "vol"]
    if not all(col in df.columns for col in required_cols):
        return None
    
    df = df.copy().reset_index(drop=True)
    
    # 如果指定了 target_date，则只检测到该日期为止的数据
    if target_date is not None:
        try:
            target_dt = pd.to_datetime(target_date, format="%Y%m%d")
            df = df[df["datetime"] <= target_dt].reset_index(drop=True)
            if df.empty:
                return None
        except Exception:
            pass
    
    # 1. 识别局部低点
    low_indices = _find_local_extrema(df, price_col="low", window=pivot_window, extrema_type="low")
    
    if len(low_indices) < 2:
        return None
    
    candidate_lows = low_indices[-10:] if len(low_indices) > 10 else low_indices
    
    for j in range(1, len(candidate_lows)):
        L1_idx = candidate_lows[j - 1]
        L2_idx = candidate_lows[j]
        
        # 检查时间间隔
        days_between = L2_idx - L1_idx
        if days_between < min_days_between_bottoms:
            continue
        
        L1_price = df.loc[L1_idx, "low"]
        L2_price = df.loc[L2_idx, "low"]
        
        # 4. 检查右底是否不创新低（价格差异在允许范围内）
        price_diff_ratio = abs(L2_price - L1_price) / L1_price
        if price_diff_ratio > max_price_diff:
            continue
        
        # 右底略高于左底更优，但允许相等或略低（在容差范围内）
        if L2_price < L1_price * (1 - max_price_diff):
            continue
        
        # 5. 识别颈线（两底之间的反弹高点）
        neckline_region = df.loc[L1_idx : L2_idx + 1]
        if neckline_region.empty:
            continue
        
        N_price = float(neckline_region["high"].max())
        N_idx = int(neckline_region["high"].idxmax())
        
        # 确保颈线在两底之间
        if N_idx <= L1_idx or N_idx >= L2_idx:
            continue
        
        # 6. 检查突破条件
        post_L2 = df[df.index > L2_idx].copy()
        if post_L2.empty:
            continue
        
        breakout_level = N_price * (1 + breakout_threshold)
        breakout_mask = post_L2["close"] > breakout_level
        
        if not breakout_mask.any():
            continue
        
        breakout_indices = post_L2[breakout_mask].index.tolist()
        breakout_idx = breakout_indices[0]
        breakout_date = df.loc[breakout_idx, "datetime"]
        
        # 7. 检查突破时成交量
        vol_start = max(0, breakout_idx - volume_ma_window)
        vol_region = df.loc[vol_start : breakout_idx - 1, "vol"]
        
        if vol_region.empty or vol_region.mean() == 0:
            continue
        
        avg_volume = float(vol_region.mean())
        breakout_volume = float(df.loc[breakout_idx, "vol"])
        
        if breakout_volume < avg_volume * volume_ratio_threshold:
            continue
        
        # 8. 计算形态高度
        bottom_price = max(L1_price, L2_price)  # 取较高的底部作为基准
        H = N_price - bottom_price
        
        # 9. 计算目标位
        target_1 = N_price + H
        target_2 = N_price + 2 * H
        
        # 10. 计算止损位（右底下方 3-5%）
        stop_loss = L2_price * 0.97
        
        # 11. 计算右底相对左底的成交量比例
        L1_vol_start = max(0, L1_idx - 5)
        L1_vol_end = min(len(df), L1_idx + 6)
        L2_vol_start = max(0, L2_idx - 5)
        L2_vol_end = min(len(df), L2_idx + 6)
        
        L1_avg_vol = df.loc[L1_vol_start:L1_vol_end, "vol"].mean() if L1_vol_start <= L1_vol_end else 0
        L2_avg_vol = df.loc[L2_vol_start:L2_vol_end, "vol"].mean() if L2_vol_start <= L2_vol_end else 0
        
        vol_ratio = L2_avg_vol / L1_avg_vol if L1_avg_vol > 0 else 1.0
        
        return {
            "L1_idx": int(L1_idx),
            "L1_price": float(L1_price),
            "L1_date": df.loc[L1_idx, "datetime"],
            "L2_idx": int(L2_idx),
            "L2_price": float(L2_price),
            "L2_date": df.loc[L2_idx, "datetime"],
            "N_idx": int(N_idx),
            "N_price": float(N_price),
            "N_date": df.loc[N_idx, "datetime"],
            "breakout_idx": int(breakout_idx),
            "breakout_price": float(df.loc[breakout_idx, "close"]),
            "breakout_date": breakout_date,
            "breakout_volume": float(breakout_volume),
            "avg_volume": float(avg_volume),
            "volume_ratio": float(breakout_volume / avg_volume) if avg_volume > 0 else 0,
            "H": float(H),
            "target_1": float(target_1),
            "target_2": float(target_2),
            "stop_loss": float(stop_loss),
            "price_diff_ratio": float(price_diff_ratio),
            "L2_L1_vol_ratio": float(vol_ratio),
            "days_between_bottoms": int(days_between),
            "signal": "double_bottom",
        }
    
    for j in range(len(candidate_lows) - 1, 0, -1):
        L2_idx = candidate_lows[j]
        for i in range(j - 1, -1, -1):
            L1_idx = candidate_lows[i]

            days_between = L2_idx - L1_idx
            if days_between < min_days_between_bottoms:
                continue

            L1_price = df.loc[L1_idx, "low"]
            L2_price = df.loc[L2_idx, "low"]

            price_diff_ratio = abs(L2_price - L1_price) / L1_price
            if price_diff_ratio > max_price_diff:
                continue

            if L2_price < L1_price * (1 - max_price_diff):
                continue

            neckline_region = df.loc[L1_idx : L2_idx + 1]
            if neckline_region.empty:
                continue

            N_price = float(neckline_region["high"].max())
            N_idx = int(neckline_region["high"].idxmax())

            if N_idx <= L1_idx or N_idx >= L2_idx:
                continue

            post_L2 = df[df.index > L2_idx].copy()
            if post_L2.empty:
                continue

            breakout_level = N_price * (1 + breakout_threshold)
            breakout_mask = post_L2["close"] > breakout_level

            if not breakout_mask.any():
                continue

            breakout_indices = post_L2[breakout_mask].index.tolist()
            breakout_idx = breakout_indices[0]
            breakout_date = df.loc[breakout_idx, "datetime"]

            vol_start = max(0, breakout_idx - volume_ma_window)
            vol_region = df.loc[vol_start : breakout_idx - 1, "vol"]

            if vol_region.empty or vol_region.mean() == 0:
                continue

            avg_volume = float(vol_region.mean())
            breakout_volume = float(df.loc[breakout_idx, "vol"])

            if breakout_volume < avg_volume * volume_ratio_threshold:
                continue

            bottom_price = max(L1_price, L2_price)
            H = N_price - bottom_price

            target_1 = N_price + H
            target_2 = N_price + 2 * H

            stop_loss = L2_price * 0.97

            L1_vol_start = max(0, L1_idx - 5)
            L1_vol_end = min(len(df), L1_idx + 6)
            L2_vol_start = max(0, L2_idx - 5)
            L2_vol_end = min(len(df), L2_idx + 6)

            L1_avg_vol = df.loc[L1_vol_start:L1_vol_end, "vol"].mean() if L1_vol_start <= L1_vol_end else 0
            L2_avg_vol = df.loc[L2_vol_start:L2_vol_end, "vol"].mean() if L2_vol_start <= L2_vol_end else 0

            vol_ratio = L2_avg_vol / L1_avg_vol if L1_avg_vol > 0 else 1.0

            return {
                "L1_idx": int(L1_idx),
                "L1_price": float(L1_price),
                "L1_date": df.loc[L1_idx, "datetime"],
                "L2_idx": int(L2_idx),
                "L2_price": float(L2_price),
                "L2_date": df.loc[L2_idx, "datetime"],
                "N_idx": int(N_idx),
                "N_price": float(N_price),
                "N_date": df.loc[N_idx, "datetime"],
                "breakout_idx": int(breakout_idx),
                "breakout_price": float(df.loc[breakout_idx, "close"]),
                "breakout_date": breakout_date,
                "breakout_volume": float(breakout_volume),
                "avg_volume": float(avg_volume),
                "volume_ratio": float(breakout_volume / avg_volume) if avg_volume > 0 else 0,
                "H": float(H),
                "target_1": float(target_1),
                "target_2": float(target_2),
                "stop_loss": float(stop_loss),
                "price_diff_ratio": float(price_diff_ratio),
                "L2_L1_vol_ratio": float(vol_ratio),
                "days_between_bottoms": int(days_between),
                "signal": "double_bottom",
            }

    return None


def _scan_market(
    stocks: List[Dict],
    pivot_window: int = 20,
    max_price_diff: float = 0.03,
    min_days_between_bottoms: int = 20,
    breakout_threshold: float = 0.03,
    volume_ratio_threshold: float = 1.5,
    lookback_days: int = 365,
    chunk_size: int = 50,
    target_date: Optional[str] = None,
    show_progress: bool = True,
) -> Tuple[List[Dict], Dict[str, int]]:
    """扫描市场中的双底形态"""
    results = []
    total = len(stocks)
    processed = 0
    started_at = time.time()
    stats: Dict[str, int] = {
        "total": int(total),
        "empty_or_invalid_daily_bars": 0,
        "scanned_ok": 0,
        "signals": 0,
    }
    bad_reason_counts: Dict[str, int] = {}
    
    for idxs in _chunks(list(range(len(stocks))), chunk_size):
        chunk_stocks = [stocks[i] for i in idxs]
        
        for stock in chunk_stocks:
            market = int(stock["market"])
            code = str(stock["code"]).zfill(6)
            name = str(stock.get("name", ""))
            
            # 获取日线数据
            df, status = _fetch_daily_bars(tdx, market=market, code=code, count=lookback_days)
            
            if status != "ok" or df is None or df.empty:
                stats["empty_or_invalid_daily_bars"] += 1
                key = str(status).split(":", 2)[1] if ":" in str(status) else str(status)
                bad_reason_counts[key] = int(bad_reason_counts.get(key, 0)) + 1
                processed += 1
                if show_progress and total > 0:
                    elapsed = time.time() - started_at
                    speed = processed / elapsed if elapsed > 0 else 0
                    percent = processed * 100 / total
                    sys.stdout.write(
                        f"\r进度 {processed}/{total} ({percent:5.1f}%) 命中 {len(results)} 速度 {speed:5.1f}只/s 当前 {code} {name[:10]}"
                    )
                    sys.stdout.flush()
                continue
            
            stats["scanned_ok"] += 1
            
            # 检测双底形态
            signal = _detect_double_bottom(
                df,
                pivot_window=pivot_window,
                max_price_diff=max_price_diff,
                min_days_between_bottoms=min_days_between_bottoms,
                breakout_threshold=breakout_threshold,
                volume_ratio_threshold=volume_ratio_threshold,
                target_date=target_date,
            )
            
            if signal:
                results.append({
                    "market": market,
                    "code": code,
                    "name": name,
                    **signal,
                })
                stats["signals"] += 1

            processed += 1
            if show_progress and total > 0:
                elapsed = time.time() - started_at
                speed = processed / elapsed if elapsed > 0 else 0
                percent = processed * 100 / total
                sys.stdout.write(
                    f"\r进度 {processed}/{total} ({percent:5.1f}%) 命中 {len(results)} 速度 {speed:5.1f}只/s 当前 {code} {name[:10]}"
                )
                sys.stdout.flush()
    
    if show_progress:
        sys.stdout.write("\n")
        sys.stdout.flush()
    if bad_reason_counts:
        top = sorted(bad_reason_counts.items(), key=lambda x: x[1], reverse=True)[:6]
        stats["bad_reasons_top"] = 1
        for k, v in top:
            stats[f"bad_reason:{k}"] = int(v)
    return results, stats


def main():
    parser = argparse.ArgumentParser(description="双底（W 底）形态检测脚本")
    
    parser.add_argument(
        "--markets",
        type=str,
        default="all",
        help="市场：all, sz, sh",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=365,
        help="回看天数（默认 365，用于获取足够的历史数据）",
    )
    parser.add_argument(
        "--pivot-window",
        type=int,
        default=20,
        help="识别枢轴点的窗口大小（默认 20）",
    )
    parser.add_argument(
        "--max-price-diff",
        type=float,
        default=0.03,
        help="两底最大价格差异（默认 0.03 即百分之三）",
    )
    parser.add_argument(
        "--min-days-between-bottoms",
        type=int,
        default=20,
        help="两底之间最小交易日数（默认 20，约 4 周）",
    )
    parser.add_argument(
        "--breakout-threshold",
        type=float,
        default=0.03,
        help="突破颈线的最小幅度（默认 0.03 即百分之三）",
    )
    parser.add_argument(
        "--volume-ratio-threshold",
        type=float,
        default=1.5,
        help="突破时成交量放大倍数（默认 1.5）",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=50,
        help="批量处理大小（默认 50）",
    )
    parser.add_argument(
        "--target-date",
        type=str,
        default=None,
        help="指定检测日期（YYYYMMDD 格式），不指定则默认为最近一个交易日",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="输出 CSV 文件路径（默认输出到脚本同目录）",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="关闭扫描进度显示",
    )
    parser.add_argument(
        "--self-check",
        action="store_true",
        help="仅做接口/数据结构自检，不跑全市场形态扫描",
    )
    parser.add_argument(
        "--self-check-samples",
        type=int,
        default=30,
        help="自检抽样数量（默认 30）",
    )
    
    args = parser.parse_args()
    
    # 检查并建立连接
    if not connected_endpoint():
        print("正在连接 pytdx 服务器...")
        try:
            test_df, status = _fetch_daily_bars(tdx, market=0, code="000001", count=5)
            if status != "ok" or test_df is None or test_df.empty:
                raise RuntimeError("无法获取测试数据")
            print(f"连接成功：{connected_endpoint()}")
        except Exception as e:
            print(f"错误：无法连接到 pytdx 服务器 - {e}")
            print("建议：")
            print("  1. 检查网络连接")
            print("  2. 稍后再试（服务器可能暂时不可用）")
            sys.exit(1)
    
    # 确定检测日期
    if args.target_date:
        target_date = str(args.target_date).strip()
        try:
            datetime.strptime(target_date, "%Y%m%d")
        except ValueError:
            print(f"错误：日期格式不正确，请使用 YYYYMMDD 格式（例如：20250115）")
            sys.exit(1)
        print(f"检测日期：{target_date}（用户指定）")
    else:
        target_date = _get_latest_trading_date()
        print(f"检测日期：{target_date}（最近交易日）")
    
    print(f"开始扫描双底形态...")
    print(f"参数设置:")
    print(f"  - 回看天数：{args.lookback_days}")
    print(f"  - 枢轴窗口：{args.pivot_window}")
    print(f"  - 两底最大价差：{args.max_price_diff*100}%")
    print(f"  - 两底最小间隔：{args.min_days_between_bottoms} 天")
    print(f"  - 突破阈值：{args.breakout_threshold*100}%")
    print(f"  - 成交量放大倍数：{args.volume_ratio_threshold}x")
    
    # 获取股票列表
    markets = _parse_markets(args.markets)
    all_codes = get_all_a_share_codes()
    if all_codes is None or getattr(all_codes, "empty", True):
        raise SystemExit("股票列表为空：get_all_a_share_codes() 未返回有效数据")
    for col in ("market", "code"):
        if col not in all_codes.columns:
            raise SystemExit(f"股票列表缺少字段: {col}")
    
    # 根据市场过滤
    stocks = []
    for m in markets:
        filtered = all_codes[all_codes["market"] == m]
        for _, row in filtered.iterrows():
            stocks.append({
                "market": int(row["market"]),
                "code": str(row["code"]).zfill(6),
                "name": str(row.get("name", "")),
            })
    
    print(f"扫描市场：{['深市' if m == 0 else '沪市' for m in markets]}")
    print(f"股票数量：{len(stocks)}")

    if args.self_check:
        n = max(1, min(int(args.self_check_samples), len(stocks)))
        print(f"\n开始自检：抽样 {n} 只，验证字段/时间顺序/数值合理性")
        bad = 0
        ok = 0
        for stock in stocks[:n]:
            market = int(stock["market"])
            code = str(stock["code"]).zfill(6)
            df = None
            try:
                bars = tdx.get_security_bars(9, int(market), str(code).zfill(6), 0, int(min(200, args.lookback_days)))
                df0 = tdx.to_df(bars) if bars else pd.DataFrame()
                df, status = _normalize_and_validate_daily_bars_df(df0, market=market, code=code)
            except Exception:
                df, status = None, "exception"
            if df is None or status != "ok":
                bad += 1
                print(f"  FAIL {market}-{code} status={status}")
            else:
                ok += 1
        print(f"\n自检结果：OK {ok} / FAIL {bad}")
        return
    
    # 扫描市场
    results, stats = _scan_market(
        stocks,
        pivot_window=args.pivot_window,
        max_price_diff=args.max_price_diff,
        min_days_between_bottoms=args.min_days_between_bottoms,
        breakout_threshold=args.breakout_threshold,
        volume_ratio_threshold=args.volume_ratio_threshold,
        lookback_days=args.lookback_days,
        chunk_size=args.chunk_size,
        target_date=target_date,
        show_progress=not bool(args.no_progress),
    )
    
    print(f"\n检测到 {len(results)} 个双底形态")
    print(
        f"数据质量统计：总{stats.get('total', 0)} "
        f"有效{stats.get('scanned_ok', 0)} "
        f"无效/空{stats.get('empty_or_invalid_daily_bars', 0)}"
    )
    bad_items = [(k.split(":", 1)[1], int(v)) for k, v in stats.items() if str(k).startswith("bad_reason:")]
    bad_items = sorted(bad_items, key=lambda x: x[1], reverse=True)[:6]
    if bad_items:
        print("无效原因Top：", "；".join([f"{k}={v}" for k, v in bad_items]))
    
    if results:
        # 转换为 DataFrame
        df_results = pd.DataFrame(results)
        
        # 排序：优先显示形态更标准的（价格差异小、成交量放大明显）
        df_results["score"] = (
            (1 - df_results["price_diff_ratio"]) * 50 +  # 价格差异越小越好
            (df_results["volume_ratio"] / df_results["volume_ratio"].max()) * 30 +  # 成交量放大越明显越好
            (df_results["days_between_bottoms"] / df_results["days_between_bottoms"].max()) * 20  # 间隔越长越好
        )
        
        df_results = df_results.sort_values("score", ascending=False).reset_index(drop=True)
        
        # 显示关键信息
        display_cols = [
            "market", "code", "name",
            "L1_date", "L1_price", "L2_date", "L2_price",
            "N_price", "breakout_date", "breakout_price",
            "volume_ratio", "L2_L1_vol_ratio", "days_between_bottoms",
            "target_1", "target_2", "stop_loss", "score"
        ]
        
        available_cols = [c for c in display_cols if c in df_results.columns]
        print("\n检测结果（按得分排序）:")
        print(df_results[available_cols].to_string(index=True, max_rows=50))
        
        # 保存结果
        output_path = args.output
        if not output_path:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(script_dir, f"w 底形态_{ts}.csv")
        
        df_results.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"\n结果已保存到：{output_path}")
    else:
        print("\n未检测到符合条件的双底形态")
        print("建议调整参数：")
        print("  - 减小 --min-days-between-bottoms（降低时间间隔要求）")
        print("  - 增大 --max-price-diff（放宽价格差异限制）")
        print("  - 减小 --breakout-threshold（降低突破要求）")
        print("  - 减小 --volume-ratio-threshold（降低成交量要求）")


if __name__ == "__main__":
    main()
