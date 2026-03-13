"""
早盘 V 型反转策略：捕捉开盘半小时挖坑后快速拉升的强主力信号
支持多种变种策略，适应不同市场环境和个股风格

核心逻辑：主力通过打压制造恐慌，再快速吸筹/拉升并守住阵地

变种策略：
1. standard（标准V型反转）：前30分钟跌幅后反弹站上开盘价并站稳
2. deep_v（急跌深V型）：更严格的跌幅要求，更短时间内反弹
3. volume_validated（量能验证策略）：结合成交量分析，反弹时成交量放大
4. sector_sync（板块联动型）：板块同步V型反转，个股跟随且相对强势
5. breakout（横盘突破型）：横盘整理后挖坑突破
6. double_dip（二次回踩型）：二次回踩清洗浮筹后反弹
7. market_sync（大盘同步型）：大盘同步V型反转，个股跟随且相对强势

典型用法（盘中任意时刻运行，asof_time 默认取当前时间）：

# 标准策略 - 适用场景：震荡市场，捕捉常规的早盘洗盘后拉升机会
# 推荐使用时间：09:45-10:30（此时已能观察到前30分钟走势）
# 默认同时检测: standard(标准)、deep_v(急跌深V)、volume_validated(量能验证)
python3 "backend/scripts/(高胜率)早盘 V 型反转/早盘 V 型反转.py"

# 复盘模式 - 适用场景：回测历史数据，分析特定时间点的策略表现
# 复盘 20260311 10:09 的行情
python3 "backend/scripts/(高胜率)早盘 V 型反转/早盘 V 型反转.py" --replay-datetime 20260311_10:09

# 复盘模式 + 调整站稳时间 - 更早发现信号
python3 "backend/scripts/(高胜率)早盘 V 型反转/早盘 V 型反转.py" --replay-datetime 20260311_10:09 --min-hold-minutes 5

# 复盘模式 + 板块联动策略
python3 "backend/scripts/(高胜率)早盘 V 型反转/早盘 V 型反转.py" --replay-datetime 20260311_10:25 --enable-sector-sync

# 急跌深V型 - 适用场景：强势股快速洗盘，主力打压坚决后迅速拉升，适合捕捉强势反转
# 推荐使用时间：09:45-10:15（需要更早发现急跌信号）
python3 "backend/scripts/(高胜率)早盘 V 型反转/早盘 V 型反转.py" --strategy-type deep_v

# 量能验证策略 - 适用场景：需要确认主力资金进场，通过成交量放大验证反弹真实性
# 推荐使用时间：09:45-10:30（需要观察成交量变化）
python3 "backend/scripts/(高胜率)早盘 V 型反转/早盘 V 型反转.py" --strategy-type volume_validated

# 组合策略 - 在默认基础上启用额外策略
# 启用大盘同步策略 - 推荐使用时间：09:45-10:30
python3 "backend/scripts/(高胜率)早盘 V 型反转/早盘 V 型反转.py" --enable-market-sync

# 启用二次回踩策略 - 推荐使用时间：10:00-10:45（需要等待二次回踩）
python3 "backend/scripts/(高胜率)早盘 V 型反转/早盘 V 型反转.py" --enable-double-dip

# 启用横盘突破策略 - 推荐使用时间：09:45-10:30（需要观察突破信号）
python3 "backend/scripts/(高胜率)早盘 V 型反转/早盘 V 型反转.py" --enable-breakout

# 组合启用多个策略 - 推荐使用时间：10:00-10:45（综合多个信号）
python3 "backend/scripts/(高胜率)早盘 V 型反转/早盘 V 型反转.py" --enable-market-sync --enable-double-dip --enable-breakout

# 启用板块联动策略（需要tushare）- 推荐使用时间：09:45-10:30
python3 "backend/scripts/(高胜率)早盘 V 型反转/早盘 V 型反转.py" --enable-sector-sync

# 组合启用板块联动和大盘同步策略 - 推荐使用时间：09:45-10:30
python3 "backend/scripts/(高胜率)早盘 V 型反转/早盘 V 型反转.py" --enable-sector-sync --enable-market-sync

# 启用筹码聚集策略（需要tushare）- 推荐使用时间：09:45-10:30
python3 "backend/scripts/(高胜率)早盘 V 型反转/早盘 V 型反转.py" --enable-chip-concentrated

# 组合启用筹码聚集和板块联动策略 - 推荐使用时间：09:45-10:30
python3 "backend/scripts/(高胜率)早盘 V 型反转/早盘 V 型反转.py" --enable-chip-concentrated --enable-sector-sync

输出字段说明：
- ts_code: 股票代码
- name: 股票名称
- strategy_type: 策略类型
- open: 开盘价
- low_0930_1000: 前30分钟最低价
- low_0930_1000_pct: 前30分钟最低价相对开盘价跌幅(%)
- cross_time: 站上开盘价时间
- cross_minutes: 站上开盘价所需分钟数
- last_close: 最新收盘价
- pct_from_open: 相对开盘价涨幅(%)
- score: 综合评分
- reason: 入选原因

策略配置：
所有策略参数集中在 STRATEGY_CONFIGS 字典中管理，可通过命令行参数覆盖。
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

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

try:
    from utils.tushare_client import pro
    _tushare_available = True
except Exception:
    _tushare_available = False


STRATEGY_CONFIGS = {
    "standard": {
        "first30_drop_pct": 0.4,
        "first30_close_below_open_pct": 0.05,
        "min_rebound_pct": 0.1,
        "cross_above_open_pct": 0.1,
        "max_cross_minutes": 75,
        "hold_tolerance_pct": 0.05,
        "min_hold_minutes": 10,
        "enable_volume_validation": False,
        "min_volume_ratio": 2.0,
        "description": "标准V型反转：前30分钟跌幅后反弹站上开盘价并站稳",
    },
    "deep_v": {
        "first30_drop_pct": 0.8,
        "first30_close_below_open_pct": 0.2,
        "min_rebound_pct": 0.1,
        "cross_above_open_pct": 0.3,
        "max_cross_minutes": 60,
        "hold_tolerance_pct": 0.0,
        "min_hold_minutes": 15,
        "enable_volume_validation": False,
        "min_volume_ratio": 2.0,
        "description": "急跌深V型：更严格的跌幅要求，更短时间内反弹",
    },
    "volume_validated": {
        "first30_drop_pct": 0.4,
        "first30_close_below_open_pct": 0.05,
        "min_rebound_pct": 0.1,
        "cross_above_open_pct": 0.1,
        "max_cross_minutes": 75,
        "hold_tolerance_pct": 0.05,
        "min_hold_minutes": 10,
        "enable_volume_validation": True,
        "min_volume_ratio": 2.0,
        "description": "量能验证策略：结合成交量分析，反弹时成交量放大",
    },
    "sector_sync": {
        "first30_drop_pct": 0.3,
        "first30_close_below_open_pct": 0.05,
        "min_rebound_pct": 0.1,
        "cross_above_open_pct": 0.1,
        "max_cross_minutes": 75,
        "hold_tolerance_pct": 0.05,
        "min_hold_minutes": 10,
        "enable_volume_validation": False,
        "min_volume_ratio": 2.0,
        "sector_drop_threshold": 0.3,
        "sector_rebound_threshold": 0.1,
        "description": "板块联动型：板块同步V型反转，个股跟随且相对强势",
    },
    "breakout": {
        "first30_drop_pct": 0.5,
        "first30_close_below_open_pct": 0.1,
        "min_rebound_pct": 0.2,
        "cross_above_open_pct": 0.2,
        "max_cross_minutes": 75,
        "hold_tolerance_pct": 0.05,
        "min_hold_minutes": 10,
        "enable_volume_validation": False,
        "min_volume_ratio": 2.0,
        "consolidation_days": 20,
        "consolidation_amplitude_pct": 8.0,
        "description": "横盘突破型：横盘整理后挖坑突破",
    },
    "double_dip": {
        "first30_drop_pct": 0.5,
        "first30_close_below_open_pct": 0.05,
        "min_rebound_pct": 0.1,
        "cross_above_open_pct": 0.1,
        "max_cross_minutes": 90,
        "hold_tolerance_pct": 0.05,
        "min_hold_minutes": 10,
        "enable_volume_validation": False,
        "min_volume_ratio": 2.0,
        "first_dip_min_pct": 0.5,
        "second_dip_higher": True,
        "description": "二次回踩型：二次回踩清洗浮筹后反弹",
    },
    "market_sync": {
        "first30_drop_pct": 0.3,
        "first30_close_below_open_pct": 0.05,
        "min_rebound_pct": 0.1,
        "cross_above_open_pct": 0.1,
        "max_cross_minutes": 75,
        "hold_tolerance_pct": 0.05,
        "min_hold_minutes": 10,
        "enable_volume_validation": False,
        "min_volume_ratio": 2.0,
        "market_drop_threshold": 0.3,
        "market_rebound_threshold": 0.1,
        "description": "大盘同步型：大盘同步V型反转，个股跟随且相对强势",
    },
    "chip_concentrated": {
        "first30_drop_pct": 0.4,
        "first30_close_below_open_pct": 0.05,
        "min_rebound_pct": 0.1,
        "cross_above_open_pct": 0.1,
        "max_cross_minutes": 75,
        "hold_tolerance_pct": 0.05,
        "min_hold_minutes": 10,
        "enable_volume_validation": False,
        "min_volume_ratio": 2.0,
        "chip_concentration_threshold": 15.0,
        "min_winner_rate": 40.0,
        "description": "筹码聚集型：筹码高度集中，主力资金吸筹完成",
    },
}


_market_data_cache: Dict[str, Dict] = {}
_sector_data_cache: Dict[str, Dict] = {}
_daily_bars_cache: Dict[str, pd.DataFrame] = {}
_chip_data_cache: Dict[str, pd.DataFrame] = {}


def _get_cached_chip_data(ts_code: str, trade_date: str) -> Optional[pd.DataFrame]:
    return _chip_data_cache.get(f"{ts_code}_{trade_date}")


def _set_cached_chip_data(ts_code: str, trade_date: str, df: pd.DataFrame) -> None:
    _chip_data_cache[f"{ts_code}_{trade_date}"] = df


def _get_cached_market_data(trade_date: str, cache_key: str = "default") -> Optional[Dict]:
    return _market_data_cache.get(f"{trade_date}_{cache_key}")


def _set_cached_market_data(trade_date: str, data: Dict, cache_key: str = "default") -> None:
    _market_data_cache[f"{trade_date}_{cache_key}"] = data


def _get_cached_sector_data(sector_code: str, trade_date: str) -> Optional[Dict]:
    return _sector_data_cache.get(f"{sector_code}_{trade_date}")


def _set_cached_sector_data(sector_code: str, trade_date: str, data: Dict) -> None:
    _sector_data_cache[f"{sector_code}_{trade_date}"] = data


def _get_cached_daily_bars(code: str) -> Optional[pd.DataFrame]:
    return _daily_bars_cache.get(code)


def _set_cached_daily_bars(code: str, df: pd.DataFrame) -> None:
    _daily_bars_cache[code] = df


def _clear_all_caches() -> None:
    global _market_data_cache, _sector_data_cache, _daily_bars_cache, _chip_data_cache
    _market_data_cache.clear()
    _sector_data_cache.clear()
    _daily_bars_cache.clear()
    _chip_data_cache.clear()


def _check_chip_concentration(
    ts_code: str,
    trade_date: str,
    current_price: float,
    chip_concentration_threshold: float = 15.0,
    min_winner_rate: float = 40.0,
) -> Optional[Dict]:
    """
    检查筹码聚集信号
    
    参数：
        ts_code: 股票代码
        trade_date: 交易日期
        current_price: 当前价格
        chip_concentration_threshold: 筹码集中度阈值（%），默认15%
        min_winner_rate: 最小胜率阈值（%），默认40%
    
    返回：
        包含筹码聚集信号的字典，如果不满足条件则返回None
    """
    if not _tushare_available:
        return None
    
    cached = _get_cached_chip_data(ts_code, trade_date)
    if cached is not None:
        chip_df = cached
    else:
        try:
            from utils.tushare_client import get_chip_performance
            chip_df = get_chip_performance(ts_code, trade_date)
            if chip_df is None or chip_df.empty:
                return None
            _set_cached_chip_data(ts_code, trade_date, chip_df)
        except Exception as e:
            print(f"获取筹码数据失败 {ts_code}: {e}", flush=True)
            return None
    
    if chip_df.empty:
        return None
    
    row = chip_df.iloc[0]
    
    cost_5pct = float(row.get("cost_5pct", 0.0) or 0.0)
    cost_95pct = float(row.get("cost_95pct", 0.0) or 0.0)
    weight_avg = float(row.get("weight_avg", 0.0) or 0.0)
    winner_rate = float(row.get("winner_rate", 0.0) or 0.0)
    
    if cost_5pct <= 0 or cost_95pct <= 0 or weight_avg <= 0:
        return None
    
    chip_concentration = ((cost_95pct - cost_5pct) / weight_avg * 100.0)
    
    if chip_concentration > chip_concentration_threshold:
        return None
    
    if winner_rate < min_winner_rate:
        return None
    
    price_position_pct = ((current_price - cost_5pct) / (cost_95pct - cost_5pct) * 100.0) if cost_95pct > cost_5pct else 50.0
    
    return {
        "chip_concentration": chip_concentration,
        "winner_rate": winner_rate,
        "price_position_pct": price_position_pct,
        "cost_5pct": cost_5pct,
        "cost_95pct": cost_95pct,
        "weight_avg": weight_avg,
    }


SECTOR_INDEX_MAP = {
    "银行": {"market": 1, "code": "000001"},
    "证券": {"market": 1, "code": "000002"},
    "保险": {"market": 1, "code": "000003"},
    "房地产": {"market": 1, "code": "000006"},
    "煤炭": {"market": 1, "code": "000008"},
    "石油": {"market": 1, "code": "000010"},
    "钢铁": {"market": 1, "code": "000012"},
    "有色金属": {"market": 1, "code": "000013"},
    "电力": {"market": 1, "code": "000014"},
    "汽车": {"market": 1, "code": "000016"},
    "家电": {"market": 1, "code": "000020"},
    "酿酒": {"market": 1, "code": "000021"},
    "医药": {"market": 1, "code": "000023"},
    "半导体": {"market": 1, "code": "000025"},
    "元器件": {"market": 1, "code": "000026"},
    "通信设备": {"market": 1, "code": "000027"},
    "软件服务": {"market": 1, "code": "000028"},
    "互联网": {"market": 1, "code": "000029"},
    "传媒娱乐": {"market": 1, "code": "000030"},
    "电气设备": {"market": 1, "code": "000031"},
    "化工": {"market": 1, "code": "000032"},
    "建材": {"market": 1, "code": "000033"},
    "建筑": {"market": 1, "code": "000034"},
    "工程机械": {"market": 1, "code": "000035"},
    "通用机械": {"market": 1, "code": "000036"},
    "商业连锁": {"market": 1, "code": "000037"},
    "旅游": {"market": 1, "code": "000038"},
    "酒店餐饮": {"market": 1, "code": "000039"},
    "运输服务": {"market": 1, "code": "000040"},
    "仓储物流": {"market": 1, "code": "000041"},
    "环境保护": {"market": 1, "code": "000042"},
    "水务": {"market": 1, "code": "000043"},
    "供气供热": {"market": 1, "code": "000044"},
    "综合类": {"market": 1, "code": "000045"},
}

MARKET_INDEX_CODE = {"sh": {"market": 1, "code": "000001"}, "sz": {"market": 0, "code": "399001"}}


def _fetch_market_intraday_data(
    tdx_,
    trade_date: str,
    asof_time: str,
    market_key: str = "sh",
    start_offset_hint: Optional[int] = None,
) -> Optional[Dict]:
    if market_key not in MARKET_INDEX_CODE:
        market_key = "sh"
    idx_info = MARKET_INDEX_CODE[market_key]
    market = idx_info["market"]
    code = idx_info["code"]
    
    cached = _get_cached_market_data(trade_date, market_key)
    if cached is not None:
        return cached
    
    try:
        trade_date_dt = pd.to_datetime(trade_date, format="%Y%m%d")
        open_start = trade_date_dt + pd.Timedelta(hours=9, minutes=30)
        asof_dt = trade_date_dt + pd.Timedelta(hours=int(asof_time.split(":")[0]), minutes=int(asof_time.split(":")[1]))
    except Exception:
        return None
    
    df_1m = None
    start = int(start_offset_hint) if start_offset_hint is not None else 200
    
    for start in [start, 200, 300, 400]:
        try:
            bars = tdx_.get_index_bars(8, int(market), code, int(start), 200)
        except Exception:
            bars = []
        
        if not bars:
            continue
        
        part = pd.DataFrame(bars)
        if part.empty or "datetime" not in part.columns:
            continue
        
        part = part.copy()
        part["datetime"] = pd.to_datetime(part["datetime"], errors="coerce")
        part = part.dropna(subset=["datetime"])
        
        for c in ("open", "close", "high", "low", "vol", "amount"):
            if c in part.columns:
                part[c] = pd.to_numeric(part[c], errors="coerce")
        
        part = part.dropna(subset=["open", "close"], how="any")
        if part.empty:
            continue
        
        df_1m = part
        break
    
    if df_1m is None or df_1m.empty:
        return None
    
    try:
        first30_end = pd.to_datetime(trade_date, format="%Y%m%d") + pd.Timedelta(hours=10, minutes=0)
    except Exception:
        return None
    
    first30 = df_1m[df_1m["datetime"] < first30_end].copy()
    if first30.empty or len(first30) < 25:
        return None
    
    open_px = float(df_1m["open"].iloc[0] or 0.0)
    if open_px <= 0:
        return None
    
    low30 = float(pd.to_numeric(first30.get("low", pd.Series(dtype=float)), errors="coerce").min() or float("nan"))
    if not (low30 > 0):
        low30 = float(pd.to_numeric(first30["close"], errors="coerce").min() or float("nan"))
    if low30 <= 0:
        return None
    
    low30_pct = (low30 - open_px) / open_px * 100.0
    last_close = float(df_1m["close"].iloc[-1] or 0.0)
    rebound_pct = ((last_close - low30) / low30 * 100.0) if low30 > 0 and last_close > 0 else 0.0
    
    cross_open = last_close >= open_px
    
    cross_time_str = "N/A"
    if cross_open:
        cross_mask = pd.to_numeric(df_1m["close"], errors="coerce") >= open_px
        if cross_mask.any():
            cross_pos = int(cross_mask.idxmax())
            cross_time = df_1m.loc[cross_pos, "datetime"].to_pydatetime()
            cross_time_str = cross_time.strftime("%H:%M")
    
    market_data = {
        "market_low_pct": low30_pct,
        "market_rebound_pct": rebound_pct,
        "market_cross_open": cross_open,
        "market_cross_time": cross_time_str,
        "market_open": open_px,
        "market_low": low30,
        "market_last_close": last_close,
    }
    
    _set_cached_market_data(trade_date, market_data, market_key)
    return market_data


_stock_sector_cache: Dict[str, Dict] = {}
_sw_index_map: Dict[str, Dict] = {}

def _build_sw_index_map() -> None:
    if _sw_index_map:
        return
    
    if not _tushare_available:
        return
    
    try:
        df = pro.index_classify(level="L1", src="SW2021")
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                index_code = str(row.get("index_code", ""))
                industry_name = str(row.get("industry_name", ""))
                if index_code and industry_name:
                    _sw_index_map[industry_name] = {
                        "index_code": index_code,
                        "level": "L1",
                        "industry_name": industry_name,
                    }
    except Exception as e:
        print(f"构建申万行业映射失败: {e}", flush=True)

def _get_cached_stock_sector(code: str) -> Optional[Dict]:
    return _stock_sector_cache.get(str(code).zfill(6))

def _set_cached_stock_sector(code: str, data: Dict) -> None:
    _stock_sector_cache[str(code).zfill(6)] = data

def _get_stock_sector(code: str) -> Optional[Dict]:
    code = str(code).zfill(6)
    
    cached = _get_cached_stock_sector(code)
    if cached is not None:
        return cached
    
    if not _tushare_available:
        return None
    
    try:
        ts_code = f"{code}.SH" if code.startswith("60") else f"{code}.SZ"
        df = pro.index_member_all(ts_code=ts_code, is_new="Y")
        
        if df is None or df.empty:
            return None
        
        df = df[df["is_new"] == "Y"].copy()
        if df.empty:
            return None
        
        row = df.iloc[0]
        sector_data = {
            "l1_code": row.get("l1_code", ""),
            "l1_name": row.get("l1_name", ""),
            "l2_code": row.get("l2_code", ""),
            "l2_name": row.get("l2_name", ""),
            "l3_code": row.get("l3_code", ""),
            "l3_name": row.get("l3_name", ""),
            "in_date": row.get("in_date", ""),
            "out_date": row.get("out_date", ""),
        }
        
        _set_cached_stock_sector(code, sector_data)
        return sector_data
    except Exception as e:
        print(f"获取股票行业信息失败 {code}: {e}", flush=True)
        return None


def _fetch_sector_intraday_data(
    tdx_,
    sector_name: str,
    trade_date: str,
    asof_time: str,
    start_offset_hint: Optional[int] = None,
) -> Optional[Dict]:
    if sector_name not in SECTOR_INDEX_MAP:
        return None
    
    cached = _get_cached_sector_data(sector_name, trade_date)
    if cached is not None:
        return cached
    
    idx_info = SECTOR_INDEX_MAP[sector_name]
    market = idx_info["market"]
    code = idx_info["code"]
    
    df_1m = _fetch_intraday_1m_bars(
        tdx_,
        market=market,
        code=code,
        trade_date=trade_date,
        asof_time=asof_time,
        max_total=800,
        step=200,
        start_offset_hint=start_offset_hint,
    )
    
    if df_1m is None or df_1m.empty:
        return None
    
    try:
        first30_end = pd.to_datetime(trade_date, format="%Y%m%d") + pd.Timedelta(hours=10, minutes=0)
    except Exception:
        return None
    
    first30 = df_1m[df_1m["datetime"] < first30_end].copy()
    if first30.empty or len(first30) < 25:
        return None
    
    open_px = float(df_1m["open"].iloc[0] or 0.0)
    if open_px <= 0:
        return None
    
    low30 = float(pd.to_numeric(first30.get("low", pd.Series(dtype=float)), errors="coerce").min() or float("nan"))
    if not (low30 > 0):
        low30 = float(pd.to_numeric(first30["close"], errors="coerce").min() or float("nan"))
    if low30 <= 0:
        return None
    
    low30_pct = (low30 - open_px) / open_px * 100.0
    last_close = float(df_1m["close"].iloc[-1] or 0.0)
    rebound_pct = ((last_close - low30) / low30 * 100.0) if low30 > 0 and last_close > 0 else 0.0
    
    cross_open = last_close >= open_px
    
    cross_time_str = "N/A"
    if cross_open:
        cross_mask = pd.to_numeric(df_1m["close"], errors="coerce") >= open_px
        if cross_mask.any():
            cross_pos = int(cross_mask.idxmax())
            cross_time = df_1m.loc[cross_pos, "datetime"].to_pydatetime()
            cross_time_str = cross_time.strftime("%H:%M")
    
    sector_data = {
        "sector_name": sector_name,
        "sector_low_pct": low30_pct,
        "sector_rebound_pct": rebound_pct,
        "sector_cross_open": cross_open,
        "sector_cross_time": cross_time_str,
        "sector_open": open_px,
        "sector_low": low30,
        "sector_last_close": last_close,
    }
    
    _set_cached_sector_data(sector_name, trade_date, sector_data)
    return sector_data


def _fetch_sw_sector_daily_data(
    sector_code: str,
    trade_date: str,
) -> Optional[Dict]:
    if not _tushare_available:
        return None
    
    cache_key = f"{sector_code}_{trade_date}"
    cached = _get_cached_sector_data(cache_key, trade_date)
    if cached is not None:
        return cached
    
    try:
        df = pro.index_daily(ts_code=sector_code, trade_date=trade_date)
        if df is None or df.empty:
            return None
        
        row = df.iloc[0]
        sector_data = {
            "sector_code": sector_code,
            "sector_open": float(row.get("open", 0.0) or 0.0),
            "sector_close": float(row.get("close", 0.0) or 0.0),
            "sector_high": float(row.get("high", 0.0) or 0.0),
            "sector_low": float(row.get("low", 0.0) or 0.0),
            "sector_pre_close": float(row.get("pre_close", 0.0) or 0.0),
            "sector_pct_chg": float(row.get("pct_chg", 0.0) or 0.0),
        }
        
        if sector_data["sector_open"] > 0:
            sector_data["sector_low_pct"] = ((sector_data["sector_low"] - sector_data["sector_open"]) / sector_data["sector_open"] * 100.0)
            sector_data["sector_rebound_pct"] = ((sector_data["sector_close"] - sector_data["sector_low"]) / sector_data["sector_low"] * 100.0) if sector_data["sector_low"] > 0 else 0.0
            sector_data["sector_cross_open"] = sector_data["sector_close"] >= sector_data["sector_open"]
        else:
            sector_data["sector_low_pct"] = 0.0
            sector_data["sector_rebound_pct"] = 0.0
            sector_data["sector_cross_open"] = False
        
        _set_cached_sector_data(cache_key, trade_date, sector_data)
        return sector_data
    except Exception as e:
        print(f"获取申万行业日线数据失败 {sector_code}: {e}", flush=True)
        return None


def _fetch_daily_bars_for_breakout(
    tdx_,
    market: int,
    code: str,
    days: int = 30,
) -> Optional[pd.DataFrame]:
    cached = _get_cached_daily_bars(code)
    if cached is not None:
        return cached
    
    try:
        bars = tdx_.get_security_bars(9, int(market), str(code).zfill(6), 0, int(days))
    except Exception:
        bars = []
    
    df = tdx_.to_df(bars) if bars else pd.DataFrame()
    if df is None or df.empty:
        return None
    
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"])
    for c in ("open", "close", "high", "low", "vol", "amount"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    
    df = df.sort_values("datetime", ascending=True).reset_index(drop=True)
    
    _set_cached_daily_bars(code, df)
    return df


def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _trade_date_default() -> str:
    return datetime.now().strftime("%Y%m%d")


def _asof_time_default_hhmm() -> str:
    return datetime.now().strftime("%H:%M")


def _parse_markets(s: str) -> List[int]:
    s = str(s or "").strip().lower()
    if s in {"sz", "0"}:
        return [0]
    if s in {"sh", "1"}:
        return [1]
    return [0, 1]


def _chunks(items: List, n: int) -> Iterable[List]:
    n = max(1, int(n))
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _minute_bars_needed(open_start: datetime, asof_dt: datetime) -> int:
    d = open_start.strftime("%Y%m%d")
    am_start = datetime.strptime(f"{d} 09:30", "%Y%m%d %H:%M")
    am_end = datetime.strptime(f"{d} 11:30", "%Y%m%d %H:%M")
    pm_start = datetime.strptime(f"{d} 13:00", "%Y%m%d %H:%M")
    pm_end = datetime.strptime(f"{d} 15:00", "%Y%m%d %H:%M")

    def _mins(a: datetime, b: datetime) -> int:
        if b <= a:
            return 0
        return int((b - a).total_seconds() // 60)

    asof_dt = min(max(asof_dt, am_start), pm_end)
    am = _mins(am_start, min(asof_dt, am_end))
    pm = _mins(pm_start, min(asof_dt, pm_end)) if asof_dt > pm_start else 0
    return max(1, am + pm + 5)


def _compute_start_offset_by_probe(
    tdx_,
    trade_date: str,
    asof_time: str,
    probe_market: int = 0,
    probe_code: str = "000001",
    probe_count: int = 5000,
) -> Optional[int]:
    trade_date = str(trade_date).strip()
    asof_time = str(asof_time).strip()
    try:
        target_dt = datetime.strptime(f"{trade_date} {asof_time}", "%Y%m%d %H:%M")
    except ValueError as e:
        print(f"时间格式错误: trade_date={trade_date}, asof_time={asof_time}, error={e}", flush=True)
        return None
    except Exception as e:
        print(f"时间解析异常: {e}", flush=True)
        return None

    probe_count = max(50, min(int(probe_count), 800))
    probe_step = int(probe_count)
    max_probe_start = int(240 * 40)
    base = 0
    while base <= max_probe_start:
        try:
            bars = tdx_.get_security_bars(8, int(probe_market), str(probe_code).zfill(6), int(base), int(probe_step))
        except Exception as e:
            bars = []
        df = tdx_.to_df(bars) if bars else pd.DataFrame()
        if df is None or df.empty or "datetime" not in df.columns:
            base += probe_step
            continue
        try:
            df = df.copy()
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
            df = df.dropna(subset=["datetime"])
            if df.empty:
                base += probe_step
                continue
            df = df.sort_values("datetime", ascending=False).reset_index(drop=True)

            max_dt = df["datetime"].max().to_pydatetime()
            min_dt = df["datetime"].min().to_pydatetime()
            if max_dt < target_dt:
                return None
            if min_dt > target_dt:
                base += probe_step
                continue

            hit = df[df["datetime"] <= target_dt]
            if hit.empty:
                base += probe_step
                continue
            return int(base + int(hit.index[0]))
        except Exception as e:
            base += probe_step
            continue

    return None


def _probe_1m_window(
    tdx_,
    market: int,
    code: str,
    start: int,
    count: int,
) -> Tuple[bool, Optional[datetime], Optional[datetime]]:
    try:
        bars = tdx_.get_security_bars(8, int(market), str(code).zfill(6), int(start), int(count))
    except Exception:
        bars = []
    df = tdx_.to_df(bars) if bars else pd.DataFrame()
    if df is None or df.empty or "datetime" not in df.columns:
        return False, None, None
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"])
    if df.empty:
        return False, None, None
    return True, df["datetime"].min().to_pydatetime(), df["datetime"].max().to_pydatetime()


def _find_start_offset_for_window(
    tdx_,
    market: int,
    code: str,
    open_start: datetime,
    asof_dt: datetime,
    start_offset_hint: Optional[int],
    probe_count: int,
    step: int,
    max_iter: int,
) -> Optional[int]:
    if start_offset_hint is None:
        return 0
    probe_count = max(50, min(int(probe_count), 600))
    step = max(50, int(step))
    start = max(0, int(start_offset_hint))
    visited = set()
    for _ in range(max(1, int(max_iter))):
        if start in visited:
            break
        visited.add(start)
        ok, mn, mx = _probe_1m_window(tdx_, market=int(market), code=str(code).zfill(6), start=int(start), count=int(probe_count))
        if not ok or mn is None or mx is None:
            return None
        if mx < open_start:
            if start <= 0:
                return None
            start = max(0, start - step)
            continue
        if mn > asof_dt:
            start = start + step
            continue
        if mn <= asof_dt <= mx:
            return start
        if mx < asof_dt:
            if start <= 0:
                return None
            start = max(0, start - step)
            continue
        start = start + step
    return start


def _quotes_snapshot_df(
    tdx_,
    df_codes: pd.DataFrame,
    chunk_size: int,
    sleep_s: float,
) -> pd.DataFrame:
    req_pairs = [(int(r["market"]), str(r["code"]).zfill(6)) for _, r in df_codes.iterrows()]
    rows: List[Dict] = []
    for idxs in _chunks(list(range(len(req_pairs))), int(chunk_size)):
        req = [req_pairs[i] for i in idxs]
        try:
            ret = tdx_.get_security_quotes(req)
        except Exception:
            ret = []
        if not isinstance(ret, list):
            ret = []
        for i, q in zip(idxs, ret):
            if not isinstance(q, dict):
                continue
            market = int(df_codes.iloc[i]["market"])
            code = str(df_codes.iloc[i]["code"]).zfill(6)
            name = str(df_codes.iloc[i].get("name") or "").strip()
            open_px = float(q.get("open") or 0.0)
            price = float(q.get("price") or 0.0)
            last_close = float(q.get("last_close") or 0.0)
            pct_from_open = ((price - open_px) / open_px * 100.0) if open_px > 0 and price > 0 else float("nan")
            rows.append(
                {
                    "market": market,
                    "code": code,
                    "name": name,
                    "open": open_px,
                    "price": price,
                    "last_close": last_close,
                    "pct_from_open": pct_from_open,
                }
            )
        if float(sleep_s) > 0:
            time.sleep(float(sleep_s))
    df = pd.DataFrame(rows)
    if df is None or df.empty:
        return pd.DataFrame()
    df["market"] = df["market"].astype(int)
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["name"] = df["name"].astype(str)
    return df


def _fetch_intraday_1m_bars(
    tdx_,
    market: int,
    code: str,
    trade_date: str,
    asof_time: str,
    max_total: int,
    step: int,
    start_offset_hint: Optional[int] = None,
) -> Optional[pd.DataFrame]:
    trade_date = str(trade_date).strip()
    asof_time = str(asof_time).strip()
    code = str(code).zfill(6)
    
    try:
        trade_date_dt = pd.to_datetime(trade_date, format="%Y%m%d")
        open_start = trade_date_dt + pd.Timedelta(hours=9, minutes=30)
        asof_dt = trade_date_dt + pd.Timedelta(hours=int(asof_time.split(":")[0]), minutes=int(asof_time.split(":")[1]))
    except Exception:
        try:
            open_start = datetime.strptime(f"{trade_date} 09:30", "%Y%m%d %H:%M")
            asof_dt = datetime.strptime(f"{trade_date} {asof_time}", "%Y%m%d %H:%M")
        except Exception:
            return None

    max_total = max(60, int(max_total))
    step = max(50, int(step))
    need = _minute_bars_needed(open_start=open_start, asof_dt=asof_dt)
    max_total = max(max_total, int(need) + int(step) * 2)

    start = int(start_offset_hint) if start_offset_hint is not None else 0
    if start_offset_hint is not None:
        adj = _find_start_offset_for_window(
            tdx_,
            market=int(market),
            code=str(code).zfill(6),
            open_start=open_start,
            asof_dt=asof_dt,
            start_offset_hint=int(start_offset_hint),
            probe_count=min(600, int(step) * 2),
            step=int(step),
            max_iter=8,
        )
        if adj is None:
            return None
        start = int(adj)
    fetched = 0
    frames: List[pd.DataFrame] = []
    while fetched < max_total:
        count = min(step, max_total - fetched)
        try:
            bars = tdx_.get_security_bars(8, int(market), code, int(start), int(count))
        except Exception:
            bars = []
        part = tdx_.to_df(bars) if bars else pd.DataFrame()
        if part is None or part.empty or "datetime" not in part.columns:
            break
        part = part.copy()
        part["datetime"] = pd.to_datetime(part["datetime"], errors="coerce")
        part = part.dropna(subset=["datetime"])
        for c in ("open", "close", "high", "low", "vol", "amount"):
            if c in part.columns:
                part[c] = pd.to_numeric(part[c], errors="coerce")
        part = part.dropna(subset=["open", "close"], how="any")
        if part.empty:
            break
        frames.append(part)
        fetched += len(part)

        min_dt = part["datetime"].min()
        if pd.notna(min_dt) and min_dt.to_pydatetime() <= open_start:
            break
        if len(part) < count:
            break
        start += count

    if not frames:
        return None

    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=["datetime"]).sort_values("datetime", ascending=True).reset_index(drop=True)
    if df.empty:
        return None
    
    try:
        trade_date_obj = pd.to_datetime(trade_date, format="%Y%m%d").date()
        df = df[df["datetime"].dt.date == trade_date_obj].copy()
    except Exception:
        df = df[df["datetime"].dt.strftime("%Y%m%d") == trade_date].copy()
    
    if df.empty:
        return None
    df = df[(df["datetime"] >= open_start) & (df["datetime"] <= asof_dt)].copy()
    if df.empty:
        return None
    return df.reset_index(drop=True)


def _has_trade_date_in_probe_window(
    tdx_,
    trade_date: str,
    start_offset: int,
    probe_market: int = 0,
    probe_code: str = "000001",
    probe_count: int = 800,
) -> bool:
    trade_date = str(trade_date).strip()
    try:
        bars = tdx_.get_security_bars(8, int(probe_market), str(probe_code).zfill(6), int(start_offset), int(probe_count))
    except Exception:
        bars = []
    df = tdx_.to_df(bars) if bars else pd.DataFrame()
    if df is None or df.empty or "datetime" not in df.columns:
        return False
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"])
    if df.empty:
        return False
    return bool((df["datetime"].dt.strftime("%Y%m%d") == trade_date).any())


def _resolve_recent_trade_dates_with_offsets(
    tdx_,
    base_trade_date: str,
    want: int = 2,
    asof_time: str = "15:00",
    probe_market: int = 0,
    probe_code: str = "000001",
) -> Tuple[List[str], Dict[str, int]]:
    base_trade_date = str(base_trade_date).strip()
    try:
        base_dt = datetime.strptime(base_trade_date, "%Y%m%d")
    except Exception:
        return [], {}

    want = max(0, int(want))
    out_dates: List[str] = []
    out_offsets: Dict[str, int] = {}
    for d in range(1, 25):
        if len(out_dates) >= want:
            break
        cand = (base_dt - timedelta(days=int(d))).strftime("%Y%m%d")
        off = _compute_start_offset_by_probe(
            tdx_,
            trade_date=cand,
            asof_time=str(asof_time),
            probe_market=int(probe_market),
            probe_code=str(probe_code).zfill(6),
        )
        if off is None:
            continue
        if not _has_trade_date_in_probe_window(
            tdx_,
            trade_date=cand,
            start_offset=int(off),
            probe_market=int(probe_market),
            probe_code=str(probe_code).zfill(6),
            probe_count=800,
        ):
            continue
        out_dates.append(cand)
        out_offsets[cand] = int(off)
    return out_dates, out_offsets


def _deepdrop_quick_rebound_strength(
    df_1m: pd.DataFrame,
    drop_window_minutes: int,
    rebound_window_minutes: int,
    deep_drop_pct: float,
    min_rebound_pct: float,
    min_recover_ratio: float,
) -> float:
    if df_1m is None or df_1m.empty:
        return 0.0
    if "high" not in df_1m.columns or "low" not in df_1m.columns:
        return 0.0

    drop_window_minutes = max(3, int(drop_window_minutes))
    rebound_window_minutes = max(3, int(rebound_window_minutes))
    deep_drop_thr = max(0.0001, float(deep_drop_pct) / 100.0)
    rebound_thr = max(0.0001, float(min_rebound_pct) / 100.0)
    min_recover_ratio = max(0.0, float(min_recover_ratio))

    highs = pd.to_numeric(df_1m["high"], errors="coerce").ffill().bfill()
    lows = pd.to_numeric(df_1m["low"], errors="coerce").ffill().bfill()
    if highs.isna().all() or lows.isna().all():
        return 0.0

    n = len(df_1m)
    best = 0.0
    j_start = drop_window_minutes - 1
    j_end = n - rebound_window_minutes - 1
    if j_end < j_start:
        return 0.0

    for j in range(j_start, j_end + 1):
        peak = float(highs.iloc[j - drop_window_minutes + 1 : j + 1].max() or 0.0)
        trough = float(lows.iloc[j] or 0.0)
        if peak <= 0 or trough <= 0 or trough >= peak:
            continue
        drop_ratio = (peak - trough) / peak
        if drop_ratio < deep_drop_thr:
            continue
        rebound_high = float(highs.iloc[j + 1 : j + 1 + rebound_window_minutes].max() or 0.0)
        if rebound_high <= 0 or rebound_high <= trough:
            continue
        rebound_ratio = (rebound_high - trough) / trough
        if rebound_ratio < rebound_thr:
            continue
        recover_ratio = (rebound_high - trough) / max(1e-12, (peak - trough))
        if recover_ratio < min_recover_ratio:
            continue
        drop_factor = min(1.0, drop_ratio / deep_drop_thr)
        recover_factor = min(1.0, recover_ratio / max(1e-12, min_recover_ratio if min_recover_ratio > 0 else 1.0))
        strength = min(1.0, 0.5 * drop_factor + 0.5 * recover_factor)
        if strength > best:
            best = strength
            if best >= 1.0:
                break
    return float(best)


def _recent_deepdrop_rebound_score(
    tdx_,
    market: int,
    code: str,
    recent_trade_dates: List[str],
    recent_offsets: Dict[str, int],
    drop_window_minutes: int,
    rebound_window_minutes: int,
    deep_drop_pct: float,
    min_rebound_pct: float,
    min_recover_ratio: float,
    weight_yesterday: float,
    weight_daybefore: float,
    bars_max_total: int,
    bars_step: int,
) -> Tuple[float, float, float]:
    if not recent_trade_dates:
        return 0.0, 0.0, 0.0

    day_scores: Dict[str, float] = {}
    for d in recent_trade_dates[:2]:
        off = recent_offsets.get(d)
        if off is None:
            continue
        df_day = _fetch_intraday_1m_bars(
            tdx_,
            market=int(market),
            code=str(code).zfill(6),
            trade_date=str(d),
            asof_time="15:00",
            max_total=int(bars_max_total),
            step=int(bars_step),
            start_offset_hint=int(off),
        )
        if df_day is None or df_day.empty:
            continue
        day_scores[d] = _deepdrop_quick_rebound_strength(
            df_day,
            drop_window_minutes=int(drop_window_minutes),
            rebound_window_minutes=int(rebound_window_minutes),
            deep_drop_pct=float(deep_drop_pct),
            min_rebound_pct=float(min_rebound_pct),
            min_recover_ratio=float(min_recover_ratio),
        )

    y = float(day_scores.get(recent_trade_dates[0], 0.0) if len(recent_trade_dates) >= 1 else 0.0)
    p = float(day_scores.get(recent_trade_dates[1], 0.0) if len(recent_trade_dates) >= 2 else 0.0)
    total = y * float(weight_yesterday) + p * float(weight_daybefore)
    return float(total), float(y), float(p)


def _detect_single_strategy(
    df_1m: pd.DataFrame,
    trade_date: str,
    strategy_type: str,
    first30_drop_pct: float,
    first30_close_below_open_pct: float,
    min_rebound_pct: float,
    cross_above_open_pct: float,
    max_cross_minutes: int,
    hold_tolerance_pct: float,
    min_hold_minutes: int,
    enable_volume_validation: bool,
    min_volume_ratio: float,
    quote_open_px: Optional[float] = None,
    chip_concentration_threshold: float = 15.0,
    min_winner_rate: float = 40.0,
) -> Optional[Dict]:
    if df_1m is None or df_1m.empty:
        return None
    
    try:
        open_start = datetime.strptime(f"{trade_date} 09:30", "%Y%m%d %H:%M")
        first30_end = datetime.strptime(f"{trade_date} 10:00", "%Y%m%d %H:%M")
    except Exception:
        return None

    if "datetime" not in df_1m.columns or "open" not in df_1m.columns or "close" not in df_1m.columns:
        return None

    df_1m = df_1m.copy()
    df_1m = df_1m.dropna(subset=["datetime", "open", "close"])
    if df_1m.empty:
        return None

    if df_1m["datetime"].max().to_pydatetime() < first30_end:
        return None

    if quote_open_px is not None and quote_open_px > 0:
        open_px = float(quote_open_px)
    else:
        open_px = float(df_1m["open"].iloc[0] or 0.0)
    if open_px <= 0:
        return None

    first30 = df_1m[df_1m["datetime"] < first30_end].copy()
    if first30.empty or len(first30) < 25:
        return None

    low30 = float(pd.to_numeric(first30.get("low", pd.Series(dtype=float)), errors="coerce").min() or float("nan"))
    if not (low30 > 0):
        low30 = float(pd.to_numeric(first30["close"], errors="coerce").min() or float("nan"))
    close30 = float(first30["close"].iloc[-1] or 0.0)
    if low30 <= 0 or close30 <= 0:
        return None

    low30_pct = (low30 - open_px) / open_px * 100.0
    close30_pct = (close30 - open_px) / open_px * 100.0
    
    if strategy_type == "deep_v":
        if low30_pct > -0.8 or close30_pct > -0.2:
            return None
    else:
        if low30_pct > -abs(float(first30_drop_pct)):
            return None
        if close30_pct > -abs(float(first30_close_below_open_pct)):
            return None

    post = df_1m[df_1m["datetime"] >= first30_end].copy()
    if post.empty:
        return None

    cross_line = open_px * (1.0 + float(cross_above_open_pct) / 100.0)
    cross_mask = pd.to_numeric(post["close"], errors="coerce") >= cross_line
    if not bool(cross_mask.any()):
        return None

    cross_pos = int(cross_mask.idxmax())
    cross_time = post.loc[cross_pos, "datetime"].to_pydatetime()
    cross_minutes = int((cross_time - open_start).total_seconds() / 60.0)
    if cross_minutes > int(max_cross_minutes):
        return None

    after_cross = post[post["datetime"] >= cross_time].copy()
    if after_cross.empty:
        return None

    min_close_after_cross = float(pd.to_numeric(after_cross["close"], errors="coerce").min() or float("nan"))
    if not (min_close_after_cross > 0):
        return None
    hold_line = open_px * (1.0 - float(hold_tolerance_pct) / 100.0)

    if strategy_type == "deep_v":
        hold_line = open_px
        min_hold_minutes = max(15, min_hold_minutes)

    hold_mask = pd.to_numeric(after_cross["close"], errors="coerce") >= hold_line
    if not bool(hold_mask.any()):
        return None
    
    last_hold_time = after_cross.loc[hold_mask, "datetime"].max().to_pydatetime()
    hold_duration = int((last_hold_time - cross_time).total_seconds() / 60.0)
    if hold_duration < int(min_hold_minutes):
        return None

    last_close = float(df_1m["close"].iloc[-1] or 0.0)
    pct_from_open = ((last_close - open_px) / open_px * 100.0) if open_px > 0 and last_close > 0 else float("nan")

    signal_info = {
        "open": open_px,
        "low_0930_1000": low30,
        "low_0930_1000_pct": low30_pct,
        "close_0959": close30,
        "close_0959_pct": close30_pct,
        "cross_time": cross_time.strftime("%H:%M"),
        "cross_minutes": cross_minutes,
        "last_close": last_close,
        "pct_from_open": pct_from_open,
        "strategy_type": strategy_type,
    }

    if enable_volume_validation and "vol" in df_1m.columns:
        first30_vol = float(pd.to_numeric(first30["vol"], errors="coerce").mean() or 0.0)
        after_cross_vol = float(pd.to_numeric(after_cross["vol"], errors="coerce").mean() or 0.0)
        volume_ratio = after_cross_vol / first30_vol if first30_vol > 0 else 0.0
        signal_info["volume_ratio"] = volume_ratio
        if volume_ratio < float(min_volume_ratio):
            return None

    if strategy_type == "chip_concentrated":
        ts_code = df_1m.attrs.get("ts_code", "")
        if not ts_code:
            return None
        chip_signal = _check_chip_concentration(
            ts_code=ts_code,
            trade_date=trade_date,
            current_price=last_close,
            chip_concentration_threshold=chip_concentration_threshold,
            min_winner_rate=min_winner_rate,
        )
        if chip_signal is None:
            return None
        signal_info.update(chip_signal)

    return signal_info


def _detect_sector_sync_strategy(
    df_1m: pd.DataFrame,
    trade_date: str,
    sector_data: Optional[Dict] = None,
) -> Optional[Dict]:
    """
    板块联动型策略检测
    条件：
    1. 板块同步出现V型反转（板块跌幅 > 0.3%，反弹站上开盘价）
    2. 个股跌幅大于板块跌幅（相对强势）
    3. 个股反弹幅度大于板块反弹幅度
    """
    if df_1m is None or df_1m.empty or sector_data is None:
        return None
    
    config = STRATEGY_CONFIGS.get("sector_sync", {})
    sector_drop_threshold = config.get("sector_drop_threshold", 0.3)
    sector_rebound_threshold = config.get("sector_rebound_threshold", 0.1)
    
    try:
        open_start = datetime.strptime(f"{trade_date} 09:30", "%Y%m%d %H:%M")
        first30_end = datetime.strptime(f"{trade_date} 10:00", "%Y%m%d %H:%M")
    except Exception:
        return None
    
    if "datetime" not in df_1m.columns or "open" not in df_1m.columns or "close" not in df_1m.columns:
        return None
    
    df_1m = df_1m.copy()
    df_1m = df_1m.dropna(subset=["datetime", "open", "close"])
    if df_1m.empty:
        return None
    
    if df_1m["datetime"].max().to_pydatetime() < first30_end:
        return None
    
    open_px = float(df_1m["open"].iloc[0] or 0.0)
    if open_px <= 0:
        return None
    
    first30 = df_1m[df_1m["datetime"] < first30_end].copy()
    if first30.empty or len(first30) < 25:
        return None
    
    stock_low30 = float(pd.to_numeric(first30.get("low", pd.Series(dtype=float)), errors="coerce").min() or float("nan"))
    if not (stock_low30 > 0):
        stock_low30 = float(pd.to_numeric(first30["close"], errors="coerce").min() or float("nan"))
    if stock_low30 <= 0:
        return None
    
    stock_low30_pct = (stock_low30 - open_px) / open_px * 100.0
    stock_last_close = float(df_1m["close"].iloc[-1] or 0.0)
    stock_rebound_pct = ((stock_last_close - stock_low30) / stock_low30 * 100.0) if stock_low30 > 0 and stock_last_close > 0 else 0.0
    
    sector_low_pct = sector_data.get("sector_low_pct", 0.0)
    sector_rebound_pct = sector_data.get("sector_rebound_pct", 0.0)
    sector_cross_open = sector_data.get("sector_cross_open", False)
    sector_name = sector_data.get("sector_name", "未知")
    
    if abs(sector_low_pct) < sector_drop_threshold:
        return None
    
    if not sector_cross_open:
        return None
    
    if sector_rebound_pct < sector_rebound_threshold:
        return None
    
    if stock_low30_pct > sector_low_pct:
        return None
    
    if stock_rebound_pct < sector_rebound_pct:
        return None
    
    pct_from_open = ((stock_last_close - open_px) / open_px * 100.0) if open_px > 0 and stock_last_close > 0 else float("nan")
    
    cross_time_str = "N/A"
    cross_mask = pd.to_numeric(df_1m["close"], errors="coerce") >= open_px
    if cross_mask.any():
        cross_pos = int(cross_mask.idxmax())
        cross_time = df_1m.loc[cross_pos, "datetime"].to_pydatetime()
        cross_time_str = cross_time.strftime("%H:%M")
    
    return {
        "open": open_px,
        "low_0930_1000": stock_low30,
        "low_0930_1000_pct": stock_low30_pct,
        "close_0959": float(first30["close"].iloc[-1] or 0.0),
        "close_0959_pct": (float(first30["close"].iloc[-1] or 0.0) - open_px) / open_px * 100.0,
        "cross_time": cross_time_str,
        "cross_minutes": 0,
        "last_close": stock_last_close,
        "pct_from_open": pct_from_open,
        "strategy_type": "sector_sync",
        "sector_name": sector_name,
        "sector_low_pct": sector_low_pct,
        "sector_rebound_pct": sector_rebound_pct,
        "stock_vs_sector_strength": stock_rebound_pct - sector_rebound_pct,
    }


def _detect_breakout_strategy(
    df_1m: pd.DataFrame,
    trade_date: str,
    daily_data: Optional[pd.DataFrame] = None,
) -> Optional[Dict]:
    """
    横盘突破型策略检测
    条件：
    1. 日线处于横盘整理（近20日振幅 < 8%）
    2. 早盘挖坑跌破横盘下沿
    3. 快速反弹突破横盘上沿
    """
    if df_1m is None or df_1m.empty or daily_data is None or daily_data.empty:
        return None
    
    config = STRATEGY_CONFIGS.get("breakout", {})
    consolidation_days = config.get("consolidation_days", 20)
    consolidation_amplitude_pct = config.get("consolidation_amplitude_pct", 8.0)
    
    try:
        open_start = datetime.strptime(f"{trade_date} 09:30", "%Y%m%d %H:%M")
        first30_end = datetime.strptime(f"{trade_date} 10:00", "%Y%m%d %H:%M")
    except Exception:
        return None
    
    if "datetime" not in df_1m.columns or "open" not in df_1m.columns or "close" not in df_1m.columns:
        return None
    
    df_1m = df_1m.copy()
    df_1m = df_1m.dropna(subset=["datetime", "open", "close"])
    if df_1m.empty:
        return None
    
    if df_1m["datetime"].max().to_pydatetime() < first30_end:
        return None
    
    open_px = float(df_1m["open"].iloc[0] or 0.0)
    if open_px <= 0:
        return None
    
    recent_daily = daily_data.tail(consolidation_days) if len(daily_data) >= consolidation_days else daily_data
    if recent_daily.empty:
        return None
    
    if "high" not in recent_daily.columns or "low" not in recent_daily.columns:
        return None
    
    daily_high = float(pd.to_numeric(recent_daily["high"], errors="coerce").max() or 0.0)
    daily_low = float(pd.to_numeric(recent_daily["low"], errors="coerce").min() or 0.0)
    daily_mid = (daily_high + daily_low) / 2.0
    
    if daily_mid <= 0:
        return None
    
    amplitude_pct = (daily_high - daily_low) / daily_mid * 100.0
    if amplitude_pct > consolidation_amplitude_pct:
        return None
    
    consolidation_upper = daily_high
    consolidation_lower = daily_low
    
    first30 = df_1m[df_1m["datetime"] < first30_end].copy()
    if first30.empty or len(first30) < 25:
        return None
    
    intraday_low = float(pd.to_numeric(first30.get("low", pd.Series(dtype=float)), errors="coerce").min() or float("nan"))
    if not (intraday_low > 0):
        intraday_low = float(pd.to_numeric(first30["close"], errors="coerce").min() or float("nan"))
    if intraday_low <= 0:
        return None
    
    broke_lower = intraday_low < consolidation_lower
    
    stock_last_close = float(df_1m["close"].iloc[-1] or 0.0)
    broke_upper = stock_last_close > consolidation_upper
    
    if not broke_lower:
        return None
    
    pct_from_open = ((stock_last_close - open_px) / open_px * 100.0) if open_px > 0 and stock_last_close > 0 else float("nan")
    
    return {
        "open": open_px,
        "low_0930_1000": intraday_low,
        "low_0930_1000_pct": (intraday_low - open_px) / open_px * 100.0,
        "close_0959": float(first30["close"].iloc[-1] or 0.0),
        "close_0959_pct": (float(first30["close"].iloc[-1] or 0.0) - open_px) / open_px * 100.0,
        "cross_time": "N/A",
        "cross_minutes": 0,
        "last_close": stock_last_close,
        "pct_from_open": pct_from_open,
        "strategy_type": "breakout",
        "consolidation_amplitude_pct": amplitude_pct,
        "broke_lower": broke_lower,
        "broke_upper": broke_upper,
        "consolidation_upper": consolidation_upper,
        "consolidation_lower": consolidation_lower,
    }


def _detect_double_dip_strategy(
    df_1m: pd.DataFrame,
    trade_date: str,
) -> Optional[Dict]:
    """
    二次回踩型策略检测
    条件：
    1. 早盘出现第一次低点（跌幅 > 0.5%）
    2. 反弹后再次回踩，第二个低点高于第一个低点
    3. 第二次反弹站上开盘价并稳住
    """
    if df_1m is None or df_1m.empty:
        return None
    
    config = STRATEGY_CONFIGS.get("double_dip", {})
    first_dip_min_pct = config.get("first_dip_min_pct", 0.5)
    
    try:
        open_start = datetime.strptime(f"{trade_date} 09:30", "%Y%m%d %H:%M")
        first30_end = datetime.strptime(f"{trade_date} 10:00", "%Y%m%d %H:%M")
    except Exception:
        return None
    
    if "datetime" not in df_1m.columns or "open" not in df_1m.columns or "close" not in df_1m.columns:
        return None
    
    df_1m = df_1m.copy()
    df_1m = df_1m.dropna(subset=["datetime", "open", "close"])
    if df_1m.empty:
        return None
    
    if df_1m["datetime"].max().to_pydatetime() < first30_end:
        return None
    
    open_px = float(df_1m["open"].iloc[0] or 0.0)
    if open_px <= 0:
        return None
    
    if "low" not in df_1m.columns:
        return None
    
    lows = pd.to_numeric(df_1m["low"], errors="coerce")
    if lows.isna().all():
        lows = pd.to_numeric(df_1m["close"], errors="coerce")
    
    first_dip_idx = lows[:60].idxmin() if len(lows) >= 60 else lows.idxmin()
    if first_dip_idx is None or pd.isna(first_dip_idx):
        return None
    
    first_dip_low = float(lows.iloc[first_dip_idx] or 0.0)
    first_dip_pct = (first_dip_low - open_px) / open_px * 100.0
    
    if first_dip_pct > -first_dip_min_pct:
        return None
    
    rebound_after_first = df_1m.iloc[first_dip_idx + 1:first_dip_idx + 31] if first_dip_idx + 31 <= len(df_1m) else df_1m.iloc[first_dip_idx + 1:]
    if rebound_after_first.empty:
        return None
    
    rebound_high = float(pd.to_numeric(rebound_after_first["high"], errors="coerce").max() or first_dip_low)
    
    after_rebound = df_1m.iloc[first_dip_idx + 31:] if first_dip_idx + 31 < len(df_1m) else pd.DataFrame()
    if after_rebound.empty:
        return None
    
    second_dip_lows = pd.to_numeric(after_rebound["low"], errors="coerce")
    second_dip_idx_in_after = second_dip_lows.idxmin()
    if second_dip_idx_in_after is None or pd.isna(second_dip_idx_in_after):
        return None
    
    second_dip_low = float(second_dip_lows.loc[second_dip_idx_in_after] or 0.0)
    
    if second_dip_low <= first_dip_low:
        return None
    
    second_dip_pct = (second_dip_low - open_px) / open_px * 100.0
    
    final_close = float(df_1m["close"].iloc[-1] or 0.0)
    if final_close < open_px:
        return None
    
    pct_from_open = ((final_close - open_px) / open_px * 100.0) if open_px > 0 and final_close > 0 else float("nan")
    
    return {
        "open": open_px,
        "low_0930_1000": first_dip_low,
        "low_0930_1000_pct": first_dip_pct,
        "close_0959": float(df_1m[df_1m["datetime"] < first30_end]["close"].iloc[-1] or 0.0),
        "close_0959_pct": 0.0,
        "cross_time": "N/A",
        "cross_minutes": 0,
        "last_close": final_close,
        "pct_from_open": pct_from_open,
        "strategy_type": "double_dip",
        "first_dip_low": first_dip_low,
        "first_dip_pct": first_dip_pct,
        "second_dip_low": second_dip_low,
        "second_dip_pct": second_dip_pct,
        "dip_improvement": second_dip_low - first_dip_low,
    }


def _detect_market_sync_strategy(
    df_1m: pd.DataFrame,
    trade_date: str,
    market_data: Optional[Dict] = None,
) -> Optional[Dict]:
    """
    大盘同步型策略检测
    条件：
    1. 大盘同步出现V型反转
    2. 个股反弹幅度大于大盘
    3. 个股站稳时间足够
    """
    if df_1m is None or df_1m.empty or market_data is None:
        return None
    
    config = STRATEGY_CONFIGS.get("market_sync", {})
    market_drop_threshold = config.get("market_drop_threshold", 0.3)
    market_rebound_threshold = config.get("market_rebound_threshold", 0.1)
    
    try:
        open_start = datetime.strptime(f"{trade_date} 09:30", "%Y%m%d %H:%M")
        first30_end = datetime.strptime(f"{trade_date} 10:00", "%Y%m%d %H:%M")
    except Exception:
        return None
    
    if "datetime" not in df_1m.columns or "open" not in df_1m.columns or "close" not in df_1m.columns:
        return None
    
    df_1m = df_1m.copy()
    df_1m = df_1m.dropna(subset=["datetime", "open", "close"])
    if df_1m.empty:
        return None
    
    if df_1m["datetime"].max().to_pydatetime() < first30_end:
        return None
    
    open_px = float(df_1m["open"].iloc[0] or 0.0)
    if open_px <= 0:
        return None
    
    first30 = df_1m[df_1m["datetime"] < first30_end].copy()
    if first30.empty or len(first30) < 25:
        return None
    
    stock_low30 = float(pd.to_numeric(first30.get("low", pd.Series(dtype=float)), errors="coerce").min() or float("nan"))
    if not (stock_low30 > 0):
        stock_low30 = float(pd.to_numeric(first30["close"], errors="coerce").min() or float("nan"))
    if stock_low30 <= 0:
        return None
    
    stock_low30_pct = (stock_low30 - open_px) / open_px * 100.0
    stock_last_close = float(df_1m["close"].iloc[-1] or 0.0)
    stock_rebound_pct = ((stock_last_close - stock_low30) / stock_low30 * 100.0) if stock_low30 > 0 and stock_last_close > 0 else 0.0
    
    market_low_pct = market_data.get("market_low_pct", 0.0)
    market_rebound_pct = market_data.get("market_rebound_pct", 0.0)
    market_cross_open = market_data.get("market_cross_open", False)
    
    if abs(market_low_pct) < market_drop_threshold:
        return None
    
    if not market_cross_open:
        return None
    
    if market_rebound_pct < market_rebound_threshold:
        return None
    
    if stock_rebound_pct < market_rebound_pct:
        return None
    
    pct_from_open = ((stock_last_close - open_px) / open_px * 100.0) if open_px > 0 and stock_last_close > 0 else float("nan")
    
    return {
        "open": open_px,
        "low_0930_1000": stock_low30,
        "low_0930_1000_pct": stock_low30_pct,
        "close_0959": float(first30["close"].iloc[-1] or 0.0),
        "close_0959_pct": (float(first30["close"].iloc[-1] or 0.0) - open_px) / open_px * 100.0,
        "cross_time": market_data.get("market_cross_time", "N/A"),
        "cross_minutes": 0,
        "last_close": stock_last_close,
        "pct_from_open": pct_from_open,
        "strategy_type": "market_sync",
        "market_low_pct": market_low_pct,
        "market_rebound_pct": market_rebound_pct,
        "stock_vs_market_strength": stock_rebound_pct - market_rebound_pct,
    }


def _detect_all_strategies(
    df_1m: pd.DataFrame,
    trade_date: str,
    first30_drop_pct: float,
    first30_close_below_open_pct: float,
    min_rebound_pct: float,
    cross_above_open_pct: float,
    max_cross_minutes: int,
    hold_tolerance_pct: float,
    min_hold_minutes: int,
    enable_volume_validation: bool,
    min_volume_ratio: float,
    sector_data: Optional[Dict] = None,
    daily_data: Optional[pd.DataFrame] = None,
    market_data: Optional[Dict] = None,
    enable_double_dip: bool = False,
    quote_open_px: Optional[float] = None,
    chip_concentration_threshold: float = 15.0,
    min_winner_rate: float = 40.0,
) -> List[Dict]:
    if df_1m is None or df_1m.empty:
        return []
    
    base_params = {
        "first30_drop_pct": first30_drop_pct,
        "first30_close_below_open_pct": first30_close_below_open_pct,
        "min_rebound_pct": min_rebound_pct,
        "cross_above_open_pct": cross_above_open_pct,
        "max_cross_minutes": max_cross_minutes,
        "hold_tolerance_pct": hold_tolerance_pct,
        "min_hold_minutes": min_hold_minutes,
        "min_volume_ratio": min_volume_ratio,
        "chip_concentration_threshold": chip_concentration_threshold,
        "min_winner_rate": min_winner_rate,
    }
    
    strategies_to_check = ["standard", "deep_v", "volume_validated"]
    
    results = []
    for strategy_name in strategies_to_check:
        if strategy_name not in STRATEGY_CONFIGS:
            continue
        config = STRATEGY_CONFIGS[strategy_name].copy()
        params = base_params.copy()
        params.update({k: v for k, v in config.items() if k != "description" and k not in base_params.keys()})
        
        sig = _detect_single_strategy(
            df_1m=df_1m,
            trade_date=trade_date,
            strategy_type=strategy_name,
            quote_open_px=quote_open_px,
            **params
        )
        
        if sig is not None:
            results.append(sig)
    
    if sector_data is not None:
        sig = _detect_sector_sync_strategy(
            df_1m=df_1m,
            trade_date=trade_date,
            sector_data=sector_data,
        )
        if sig is not None:
            results.append(sig)
    
    if daily_data is not None:
        sig = _detect_breakout_strategy(
            df_1m=df_1m,
            trade_date=trade_date,
            daily_data=daily_data,
        )
        if sig is not None:
            results.append(sig)
    
    if enable_double_dip:
        sig = _detect_double_dip_strategy(
            df_1m=df_1m,
            trade_date=trade_date,
        )
        if sig is not None:
            results.append(sig)
    
    if market_data is not None:
        sig = _detect_market_sync_strategy(
            df_1m=df_1m,
            trade_date=trade_date,
            market_data=market_data,
        )
        if sig is not None:
            results.append(sig)
    
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="早盘 V 型反转策略")
    parser.add_argument("--trade-date", type=str, default=_trade_date_default(), help="交易日 YYYYMMDD")
    parser.add_argument("--asof-time", type=str, default=_asof_time_default_hhmm(), help="截止时间 HH:MM")
    parser.add_argument("--replay-datetime", type=str, help="复盘模式：指定日期和时间，格式 YYYYMMDD_HH:MM（例如：20260311_10:09）")
    parser.add_argument("--markets", type=str, default="0,1", help="市场: 0=深市,1=沪市,0,1=全市场")
    parser.add_argument("--exclude-st", action="store_true", help="剔除 ST 股票")
    parser.add_argument("--max-stocks", type=int, default=5000, help="最大扫描股票数")
    parser.add_argument("--topk", type=int, default=200, help="输出 Top N")
    parser.add_argument("--output", type=str, help="输出文件路径")
    
    parser.add_argument("--test-stock", type=str, help="测试单个股票代码（如：002300）")
    
    parser.add_argument("--strategy-type", type=str, default="standard", 
                       choices=["standard", "deep_v", "volume_validated", "sector_sync", "breakout", "double_dip", "market_sync"],
                       help="策略类型: standard=标准, deep_v=急跌深V, volume_validated=量能验证, sector_sync=板块联动, breakout=横盘突破, double_dip=二次回踩, market_sync=大盘同步")
    
    parser.add_argument("--first30-drop-pct", type=float, default=0.3, help="前30分钟最低价相对开盘价跌幅阈值(%%)")
    parser.add_argument("--first30-close-below-open-pct", type=float, default=0.05, help="10:00收盘价低于开盘价阈值(%%)")
    parser.add_argument("--min-rebound-pct", type=float, default=0.1, help="最小反弹幅度(%%)")
    parser.add_argument("--cross-above-open-pct", type=float, default=0.1, help="站上开盘价幅度(%%)")
    parser.add_argument("--max-cross-minutes", type=int, default=75, help="最大站上时间(分钟)")
    parser.add_argument("--hold-tolerance-pct", type=float, default=0.05, help="站稳容忍度(%%)")
    parser.add_argument("--min-hold-minutes", type=int, default=10, help="最小站稳时间(分钟)")
    
    parser.add_argument("--enable-volume-validation", action="store_true", help="启用量能验证")
    parser.add_argument("--min-volume-ratio", type=float, default=2.0, help="最小量比阈值")
    
    parser.add_argument("--enable-sector-sync", action="store_true", help="启用板块联动策略检测")
    parser.add_argument("--enable-market-sync", action="store_true", help="启用大盘同步策略检测")
    parser.add_argument("--enable-breakout", action="store_true", help="启用横盘突破策略检测")
    parser.add_argument("--enable-double-dip", action="store_true", help="启用二次回踩策略检测")
    parser.add_argument("--enable-chip-concentrated", action="store_true", help="启用筹码聚集策略检测")
    
    parser.add_argument("--chip-concentration-threshold", type=float, default=15.0, help="筹码集中度阈值(%%)")
    parser.add_argument("--min-winner-rate", type=float, default=40.0, help="最小胜率阈值(%%)")
    
    parser.add_argument("--enable-recent-bonus", action="store_true", default=True, help="启用近两日V型反转加分")
    parser.add_argument("--recent-drop-window-minutes", type=int, default=10, help="近期深跌窗口(分钟)")
    parser.add_argument("--recent-rebound-window-minutes", type=int, default=10, help="近期反弹窗口(分钟)")
    parser.add_argument("--recent-deep-drop-pct", type=float, default=2.5, help="近期深跌阈值(%%)")
    parser.add_argument("--recent-rebound-min-pct", type=float, default=2.0, help="近期反弹阈值(%%)")
    parser.add_argument("--recent-recover-ratio", type=float, default=0.6, help="近期回补比例")
    parser.add_argument("--recent-weight-yesterday", type=float, default=1.0, help="昨日权重")
    parser.add_argument("--recent-weight-daybefore", type=float, default=0.6, help="前日权重")
    parser.add_argument("--recent-bonus-points", type=float, default=1.2, help="加分系数")
    
    parser.add_argument("--prefilter-pct-from-open", type=float, default=0.2, help="预筛涨幅阈值(%%)")
    parser.add_argument("--quotes-chunk-size", type=int, default=200, help="行情快照分块大小")
    parser.add_argument("--quotes-sleep-s", type=float, default=0.1, help="行情快照间隔(秒)")
    parser.add_argument("--bars-max-total", type=int, default=800, help="分钟K最大获取数量")
    parser.add_argument("--bars-step", type=int, default=200, help="分钟K获取步长")
    parser.add_argument("--per-stock-sleep-s", type=float, default=0.02, help="每只股票间隔(秒)")
    
    args = parser.parse_args()
    
    if args.replay_datetime:
        try:
            replay_dt = datetime.strptime(args.replay_datetime, "%Y%m%d_%H:%M")
            args.trade_date = replay_dt.strftime("%Y%m%d")
            args.asof_time = replay_dt.strftime("%H:%M")
            print(f"{_now_ts()} 复盘模式：{args.trade_date} {args.asof_time}", flush=True)
        except ValueError:
            print(f"{_now_ts()} 错误：--replay-datetime 格式不正确，应为 YYYYMMDD_HH:MM（例如：20260311_10:09）", flush=True)
            return 1
    
    if args.strategy_type == "deep_v":
        args.first30_drop_pct = 0.8
        args.first30_close_below_open_pct = 0.2
        args.cross_above_open_pct = 0.3
        args.max_cross_minutes = 60
        args.hold_tolerance_pct = 0.0
        args.min_hold_minutes = 15
    elif args.strategy_type == "volume_validated":
        args.enable_volume_validation = True
        args.min_volume_ratio = 2.0
    
    out_path = args.output or os.path.join(os.path.dirname(__file__), f"早盘V型反转_{_now_ts()}.csv")
    
    with tdx:
        ep = connected_endpoint()
        if ep is not None:
            print(f"{_now_ts()} pytdx 已连接: {ep[0]}:{ep[1]}", flush=True)
        
        markets = _parse_markets(args.markets)
        df_codes = get_all_a_share_codes()
        if df_codes is None or df_codes.empty:
            print(f"{_now_ts()} 无股票代码", flush=True)
            return 1
        
        df_codes = df_codes[df_codes["market"].isin(list(markets))].copy()
        if bool(args.exclude_st) and "name" in df_codes.columns:
            df_codes = df_codes[~df_codes["name"].astype(str).str.upper().str.contains("ST", na=False)].copy()
        
        max_stocks = max(1, int(args.max_stocks))
        df_codes = df_codes.head(max_stocks).copy()
        
        # 如果指定了单个股票代码，就只测试这个股票
        if args.test_stock:
            test_stock = str(args.test_stock).zfill(6)
            df_codes = df_codes[df_codes["code"] == test_stock].copy()
            if df_codes.empty:
                print(f"{_now_ts()} 未找到股票代码: {test_stock}", flush=True)
                return 1
            print(f"{_now_ts()} 测试单个股票: {test_stock}", flush=True)
        
        trade_date = str(args.trade_date).strip()
        asof_time = str(args.asof_time).strip()
        today = datetime.now().strftime("%Y%m%d")
        
        print(f"{_now_ts()} 开始扫描: 共 {len(df_codes)} 只股票", flush=True)
        print(f"{_now_ts()} 交易日期: {trade_date}, 截止时间: {asof_time}", flush=True)
        
        if bool(args.enable_sector_sync) and _tushare_available:
            print(f"{_now_ts()} 正在构建申万行业映射...", flush=True)
            _build_sw_index_map()
            print(f"{_now_ts()} 申万行业映射构建完成，共 {len(_sw_index_map)} 个行业", flush=True)
        
        enabled_strategies = ["standard", "deep_v", "volume_validated"]
        if bool(args.enable_sector_sync):
            enabled_strategies.append("sector_sync")
        if bool(args.enable_market_sync):
            enabled_strategies.append("market_sync")
        if bool(args.enable_breakout):
            enabled_strategies.append("breakout")
        if bool(args.enable_double_dip):
            enabled_strategies.append("double_dip")
        if bool(args.enable_chip_concentrated):
            enabled_strategies.append("chip_concentrated")
        
        print(f"{_now_ts()} 检测策略: {', '.join(enabled_strategies)}", flush=True)
        print(f"{_now_ts()} 正在获取行情快照...", flush=True)
        
        start_offset_hint = _compute_start_offset_by_probe(
            tdx,
            trade_date=trade_date,
            asof_time=asof_time,
            probe_market=0,
            probe_code="000001",
            probe_count=800,
        )
        
        market_data = None
        if bool(args.enable_market_sync):
            print(f"{_now_ts()} 正在获取大盘分时数据...", flush=True)
            market_data = _fetch_market_intraday_data(
                tdx,
                trade_date=trade_date,
                asof_time=asof_time,
                market_key="sh",
                start_offset_hint=start_offset_hint,
            )
            if market_data:
                print(f"{_now_ts()} 大盘数据: 跌幅={market_data.get('market_low_pct', 0):.2f}%, 反弹={market_data.get('market_rebound_pct', 0):.2f}%", flush=True)
            else:
                print(f"{_now_ts()} 大盘数据获取失败，跳过大盘同步策略", flush=True)
        
        recent_trade_dates = []
        recent_offsets = {}
        if bool(args.enable_recent_bonus):
            recent_trade_dates, recent_offsets = _resolve_recent_trade_dates_with_offsets(
                tdx,
                base_trade_date=trade_date,
                want=2,
                asof_time="15:00",
                probe_market=0,
                probe_code="000001",
            )
        
        df_quotes = _quotes_snapshot_df(
            tdx,
            df_codes=df_codes,
            chunk_size=int(args.quotes_chunk_size),
            sleep_s=float(args.quotes_sleep_s),
        )
        
        print(f"{_now_ts()} 行情快照获取完成: {len(df_quotes)} 只股票", flush=True)
        
        if df_quotes is None or df_quotes.empty:
            print(f"{_now_ts()} 无行情数据", flush=True)
            return 1
        
        # 复盘模式下跳过预筛过滤（因为行情快照是实时数据，不能反映历史时间点）
        is_replay_mode = args.replay_datetime is not None
        if trade_date == today and not is_replay_mode:
            pre_count = len(df_quotes)
            df_quotes = df_quotes[pd.to_numeric(df_quotes["pct_from_open"], errors="coerce") >= float(args.prefilter_pct_from_open)].copy()
            print(f"{_now_ts()} 预筛过滤: {pre_count} -> {len(df_quotes)} 只 (涨幅 >= {args.prefilter_pct_from_open}%)", flush=True)
        elif is_replay_mode:
            print(f"{_now_ts()} 复盘模式：跳过基于实时行情快照的预筛过滤", flush=True)
        
        if df_quotes.empty:
            print(f"{_now_ts()} 预筛后无股票", flush=True)
            return 1
        
        print(f"{_now_ts()} 开始逐只股票检测V型反转信号...", flush=True)
        rows: List[Dict] = []
        miss_1m = 0
        miss_1m_samples: List[str] = []
        miss_1m_reason_cnt: Dict[str, int] = {}
        
        for i, (_, r) in enumerate(df_quotes.iterrows()):
            market = int(r["market"])
            code = str(r["code"]).zfill(6)
            name = str(r.get("name") or "").strip()
            quote_open_px = float(r.get("open") or 0.0) if pd.notna(r.get("open")) else None
            
            df_1m = _fetch_intraday_1m_bars(
                tdx,
                market=int(market),
                code=str(code).zfill(6),
                trade_date=trade_date,
                asof_time=asof_time,
                max_total=int(args.bars_max_total),
                step=int(args.bars_step),
                start_offset_hint=start_offset_hint,
            )
            
            if df_1m is not None and not df_1m.empty:
                df_1m.attrs["ts_code"] = f"{code}.SZ" if market == 0 else f"{code}.SH"
            
            if df_1m is None or df_1m.empty:
                miss_1m += 1
                miss_1m_samples.append(f"{code}.SZ" if market == 0 else f"{code}.SH")
                reason = "unknown"
                if trade_date != today:
                    try:
                        open_start = datetime.strptime(f"{trade_date} 09:30", "%Y%m%d %H:%M")
                        asof_dt = datetime.strptime(f"{trade_date} {asof_time}", "%Y%m%d %H:%M")
                    except Exception:
                        open_start = None
                        asof_dt = None
                    ok0, min0, max0 = _probe_1m_window(tdx, market, code, 0, 200)
                    if not ok0:
                        reason = "tdx无分钟K"
                    else:
                        if open_start is None or asof_dt is None:
                            reason = "过滤后为空"
                        else:
                            adj = _find_start_offset_for_window(
                                tdx,
                                market=int(market),
                                code=str(code).zfill(6),
                                open_start=open_start,
                                asof_dt=asof_dt,
                                start_offset_hint=start_offset_hint,
                                probe_count=200,
                                step=int(args.bars_step),
                                max_iter=8,
                            )
                            if adj is None:
                                reason = "历史窗口未覆盖"
                            else:
                                ok1, min1, max1 = _probe_1m_window(tdx, market, code, int(adj), 200)
                                if not ok1:
                                    reason = "历史窗口未覆盖"
                                elif max1 is not None and max1 < open_start:
                                    reason = "窗口过旧"
                                elif min1 is not None and min1 > asof_dt:
                                    reason = "窗口过新"
                                else:
                                    try:
                                        bars = tdx.get_security_bars(8, int(market), str(code).zfill(6), int(adj), 800)
                                    except Exception:
                                        bars = []
                                    df2 = tdx.to_df(bars) if bars else pd.DataFrame()
                                    if df2 is None or df2.empty or "datetime" not in df2.columns:
                                        reason = "过滤后为空"
                                    else:
                                        df2 = df2.copy()
                                        df2["datetime"] = pd.to_datetime(df2["datetime"], errors="coerce")
                                        df2 = df2.dropna(subset=["datetime"])
                                        if df2.empty:
                                            reason = "过滤后为空"
                                        elif not bool((df2["datetime"].dt.strftime("%Y%m%d") == trade_date).any()):
                                            reason = "当日无分钟K"
                                        else:
                                            reason = "过滤后为空"
                else:
                    ok0, _, _ = _probe_1m_window(tdx, market, code, 0, 50)
                    reason = "实时分钟K为空" if not ok0 else "过滤后为空"
                miss_1m_reason_cnt[reason] = int(miss_1m_reason_cnt.get(reason, 0)) + 1
                if float(args.per_stock_sleep_s) > 0:
                    time.sleep(float(args.per_stock_sleep_s))
                if (i + 1) % 50 == 0:
                    print(f"{_now_ts()} 进度: {i+1}/{len(df_quotes)} | 命中: {len(rows)} | 缺分钟K: {miss_1m}", flush=True)
                continue

            if trade_date != today and not is_replay_mode:
                try:
                    if quote_open_px is not None and quote_open_px > 0:
                        open_px = float(quote_open_px)
                    else:
                        open_px = float(pd.to_numeric(df_1m["open"].iloc[0], errors="coerce") or 0.0)
                    last_close = float(pd.to_numeric(df_1m["close"].iloc[-1], errors="coerce") or 0.0)
                    pct_from_open = ((last_close - open_px) / open_px * 100.0) if open_px > 0 and last_close > 0 else float("nan")
                except Exception:
                    pct_from_open = float("nan")
                if pd.isna(pct_from_open) or pct_from_open < float(args.prefilter_pct_from_open):
                    if float(args.per_stock_sleep_s) > 0:
                        time.sleep(float(args.per_stock_sleep_s))
                    if (i + 1) % 200 == 0:
                        print(f"{_now_ts()} 进度: {i+1}/{len(df_quotes)} 命中: {len(rows)} 缺分钟K: {miss_1m}", flush=True)
                    continue

            daily_data = None
            if bool(args.enable_breakout):
                daily_data = _fetch_daily_bars_for_breakout(
                    tdx,
                    market=int(market),
                    code=str(code).zfill(6),
                    days=30,
                )
            
            sector_data = None
            if bool(args.enable_sector_sync) and _tushare_available:
                stock_sector = _get_stock_sector(code)
                if stock_sector and stock_sector.get("l1_code"):
                    l1_code = stock_sector["l1_code"]
                    l1_name = stock_sector["l1_name"]
                    sector_data = _fetch_sw_sector_daily_data(l1_code, trade_date)
                    if sector_data:
                        sector_data["sector_name"] = l1_name
                        sector_data["sector_code"] = l1_code
            
            sigs = _detect_all_strategies(
                df_1m=df_1m,
                trade_date=trade_date,
                first30_drop_pct=float(args.first30_drop_pct),
                first30_close_below_open_pct=float(args.first30_close_below_open_pct),
                min_rebound_pct=float(args.min_rebound_pct),
                cross_above_open_pct=float(args.cross_above_open_pct),
                max_cross_minutes=int(args.max_cross_minutes),
                hold_tolerance_pct=float(args.hold_tolerance_pct),
                min_hold_minutes=int(args.min_hold_minutes),
                enable_volume_validation=bool(args.enable_volume_validation),
                min_volume_ratio=float(args.min_volume_ratio),
                sector_data=sector_data,
                daily_data=daily_data,
                market_data=market_data,
                enable_double_dip=bool(args.enable_double_dip),
                quote_open_px=quote_open_px,
                chip_concentration_threshold=float(args.chip_concentration_threshold),
                min_winner_rate=float(args.min_winner_rate),
            )
            
            if sigs:
                strategies_found = [sig["strategy_type"] for sig in sigs]
                print(f"{_now_ts()} 发现信号: {code} {name} -> {', '.join(strategies_found)} (涨幅: {sigs[0]['pct_from_open']:.2f}%)", flush=True)
            
            for sig in sigs:
                recent_total = 0.0
                recent_y = 0.0
                recent_p = 0.0
                if bool(args.enable_recent_bonus) and recent_trade_dates and recent_offsets:
                    recent_total, recent_y, recent_p = _recent_deepdrop_rebound_score(
                        tdx,
                        market=int(market),
                        code=str(code).zfill(6),
                        recent_trade_dates=list(recent_trade_dates),
                        recent_offsets=dict(recent_offsets),
                        drop_window_minutes=int(args.recent_drop_window_minutes),
                        rebound_window_minutes=int(args.recent_rebound_window_minutes),
                        deep_drop_pct=float(args.recent_deep_drop_pct),
                        min_rebound_pct=float(args.recent_rebound_min_pct),
                        min_recover_ratio=float(args.recent_recover_ratio),
                        weight_yesterday=float(args.recent_weight_yesterday),
                        weight_daybefore=float(args.recent_weight_daybefore),
                        bars_max_total=max(int(args.bars_max_total), 900),
                        bars_step=max(int(args.bars_step), 250),
                    )
                bonus = float(args.recent_bonus_points) * float(recent_total)
                score = float(sig["pct_from_open"]) + float(bonus)
                
                rows.append({
                    "ts_code": f"{code}.SZ" if market == 0 else f"{code}.SH",
                    "name": name,
                    "trade_date": trade_date,
                    "asof_time": asof_time,
                    "strategy_type": sig["strategy_type"],
                    "open": sig["open"],
                    "low_0930_1000": sig["low_0930_1000"],
                    "low_0930_1000_pct": sig["low_0930_1000_pct"],
                    "close_0959": sig["close_0959"],
                    "close_0959_pct": sig["close_0959_pct"],
                    "cross_time": sig["cross_time"],
                    "cross_minutes": sig["cross_minutes"],
                    "last_close": sig["last_close"],
                    "pct_from_open": sig["pct_from_open"],
                    "recent_deepdrop_rebound_total": recent_total,
                    "recent_deepdrop_rebound_yesterday": recent_y,
                    "recent_deepdrop_rebound_daybefore": recent_p,
                    "bonus_points": bonus,
                    "score": score,
                    "reason": f"{sig['strategy_type']}策略:前30分钟回撤{sig['low_0930_1000_pct']:.2f}%,随后站上开盘价并稳住",
                })

            if float(args.per_stock_sleep_s) > 0:
                time.sleep(float(args.per_stock_sleep_s))

            if (i + 1) % 50 == 0:
                print(f"{_now_ts()} 进度: {i+1}/{len(df_quotes)} | 命中: {len(rows)} | 缺分钟K: {miss_1m}", flush=True)

        df_out = pd.DataFrame(rows)
        if miss_1m > 0:
            samples = ",".join(miss_1m_samples)
            reasons = "; ".join([f"{k}={v}" for k, v in miss_1m_reason_cnt.items()])
            print(f"{_now_ts()} 缺分钟K样本: {samples}", flush=True)
            if reasons:
                print(f"{_now_ts()} 缺分钟K原因统计: {reasons}", flush=True)

        if df_out is None or df_out.empty:
            print(
                f"{_now_ts()} 未找到符合条件的标的（trade_date={trade_date}, asof_time={asof_time}, 缺分钟K={miss_1m}/{len(df_quotes)}）",
                flush=True,
            )
            return 0

        sort_cols = ["score", "pct_from_open"]
        for c in sort_cols:
            if c not in df_out.columns:
                df_out[c] = 0.0
        df_out = df_out.sort_values(sort_cols, ascending=False).reset_index(drop=True)
        topk = max(1, int(args.topk))
        df_out = df_out.head(topk).copy()
        df_out.to_csv(out_path, index=False)
        print(f"{_now_ts()} 输出: {out_path} (rows={len(df_out)})", flush=True)
        print(df_out.head(min(30, len(df_out))).to_string(index=False), flush=True)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())