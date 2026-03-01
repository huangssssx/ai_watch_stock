#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回测与参数寻优：突破后站稳3日 + 回踩关键位缩量低吸

目标：
- 用多年日线数据做历史回测
- 对关键阈值做网格搜索 + 迭代细化，直到最优参数稳定（收敛）或达到迭代上限

参数说明：
- --start-year: 回测开始年份（含）。会自动向前预热一段日线用于均线/滚动高点计算
- --end-year: 回测结束年份（含），默认=当前年份-1
- --max-stocks: 股票池数量上限（从 A 股列表顺序截断）
- --cache-dir: 日线缓存目录（csv.gz）。缓存命中时不会请求 pytdx
- --refresh-cache: 强制刷新缓存（重新从 pytdx 拉取并覆盖缓存）
- --fetch-sleep: 每只股票拉取日线后的 sleep 秒数（限频用；缓存命中时基本无效）
- --max-iters: 参数搜索迭代轮数（第1轮粗网格，后续轮细化）
- --top-k: 每轮保留的 Top 结果数量（写入 *_topk.csv）
- --out: 输出文件前缀（默认输出到脚本同目录，文件名带时间戳）

使用示例：
1) 小规模快速验证（建议先跑这个确认环境 OK）：
   python3 "backend/scripts/突破后站稳3日策略/3_回踩低吸参数回测.py" --start-year 2022 --end-year 2024 --max-stocks 50 --max-iters 1

2) 扩大到更多股票与多年：
   python3 "backend/scripts/突破后站稳3日策略/3_回踩低吸参数回测.py" --start-year 2019 --end-year 2025 --max-stocks 800 --max-iters 3
"""

import argparse
import itertools
import os
import sys
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

_script_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = None
_probe_dir = _script_dir
for _ in range(8):
    if os.path.exists(os.path.join(_probe_dir, "utils", "pytdx_client.py")):
        backend_dir = _probe_dir
        break
    parent = os.path.dirname(_probe_dir)
    if parent == _probe_dir:
        break
    _probe_dir = parent
if backend_dir is None:
    backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from utils.pytdx_client import tdx, connected_endpoint

try:
    from utils.tushare_client import pro
except Exception:
    pro = None


PYTDX_VOL_MULTIPLIER = 100


@dataclass(frozen=True)
class StockDef:
    market: int
    code: str
    name: str


@dataclass(frozen=True)
class StrategyParams:
    breakout_buffer_high60: float
    breakout_buffer_ma: float
    vol_ratio_min: float
    vol_ratio_max: float
    stand_buffer: float
    stand_days_min: int
    stand_vol_ratio_min: float
    pullback_min: float
    pullback_max: float
    vol_contract_ratio: float
    hard_stop_ratio: float
    take_profit_pct: float
    max_hold_days: int
    max_wait_days: int


@dataclass
class Trade:
    symbol: str
    name: str
    market: int
    key_level_type: str
    key_level: float
    breakout_date: str
    entry_date: str
    exit_date: str
    entry_price: float
    exit_price: float
    ret_pct: float
    exit_reason: str


@dataclass(frozen=True)
class PreparedStockData:
    stock: StockDef
    years: np.ndarray
    trade_date: np.ndarray
    open: np.ndarray
    close: np.ndarray
    high: np.ndarray
    low: np.ndarray
    vol: np.ndarray
    ma20_vol_prev: np.ndarray
    high60: np.ndarray
    high60_prev: np.ndarray
    ma60: np.ndarray
    ma60_prev: np.ndarray
    ma120: np.ndarray
    ma120_prev: np.ndarray


@dataclass(frozen=True)
class Setup:
    breakout_idx: int
    setup_end_idx: int
    key_level_type: str
    key_level: float
    breakout_date: str


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _cache_path(cache_dir: str, market: int, code: str) -> str:
    return os.path.join(cache_dir, f"daily_{int(market)}_{str(code).zfill(6)}.csv.gz")


def _daily_bars_full(market: int, code: str, min_date: Optional[str] = None) -> pd.DataFrame:
    code = str(code).zfill(6)
    min_dt = pd.to_datetime(min_date) if min_date else None
    chunk = 800
    start = 0
    parts: List[pd.DataFrame] = []

    while True:
        data = tdx.get_security_bars(9, int(market), code, int(start), int(chunk))
        df = tdx.to_df(data) if data else pd.DataFrame()
        if df is None or df.empty or "datetime" not in df.columns:
            break
        df = df.copy()
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        df = df.dropna(subset=["datetime"])
        for c in ("open", "close", "high", "low", "vol", "amount"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["open", "close", "high", "low", "vol"])
        df["vol"] = df["vol"] * PYTDX_VOL_MULTIPLIER
        parts.append(df)

        if min_dt is not None:
            oldest_dt = df["datetime"].min()
            if pd.notna(oldest_dt) and oldest_dt <= min_dt:
                break
        if len(df) < chunk:
            break
        start += chunk

    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True)
    out = out.drop_duplicates(subset=["datetime"]).sort_values("datetime", ascending=True).reset_index(drop=True)
    return out


def load_or_fetch_daily(
    cache_dir: str,
    market: int,
    code: str,
    min_date: str,
    refresh: bool,
    sleep_s: float,
) -> pd.DataFrame:
    _ensure_dir(cache_dir)
    p = _cache_path(cache_dir, market, code)
    if (not refresh) and os.path.exists(p):
        try:
            df = pd.read_csv(p)
            if df is not None and not df.empty and "datetime" in df.columns:
                df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
                df = df.dropna(subset=["datetime"]).sort_values("datetime", ascending=True).reset_index(drop=True)
                if len(df) >= 130:
                    return df
        except Exception:
            pass

    df = _daily_bars_full(market, code, min_date=min_date)
    if df is None or df.empty:
        return pd.DataFrame()
    keep_cols = [c for c in ["datetime", "open", "close", "high", "low", "vol", "amount"] if c in df.columns]
    df_out = df[keep_cols].copy()
    df_out.to_csv(p, index=False, encoding="utf-8", compression="gzip")
    if sleep_s and sleep_s > 0:
        time.sleep(float(sleep_s))
    return df_out


def calc_ma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window, min_periods=window).mean()


def calc_rolling_high(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window, min_periods=window).max()


def prepare_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or len(df) < 130:
        return df
    df = df.copy()
    df["ma20"] = calc_ma(df["close"], 20)
    df["ma60"] = calc_ma(df["close"], 60)
    df["ma120"] = calc_ma(df["close"], 120)
    df["ma20_vol"] = calc_ma(df["vol"], 20)
    df["high60"] = calc_rolling_high(df["high"], 60)
    df["ma60_prev"] = df["ma60"].shift(1)
    df["ma120_prev"] = df["ma120"].shift(1)
    df["ma20_vol_prev"] = df["ma20_vol"].shift(1)
    df["high60_prev"] = df["high60"].shift(1)
    return df


def get_key_levels(row: pd.Series) -> Dict[str, float]:
    levels: Dict[str, float] = {}
    if pd.notna(row.get("high60_prev")):
        levels["High60"] = float(row["high60_prev"])
    elif pd.notna(row.get("high60")):
        levels["High60"] = float(row["high60"])
    if pd.notna(row.get("ma60_prev")):
        levels["MA60"] = float(row["ma60_prev"])
    elif pd.notna(row.get("ma60")):
        levels["MA60"] = float(row["ma60"])
    if pd.notna(row.get("ma120_prev")):
        levels["MA120"] = float(row["ma120_prev"])
    elif pd.notna(row.get("ma120")):
        levels["MA120"] = float(row["ma120"])
    return levels


def detect_breakout(
    row: pd.Series,
    prev_row: pd.Series,
    params: StrategyParams,
) -> Optional[Dict[str, float]]:
    close = float(row.get("close", 0))
    prev_close = float(prev_row.get("close", 0))
    vol = float(row.get("vol", 0))
    ma20_vol = float(row.get("ma20_vol_prev", 0))
    if ma20_vol <= 0:
        return None
    vol_ratio = vol / ma20_vol
    if vol_ratio < params.vol_ratio_min or vol_ratio > params.vol_ratio_max:
        return None
    key_levels = get_key_levels(row)
    if not key_levels:
        return None
    for level_type, level_value in key_levels.items():
        if level_value <= 0:
            continue
        threshold = level_value * (params.breakout_buffer_high60 if level_type == "High60" else params.breakout_buffer_ma)
        if prev_close >= threshold:
            continue
        if close >= threshold:
            return {
                "key_level_type": level_type,
                "key_level": float(level_value),
                "breakout_price": float(close),
                "breakout_vol_ratio": float(vol_ratio),
            }
    return None


def check_stand_firm(df: pd.DataFrame, breakout_idx: int, key_level: float, params: StrategyParams) -> Optional[Dict[str, float]]:
    if breakout_idx + 3 >= len(df):
        return None
    stand_days = df.iloc[breakout_idx + 1 : breakout_idx + 4]
    if len(stand_days) < 3:
        return None
    closes = stand_days["close"].values
    vols = stand_days["vol"].values
    ma20_vols = stand_days["ma20_vol_prev"].values
    if key_level <= 0:
        return None
    min_close_ratio = float(min(closes)) / float(key_level)
    if min_close_ratio < params.stand_buffer:
        return None
    days_above = int(sum(1 for c in closes if float(c) >= float(key_level)))
    if days_above < int(params.stand_days_min):
        return None
    for i in range(3):
        if float(ma20_vols[i]) <= 0:
            return None
        if float(vols[i]) < float(ma20_vols[i]) * float(params.stand_vol_ratio_min):
            return None
    return {"stand_days_above": float(days_above), "min_close_ratio": float(min_close_ratio)}


def _pct_change(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a / b - 1.0) * 100.0


def simulate_trade_exit(
    df: pd.DataFrame,
    entry_idx: int,
    entry_price: float,
    anchor_level: float,
    params: StrategyParams,
) -> Tuple[int, float, str]:
    stop_price = float(anchor_level) * float(params.hard_stop_ratio)
    take_profit_price = float(entry_price) * (1.0 + float(params.take_profit_pct))
    last_idx = min(len(df) - 1, entry_idx + int(params.max_hold_days) - 1)
    for k in range(entry_idx, last_idx + 1):
        r = df.iloc[k]
        low = float(r["low"])
        high = float(r["high"])
        if low <= stop_price:
            return k, float(stop_price), "止损"
        if high >= take_profit_price:
            return k, float(take_profit_price), "止盈"
    r_last = df.iloc[last_idx]
    return last_idx, float(r_last["close"]), "时间止盈"


def backtest_one_stock(
    stock: StockDef,
    df_raw: pd.DataFrame,
    params: StrategyParams,
    start_year: int,
    end_year: int,
    indicators_ready: bool = False,
) -> List[Trade]:
    if df_raw is None or df_raw.empty or len(df_raw) < 260:
        return []
    if indicators_ready:
        df = df_raw
    else:
        df = prepare_indicators(df_raw)
        if df is None or df.empty or len(df) < 260:
            return []
    df = df.dropna(subset=["datetime", "open", "close", "high", "low", "vol"]).reset_index(drop=True)
    if "trade_date" not in df.columns:
        df["trade_date"] = pd.to_datetime(df["datetime"]).dt.strftime("%Y-%m-%d")

    trades: List[Trade] = []
    i = 130
    while i + 4 < len(df):
        dt = df.iloc[i]["datetime"]
        if not isinstance(dt, pd.Timestamp):
            dt = pd.to_datetime(dt, errors="coerce")
        if pd.isna(dt):
            i += 1
            continue
        if dt.year < start_year:
            i += 1
            continue
        if dt.year > end_year:
            break
        prev_row = df.iloc[i - 1]
        row = df.iloc[i]
        b = detect_breakout(row=row, prev_row=prev_row, params=params)
        if b is None:
            i += 1
            continue
        key_level = float(b["key_level"])
        stand = check_stand_firm(df=df, breakout_idx=i, key_level=key_level, params=params)
        if stand is None:
            i += 1
            continue

        setup_end_idx = i + 3
        anchor_level = float(key_level)
        hard_stop = anchor_level * float(params.hard_stop_ratio)
        breakout_date = str(pd.to_datetime(df.iloc[i]["datetime"]).strftime("%Y-%m-%d"))
        key_level_type = str(b["key_level_type"])
        breakout_price = float(b["breakout_price"])

        found_entry = False
        j_start = setup_end_idx + 1
        j_end = min(len(df) - 2, setup_end_idx + int(params.max_wait_days))
        for j in range(j_start, j_end + 1):
            last = df.iloc[j]
            prev = df.iloc[j - 1]
            last_close = float(last["close"])
            last_open = float(last["open"])
            last_high = float(last["high"])
            last_low = float(last["low"])
            last_vol = float(last["vol"])
            prev_vol = float(prev["vol"])
            prev_close = float(prev["close"])

            pullback_ratio = (anchor_level - last_close) / anchor_level if anchor_level > 0 else 0.0
            if pullback_ratio < float(params.pullback_min) or pullback_ratio > float(params.pullback_max):
                continue

            chg_pct = _pct_change(last_close, prev_close)
            gap_pct = _pct_change(last_open, prev_close)
            range_pct = _pct_change(last_high, last_low)
            broke_stop = (last_close < hard_stop) or (last_low < hard_stop)
            bad_k = (chg_pct <= -5.0) or (gap_pct <= -3.0) or (range_pct >= 9.0)
            is_volume_contract = last_vol <= prev_vol * float(params.vol_contract_ratio) if prev_vol > 0 else False

            if broke_stop or bad_k or (not is_volume_contract):
                continue

            entry_idx = j + 1
            entry_row = df.iloc[entry_idx]
            entry_price = float(entry_row["open"])
            entry_date = str(pd.to_datetime(entry_row["datetime"]).strftime("%Y-%m-%d"))
            exit_idx, exit_price, exit_reason = simulate_trade_exit(
                df=df, entry_idx=entry_idx, entry_price=entry_price, anchor_level=anchor_level, params=params
            )
            exit_row = df.iloc[exit_idx]
            exit_date = str(pd.to_datetime(exit_row["datetime"]).strftime("%Y-%m-%d"))
            ret_pct = (float(exit_price) / float(entry_price) - 1.0) * 100.0 if entry_price > 0 else 0.0
            trades.append(
                Trade(
                    symbol=stock.code,
                    name=stock.name,
                    market=int(stock.market),
                    key_level_type=key_level_type,
                    key_level=round(anchor_level, 6),
                    breakout_date=breakout_date,
                    entry_date=entry_date,
                    exit_date=exit_date,
                    entry_price=round(entry_price, 6),
                    exit_price=round(exit_price, 6),
                    ret_pct=round(ret_pct, 4),
                    exit_reason=exit_reason,
                )
            )
            found_entry = True
            i = exit_idx
            break

        if not found_entry:
            i = setup_end_idx + 1
    return trades


def build_setups_for_stock(
    data: PreparedStockData,
    fixed_params: StrategyParams,
    start_year: int,
    end_year: int,
) -> List[Setup]:
    years = data.years
    trade_date = data.trade_date
    close_ = data.close
    high60 = data.high60
    high60_prev = data.high60_prev
    ma60 = data.ma60
    ma60_prev = data.ma60_prev
    ma120 = data.ma120
    ma120_prev = data.ma120_prev
    vol = data.vol
    ma20_vol_prev = data.ma20_vol_prev

    n = int(len(close_))
    setups: List[Setup] = []
    i = 130
    while i + 4 < n:
        y = int(years[i])
        if y < int(start_year):
            i += 1
            continue
        if y > int(end_year):
            break

        ma20v = float(ma20_vol_prev[i])
        if not (ma20v > 0):
            i += 1
            continue
        vol_ratio = float(vol[i]) / ma20v
        if vol_ratio < float(fixed_params.vol_ratio_min) or vol_ratio > float(fixed_params.vol_ratio_max):
            i += 1
            continue

        prev_close = float(close_[i - 1])
        cur_close = float(close_[i])

        key_level_type = None
        key_level = None

        hv = float(high60_prev[i]) if np.isfinite(high60_prev[i]) else float(high60[i])
        if np.isfinite(hv) and hv > 0:
            threshold = hv * float(fixed_params.breakout_buffer_high60)
            if prev_close < threshold and cur_close >= threshold:
                key_level_type = "High60"
                key_level = hv

        if key_level is None:
            m60 = float(ma60_prev[i]) if np.isfinite(ma60_prev[i]) else float(ma60[i])
            if np.isfinite(m60) and m60 > 0:
                threshold = m60 * float(fixed_params.breakout_buffer_ma)
                if prev_close < threshold and cur_close >= threshold:
                    key_level_type = "MA60"
                    key_level = m60

        if key_level is None:
            m120 = float(ma120_prev[i]) if np.isfinite(ma120_prev[i]) else float(ma120[i])
            if np.isfinite(m120) and m120 > 0:
                threshold = m120 * float(fixed_params.breakout_buffer_ma)
                if prev_close < threshold and cur_close >= threshold:
                    key_level_type = "MA120"
                    key_level = m120

        if key_level is None or key_level <= 0:
            i += 1
            continue

        closes = close_[i + 1 : i + 4]
        vols = vol[i + 1 : i + 4]
        ma20vs = ma20_vol_prev[i + 1 : i + 4]
        if len(closes) < 3:
            i += 1
            continue

        min_close_ratio = float(np.min(closes)) / float(key_level)
        if min_close_ratio < float(fixed_params.stand_buffer):
            i += 1
            continue

        days_above = int(np.sum(closes >= float(key_level)))
        if days_above < int(fixed_params.stand_days_min):
            i += 1
            continue

        ok = True
        for k in range(3):
            ma20v_k = float(ma20vs[k])
            if not (ma20v_k > 0):
                ok = False
                break
            if float(vols[k]) < ma20v_k * float(fixed_params.stand_vol_ratio_min):
                ok = False
                break
        if not ok:
            i += 1
            continue

        setups.append(
            Setup(
                breakout_idx=int(i),
                setup_end_idx=int(i + 3),
                key_level_type=str(key_level_type),
                key_level=float(key_level),
                breakout_date=str(trade_date[i]),
            )
        )
        i += 1
    return setups


def backtest_one_stock_from_setups(
    data: PreparedStockData,
    setups: List[Setup],
    params: StrategyParams,
) -> List[Trade]:
    stock = data.stock
    trade_date = data.trade_date
    open_ = data.open
    close_ = data.close
    high = data.high
    low = data.low
    vol = data.vol

    n = int(len(close_))
    trades: List[Trade] = []
    cur_i = 130
    for setup in setups:
        if int(setup.breakout_idx) < int(cur_i):
            continue

        setup_end_idx = int(setup.setup_end_idx)
        anchor_level = float(setup.key_level)
        hard_stop = anchor_level * float(params.hard_stop_ratio)

        found_entry = False
        j_start = setup_end_idx + 1
        j_end = min(n - 2, setup_end_idx + int(params.max_wait_days))
        for j in range(int(j_start), int(j_end) + 1):
            last_close = float(close_[j])
            prev_close = float(close_[j - 1])
            pullback_ratio = (anchor_level - last_close) / anchor_level if anchor_level > 0 else 0.0
            if pullback_ratio < float(params.pullback_min) or pullback_ratio > float(params.pullback_max):
                continue

            last_open = float(open_[j])
            last_high = float(high[j])
            last_low = float(low[j])
            last_vol = float(vol[j])
            prev_vol = float(vol[j - 1])

            chg_pct = _pct_change(last_close, prev_close)
            gap_pct = _pct_change(last_open, prev_close)
            range_pct = _pct_change(last_high, last_low)
            broke_stop = (last_close < hard_stop) or (last_low < hard_stop)
            bad_k = (chg_pct <= -5.0) or (gap_pct <= -3.0) or (range_pct >= 9.0)
            is_volume_contract = last_vol <= prev_vol * float(params.vol_contract_ratio) if prev_vol > 0 else False

            if broke_stop or bad_k or (not is_volume_contract):
                continue

            entry_idx = j + 1
            entry_price = float(open_[entry_idx])
            entry_date = str(trade_date[entry_idx])

            stop_price = float(anchor_level) * float(params.hard_stop_ratio)
            take_profit_price = float(entry_price) * (1.0 + float(params.take_profit_pct))
            last_idx = min(n - 1, entry_idx + int(params.max_hold_days) - 1)

            exit_idx = int(last_idx)
            exit_price = float(close_[exit_idx])
            exit_reason = "时间止盈"
            for k in range(int(entry_idx), int(last_idx) + 1):
                if float(low[k]) <= stop_price:
                    exit_idx = int(k)
                    exit_price = float(stop_price)
                    exit_reason = "止损"
                    break
                if float(high[k]) >= take_profit_price:
                    exit_idx = int(k)
                    exit_price = float(take_profit_price)
                    exit_reason = "止盈"
                    break

            exit_date = str(trade_date[exit_idx])
            ret_pct = (float(exit_price) / float(entry_price) - 1.0) * 100.0 if entry_price > 0 else 0.0
            trades.append(
                Trade(
                    symbol=stock.code,
                    name=stock.name,
                    market=int(stock.market),
                    key_level_type=str(setup.key_level_type),
                    key_level=round(anchor_level, 6),
                    breakout_date=str(setup.breakout_date),
                    entry_date=entry_date,
                    exit_date=exit_date,
                    entry_price=round(entry_price, 6),
                    exit_price=round(exit_price, 6),
                    ret_pct=round(ret_pct, 4),
                    exit_reason=exit_reason,
                )
            )
            found_entry = True
            cur_i = int(exit_idx)
            break

        if not found_entry:
            cur_i = int(setup_end_idx + 1)
    return trades


def _iter_all_a_share_defs(max_stocks: int) -> List[StockDef]:
    rows: List[StockDef] = []
    for market in (0, 1):
        total = tdx.get_security_count(market)
        step = 1000
        for start in range(0, int(total), step):
            items = tdx.get_security_list(market, start) or []
            for r in items:
                code = str(r.get("code", "")).zfill(6)
                name = str(r.get("name", "")).strip()
                if not code:
                    continue
                if int(market) == 0:
                    if not code.startswith(("000", "001", "002", "003", "300", "301")):
                        continue
                if int(market) == 1:
                    if not code.startswith(("600", "601", "603", "605", "688")):
                        continue
                rows.append(StockDef(market=int(market), code=code, name=name))
                if max_stocks and len(rows) >= int(max_stocks):
                    return rows
    return rows


def _build_default_grid(center: Optional[StrategyParams] = None, refine: bool = False) -> List[StrategyParams]:
    if center is None:
        base = StrategyParams(
            breakout_buffer_high60=1.005,
            breakout_buffer_ma=1.015,
            vol_ratio_min=1.5,
            vol_ratio_max=4.0,
            stand_buffer=0.99,
            stand_days_min=2,
            stand_vol_ratio_min=0.5,
            pullback_min=-0.01,
            pullback_max=0.05,
            vol_contract_ratio=0.5,
            hard_stop_ratio=0.98,
            take_profit_pct=0.08,
            max_hold_days=10,
            max_wait_days=25,
        )
    else:
        base = center

    if not refine:
        pullback_min_list = [-0.02, -0.01]
        pullback_max_list = [0.05, 0.08, 0.10]
        vol_contract_list = [0.5, 0.6, 0.7, 0.8]
        take_profit_list = [0.06, 0.08, 0.10, 0.12]
        max_hold_days_list = [5, 10, 15]
        max_wait_days_list = [15, 25, 35]
    else:
        def around(v: float, steps: List[float], lb: float, ub: float) -> List[float]:
            out = sorted(set([min(ub, max(lb, v + s)) for s in steps] + [v]))
            return out

        pullback_min_list = around(float(base.pullback_min), steps=[-0.005, 0.0, 0.005], lb=-0.05, ub=0.0)
        pullback_max_list = around(float(base.pullback_max), steps=[-0.01, 0.0, 0.01], lb=0.02, ub=0.20)
        vol_contract_list = around(float(base.vol_contract_ratio), steps=[-0.05, 0.0, 0.05], lb=0.2, ub=1.0)
        take_profit_list = around(float(base.take_profit_pct), steps=[-0.02, 0.0, 0.02], lb=0.01, ub=0.30)
        max_hold_days_list = sorted(set([max(2, int(base.max_hold_days) + d) for d in (-3, 0, 3, 6)]))
        max_wait_days_list = sorted(set([max(5, int(base.max_wait_days) + d) for d in (-10, 0, 10)]))

    fixed = dict(
        breakout_buffer_high60=float(base.breakout_buffer_high60),
        breakout_buffer_ma=float(base.breakout_buffer_ma),
        vol_ratio_min=float(base.vol_ratio_min),
        vol_ratio_max=float(base.vol_ratio_max),
        stand_buffer=float(base.stand_buffer),
        stand_days_min=int(base.stand_days_min),
        stand_vol_ratio_min=float(base.stand_vol_ratio_min),
        hard_stop_ratio=float(base.hard_stop_ratio),
    )

    out: List[StrategyParams] = []
    for pb_min, pb_max, vc, tp, mhd, mwd in itertools.product(
        pullback_min_list,
        pullback_max_list,
        vol_contract_list,
        take_profit_list,
        max_hold_days_list,
        max_wait_days_list,
    ):
        if pb_min >= pb_max:
            continue
        out.append(
            StrategyParams(
                **fixed,
                pullback_min=float(pb_min),
                pullback_max=float(pb_max),
                vol_contract_ratio=float(vc),
                take_profit_pct=float(tp),
                max_hold_days=int(mhd),
                max_wait_days=int(mwd),
            )
        )
    return out


def _metrics_from_trades(trades: List[Trade]) -> Dict[str, float]:
    if not trades:
        return {
            "trades": 0.0,
            "win_rate": 0.0,
            "avg_ret": 0.0,
            "med_ret": 0.0,
            "min_ret": 0.0,
            "max_ret": 0.0,
            "profit_factor": 0.0,
            "score": -1e9,
        }
    rets = [float(t.ret_pct) for t in trades]
    wins = [r for r in rets if r > 0]
    losses = [r for r in rets if r <= 0]
    gross_profit = sum(wins)
    gross_loss = -sum(losses) if losses else 0.0
    pf = gross_profit / gross_loss if gross_loss > 0 else (gross_profit if gross_profit > 0 else 0.0)
    avg_ret = float(sum(rets) / len(rets))
    med_ret = float(pd.Series(rets).median())
    min_ret = float(min(rets))
    max_ret = float(max(rets))
    win_rate = float(len(wins) / len(rets))
    trades_n = float(len(rets))
    score = (avg_ret * trades_n) - (2.0 * abs(min_ret)) - (1.0 * max(0.0, 0.45 - win_rate) * 100.0)
    return {
        "trades": trades_n,
        "win_rate": win_rate,
        "avg_ret": avg_ret,
        "med_ret": med_ret,
        "min_ret": min_ret,
        "max_ret": max_ret,
        "profit_factor": float(pf),
        "score": float(score),
    }


def preload_stock_data(
    stocks: List[StockDef],
    cache_dir: str,
    refresh_cache: bool,
    fetch_sleep: float,
    min_date: str,
) -> List[PreparedStockData]:
    prepared: List[PreparedStockData] = []
    for s in stocks:
        df = load_or_fetch_daily(
            cache_dir=cache_dir,
            market=s.market,
            code=s.code,
            min_date=min_date,
            refresh=refresh_cache,
            sleep_s=fetch_sleep,
        )
        if df is None or df.empty:
            continue
        df2 = prepare_indicators(df)
        if df2 is None or df2.empty or len(df2) < 260:
            continue
        df2 = df2.dropna(subset=["datetime", "open", "close", "high", "low", "vol"]).reset_index(drop=True)
        dt = pd.to_datetime(df2["datetime"], errors="coerce")
        if dt.isna().any():
            df2 = df2.loc[~dt.isna()].reset_index(drop=True)
            dt = dt.loc[~dt.isna()].reset_index(drop=True)
        if df2 is None or df2.empty or len(df2) < 260:
            continue

        prepared.append(
            PreparedStockData(
                stock=s,
                years=dt.dt.year.to_numpy(dtype=np.int16, copy=True),
                trade_date=dt.dt.strftime("%Y-%m-%d").to_numpy(copy=True),
                open=df2["open"].to_numpy(dtype=float, copy=True),
                close=df2["close"].to_numpy(dtype=float, copy=True),
                high=df2["high"].to_numpy(dtype=float, copy=True),
                low=df2["low"].to_numpy(dtype=float, copy=True),
                vol=df2["vol"].to_numpy(dtype=float, copy=True),
                ma20_vol_prev=df2["ma20_vol_prev"].to_numpy(dtype=float, copy=True),
                high60=df2["high60"].to_numpy(dtype=float, copy=True),
                high60_prev=df2["high60_prev"].to_numpy(dtype=float, copy=True),
                ma60=df2["ma60"].to_numpy(dtype=float, copy=True),
                ma60_prev=df2["ma60_prev"].to_numpy(dtype=float, copy=True),
                ma120=df2["ma120"].to_numpy(dtype=float, copy=True),
                ma120_prev=df2["ma120_prev"].to_numpy(dtype=float, copy=True),
            )
        )
    return prepared


def evaluate_params(
    params: StrategyParams,
    stocks: List[StockDef],
    cache_dir: str,
    refresh_cache: bool,
    fetch_sleep: float,
    min_date: str,
    start_year: int,
    end_year: int,
) -> Tuple[Dict[str, float], pd.DataFrame]:
    all_trades: List[Trade] = []
    for s in stocks:
        df = load_or_fetch_daily(
            cache_dir=cache_dir,
            market=s.market,
            code=s.code,
            min_date=min_date,
            refresh=refresh_cache,
            sleep_s=fetch_sleep,
        )
        if df is None or df.empty:
            continue
        all_trades.extend(backtest_one_stock(stock=s, df_raw=df, params=params, start_year=start_year, end_year=end_year))

    if not all_trades:
        return _metrics_from_trades([]), pd.DataFrame()

    df_trades = pd.DataFrame([t.__dict__ for t in all_trades])
    df_trades["entry_year"] = df_trades["entry_date"].astype(str).str.slice(0, 4).astype(int)
    per_year = []
    for y in range(int(start_year), int(end_year) + 1):
        m = _metrics_from_trades([t for t in all_trades if int(t.entry_date[:4]) == y])
        m["year"] = float(y)
        per_year.append(m)
    df_year = pd.DataFrame(per_year)

    overall = _metrics_from_trades(all_trades)
    if df_year is not None and not df_year.empty:
        s = df_year["score"].astype(float)
        overall["score_median"] = float(s.median())
        overall["score_std"] = float(s.std(ddof=0))
        overall["score"] = float(overall["score_median"] - 0.5 * overall["score_std"])
    return overall, df_trades


def evaluate_params_with_setups(
    params: StrategyParams,
    prepared_setups: List[Tuple[PreparedStockData, List[Setup]]],
    start_year: int,
    end_year: int,
) -> Tuple[Dict[str, float], pd.DataFrame]:
    all_trades: List[Trade] = []
    for d, setups in prepared_setups:
        if setups:
            all_trades.extend(backtest_one_stock_from_setups(data=d, setups=setups, params=params))
    if not all_trades:
        return _metrics_from_trades([]), pd.DataFrame()

    df_trades = pd.DataFrame([t.__dict__ for t in all_trades])
    df_trades["entry_year"] = df_trades["entry_date"].astype(str).str.slice(0, 4).astype(int)
    per_year = []
    for y in range(int(start_year), int(end_year) + 1):
        m = _metrics_from_trades([t for t in all_trades if int(t.entry_date[:4]) == y])
        m["year"] = float(y)
        per_year.append(m)
    df_year = pd.DataFrame(per_year)

    overall = _metrics_from_trades(all_trades)
    if df_year is not None and not df_year.empty:
        s = df_year["score"].astype(float)
        overall["score_median"] = float(s.median())
        overall["score_std"] = float(s.std(ddof=0))
        overall["score"] = float(overall["score_median"] - 0.5 * overall["score_std"])
    return overall, df_trades


def format_params(p: StrategyParams) -> Dict[str, float]:
    return {
        "pullback_min": float(p.pullback_min),
        "pullback_max": float(p.pullback_max),
        "vol_contract_ratio": float(p.vol_contract_ratio),
        "hard_stop_ratio": float(p.hard_stop_ratio),
        "take_profit_pct": float(p.take_profit_pct),
        "max_hold_days": float(p.max_hold_days),
        "max_wait_days": float(p.max_wait_days),
    }


def main():
    parser = argparse.ArgumentParser(description="回踩低吸策略：多年回测 + 参数寻优")
    parser.add_argument("--start-year", type=int, default=2019, help="回测开始年份（含）；会自动向前预热均线窗口")
    parser.add_argument("--end-year", type=int, default=pd.Timestamp.today().year - 1, help="回测结束年份（含），默认=当前年份-1")
    parser.add_argument("--max-stocks", type=int, default=300, help="股票池数量上限（从 A 股列表顺序截断）")
    parser.add_argument("--cache-dir", type=str, default=os.path.join(_script_dir, "_cache_daily"), help="日线缓存目录（csv.gz）")
    parser.add_argument("--refresh-cache", action="store_true", help="强制刷新缓存（重新从 pytdx 拉取并覆盖）")
    parser.add_argument("--fetch-sleep", type=float, default=0.0, help="每只股票拉取日线后 sleep 秒数（限频用）")
    parser.add_argument("--max-iters", type=int, default=2, help="参数搜索迭代轮数（第1轮粗网格，后续细化）")
    parser.add_argument("--top-k", type=int, default=10, help="每轮保留的 Top 结果数量（写入 *_topk.csv）")
    parser.add_argument("--out", type=str, default="", help="输出文件前缀（默认脚本目录，文件名带时间戳）")
    args = parser.parse_args()

    start_year = int(args.start_year)
    end_year = int(args.end_year)
    if end_year < start_year:
        raise SystemExit("end-year 必须 >= start-year")

    warmup_days = 260
    min_date = (pd.Timestamp(year=start_year, month=1, day=1) - pd.Timedelta(days=warmup_days + 30)).strftime("%Y-%m-%d")

    print("=" * 60)
    print("回踩低吸：多年回测 + 参数寻优")
    print("=" * 60)
    print(f"pytdx connected_endpoint={connected_endpoint()}")
    print(f"区间: {start_year}..{end_year}")
    print(f"股票数量上限: {args.max_stocks}")
    print(f"缓存目录: {args.cache_dir}")
    print(f"min_date(含预热): {min_date}")
    print(f"max_iters={args.max_iters}, top_k={args.top_k}")

    with tdx:
        stocks = _iter_all_a_share_defs(max_stocks=int(args.max_stocks))
    if not stocks:
        print("股票池为空，无法回测")
        return

    with tdx:
        prepared = preload_stock_data(
            stocks=stocks,
            cache_dir=str(args.cache_dir),
            refresh_cache=bool(args.refresh_cache),
            fetch_sleep=float(args.fetch_sleep),
            min_date=min_date,
        )
    if not prepared:
        print("股票数据为空，无法回测")
        return

    best_params: Optional[StrategyParams] = None
    best_score = None
    history_rows = []

    for it in range(int(args.max_iters)):
        refine = it > 0
        prev_center = best_params
        grid = _build_default_grid(center=prev_center, refine=refine)
        print("-" * 60)
        print(f"第{it+1}轮参数搜索：候选={len(grid)} refine={refine}")

        def fixed_sig(p: StrategyParams) -> Tuple[float, float, float, float, float, int, float]:
            return (
                float(p.breakout_buffer_high60),
                float(p.breakout_buffer_ma),
                float(p.vol_ratio_min),
                float(p.vol_ratio_max),
                float(p.stand_buffer),
                int(p.stand_days_min),
                float(p.stand_vol_ratio_min),
            )

        sig0 = fixed_sig(grid[0])
        fixed_is_constant = all(fixed_sig(p) == sig0 for p in grid)
        prepared_setups = (
            [(d, build_setups_for_stock(d, grid[0], start_year, end_year)) for d in prepared] if fixed_is_constant else None
        )

        iter_results = []
        for idx, p in enumerate(grid, start=1):
            t0 = time.perf_counter()
            if prepared_setups is None:
                prepared_setups_p = [(d, build_setups_for_stock(d, p, start_year, end_year)) for d in prepared]
                metrics, _ = evaluate_params_with_setups(
                    params=p,
                    prepared_setups=prepared_setups_p,
                    start_year=start_year,
                    end_year=end_year,
                )
            else:
                metrics, _ = evaluate_params_with_setups(
                    params=p,
                    prepared_setups=prepared_setups,
                    start_year=start_year,
                    end_year=end_year,
                )
            elapsed = time.perf_counter() - t0
            row = {"iter": it + 1, "idx": idx, "elapsed_s": round(elapsed, 2)}
            row.update(format_params(p))
            row.update({k: float(v) for k, v in metrics.items()})
            iter_results.append((float(metrics.get("score", -1e9)), p, row))
            if idx % 10 == 0:
                print(f"进度: {idx}/{len(grid)} score={row['score']:.2f} trades={row['trades']:.0f} win_rate={row['win_rate']:.2f}")

        iter_results.sort(key=lambda x: x[0], reverse=True)
        top_k = min(int(args.top_k), len(iter_results))
        top = iter_results[:top_k]
        for _, _, r in top:
            history_rows.append(r)

        top1_score, top1_params, top1_row = iter_results[0]
        print("-" * 60)
        print("本轮Top结果(前5)：")
        df_top = pd.DataFrame([x[2] for x in iter_results[:5]])
        show_cols = [
            "score",
            "trades",
            "win_rate",
            "avg_ret",
            "min_ret",
            "profit_factor",
            "pullback_min",
            "pullback_max",
            "vol_contract_ratio",
            "take_profit_pct",
            "max_hold_days",
            "max_wait_days",
        ]
        show_cols = [c for c in show_cols if c in df_top.columns]
        print(df_top[show_cols].to_string(index=False))

        if best_score is None or float(top1_score) > float(best_score):
            best_score = float(top1_score)
            best_params = top1_params

        if (not refine) and int(args.max_iters) == 1:
            break

        if refine and prev_center is not None and top1_params == prev_center:
            print("收敛：最优参数在细化轮次保持不变")
            break

        best_params = top1_params

    print("=" * 60)
    print("最终最优参数：")
    print(pd.Series(format_params(best_params)).to_string() if best_params else "无")
    print(f"best_score={best_score}")

    out_dir = _script_dir
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_base = args.out.strip() or os.path.join(out_dir, f"回测寻优_{ts}")
    df_hist = pd.DataFrame(history_rows)
    if df_hist is not None and not df_hist.empty:
        df_hist = df_hist.sort_values(["score"], ascending=False).reset_index(drop=True)
        df_hist.to_csv(out_base + "_topk.csv", index=False, encoding="utf-8-sig")
        print(f"输出: {out_base + '_topk.csv'}")

    if best_params is None:
        return

    final_setups = [(d, build_setups_for_stock(d, best_params, start_year, end_year)) for d in prepared]
    best_metrics, df_trades = evaluate_params_with_setups(
        params=best_params,
        prepared_setups=final_setups,
        start_year=start_year,
        end_year=end_year,
    )
    if df_trades is not None and not df_trades.empty:
        df_trades = df_trades.sort_values(["entry_date", "symbol"], ascending=[True, True]).reset_index(drop=True)
        df_trades.to_csv(out_base + "_best_trades.csv", index=False, encoding="utf-8-sig")
        print(f"输出: {out_base + '_best_trades.csv'}")
        print("-" * 60)
        print("最优参数汇总指标：")
        print(pd.Series(best_metrics).to_string())


if __name__ == "__main__":
    main()
