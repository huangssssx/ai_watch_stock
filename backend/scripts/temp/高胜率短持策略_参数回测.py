#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多年度回测与寻优：短持为主（尽量高胜率/低回撤/尽量高收益）

使用示例：
1) 先小规模跑通：
   python3 "backend/scripts/temp/高胜率短持策略_参数回测.py" --start-year 2022 --end-year 2024 --max-stocks 50 --max-iters 1

2) 做高胜率导向的参数寻优（更慢，但通常能把 win_rate 拉高）：
   python3 "backend/scripts/temp/高胜率短持策略_参数回测.py" --start-year 2019 --end-year 2025 --max-stocks 800 --max-iters 2 --target-win-rate 0.8
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
    pullback_min: float
    pullback_max: float
    vol_contract_ratio: float
    take_profit_pct: float
    stop_loss_pct: float
    trail_stop_pct: float
    breakeven_after_pct: float
    exit_on_ma_fast_break: int
    max_hold_days: int
    max_gap_up_pct: float
    trend_ma_fast: int
    trend_ma_slow: int
    trend_ma_long: int
    use_tushare_features: int
    min_turnover_rate: float
    max_turnover_rate: float
    min_volume_ratio: float
    min_net_mf_amount: float
    min_net_mf_ratio: float


@dataclass(frozen=True)
class Trade:
    symbol: str
    name: str
    market: int
    signal_date: str
    entry_date: str
    exit_date: str
    hold_days: int
    entry_price: float
    exit_price: float
    ret_pct: float
    exit_reason: str


@dataclass(frozen=True)
class PreparedStockData:
    stock: StockDef
    trade_date: np.ndarray
    open_: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    vol: np.ndarray
    amount: np.ndarray
    ma_fast: np.ndarray
    ma_fast_prev: np.ndarray
    ma_slow: np.ndarray
    ma_slow_prev: np.ndarray
    ma_long: np.ndarray
    ma_long_prev: np.ndarray
    ma_vol: np.ndarray
    ma_vol_prev: np.ndarray
    turnover_rate: np.ndarray
    volume_ratio: np.ndarray
    net_mf_amount: np.ndarray
    up_limit: np.ndarray
    down_limit: np.ndarray


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _cache_path(cache_dir: str, market: int, code: str) -> str:
    return os.path.join(cache_dir, f"daily_{int(market)}_{str(code).zfill(6)}.csv.gz")


def _cache_path_tsfeat(cache_dir: str, market: int, code: str) -> str:
    return os.path.join(cache_dir, f"tsfeat_{int(market)}_{str(code).zfill(6)}.csv.gz")


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


def _ts_code(market: int, code: str) -> str:
    code = str(code).zfill(6)
    if int(market) == 0:
        return f"{code}.SZ"
    return f"{code}.SH"


def _load_active_codes_from_tushare() -> Optional[set[str]]:
    if pro is None:
        return None
    try:
        df = pro.stock_basic(exchange="", list_status="L", fields="ts_code,name")
        if df is None or df.empty:
            return None
        df = df.dropna(subset=["ts_code"]).copy()
        df["ts_code"] = df["ts_code"].astype(str).str.strip()
        df["name"] = df.get("name", "").astype(str).str.strip()
        df = df[~df["name"].str.contains("ST", na=False)]
        return set(df["ts_code"].tolist())
    except Exception:
        return None


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


def _fetch_tushare_features_one(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    if pro is None:
        return pd.DataFrame()
    ts_code = str(ts_code).strip()
    if not ts_code:
        return pd.DataFrame()

    parts = []
    try:
        df_basic = pro.daily_basic(
            ts_code=ts_code,
            start_date=str(start_date),
            end_date=str(end_date),
            fields="ts_code,trade_date,turnover_rate,volume_ratio",
        )
        if df_basic is not None and not df_basic.empty:
            parts.append(df_basic)
    except Exception:
        df_basic = pd.DataFrame()

    try:
        df_mf = pro.moneyflow(
            ts_code=ts_code,
            start_date=str(start_date),
            end_date=str(end_date),
            fields="ts_code,trade_date,net_mf_amount",
        )
        if df_mf is not None and not df_mf.empty:
            parts.append(df_mf)
    except Exception:
        df_mf = pd.DataFrame()

    try:
        df_limit = pro.stk_limit(
            ts_code=ts_code,
            start_date=str(start_date),
            end_date=str(end_date),
            fields="ts_code,trade_date,up_limit,down_limit",
        )
        if df_limit is not None and not df_limit.empty:
            parts.append(df_limit)
    except Exception:
        df_limit = pd.DataFrame()

    if not parts:
        return pd.DataFrame()

    out = None
    for p in parts:
        p = p.copy()
        if "trade_date" not in p.columns:
            continue
        p["trade_date"] = p["trade_date"].astype(str).str.strip()
        p = p.dropna(subset=["trade_date"])
        if out is None:
            out = p
        else:
            out = out.merge(p, on=["ts_code", "trade_date"], how="outer")
    if out is None or out.empty:
        return pd.DataFrame()
    out["datetime"] = pd.to_datetime(out["trade_date"], format="%Y%m%d", errors="coerce")
    out = out.dropna(subset=["datetime"]).drop_duplicates(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
    keep = ["datetime", "turnover_rate", "volume_ratio", "net_mf_amount", "up_limit", "down_limit"]
    for c in keep:
        if c not in out.columns:
            out[c] = np.nan
        if c != "datetime":
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out[keep].copy()


def load_or_fetch_tushare_features(
    cache_dir: str,
    market: int,
    code: str,
    start_date: str,
    end_date: str,
    refresh: bool,
    sleep_s: float,
) -> pd.DataFrame:
    if pro is None:
        return pd.DataFrame()
    _ensure_dir(cache_dir)
    p = _cache_path_tsfeat(cache_dir, market, code)
    if (not refresh) and os.path.exists(p):
        try:
            df = pd.read_csv(p)
            if df is not None and not df.empty and "datetime" in df.columns:
                df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
                df = df.dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
                return df
        except Exception:
            pass

    df = _fetch_tushare_features_one(ts_code=_ts_code(market, code), start_date=str(start_date), end_date=str(end_date))
    if df is None or df.empty:
        return pd.DataFrame()
    df.to_csv(p, index=False, encoding="utf-8", compression="gzip")
    if sleep_s and sleep_s > 0:
        time.sleep(float(sleep_s))
    return df


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
                if len(df) >= 260:
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


def _calc_ma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window, min_periods=window).mean()


def prepare_indicators(df: pd.DataFrame, params: StrategyParams) -> pd.DataFrame:
    if df is None or df.empty or len(df) < max(260, int(params.trend_ma_long) + 5):
        return df
    df = df.copy()
    if "amount" not in df.columns:
        df["amount"] = pd.to_numeric(df["close"], errors="coerce") * pd.to_numeric(df["vol"], errors="coerce")
    df["ma_fast"] = _calc_ma(df["close"], int(params.trend_ma_fast))
    df["ma_slow"] = _calc_ma(df["close"], int(params.trend_ma_slow))
    df["ma_long"] = _calc_ma(df["close"], int(params.trend_ma_long))
    df["ma_vol"] = _calc_ma(df["vol"], 20)
    df["ma_fast_prev"] = df["ma_fast"].shift(1)
    df["ma_slow_prev"] = df["ma_slow"].shift(1)
    df["ma_long_prev"] = df["ma_long"].shift(1)
    df["ma_vol_prev"] = df["ma_vol"].shift(1)
    return df


def _cost_rate_from_bps(bps: float) -> float:
    return float(bps) / 10000.0


def _net_ret_pct(entry_price: float, exit_price: float, buy_cost_rate: float, sell_cost_rate: float) -> float:
    if entry_price <= 0:
        return 0.0
    entry_cost = float(entry_price) * (1.0 + float(buy_cost_rate))
    exit_net = float(exit_price) * (1.0 - float(sell_cost_rate))
    return (exit_net / entry_cost - 1.0) * 100.0


def backtest_one_stock(
    data: PreparedStockData,
    params: StrategyParams,
    start_year: int,
    end_year: int,
    buy_cost_rate: float,
    sell_cost_rate: float,
) -> List[Trade]:
    n = len(data.trade_date)
    if n < 260:
        return []
    td = data.trade_date
    open_ = data.open_
    high = data.high
    low = data.low
    close = data.close
    vol = data.vol
    amount = data.amount
    ma_fast = data.ma_fast
    ma_fast_prev = data.ma_fast_prev
    ma_slow = data.ma_slow
    ma_slow_prev = data.ma_slow_prev
    ma_long = data.ma_long
    ma_long_prev = data.ma_long_prev
    ma_vol_prev = data.ma_vol_prev
    turnover_rate = data.turnover_rate
    volume_ratio = data.volume_ratio
    net_mf_amount = data.net_mf_amount
    up_limit = data.up_limit
    down_limit = data.down_limit
    warmup = max(130, int(params.trend_ma_long) + 2, 22)

    trades: List[Trade] = []
    i = int(warmup)
    while i + 1 < n:
        y = int(str(td[i])[:4])
        if y < int(start_year):
            i += 1
            continue
        if y > int(end_year):
            break

        if not (float(close[i]) > float(ma_fast[i]) > float(ma_slow[i])):
            i += 1
            continue

        if float(ma_fast[i]) <= 0:
            i += 1
            continue
        if float(close[i]) < float(ma_fast[i]):
            i += 1
            continue
        pullback = float(low[i]) / float(ma_fast[i]) - 1.0
        if not (float(params.pullback_min) <= float(pullback) <= float(params.pullback_max)):
            i += 1
            continue

        if float(ma_vol_prev[i]) <= 0:
            i += 1
            continue
        if float(params.vol_contract_ratio) > 0:
            if float(vol[i]) > float(ma_vol_prev[i]) * float(params.vol_contract_ratio):
                i += 1
                continue

        if int(params.use_tushare_features) > 0:
            tr = float(turnover_rate[i]) if np.isfinite(turnover_rate[i]) else np.nan
            vr = float(volume_ratio[i]) if np.isfinite(volume_ratio[i]) else np.nan
            nmf = float(net_mf_amount[i]) if np.isfinite(net_mf_amount[i]) else np.nan

            if float(params.min_turnover_rate) > 0:
                if not np.isfinite(tr) or tr < float(params.min_turnover_rate):
                    i += 1
                    continue
            if float(params.max_turnover_rate) > 0:
                if not np.isfinite(tr) or tr > float(params.max_turnover_rate):
                    i += 1
                    continue
            if float(params.min_volume_ratio) > 0:
                if not np.isfinite(vr) or vr < float(params.min_volume_ratio):
                    i += 1
                    continue
            if float(params.min_net_mf_amount) > 0:
                if not np.isfinite(nmf) or nmf < float(params.min_net_mf_amount):
                    i += 1
                    continue
            if float(params.min_net_mf_ratio) > 0:
                a_wan = float(amount[i]) / 10000.0 if float(amount[i]) > 0 else 0.0
                if a_wan <= 0 or (not np.isfinite(nmf)):
                    i += 1
                    continue
                if float(nmf) / float(a_wan) < float(params.min_net_mf_ratio):
                    i += 1
                    continue

        entry_idx = int(i + 1)
        entry_price = float(open_[entry_idx])
        if entry_price <= 0:
            i += 1
            continue
        if float(close[i]) > 0:
            gap_up = float(entry_price) / float(close[i]) - 1.0
            if float(gap_up) > float(params.max_gap_up_pct):
                i += 1
                continue

        take_profit_price = float(entry_price) * (1.0 + float(params.take_profit_pct))
        fixed_stop_price = float(entry_price) * (1.0 - float(params.stop_loss_pct)) if float(params.stop_loss_pct) > 0 else None
        last_idx = min(n - 1, entry_idx + int(params.max_hold_days) - 1)

        exit_idx = int(last_idx)
        exit_price = float(close[exit_idx])
        exit_reason = "时间止盈"
        best_high = float(entry_price)
        eps = 1e-9
        for k in range(int(entry_idx), int(last_idx) + 1):
            k_open = float(open_[k])
            k_high = float(high[k])
            k_low = float(low[k])
            k_close = float(close[k])

            if np.isfinite(up_limit[k]):
                ul = float(up_limit[k])
                k_high = min(k_high, ul)
                k_open = min(k_open, ul)
                k_close = min(k_close, ul)
            if np.isfinite(down_limit[k]):
                dl = float(down_limit[k])
                k_low = max(k_low, dl)
                k_open = max(k_open, dl)
                k_close = max(k_close, dl)

            best_high = max(float(best_high), float(k_high))
            trail_stop_price = (
                float(best_high) * (1.0 - float(params.trail_stop_pct)) if float(params.trail_stop_pct) > 0 else None
            )
            breakeven_price = (
                float(entry_price)
                if float(params.breakeven_after_pct) > 0
                and float(best_high) >= float(entry_price) * (1.0 + float(params.breakeven_after_pct))
                else None
            )
            stop_candidates = [x for x in (fixed_stop_price, trail_stop_price, breakeven_price) if x is not None]
            stop_price = max(stop_candidates) if stop_candidates else None

            if stop_price is not None:
                if np.isfinite(down_limit[k]) and float(open_[k]) <= float(down_limit[k]) + eps and float(close[k]) <= float(down_limit[k]) + eps:
                    exit_idx = int(k)
                    exit_price = float(k_close)
                    exit_reason = "跌停无法止损"
                    break
                if float(k_open) <= float(stop_price):
                    exit_idx = int(k)
                    exit_price = float(k_open)
                    exit_reason = "止损开盘"
                    break
                if float(k_low) <= float(stop_price):
                    exit_idx = int(k)
                    exit_price = float(stop_price)
                    exit_reason = "止损"
                    break
            if float(k_high) >= float(take_profit_price):
                exit_idx = int(k)
                exit_price = float(take_profit_price)
                exit_reason = "止盈"
                break
            if int(params.exit_on_ma_fast_break) > 0 and float(k_close) < float(ma_fast[k]):
                exit_idx = int(k)
                exit_price = float(k_close)
                exit_reason = "破均线"
                break

        ret_pct = _net_ret_pct(entry_price, exit_price, buy_cost_rate=buy_cost_rate, sell_cost_rate=sell_cost_rate)
        hold_days = int(exit_idx - entry_idx + 1)
        trades.append(
            Trade(
                symbol=data.stock.code,
                name=data.stock.name,
                market=int(data.stock.market),
                signal_date=str(td[i]),
                entry_date=str(td[entry_idx]),
                exit_date=str(td[exit_idx]),
                hold_days=int(hold_days),
                entry_price=round(entry_price, 6),
                exit_price=round(exit_price, 6),
                ret_pct=round(float(ret_pct), 6),
                exit_reason=str(exit_reason),
            )
        )
        i = int(exit_idx + 1)

    return trades


def _metrics_from_trades(trades: List[Trade]) -> Dict[str, float]:
    if not trades:
        return {
            "trades": 0.0,
            "win_rate": 0.0,
            "avg_ret": 0.0,
            "med_ret": 0.0,
            "min_ret": 0.0,
            "max_ret": 0.0,
            "avg_win_ret": 0.0,
            "avg_loss_ret": 0.0,
            "tp_hit_rate": 0.0,
            "avg_hold_days": 0.0,
            "med_hold_days": 0.0,
            "avg_ret_per_day": 0.0,
            "med_ret_per_day": 0.0,
            "profit_factor": 0.0,
            "score": -1e9,
        }

    rets = [float(t.ret_pct) for t in trades]
    wins = [r for r in rets if r > 0]
    losses = [r for r in rets if r <= 0]

    gross_profit = float(sum(wins))
    gross_loss = -float(sum(losses)) if losses else 0.0
    pf = gross_profit / gross_loss if gross_loss > 0 else (gross_profit if gross_profit > 0 else 0.0)

    avg_ret = float(sum(rets) / len(rets))
    med_ret = float(pd.Series(rets).median())
    min_ret = float(min(rets))
    max_ret = float(max(rets))
    win_rate = float(len(wins) / len(rets))
    trades_n = float(len(rets))

    avg_win_ret = float(sum(wins) / len(wins)) if wins else 0.0
    avg_loss_ret = float(sum(losses) / len(losses)) if losses else 0.0
    tp_hits = sum(1 for t in trades if str(t.exit_reason) == "止盈")
    tp_hit_rate = float(tp_hits / len(trades))

    holds = [int(t.hold_days) for t in trades]
    avg_hold_days = float(sum(holds) / len(holds)) if holds else 0.0
    med_hold_days = float(pd.Series(holds).median()) if holds else 0.0
    ret_per_day = [float(t.ret_pct) / max(1, int(t.hold_days)) for t in trades]
    avg_ret_per_day = float(sum(ret_per_day) / len(ret_per_day)) if ret_per_day else 0.0
    med_ret_per_day = float(pd.Series(ret_per_day).median()) if ret_per_day else 0.0

    score = (
        (win_rate * 600.0)
        - (abs(min_ret) * 80.0)
        - (abs(avg_loss_ret) * 150.0)
        - (avg_hold_days * 80.0)
        + (float(np.log1p(trades_n)) * 120.0)
        + (tp_hit_rate * 40.0)
        + (float(pf) * 20.0)
        + (avg_ret * 10.0)
        + (avg_ret_per_day * 80.0)
    )
    return {
        "trades": trades_n,
        "win_rate": win_rate,
        "avg_ret": avg_ret,
        "med_ret": med_ret,
        "min_ret": min_ret,
        "max_ret": max_ret,
        "avg_win_ret": float(avg_win_ret),
        "avg_loss_ret": float(avg_loss_ret),
        "tp_hit_rate": float(tp_hit_rate),
        "avg_hold_days": float(avg_hold_days),
        "med_hold_days": float(med_hold_days),
        "avg_ret_per_day": float(avg_ret_per_day),
        "med_ret_per_day": float(med_ret_per_day),
        "profit_factor": float(pf),
        "score": float(score),
    }


def _build_default_grid(
    center: Optional[StrategyParams],
    refine: bool,
    tp_min: float,
    tp_max: float,
    hold_min: int,
    hold_max: int,
    max_gap_up_pct: float,
    use_tushare_features: int,
    min_turnover_rate: float,
    max_turnover_rate: float,
    min_volume_ratio: float,
    min_net_mf_amount: float,
    min_net_mf_ratio: float,
) -> List[StrategyParams]:
    tp_min = float(tp_min)
    tp_max = float(tp_max)
    hold_min = int(hold_min)
    hold_max = int(hold_max)
    max_gap_up_pct = float(max_gap_up_pct)

    if center is None or not refine:
        pullback_min = [-0.04, -0.03, -0.02, -0.01]
        pullback_max = [-0.005, 0.0, 0.01]
        vol_contract_ratio = [0.0, 0.75, 1.0, 1.25]
        take_profit_pct = [
            v for v in [0.006, 0.008, 0.01, 0.012, 0.015, 0.02, 0.03, 0.04, 0.05] if tp_min <= float(v) <= tp_max
        ]
        stop_loss_pct = [0.006, 0.008, 0.01, 0.012, 0.015, 0.02, 0.025, 0.03]
        trail_stop_pct = [0.006, 0.008, 0.01, 0.012, 0.015]
        breakeven_after_pct = [0.006, 0.008, 0.01, 0.012]
        exit_on_ma_fast_break = [1]
        max_hold_days = list(range(max(1, hold_min), max(1, hold_max) + 1, 1)) or [max(1, hold_min)]
        trend_ma_fast = [20]
        trend_ma_slow = [60]
        trend_ma_long = [120]
    else:
        def around(v: float, steps: List[float]) -> List[float]:
            return sorted({round(float(v) + float(s), 6) for s in steps})

        pullback_min = around(center.pullback_min, [-0.02, -0.01, -0.005, 0.0, 0.005, 0.01])
        pullback_max = around(center.pullback_max, [-0.01, -0.005, 0.0, 0.005, 0.01])
        vol_contract_ratio = around(center.vol_contract_ratio, [-0.2, -0.1, -0.05, 0.0, 0.05, 0.1, 0.2])
        take_profit_pct = around(center.take_profit_pct, [-0.01, -0.006, -0.004, -0.002, 0.0, 0.002, 0.004, 0.006, 0.01])
        stop_loss_pct = around(center.stop_loss_pct, [-0.01, -0.006, -0.004, -0.002, 0.0, 0.002, 0.004, 0.006, 0.01])
        trail_stop_pct = around(center.trail_stop_pct, [-0.01, -0.006, -0.004, -0.002, 0.0, 0.002, 0.004, 0.006, 0.01])
        breakeven_after_pct = around(center.breakeven_after_pct, [-0.01, -0.006, -0.004, -0.002, 0.0, 0.002, 0.004, 0.006, 0.01])
        exit_on_ma_fast_break = [center.exit_on_ma_fast_break]
        max_hold_days = sorted({max(1, int(center.max_hold_days) + d) for d in (-2, -1, 0, 1, 2)})
        trend_ma_fast = [center.trend_ma_fast]
        trend_ma_slow = [center.trend_ma_slow]
        trend_ma_long = [center.trend_ma_long]

    pullback_min = [float(x) for x in pullback_min]
    pullback_max = [float(x) for x in pullback_max]
    vol_contract_ratio = [float(x) for x in vol_contract_ratio]
    take_profit_pct = [float(x) for x in take_profit_pct if tp_min <= float(x) <= tp_max]
    stop_loss_pct = [float(x) for x in stop_loss_pct if float(x) >= 0.0]
    trail_stop_pct = [float(x) for x in trail_stop_pct if float(x) >= 0.0]
    breakeven_after_pct = [float(x) for x in breakeven_after_pct if float(x) >= 0.0]
    exit_on_ma_fast_break = [int(x) for x in exit_on_ma_fast_break]
    max_hold_days = [int(x) for x in max_hold_days if hold_min <= int(x) <= hold_max]

    if not take_profit_pct:
        take_profit_pct = [tp_min]
    if not max_hold_days:
        max_hold_days = [hold_min]

    grid = []
    for a, b, c, tp, sl, ts, be, exit_ma, mh, f, s, l in itertools.product(
        pullback_min,
        pullback_max,
        vol_contract_ratio,
        take_profit_pct,
        stop_loss_pct,
        trail_stop_pct,
        breakeven_after_pct,
        exit_on_ma_fast_break,
        max_hold_days,
        trend_ma_fast,
        trend_ma_slow,
        trend_ma_long,
    ):
        if float(b) < float(a):
            continue
        if float(tp) < tp_min or float(tp) > tp_max:
            continue
        if float(tp) <= 0 or float(sl) < 0 or int(mh) < 1:
            continue
        if float(sl) > 0.30:
            continue
        grid.append(
            StrategyParams(
                pullback_min=float(a),
                pullback_max=float(b),
                vol_contract_ratio=float(c),
                take_profit_pct=float(tp),
                stop_loss_pct=float(sl),
                trail_stop_pct=float(ts),
                breakeven_after_pct=float(be),
                exit_on_ma_fast_break=int(exit_ma),
                max_hold_days=int(mh),
                max_gap_up_pct=float(center.max_gap_up_pct if center is not None else max_gap_up_pct),
                trend_ma_fast=int(f),
                trend_ma_slow=int(s),
                trend_ma_long=int(l),
                use_tushare_features=int(center.use_tushare_features if center is not None else use_tushare_features),
                min_turnover_rate=float(center.min_turnover_rate if center is not None else min_turnover_rate),
                max_turnover_rate=float(center.max_turnover_rate if center is not None else max_turnover_rate),
                min_volume_ratio=float(center.min_volume_ratio if center is not None else min_volume_ratio),
                min_net_mf_amount=float(center.min_net_mf_amount if center is not None else min_net_mf_amount),
                min_net_mf_ratio=float(center.min_net_mf_ratio if center is not None else min_net_mf_ratio),
            )
        )
    return grid


def preload_stock_data(
    stocks: List[StockDef],
    cache_dir: str,
    refresh_cache: bool,
    fetch_sleep: float,
    ts_refresh_cache: bool,
    ts_fetch_sleep: float,
    min_date: str,
    max_date: str,
    params_for_indicators: StrategyParams,
) -> List[PreparedStockData]:
    prepared: List[PreparedStockData] = []
    with tdx:
        connected_endpoint()
        for idx, s in enumerate(stocks, start=1):
            df = load_or_fetch_daily(
                cache_dir=cache_dir,
                market=int(s.market),
                code=str(s.code),
                min_date=min_date,
                refresh=bool(refresh_cache),
                sleep_s=float(fetch_sleep),
            )
            if df is None or df.empty:
                continue
            df = prepare_indicators(df, params=params_for_indicators)
            if df is None or df.empty:
                continue
            if int(params_for_indicators.use_tushare_features) > 0 and pro is not None:
                df_ts = load_or_fetch_tushare_features(
                    cache_dir=cache_dir,
                    market=int(s.market),
                    code=str(s.code),
                    start_date=str(pd.to_datetime(min_date).strftime("%Y%m%d")),
                    end_date=str(pd.to_datetime(max_date).strftime("%Y%m%d")),
                    refresh=bool(ts_refresh_cache),
                    sleep_s=float(ts_fetch_sleep),
                )
                if df_ts is not None and not df_ts.empty:
                    df = df.merge(df_ts, on="datetime", how="left")
                else:
                    for c in ["turnover_rate", "volume_ratio", "net_mf_amount", "up_limit", "down_limit"]:
                        if c not in df.columns:
                            df[c] = np.nan
            else:
                for c in ["turnover_rate", "volume_ratio", "net_mf_amount", "up_limit", "down_limit"]:
                    if c not in df.columns:
                        df[c] = np.nan
            df = df.dropna(subset=["datetime", "open", "close", "high", "low", "vol", "ma_fast", "ma_slow", "ma_long", "ma_vol"]).reset_index(
                drop=True
            )
            if len(df) < 260:
                continue
            td = pd.to_datetime(df["datetime"]).dt.strftime("%Y-%m-%d").astype(str).values
            if "amount" not in df.columns:
                df["amount"] = df["close"].astype(float).values * df["vol"].astype(float).values
            prepared.append(
                PreparedStockData(
                    stock=s,
                    trade_date=td,
                    open_=df["open"].astype(float).values,
                    high=df["high"].astype(float).values,
                    low=df["low"].astype(float).values,
                    close=df["close"].astype(float).values,
                    vol=df["vol"].astype(float).values,
                    amount=df["amount"].astype(float).values,
                    ma_fast=df["ma_fast"].astype(float).values,
                    ma_fast_prev=df["ma_fast_prev"].astype(float).values,
                    ma_slow=df["ma_slow"].astype(float).values,
                    ma_slow_prev=df["ma_slow_prev"].astype(float).values,
                    ma_long=df["ma_long"].astype(float).values,
                    ma_long_prev=df["ma_long_prev"].astype(float).values,
                    ma_vol=df["ma_vol"].astype(float).values,
                    ma_vol_prev=df["ma_vol_prev"].astype(float).values,
                    turnover_rate=df["turnover_rate"].astype(float).values,
                    volume_ratio=df["volume_ratio"].astype(float).values,
                    net_mf_amount=df["net_mf_amount"].astype(float).values,
                    up_limit=df["up_limit"].astype(float).values,
                    down_limit=df["down_limit"].astype(float).values,
                )
            )
            if idx % 200 == 0:
                print(f"预加载进度: {idx}/{len(stocks)} 已准备={len(prepared)}")
    return prepared


def evaluate_params(
    params: StrategyParams,
    prepared: List[PreparedStockData],
    start_year: int,
    end_year: int,
    buy_cost_rate: float,
    sell_cost_rate: float,
    min_trades: int,
    target_win_rate: float,
    min_profit_factor: float,
    max_abs_min_ret: float,
    max_abs_avg_loss_ret: float,
) -> Tuple[Dict[str, float], pd.DataFrame]:
    all_trades: List[Trade] = []
    for d in prepared:
        all_trades.extend(
            backtest_one_stock(
                data=d,
                params=params,
                start_year=start_year,
                end_year=end_year,
                buy_cost_rate=buy_cost_rate,
                sell_cost_rate=sell_cost_rate,
            )
        )
    m = _metrics_from_trades(all_trades)
    penalty = 0.0
    trades_n = float(m.get("trades", 0.0))
    pf = float(m.get("profit_factor", 0.0))
    wr = float(m.get("win_rate", 0.0))
    min_ret = float(m.get("min_ret", 0.0))
    avg_loss_ret = float(m.get("avg_loss_ret", 0.0))
    feasible = 1.0
    if trades_n < float(min_trades):
        penalty += (float(min_trades) - trades_n) * 2.0
        feasible = 0.0
    if pf < float(min_profit_factor):
        penalty += (float(min_profit_factor) - pf) * 2000.0
        feasible = 0.0
    if float(target_win_rate) > 0.0 and wr < float(target_win_rate):
        penalty += (float(target_win_rate) - wr) * 10000.0
        feasible = 0.0
    if float(max_abs_min_ret) > 0.0 and abs(float(min_ret)) > float(max_abs_min_ret):
        penalty += (abs(float(min_ret)) - float(max_abs_min_ret)) * 200.0
        feasible = 0.0
    if float(max_abs_avg_loss_ret) > 0.0 and abs(float(avg_loss_ret)) > float(max_abs_avg_loss_ret):
        penalty += (abs(float(avg_loss_ret)) - float(max_abs_avg_loss_ret)) * 100.0
        feasible = 0.0

    base_score = float(m.get("score", -1e9))
    m["score"] = (base_score - float(penalty)) if feasible > 0.0 else (-1e9 - float(penalty))
    m["penalty"] = float(penalty)
    m["feasible"] = float(feasible)
    df = pd.DataFrame([t.__dict__ for t in all_trades]) if all_trades else pd.DataFrame()
    return m, df


def format_params(p: StrategyParams) -> Dict[str, float]:
    return {
        "pullback_min": float(p.pullback_min),
        "pullback_max": float(p.pullback_max),
        "vol_contract_ratio": float(p.vol_contract_ratio),
        "take_profit_pct": float(p.take_profit_pct),
        "stop_loss_pct": float(p.stop_loss_pct),
        "trail_stop_pct": float(p.trail_stop_pct),
        "breakeven_after_pct": float(p.breakeven_after_pct),
        "exit_on_ma_fast_break": float(p.exit_on_ma_fast_break),
        "max_hold_days": float(p.max_hold_days),
        "max_gap_up_pct": float(p.max_gap_up_pct),
        "trend_ma_fast": float(p.trend_ma_fast),
        "trend_ma_slow": float(p.trend_ma_slow),
        "trend_ma_long": float(p.trend_ma_long),
        "use_tushare_features": float(p.use_tushare_features),
        "min_turnover_rate": float(p.min_turnover_rate),
        "max_turnover_rate": float(p.max_turnover_rate),
        "min_volume_ratio": float(p.min_volume_ratio),
        "min_net_mf_amount": float(p.min_net_mf_amount),
        "min_net_mf_ratio": float(p.min_net_mf_ratio),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="短持为主：多年回测 + 参数寻优")
    parser.add_argument("--start-year", type=int, default=2019, help="回测开始年份（含）；会自动向前预热均线窗口")
    parser.add_argument("--end-year", type=int, default=pd.Timestamp.today().year - 1, help="回测结束年份（含），默认=当前年份-1")
    parser.add_argument("--max-stocks", type=int, default=300, help="股票池数量上限（从 A 股列表顺序截断）")
    parser.add_argument("--cache-dir", type=str, default=os.path.join(_script_dir, "_cache_daily"), help="日线缓存目录（csv.gz）")
    parser.add_argument("--refresh-cache", action="store_true", help="强制刷新缓存（重新从 pytdx 拉取并覆盖）")
    parser.add_argument("--fetch-sleep", type=float, default=0.0, help="每只股票拉取日线后 sleep 秒数（限频用）")
    parser.add_argument("--max-iters", type=int, default=4, help="参数搜索迭代轮数（第1轮粗网格，后续细化，收敛会提前停止）")
    parser.add_argument("--top-k", type=int, default=10, help="每轮保留的 Top 结果数量（写入 *_topk.csv）")
    parser.add_argument("--out", type=str, default="", help="输出文件前缀（默认脚本目录，文件名带时间戳）")

    parser.add_argument("--tp-min", type=float, default=0.01, help="止盈下限（例如 0.01 表示 +1%）")
    parser.add_argument("--tp-max", type=float, default=0.05, help="止盈上限（例如 0.05 表示 +5%）")
    parser.add_argument("--hold-min", type=int, default=1, help="最大持有天数搜索下限")
    parser.add_argument("--hold-max", type=int, default=6, help="最大持有天数搜索上限")
    parser.add_argument("--max-gap-up-pct", type=float, default=0.015, help="入场日开盘相对信号日收盘最大跳空比例")

    parser.add_argument("--min-trades", type=int, default=200, help="参数筛选的最小交易数（避免样本过小）")
    parser.add_argument("--target-win-rate", type=float, default=0.8, help="胜率目标（可为 0，表示不硬性约束）")
    parser.add_argument("--min-profit-factor", type=float, default=1.0, help="参数筛选的最小盈亏比（利润因子）")
    parser.add_argument("--max-abs-min-ret", type=float, default=6.0, help="单笔最大允许亏损幅度（百分比；0 表示不约束）")
    parser.add_argument("--max-abs-avg-loss-ret", type=float, default=2.5, help="亏损单平均亏损幅度（百分比；0 表示不约束）")

    parser.add_argument("--use-tushare-features", action="store_true", help="使用 Tushare 的换手/量比/资金流/涨跌停价增强过滤与模拟")
    parser.add_argument("--ts-refresh-cache", action="store_true", help="强制刷新 Tushare 特征缓存（覆盖 tsfeat_*.csv.gz）")
    parser.add_argument("--ts-fetch-sleep", type=float, default=0.0, help="每只股票拉取 Tushare 特征后 sleep 秒数（限频用）")
    parser.add_argument("--turnover-min", type=float, default=0.0, help="信号日换手率下限（%）；0 表示不启用")
    parser.add_argument("--turnover-max", type=float, default=0.0, help="信号日换手率上限（%）；0 表示不启用")
    parser.add_argument("--vr-min", type=float, default=0.0, help="信号日量比下限；0 表示不启用")
    parser.add_argument("--netmf-min", type=float, default=0.0, help="信号日净流入额下限（万元）；0 表示不启用")
    parser.add_argument("--netmf-ratio-min", type=float, default=0.0, help="信号日净流入/成交额下限（净流入万元 / 成交额万元）；0 表示不启用")

    parser.add_argument("--buy-fee-bps", type=float, default=3.0, help="买入总成本（bps），例如佣金/滑点等")
    parser.add_argument("--sell-fee-bps", type=float, default=13.0, help="卖出总成本（bps），例如佣金/滑点/印花税等")
    args = parser.parse_args()

    start_year = int(args.start_year)
    end_year = int(args.end_year)
    if end_year < start_year:
        raise SystemExit("end-year 必须 >= start-year")

    warmup_days = 260
    min_date = (pd.Timestamp(year=start_year, month=1, day=1) - pd.Timedelta(days=warmup_days + 30)).strftime("%Y-%m-%d")
    max_date = (pd.Timestamp(year=end_year, month=12, day=31) + pd.Timedelta(days=10)).strftime("%Y-%m-%d")

    max_stocks = int(args.max_stocks)
    active = _load_active_codes_from_tushare()
    stocks: List[StockDef] = []
    for s in _iter_all_a_share_defs():
        if active is not None:
            if _ts_code(s.market, s.code) not in active:
                continue
        stocks.append(s)
        if max_stocks > 0 and len(stocks) >= max_stocks:
            break

    if not stocks:
        print("股票池为空")
        return

    print("=" * 60)
    print("短持为主：多年回测 + 参数寻优")
    print(f"tdx_endpoint={connected_endpoint()}")
    print(f"years={start_year}-{end_year} max_stocks={max_stocks} stocks={len(stocks)}")
    print(f"cache_dir={args.cache_dir} refresh_cache={bool(args.refresh_cache)}")
    print(f"cost_buy_bps={args.buy_fee_bps} cost_sell_bps={args.sell_fee_bps}")
    print(f"tp_min={args.tp_min} tp_max={args.tp_max} hold_min={args.hold_min} hold_max={args.hold_max}")
    print(f"target_win_rate={args.target_win_rate} min_pf={args.min_profit_factor} min_trades={args.min_trades}")
    print(f"max_abs_min_ret={args.max_abs_min_ret} max_abs_avg_loss_ret={args.max_abs_avg_loss_ret}")
    print(
        f"use_tushare_features={bool(args.use_tushare_features)} turnover=[{args.turnover_min},{args.turnover_max}] vr_min={args.vr_min} netmf_min={args.netmf_min} netmf_ratio_min={args.netmf_ratio_min}"
    )
    print("=" * 60)

    buy_cost_rate = _cost_rate_from_bps(float(args.buy_fee_bps))
    sell_cost_rate = _cost_rate_from_bps(float(args.sell_fee_bps))

    base_params = StrategyParams(
        pullback_min=-0.03,
        pullback_max=0.01,
        vol_contract_ratio=0.75,
        take_profit_pct=0.01,
        stop_loss_pct=0.03,
        trail_stop_pct=0.012,
        breakeven_after_pct=0.01,
        exit_on_ma_fast_break=1,
        max_hold_days=3,
        max_gap_up_pct=float(args.max_gap_up_pct),
        trend_ma_fast=20,
        trend_ma_slow=60,
        trend_ma_long=120,
        use_tushare_features=1 if bool(args.use_tushare_features) else 0,
        min_turnover_rate=float(args.turnover_min),
        max_turnover_rate=float(args.turnover_max),
        min_volume_ratio=float(args.vr_min),
        min_net_mf_amount=float(args.netmf_min),
        min_net_mf_ratio=float(args.netmf_ratio_min),
    )
    prepared = preload_stock_data(
        stocks=stocks,
        cache_dir=str(args.cache_dir),
        refresh_cache=bool(args.refresh_cache),
        fetch_sleep=float(args.fetch_sleep),
        ts_refresh_cache=bool(args.ts_refresh_cache),
        ts_fetch_sleep=float(args.ts_fetch_sleep),
        min_date=str(min_date),
        max_date=str(max_date),
        params_for_indicators=base_params,
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
        grid = _build_default_grid(
            center=prev_center,
            refine=refine,
            tp_min=float(args.tp_min),
            tp_max=float(args.tp_max),
            hold_min=int(args.hold_min),
            hold_max=int(args.hold_max),
            max_gap_up_pct=float(args.max_gap_up_pct),
            use_tushare_features=int(base_params.use_tushare_features),
            min_turnover_rate=float(base_params.min_turnover_rate),
            max_turnover_rate=float(base_params.max_turnover_rate),
            min_volume_ratio=float(base_params.min_volume_ratio),
            min_net_mf_amount=float(base_params.min_net_mf_amount),
            min_net_mf_ratio=float(base_params.min_net_mf_ratio),
        )
        print("-" * 60)
        print(f"第{it+1}轮参数搜索：候选={len(grid)} refine={refine}")
        iter_results = []
        for idx, p in enumerate(grid, start=1):
            t0 = time.perf_counter()
            m, _ = evaluate_params(
                params=p,
                prepared=prepared,
                start_year=start_year,
                end_year=end_year,
                buy_cost_rate=buy_cost_rate,
                sell_cost_rate=sell_cost_rate,
                min_trades=int(args.min_trades),
                target_win_rate=float(args.target_win_rate),
                min_profit_factor=float(args.min_profit_factor),
                max_abs_min_ret=float(args.max_abs_min_ret),
                max_abs_avg_loss_ret=float(args.max_abs_avg_loss_ret),
            )
            elapsed = time.perf_counter() - t0
            row = {"iter": it + 1, "idx": idx, "elapsed_s": round(elapsed, 2)}
            row.update(format_params(p))
            row.update({k: float(v) for k, v in m.items()})
            iter_results.append((float(m.get("score", -1e9)), p, row))
            if idx % 10 == 0:
                print(
                    f"进度: {idx}/{len(grid)} score={row['score']:.2f} feasible={row.get('feasible', 0.0):.0f} trades={row['trades']:.0f} win_rate={row['win_rate']:.2f} avg_ret={row['avg_ret']:.3f} avg_win={row.get('avg_win_ret', 0.0):.2f} tp_hit={row.get('tp_hit_rate', 0.0):.2f} pf={row['profit_factor']:.2f} pen={row.get('penalty', 0.0):.0f}"
                )

        iter_results.sort(key=lambda x: x[0], reverse=True)
        best_feasible = next((x for x in iter_results if float(x[2].get("feasible", 0.0)) > 0.0), None)
        if best_feasible is None:
            print(
                f"本轮没有满足约束的参数组合（target_win_rate={float(args.target_win_rate):.2f} min_profit_factor={float(args.min_profit_factor):.2f} min_trades={int(args.min_trades)}）"
            )
        top_k = min(int(args.top_k), len(iter_results))
        top = iter_results[:top_k]
        for _, _, r in top:
            history_rows.append(r)

        top1_score, top1_params, _ = best_feasible if best_feasible is not None else iter_results[0]
        print("-" * 60)
        print("本轮Top结果(前5)：")
        df_top = pd.DataFrame([x[2] for x in iter_results[:5]])
        show_cols = [
            "score",
            "feasible",
            "penalty",
            "trades",
            "win_rate",
            "avg_ret",
            "avg_ret_per_day",
            "avg_win_ret",
            "tp_hit_rate",
            "avg_hold_days",
            "min_ret",
            "profit_factor",
            "pullback_min",
            "pullback_max",
            "vol_contract_ratio",
            "take_profit_pct",
            "stop_loss_pct",
            "max_hold_days",
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

    ts = time.strftime("%Y%m%d_%H%M%S")
    out_base = args.out.strip() or os.path.join(_script_dir, f"高胜率短持寻优_{ts}")
    df_hist = pd.DataFrame(history_rows)
    if df_hist is not None and not df_hist.empty:
        df_hist = df_hist.sort_values(["score"], ascending=False).reset_index(drop=True)
        df_hist.to_csv(out_base + "_topk.csv", index=False, encoding="utf-8-sig")
        print(f"输出: {out_base + '_topk.csv'}")

    if best_params is None:
        return

    best_metrics, df_trades = evaluate_params(
        params=best_params,
        prepared=prepared,
        start_year=start_year,
        end_year=end_year,
        buy_cost_rate=buy_cost_rate,
        sell_cost_rate=sell_cost_rate,
        min_trades=int(args.min_trades),
        target_win_rate=float(args.target_win_rate),
        min_profit_factor=float(args.min_profit_factor),
        max_abs_min_ret=float(args.max_abs_min_ret),
        max_abs_avg_loss_ret=float(args.max_abs_avg_loss_ret),
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
