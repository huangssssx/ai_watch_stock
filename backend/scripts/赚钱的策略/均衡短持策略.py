"""
顺势回踩短持：backtest/optimize/scan
"""

import argparse
import itertools
import os
import signal
import sys
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

_script_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = None
_probe_dir = _script_dir
for _ in range(10):
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


class _TimeoutError(Exception):
    pass


def _run_with_timeout(timeout_s: float, fn, *args, **kwargs):
    timeout_s = float(timeout_s or 0.0)
    if timeout_s <= 0:
        return fn(*args, **kwargs)
    if not hasattr(signal, "SIGALRM"):
        return fn(*args, **kwargs)

    def _handler(_signum, _frame):
        raise _TimeoutError()

    old_handler = signal.getsignal(signal.SIGALRM)
    try:
        signal.signal(signal.SIGALRM, _handler)
        signal.setitimer(signal.ITIMER_REAL, timeout_s)
        return fn(*args, **kwargs)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        try:
            signal.signal(signal.SIGALRM, old_handler)
        except Exception:
            pass


_TS_DEBUG = False
_TS_SLOW_CALL_S = 3.0


def _ts_call(name: str, fn, **kwargs) -> pd.DataFrame:
    t0 = time.time()
    err = None
    df = None
    try:
        df = fn(**kwargs)
    except Exception as e:
        err = f"{type(e).__name__}:{e}"
        df = pd.DataFrame()
    elapsed = time.time() - t0
    if bool(_TS_DEBUG) and (err is not None or elapsed >= float(_TS_SLOW_CALL_S)):
        rows = 0 if (df is None or getattr(df, "empty", True)) else int(len(df))
        reason = ""
        if err:
            low = err.lower()
            if ("频率" in err) or ("freq" in low) or ("too many" in low) or ("429" in low):
                reason = "rate_limit"
            elif ("timeout" in low) or ("timed out" in low):
                reason = "timeout"
            elif ("connection" in low) or ("connect" in low):
                reason = "connection"
            elif ("502" in low) or ("503" in low) or ("504" in low):
                reason = "server"
        if reason:
            reason = f" reason={reason}"
        print(f"DEBUG: ts_call name={name} elapsed_s={elapsed:.2f} rows={rows}{reason} err={err or ''}", flush=True)
    return df if df is not None else pd.DataFrame()


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
    use_chip_features: int
    min_turnover_rate: float
    max_turnover_rate: float
    min_volume_ratio: float
    min_net_mf_amount: float
    min_net_mf_ratio: float
    min_winner_rate: float
    max_winner_rate: float
    min_chip_pos: float
    max_chip_band: float


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
    mae_pct: float
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
    chip_weight_avg: np.ndarray
    chip_winner_rate: np.ndarray
    chip_band: np.ndarray
    chip_pos: np.ndarray


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _cache_path(cache_dir: str, market: int, code: str) -> str:
    return os.path.join(cache_dir, f"daily_{int(market)}_{str(code).zfill(6)}.csv.gz")


def _cache_path_tsfeat(cache_dir: str, market: int, code: str) -> str:
    return os.path.join(cache_dir, f"tsfeat_{int(market)}_{str(code).zfill(6)}.csv.gz")


def _cache_path_cyqperf(cache_dir: str, market: int, code: str) -> str:
    return os.path.join(cache_dir, f"cyqperf_{int(market)}_{str(code).zfill(6)}.csv.gz")


def _count_cache_files(cache_dir: str, prefix: str) -> int:
    try:
        n = 0
        for e in os.scandir(cache_dir):
            if not e.is_file():
                continue
            name = e.name
            if name.startswith(prefix) and name.endswith(".csv.gz"):
                n += 1
        return int(n)
    except Exception:
        return -1


def _is_a_share_stock(market: int, code: str) -> bool:
    code = str(code or "").zfill(6)
    if int(market) == 0:
        return code.startswith(("000", "001", "002", "003", "300", "301"))
    if int(market) == 1:
        return code.startswith(("600", "601", "603", "605", "688"))
    return False


def _iter_all_a_share_defs(exclude_st: bool) -> Iterable[StockDef]:
    for market in (0, 1):
        total = tdx.get_security_count(market)
        step = 1000
        for start in range(0, int(total), step):
            rows = tdx.get_security_list(market, start) or []
            for r in rows:
                code = str(r.get("code", "")).zfill(6)
                name = str(r.get("name", "")).strip()
                if exclude_st and ("ST" in name.upper()):
                    continue
                if code and _is_a_share_stock(market, code):
                    yield StockDef(market=int(market), code=code, name=name)


def _ts_code(market: int, code: str) -> str:
    code = str(code).zfill(6)
    if int(market) == 0:
        return f"{code}.SZ"
    return f"{code}.SH"


def _load_active_codes_from_tushare(exclude_st: bool) -> Optional[set[str]]:
    if pro is None:
        return None
    try:
        df = pro.stock_basic(exchange="", list_status="L", fields="ts_code,name")
        if df is None or df.empty:
            return None
        df = df.dropna(subset=["ts_code"]).copy()
        df["ts_code"] = df["ts_code"].astype(str).str.strip()
        if exclude_st:
            df["name"] = df.get("name", "").astype(str).str.strip()
            df = df[~df["name"].str.contains("ST", na=False)]
        return set(df["ts_code"].tolist())
    except Exception:
        return None


def _daily_bars_full(market: int, code: str, min_date: Optional[str], timeout_s: float = 25.0) -> Tuple[pd.DataFrame, str]:
    code = str(code).zfill(6)
    min_dt = pd.to_datetime(min_date) if min_date else None
    chunk = 800
    start = 0
    parts: List[pd.DataFrame] = []
    timed_out = False
    last_err = ""
    while True:
        try:
            data = _run_with_timeout(float(timeout_s or 0.0), tdx.get_security_bars, 9, int(market), code, int(start), int(chunk))
        except _TimeoutError:
            timed_out = True
            break
        except Exception as e:
            last_err = f"{type(e).__name__}:{e}"
            break
        df = tdx.to_df(data) if data else pd.DataFrame()
        if df is None or df.empty or "datetime" not in df.columns:
            break
        df = df.copy()
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        df["datetime"] = df["datetime"].dt.normalize()
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
        if last_err:
            return pd.DataFrame(), f"error:{last_err}"
        if timed_out:
            return pd.DataFrame(), "timeout"
        return pd.DataFrame(), "empty"
    out = pd.concat(parts, ignore_index=True)
    out = out.drop_duplicates(subset=["datetime"]).sort_values("datetime", ascending=True).reset_index(drop=True)
    if timed_out:
        return out, "partial_timeout"
    return out, "ok"


def _fetch_tushare_features_one(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    if pro is None:
        return pd.DataFrame()
    ts_code = str(ts_code).strip()
    if not ts_code:
        return pd.DataFrame()

    parts = []
    df_basic = _ts_call(
        "daily_basic",
        pro.daily_basic,
        ts_code=ts_code,
        start_date=str(start_date),
        end_date=str(end_date),
        fields="ts_code,trade_date,turnover_rate,volume_ratio",
    )
    if df_basic is not None and not df_basic.empty:
        parts.append(df_basic)

    df_mf = _ts_call(
        "moneyflow",
        pro.moneyflow,
        ts_code=ts_code,
        start_date=str(start_date),
        end_date=str(end_date),
        fields="ts_code,trade_date,net_mf_amount",
    )
    if df_mf is not None and not df_mf.empty:
        parts.append(df_mf)

    df_limit = _ts_call(
        "stk_limit",
        pro.stk_limit,
        ts_code=ts_code,
        start_date=str(start_date),
        end_date=str(end_date),
        fields="ts_code,trade_date,up_limit,down_limit",
    )
    if df_limit is not None and not df_limit.empty:
        parts.append(df_limit)

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
    out["datetime"] = out["datetime"].dt.normalize()
    out = out.dropna(subset=["datetime"]).drop_duplicates(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
    keep = ["datetime", "turnover_rate", "volume_ratio", "net_mf_amount", "up_limit", "down_limit"]
    for c in keep:
        if c not in out.columns:
            out[c] = np.nan
        if c != "datetime":
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out[keep].copy()


def _fetch_cyq_perf_one(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    if pro is None:
        return pd.DataFrame()
    ts_code = str(ts_code).strip()
    if not ts_code:
        return pd.DataFrame()
    df = _ts_call(
        "cyq_perf",
        pro.cyq_perf,
        ts_code=ts_code,
        start_date=str(start_date),
        end_date=str(end_date),
        fields="ts_code,trade_date,cost_5pct,cost_50pct,cost_95pct,weight_avg,winner_rate",
    )
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    if "trade_date" not in df.columns:
        return pd.DataFrame()
    df["trade_date"] = df["trade_date"].astype(str).str.strip()
    df = df.dropna(subset=["trade_date"])
    df["datetime"] = pd.to_datetime(df["trade_date"], format="%Y%m%d", errors="coerce")
    df["datetime"] = df["datetime"].dt.normalize()
    df = df.dropna(subset=["datetime"]).drop_duplicates(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)

    rename = {
        "cost_5pct": "chip_cost_5pct",
        "cost_50pct": "chip_cost_50pct",
        "cost_95pct": "chip_cost_95pct",
        "weight_avg": "chip_weight_avg",
        "winner_rate": "chip_winner_rate",
    }
    for src, dst in rename.items():
        if src not in df.columns:
            df[src] = np.nan
        df[dst] = pd.to_numeric(df[src], errors="coerce")

    w = df["chip_weight_avg"].astype(float)
    c5 = df["chip_cost_5pct"].astype(float)
    c95 = df["chip_cost_95pct"].astype(float)
    df["chip_band"] = np.where((w > 0) & np.isfinite(w) & np.isfinite(c5) & np.isfinite(c95), (c95 - c5) / w, np.nan)

    keep = ["datetime", "chip_weight_avg", "chip_winner_rate", "chip_cost_5pct", "chip_cost_50pct", "chip_cost_95pct", "chip_band"]
    return df[keep].copy()


def load_or_fetch_tushare_features(
    cache_dir: str,
    market: int,
    code: str,
    start_date: str,
    end_date: str,
    strict_range: bool,
    refresh: bool,
    sleep_s: float,
    ts_timeout_s: float = 60.0,
    cache_read_timeout_s: float = 8.0,
) -> Tuple[pd.DataFrame, bool]:
    if pro is None:
        return pd.DataFrame(), False
    _ensure_dir(cache_dir)
    p = _cache_path_tsfeat(cache_dir, market, code)
    start_dt = pd.to_datetime(str(start_date), format="%Y%m%d", errors="coerce")
    end_dt = pd.to_datetime(str(end_date), format="%Y%m%d", errors="coerce")
    if pd.notna(start_dt):
        start_dt = start_dt.normalize()
    if pd.notna(end_dt):
        end_dt = end_dt.normalize()
    if (not refresh) and os.path.exists(p):
        try:
            df = _run_with_timeout(float(cache_read_timeout_s or 0.0), pd.read_csv, p)
            if df is not None and not df.empty and "datetime" in df.columns:
                df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
                df["datetime"] = df["datetime"].dt.normalize()
                df = df.dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
                need_cols = ["turnover_rate", "volume_ratio", "net_mf_amount", "up_limit", "down_limit"]
                ok_cols = all((c in df.columns) for c in need_cols)
                dt_min = df["datetime"].min() if "datetime" in df.columns else pd.NaT
                dt_max = df["datetime"].max() if "datetime" in df.columns else pd.NaT
                in_range = df
                if pd.notna(start_dt):
                    in_range = in_range[in_range["datetime"] >= start_dt]
                if pd.notna(end_dt):
                    in_range = in_range[in_range["datetime"] <= end_dt]
                has_rows = in_range is not None and (not in_range.empty)
                has_values = False
                if has_rows and ok_cols:
                    for c in need_cols:
                        if np.isfinite(pd.to_numeric(in_range[c], errors="coerce").to_numpy(dtype=float)).any():
                            has_values = True
                            break
                ok_range = True
                if bool(strict_range) and pd.notna(start_dt) and (pd.isna(dt_min) or dt_min > start_dt):
                    ok_range = False
                if pd.notna(end_dt):
                    tol = pd.Timedelta(days=15)
                    if pd.isna(dt_max) or dt_max < (end_dt - tol):
                        ok_range = False
                if ok_cols and ok_range and has_rows and has_values:
                    return in_range.reset_index(drop=True), True
        except Exception:
            pass

    df = pd.DataFrame()
    reason = ""
    base_timeout_s = float(ts_timeout_s or 0.0)
    retry_wait_s = [1.0, 2.0, 4.0]
    for retry_idx in range(0, len(retry_wait_s) + 1):
        try:
            call_timeout_s = base_timeout_s
            if retry_idx > 0 and call_timeout_s > 0:
                call_timeout_s = min(120.0, max(10.0, call_timeout_s * (2.0 ** float(retry_idx))))
            df = _run_with_timeout(
                float(call_timeout_s or 0.0),
                _fetch_tushare_features_one,
                ts_code=_ts_code(market, code),
                start_date=str(start_date),
                end_date=str(end_date),
            )
            reason = "empty" if (df is None or df.empty) else "ok"
        except _TimeoutError:
            df = pd.DataFrame()
            reason = "timeout"
        except Exception as e:
            df = pd.DataFrame()
            reason = f"error:{type(e).__name__}:{e}"

        if df is not None and not df.empty:
            break
        if retry_idx >= len(retry_wait_s):
            break
        wait_s = float(retry_wait_s[retry_idx])
        if bool(_TS_DEBUG):
            print(
                f"DEBUG: tsfeat_retry market={int(market)} code={str(code).zfill(6)} "
                f"retry={retry_idx+1}/{len(retry_wait_s)} wait_s={wait_s:.2f} reason={str(reason)}",
                flush=True,
            )
        time.sleep(wait_s)

    if df is None or df.empty:
        print(
            f"ERROR: tsfeat_failed market={int(market)} code={str(code).zfill(6)} reason={str(reason)}",
            flush=True,
        )
        return pd.DataFrame(), False
    df.to_csv(p, index=False, encoding="utf-8", compression="gzip")
    if sleep_s and sleep_s > 0:
        time.sleep(float(sleep_s))
    df = df.copy()
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.normalize()
        df = df.dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
        if pd.notna(start_dt):
            df = df[df["datetime"] >= start_dt]
        if pd.notna(end_dt):
            df = df[df["datetime"] <= end_dt]
    return df.reset_index(drop=True), False


def load_or_fetch_cyq_perf(
    cache_dir: str,
    market: int,
    code: str,
    start_date: str,
    end_date: str,
    strict_range: bool,
    refresh: bool,
    sleep_s: float,
    ts_timeout_s: float = 60.0,
    cache_read_timeout_s: float = 8.0,
) -> Tuple[pd.DataFrame, bool]:
    if pro is None:
        return pd.DataFrame(), False
    _ensure_dir(cache_dir)
    p = _cache_path_cyqperf(cache_dir, market, code)
    start_dt = pd.to_datetime(str(start_date), format="%Y%m%d", errors="coerce")
    end_dt = pd.to_datetime(str(end_date), format="%Y%m%d", errors="coerce")
    if pd.notna(start_dt):
        start_dt = start_dt.normalize()
    if pd.notna(end_dt):
        end_dt = end_dt.normalize()

    if (not refresh) and os.path.exists(p):
        try:
            df = _run_with_timeout(float(cache_read_timeout_s or 0.0), pd.read_csv, p)
            if df is not None and not df.empty and "datetime" in df.columns:
                df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
                df["datetime"] = df["datetime"].dt.normalize()
                df = df.dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
                need_cols = ["chip_weight_avg", "chip_winner_rate", "chip_band"]
                ok_cols = all((c in df.columns) for c in need_cols)
                dt_min = df["datetime"].min()
                dt_max = df["datetime"].max()
                ok_range = True
                if bool(strict_range) and pd.notna(start_dt) and (pd.isna(dt_min) or dt_min > start_dt):
                    ok_range = False
                if pd.notna(end_dt):
                    tol = pd.Timedelta(days=15)
                    if pd.isna(dt_max) or dt_max < (end_dt - tol):
                        ok_range = False
                in_range = df
                if pd.notna(start_dt):
                    in_range = in_range[in_range["datetime"] >= start_dt]
                if pd.notna(end_dt):
                    in_range = in_range[in_range["datetime"] <= end_dt]
                if ok_cols and ok_range and (in_range is not None) and (len(in_range) > 0):
                    return in_range.reset_index(drop=True), True
        except Exception:
            pass

    ts_code = _ts_code(market, code)
    df = pd.DataFrame()
    reason = ""
    base_timeout_s = float(ts_timeout_s or 0.0)
    retry_wait_s = [1.0, 2.0, 4.0]
    for retry_idx in range(0, len(retry_wait_s) + 1):
        try:
            call_timeout_s = base_timeout_s
            if retry_idx > 0 and call_timeout_s > 0:
                call_timeout_s = min(120.0, max(10.0, call_timeout_s * (2.0 ** float(retry_idx))))
            df = _run_with_timeout(
                float(call_timeout_s or 0.0),
                _fetch_cyq_perf_one,
                ts_code=ts_code,
                start_date=str(start_date),
                end_date=str(end_date),
            )
            reason = "empty" if (df is None or df.empty) else "ok"
        except _TimeoutError:
            df = pd.DataFrame()
            reason = "timeout"
        except Exception as e:
            df = pd.DataFrame()
            reason = f"error:{type(e).__name__}:{e}"

        if df is not None and not df.empty:
            break
        if retry_idx >= len(retry_wait_s):
            break
        wait_s = float(retry_wait_s[retry_idx])
        if bool(_TS_DEBUG):
            print(
                f"DEBUG: cyqperf_retry market={int(market)} code={str(code).zfill(6)} "
                f"retry={retry_idx+1}/{len(retry_wait_s)} wait_s={wait_s:.2f} reason={str(reason)}",
                flush=True,
            )
        time.sleep(wait_s)

    if df is None or df.empty:
        print(f"ERROR: cyqperf_failed market={int(market)} code={str(code).zfill(6)} ts_code={ts_code} reason={str(reason)}", flush=True)
        return pd.DataFrame(), False
    df.to_csv(p, index=False, encoding="utf-8", compression="gzip")
    if sleep_s and sleep_s > 0:
        time.sleep(float(sleep_s))
    df = df.copy()
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.normalize()
        df = df.dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
        if pd.notna(start_dt):
            df = df[df["datetime"] >= start_dt]
        if pd.notna(end_dt):
            df = df[df["datetime"] <= end_dt]
    return df.reset_index(drop=True), False


def load_or_fetch_daily(
    cache_dir: str,
    market: int,
    code: str,
    min_date: str,
    max_date: str,
    required_rows: int,
    strict_range: bool,
    refresh: bool,
    sleep_s: float,
    fetch_timeout_s: float = 25.0,
    cache_read_timeout_s: float = 8.0,
) -> Tuple[pd.DataFrame, bool]:
    _ensure_dir(cache_dir)
    p = _cache_path(cache_dir, market, code)
    min_dt = pd.to_datetime(min_date, errors="coerce")
    max_dt = pd.to_datetime(max_date, errors="coerce")
    if pd.notna(min_dt):
        min_dt = min_dt.normalize()
    if pd.notna(max_dt):
        max_dt = max_dt.normalize()
    if (not refresh) and os.path.exists(p):
        try:
            df = _run_with_timeout(float(cache_read_timeout_s or 0.0), pd.read_csv, p)
            if df is not None and not df.empty and "datetime" in df.columns:
                df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
                df["datetime"] = df["datetime"].dt.normalize()
                df = df.dropna(subset=["datetime"]).sort_values("datetime", ascending=True).reset_index(drop=True)
                dt_min = df["datetime"].min()
                dt_max = df["datetime"].max()
                ok_range = True
                if bool(strict_range) and pd.notna(min_dt) and (pd.isna(dt_min) or dt_min > min_dt):
                    ok_range = False
                if pd.notna(max_dt):
                    tol = pd.Timedelta(days=15)
                    if pd.isna(dt_max) or dt_max < (max_dt - tol):
                        ok_range = False
                in_range = df
                if pd.notna(min_dt):
                    in_range = in_range[in_range["datetime"] >= min_dt]
                if pd.notna(max_dt):
                    in_range = in_range[in_range["datetime"] <= max_dt]
                if ok_range and (in_range is not None) and (len(in_range) >= int(required_rows)):
                    return in_range.reset_index(drop=True), True
        except Exception:
            pass
    base_timeout_s = float(fetch_timeout_s or 0.0)
    df, reason = _daily_bars_full(market, code, min_date=min_date, timeout_s=base_timeout_s)
    retry_wait_s = [1.0, 2.0, 4.0]
    for retry_idx, wait_s in enumerate(retry_wait_s, start=1):
        if df is not None and not df.empty:
            break
        ep = None
        try:
            ep = connected_endpoint()
        except Exception:
            ep = None
        print(
            f"DEBUG: tdx_daily_retry market={int(market)} code={str(code).zfill(6)} "
            f"retry={retry_idx}/{len(retry_wait_s)} wait_s={float(wait_s):.2f} reason={str(reason)} endpoint={ep}",
            flush=True,
        )
        try:
            tdx.disconnect()
        except Exception:
            pass
        time.sleep(float(wait_s))
        retry_timeout_s = base_timeout_s
        if retry_timeout_s > 0:
            retry_timeout_s = min(60.0, max(10.0, retry_timeout_s * (2.0 ** float(retry_idx))))
        df, reason = _daily_bars_full(market, code, min_date=min_date, timeout_s=retry_timeout_s)
    if df is None or df.empty:
        ep = None
        try:
            ep = connected_endpoint()
        except Exception:
            ep = None
        print(
            f"ERROR: tdx_daily_failed market={int(market)} code={str(code).zfill(6)} reason={str(reason)} endpoint={ep}",
            flush=True,
        )
        return pd.DataFrame(), False
    keep_cols = [c for c in ["datetime", "open", "close", "high", "low", "vol", "amount"] if c in df.columns]
    df_out = df[keep_cols].copy()
    df_out.to_csv(p, index=False, encoding="utf-8", compression="gzip")
    if sleep_s and sleep_s > 0:
        time.sleep(float(sleep_s))
    df_run = df_out.copy()
    if "datetime" in df_run.columns:
        df_run["datetime"] = pd.to_datetime(df_run["datetime"], errors="coerce").dt.normalize()
        df_run = df_run.dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
        if pd.notna(min_dt):
            df_run = df_run[df_run["datetime"] >= min_dt]
        if pd.notna(max_dt):
            df_run = df_run[df_run["datetime"] <= max_dt]
    return df_run.reset_index(drop=True), False


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


def _signal_ok_at(i: int, data: PreparedStockData, params: StrategyParams) -> bool:
    close = data.close
    low = data.low
    vol = data.vol
    ma_fast = data.ma_fast
    ma_slow = data.ma_slow
    ma_vol_prev = data.ma_vol_prev

    if not (float(close[i]) > float(ma_fast[i]) > float(ma_slow[i])):
        return False
    if float(ma_fast[i]) <= 0:
        return False
    pullback = float(low[i]) / float(ma_fast[i]) - 1.0
    if not (float(params.pullback_min) <= float(pullback) <= float(params.pullback_max)):
        return False
    if float(ma_vol_prev[i]) <= 0:
        return False
    if float(params.vol_contract_ratio) > 0:
        if float(vol[i]) > float(ma_vol_prev[i]) * float(params.vol_contract_ratio):
            return False

    if int(params.use_tushare_features) > 0:
        tr = float(data.turnover_rate[i]) if np.isfinite(data.turnover_rate[i]) else np.nan
        vr = float(data.volume_ratio[i]) if np.isfinite(data.volume_ratio[i]) else np.nan
        nmf = float(data.net_mf_amount[i]) if np.isfinite(data.net_mf_amount[i]) else np.nan

        if float(params.min_turnover_rate) > 0:
            if not np.isfinite(tr) or tr < float(params.min_turnover_rate):
                return False
        if float(params.max_turnover_rate) > 0:
            if not np.isfinite(tr) or tr > float(params.max_turnover_rate):
                return False
        if float(params.min_volume_ratio) > 0:
            if not np.isfinite(vr) or vr < float(params.min_volume_ratio):
                return False
        if float(params.min_net_mf_amount) > 0:
            if not np.isfinite(nmf) or nmf < float(params.min_net_mf_amount):
                return False
        if float(params.min_net_mf_ratio) > 0:
            a_wan = float(data.amount[i]) / 10000.0 if float(data.amount[i]) > 0 else 0.0
            if a_wan <= 0 or (not np.isfinite(nmf)):
                return False
            if float(nmf) / float(a_wan) < float(params.min_net_mf_ratio):
                return False

    if int(params.use_chip_features) > 0:
        wr = float(data.chip_winner_rate[i]) if np.isfinite(data.chip_winner_rate[i]) else np.nan
        chip_pos = float(data.chip_pos[i]) if np.isfinite(data.chip_pos[i]) else np.nan
        chip_band = float(data.chip_band[i]) if np.isfinite(data.chip_band[i]) else np.nan

        if float(params.min_winner_rate) > 0:
            if not np.isfinite(wr) or wr < float(params.min_winner_rate):
                return False
        if float(params.max_winner_rate) > 0:
            if not np.isfinite(wr) or wr > float(params.max_winner_rate):
                return False
        if float(params.min_chip_pos) > 0:
            if not np.isfinite(chip_pos) or chip_pos < float(params.min_chip_pos):
                return False
        if float(params.max_chip_band) > 0:
            if not np.isfinite(chip_band) or chip_band > float(params.max_chip_band):
                return False

    return True


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
    ma_fast = data.ma_fast
    up_limit = data.up_limit
    down_limit = data.down_limit
    warmup = max(130, int(params.trend_ma_long) + 2, 22)

    trades: List[Trade] = []
    i = int(warmup)
    eps = 1e-9
    while i + 1 < n:
        y = int(str(td[i])[:4])
        if y < int(start_year):
            i += 1
            continue
        if y > int(end_year):
            break

        if not _signal_ok_at(i=i, data=data, params=params):
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
        exit_reason = "时间退出"
        best_high = float(entry_price)
        min_price_seen = float(entry_price)

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

            trail_stop_price = float(best_high) * (1.0 - float(params.trail_stop_pct)) if float(params.trail_stop_pct) > 0 else None
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
                    min_price_seen = min(float(min_price_seen), float(exit_price))
                    break
                if float(k_open) <= float(stop_price):
                    exit_idx = int(k)
                    exit_price = float(k_open)
                    exit_reason = "止损开盘"
                    min_price_seen = min(float(min_price_seen), float(exit_price))
                    break
                if float(k_low) <= float(stop_price):
                    exit_idx = int(k)
                    exit_price = float(stop_price)
                    exit_reason = "止损"
                    min_price_seen = min(float(min_price_seen), float(exit_price))
                    break

            if float(k_high) >= float(take_profit_price):
                exit_idx = int(k)
                exit_price = float(take_profit_price)
                exit_reason = "止盈"
                min_price_seen = min(float(min_price_seen), float(k_low))
                break

            if int(params.exit_on_ma_fast_break) > 0 and float(k_close) < float(ma_fast[k]):
                exit_idx = int(k)
                exit_price = float(k_close)
                exit_reason = "破均线"
                min_price_seen = min(float(min_price_seen), float(k_low))
                break

            min_price_seen = min(float(min_price_seen), float(k_low))
            best_high = max(float(best_high), float(k_high))

        ret_pct = _net_ret_pct(entry_price, exit_price, buy_cost_rate=buy_cost_rate, sell_cost_rate=sell_cost_rate)
        hold_days = int(exit_idx - entry_idx + 1)
        mae_pct = (float(min_price_seen) / float(entry_price) - 1.0) * 100.0 if float(entry_price) > 0 else 0.0

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
                mae_pct=round(float(mae_pct), 6),
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
            "avg_mae": 0.0,
            "worst_mae": 0.0,
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

    maes = [float(t.mae_pct) for t in trades]
    avg_mae = float(sum(maes) / len(maes)) if maes else 0.0
    worst_mae = float(min(maes)) if maes else 0.0

    score = (
        (win_rate * 650.0)
        + (float(pf) * 25.0)
        + (avg_ret_per_day * 90.0)
        + (avg_ret * 10.0)
        + (tp_hit_rate * 30.0)
        + (float(np.log1p(trades_n)) * 120.0)
        - (abs(min_ret) * 90.0)
        - (abs(avg_loss_ret) * 160.0)
        - (abs(worst_mae) * 25.0)
        - (abs(avg_mae) * 10.0)
        - (avg_hold_days * 90.0)
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
        "avg_mae": float(avg_mae),
        "worst_mae": float(worst_mae),
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
    use_chip_features: int,
    min_turnover_rate: float,
    max_turnover_rate: float,
    min_volume_ratio: float,
    min_net_mf_amount: float,
    min_net_mf_ratio: float,
    min_winner_rate: float,
    max_winner_rate: float,
    min_chip_pos: float,
    max_chip_band: float,
) -> List[StrategyParams]:
    tp_min = float(tp_min)
    tp_max = float(tp_max)
    hold_min = int(hold_min)
    hold_max = int(hold_max)
    max_gap_up_pct = float(max_gap_up_pct)

    if center is None or not refine:
        pullback_min = [-0.035, -0.03, -0.025, -0.02, -0.015]
        pullback_max = [-0.005, 0.0, 0.01]
        vol_contract_ratio = [0.6, 0.75, 0.9, 1.0]
        take_profit_pct = [v for v in [0.008, 0.01, 0.012, 0.015, 0.018, 0.02, 0.025] if tp_min <= float(v) <= tp_max]
        stop_loss_pct = [0.01, 0.012, 0.015, 0.018, 0.02, 0.025, 0.03]
        trail_stop_pct = [0.008, 0.01, 0.012, 0.015]
        breakeven_after_pct = [0.008, 0.01, 0.012]
        exit_on_ma_fast_break = [1]
        max_hold_days = list(range(max(1, hold_min), max(1, hold_max) + 1, 1)) or [max(1, hold_min)]
        trend_ma_fast = [20]
        trend_ma_slow = [60]
        trend_ma_long = [120]
    else:
        def around(v: float, steps: List[float]) -> List[float]:
            return sorted({round(float(v) + float(s), 6) for s in steps})

        pullback_min = around(center.pullback_min, [-0.01, -0.005, 0.0, 0.005])
        pullback_max = around(center.pullback_max, [-0.01, -0.005, 0.0, 0.005, 0.01])
        vol_contract_ratio = around(center.vol_contract_ratio, [-0.15, -0.08, -0.04, 0.0, 0.04, 0.08, 0.15])
        take_profit_pct = around(center.take_profit_pct, [-0.006, -0.004, -0.002, 0.0, 0.002, 0.004, 0.006])
        stop_loss_pct = around(center.stop_loss_pct, [-0.008, -0.006, -0.004, -0.002, 0.0, 0.002, 0.004, 0.006])
        trail_stop_pct = around(center.trail_stop_pct, [-0.008, -0.006, -0.004, -0.002, 0.0, 0.002, 0.004, 0.006])
        breakeven_after_pct = around(center.breakeven_after_pct, [-0.006, -0.004, -0.002, 0.0, 0.002, 0.004, 0.006])
        exit_on_ma_fast_break = [center.exit_on_ma_fast_break]
        max_hold_days = sorted({max(1, int(center.max_hold_days) + d) for d in (-1, 0, 1)})
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
                use_chip_features=int(center.use_chip_features if center is not None else use_chip_features),
                min_turnover_rate=float(center.min_turnover_rate if center is not None else min_turnover_rate),
                max_turnover_rate=float(center.max_turnover_rate if center is not None else max_turnover_rate),
                min_volume_ratio=float(center.min_volume_ratio if center is not None else min_volume_ratio),
                min_net_mf_amount=float(center.min_net_mf_amount if center is not None else min_net_mf_amount),
                min_net_mf_ratio=float(center.min_net_mf_ratio if center is not None else min_net_mf_ratio),
                min_winner_rate=float(center.min_winner_rate if center is not None else min_winner_rate),
                max_winner_rate=float(center.max_winner_rate if center is not None else max_winner_rate),
                min_chip_pos=float(center.min_chip_pos if center is not None else min_chip_pos),
                max_chip_band=float(center.max_chip_band if center is not None else max_chip_band),
            )
        )
    return grid


def _finite_ratio(x: np.ndarray) -> float:
    if x is None:
        return 0.0
    try:
        n = int(len(x))
    except Exception:
        return 0.0
    if n <= 0:
        return 0.0
    return float(np.isfinite(x).sum()) / float(n)


def _merge_reason_counts(dst: Dict[str, int], src: Dict[str, int]) -> Dict[str, int]:
    for k, v in (src or {}).items():
        dst[k] = int(dst.get(k, 0)) + int(v)
    return dst


def _debug_signal_rejections_one(
    data: PreparedStockData,
    params: StrategyParams,
    start_year: int,
    end_year: int,
    max_checks: int,
) -> Dict[str, int]:
    close = data.close
    low = data.low
    vol = data.vol
    ma_fast = data.ma_fast
    ma_slow = data.ma_slow
    ma_vol_prev = data.ma_vol_prev

    td = data.trade_date
    n = int(len(td))
    warmup = max(130, int(params.trend_ma_long) + 2, 22)

    out: Dict[str, int] = {}
    checks = 0
    passes = 0
    for i in range(int(warmup), n - 1):
        if checks >= int(max_checks):
            break
        y = int(str(td[i])[:4])
        if y < int(start_year) or y > int(end_year):
            continue
        checks += 1

        if not (float(close[i]) > float(ma_fast[i]) > float(ma_slow[i])):
            out["trend"] = out.get("trend", 0) + 1
            continue
        if float(ma_fast[i]) <= 0:
            out["ma_fast_nonpos"] = out.get("ma_fast_nonpos", 0) + 1
            continue
        pullback = float(low[i]) / float(ma_fast[i]) - 1.0
        if not (float(params.pullback_min) <= float(pullback) <= float(params.pullback_max)):
            out["pullback"] = out.get("pullback", 0) + 1
            continue
        if float(ma_vol_prev[i]) <= 0:
            out["ma_vol_prev_nonpos"] = out.get("ma_vol_prev_nonpos", 0) + 1
            continue
        if float(params.vol_contract_ratio) > 0:
            if float(vol[i]) > float(ma_vol_prev[i]) * float(params.vol_contract_ratio):
                out["vol_contract"] = out.get("vol_contract", 0) + 1
                continue

        if int(params.use_tushare_features) > 0:
            tr = float(data.turnover_rate[i]) if np.isfinite(data.turnover_rate[i]) else np.nan
            vr = float(data.volume_ratio[i]) if np.isfinite(data.volume_ratio[i]) else np.nan
            nmf = float(data.net_mf_amount[i]) if np.isfinite(data.net_mf_amount[i]) else np.nan

            if float(params.min_turnover_rate) > 0:
                if not np.isfinite(tr):
                    out["turnover_nan"] = out.get("turnover_nan", 0) + 1
                    continue
                if tr < float(params.min_turnover_rate):
                    out["turnover_lt"] = out.get("turnover_lt", 0) + 1
                    continue
            if float(params.max_turnover_rate) > 0:
                if not np.isfinite(tr):
                    out["turnover_nan"] = out.get("turnover_nan", 0) + 1
                    continue
                if tr > float(params.max_turnover_rate):
                    out["turnover_gt"] = out.get("turnover_gt", 0) + 1
                    continue
            if float(params.min_volume_ratio) > 0:
                if not np.isfinite(vr):
                    out["vr_nan"] = out.get("vr_nan", 0) + 1
                    continue
                if vr < float(params.min_volume_ratio):
                    out["vr_lt"] = out.get("vr_lt", 0) + 1
                    continue
            if float(params.min_net_mf_amount) > 0:
                if not np.isfinite(nmf):
                    out["nmf_nan"] = out.get("nmf_nan", 0) + 1
                    continue
                if nmf < float(params.min_net_mf_amount):
                    out["nmf_amt_lt"] = out.get("nmf_amt_lt", 0) + 1
                    continue
            if float(params.min_net_mf_ratio) > 0:
                a_wan = float(data.amount[i]) / 10000.0 if float(data.amount[i]) > 0 else 0.0
                if a_wan <= 0 or (not np.isfinite(nmf)):
                    out["nmf_ratio_missing"] = out.get("nmf_ratio_missing", 0) + 1
                    continue
                if float(nmf) / float(a_wan) < float(params.min_net_mf_ratio):
                    out["nmf_ratio_lt"] = out.get("nmf_ratio_lt", 0) + 1
                    continue

        if int(params.use_chip_features) > 0:
            wr = float(data.chip_winner_rate[i]) if np.isfinite(data.chip_winner_rate[i]) else np.nan
            chip_pos = float(data.chip_pos[i]) if np.isfinite(data.chip_pos[i]) else np.nan
            chip_band = float(data.chip_band[i]) if np.isfinite(data.chip_band[i]) else np.nan

            if float(params.min_winner_rate) > 0:
                if not np.isfinite(wr):
                    out["winner_nan"] = out.get("winner_nan", 0) + 1
                    continue
                if wr < float(params.min_winner_rate):
                    out["winner_lt"] = out.get("winner_lt", 0) + 1
                    continue
            if float(params.max_winner_rate) > 0:
                if not np.isfinite(wr):
                    out["winner_nan"] = out.get("winner_nan", 0) + 1
                    continue
                if wr > float(params.max_winner_rate):
                    out["winner_gt"] = out.get("winner_gt", 0) + 1
                    continue
            if float(params.min_chip_pos) > 0:
                if not np.isfinite(chip_pos):
                    out["chip_pos_nan"] = out.get("chip_pos_nan", 0) + 1
                    continue
                if chip_pos < float(params.min_chip_pos):
                    out["chip_pos_lt"] = out.get("chip_pos_lt", 0) + 1
                    continue
            if float(params.max_chip_band) > 0:
                if not np.isfinite(chip_band):
                    out["chip_band_nan"] = out.get("chip_band_nan", 0) + 1
                    continue
                if chip_band > float(params.max_chip_band):
                    out["chip_band_gt"] = out.get("chip_band_gt", 0) + 1
                    continue

        passes += 1

    out["checked"] = int(checks)
    out["passes"] = int(passes)
    return out


def _debug_diagnose_prepared(
    prepared: List[PreparedStockData],
    params: StrategyParams,
    start_year: int,
    end_year: int,
    sample_stocks: int,
    max_checks_per_stock: int,
) -> None:
    if not prepared:
        print("DEBUG: prepared 为空")
        return

    sample_n = min(int(sample_stocks), len(prepared))
    picked = prepared[:sample_n]

    cover_turnover = float(np.mean([_finite_ratio(d.turnover_rate) for d in picked]))
    cover_vr = float(np.mean([_finite_ratio(d.volume_ratio) for d in picked]))
    cover_nmf = float(np.mean([_finite_ratio(d.net_mf_amount) for d in picked]))
    cover_ul = float(np.mean([_finite_ratio(d.up_limit) for d in picked]))
    cover_dl = float(np.mean([_finite_ratio(d.down_limit) for d in picked]))
    print(
        "DEBUG: tsfeat覆盖率(样本均值) "
        f"turnover={cover_turnover:.3f} vr={cover_vr:.3f} nmf={cover_nmf:.3f} up_limit={cover_ul:.3f} down_limit={cover_dl:.3f}"
    )

    total: Dict[str, int] = {}
    for d in picked:
        rc = _debug_signal_rejections_one(
            data=d,
            params=params,
            start_year=int(start_year),
            end_year=int(end_year),
            max_checks=int(max_checks_per_stock),
        )
        _merge_reason_counts(total, rc)

    checked = int(total.get("checked", 0))
    passes = int(total.get("passes", 0))
    pass_rate = float(passes) / float(checked) if checked > 0 else 0.0
    print(f"DEBUG: 信号通过率(样本) passes={passes} checked={checked} pass_rate={pass_rate:.6f}")
    reasons = {k: v for k, v in total.items() if k not in {"checked", "passes"}}
    top = sorted(reasons.items(), key=lambda x: int(x[1]), reverse=True)[:12]
    if top:
        s = " ".join([f"{k}={v}" for k, v in top])
        print(f"DEBUG: 拒绝原因Top {s}")


def preload_stock_data(
    stocks: List[StockDef],
    cache_dir: str,
    refresh_cache: bool,
    fetch_sleep: float,
    ts_refresh_cache: bool,
    ts_fetch_sleep: float,
    chip_refresh_cache: bool,
    chip_fetch_sleep: float,
    min_date: str,
    max_date: str,
    params_for_indicators: StrategyParams,
    strict_range: bool,
    progress_every: int = 50,
    fetch_timeout_s: float = 25.0,
    ts_timeout_s: float = 60.0,
    cache_read_timeout_s: float = 8.0,
    debug: bool = False,
) -> List[PreparedStockData]:
    prepared: List[PreparedStockData] = []
    daily_cache_hit = 0
    daily_cache_miss = 0
    ts_cache_hit = 0
    ts_cache_miss = 0
    chip_cache_hit = 0
    chip_cache_miss = 0
    ts_ok = 0
    ts_empty = 0
    chip_ok = 0
    chip_empty = 0
    ts_fail_merge = 0
    ts_cover_turnover_sum = 0.0
    ts_cover_vr_sum = 0.0
    ts_cover_nmf_sum = 0.0
    ts_cover_ul_sum = 0.0
    ts_cover_dl_sum = 0.0
    skip_daily_empty = 0
    skip_prepare_empty = 0
    skip_missing_indicators = 0
    skip_too_short_after_dropna = 0
    required_rows = 260 + max(20, int(params_for_indicators.trend_ma_long)) + 5
    progress_every = max(1, int(progress_every))
    with tdx:
        connected_endpoint()
        for idx, s in enumerate(stocks, start=1):
            if idx % progress_every == 0:
                print(
                    f"预加载进度: {idx}/{len(stocks)} 已准备={len(prepared)} last={int(s.market)}_{str(s.code).zfill(6)}",
                    flush=True,
                )
                if bool(debug) and int(params_for_indicators.use_tushare_features) > 0:
                    denom = max(1, len(prepared))
                    print(
                        "DEBUG: tsfeat "
                        f"ok={ts_ok} empty={ts_empty} fail_merge={ts_fail_merge} "
                        f"cover(turnover/vr/nmf/ul/dl)={ts_cover_turnover_sum/denom:.3f}/{ts_cover_vr_sum/denom:.3f}/{ts_cover_nmf_sum/denom:.3f}/{ts_cover_ul_sum/denom:.3f}/{ts_cover_dl_sum/denom:.3f}"
                    )
                    ts_files = _count_cache_files(str(cache_dir), "tsfeat_")
                    if int(ts_files) >= 0:
                        print(f"DEBUG: ts_cache_files={int(ts_files)}/{len(stocks)}", flush=True)
                if bool(debug):
                    print(
                        f"DEBUG: cache daily_hit={daily_cache_hit} daily_miss={daily_cache_miss} "
                        f"ts_hit={ts_cache_hit} ts_miss={ts_cache_miss} chip_hit={chip_cache_hit} chip_miss={chip_cache_miss}",
                        flush=True,
                    )
            t0 = time.time()
            df, used_cache = load_or_fetch_daily(
                cache_dir=cache_dir,
                market=int(s.market),
                code=str(s.code),
                min_date=min_date,
                max_date=max_date,
                required_rows=int(required_rows),
                strict_range=bool(strict_range),
                refresh=bool(refresh_cache),
                sleep_s=float(fetch_sleep),
                fetch_timeout_s=float(fetch_timeout_s or 0.0),
                cache_read_timeout_s=float(cache_read_timeout_s or 0.0),
            )
            t1 = time.time()
            if bool(debug) and (t1 - t0) >= 3.0:
                print(
                    f"DEBUG: slow_daily market={int(s.market)} code={str(s.code).zfill(6)} cache={int(bool(used_cache))} elapsed_s={(t1 - t0):.2f}",
                    flush=True,
                )
            if bool(used_cache):
                daily_cache_hit += 1
            else:
                daily_cache_miss += 1
            if df is None or df.empty:
                skip_daily_empty += 1
                continue
            df = prepare_indicators(df, params=params_for_indicators)
            if df is None or df.empty:
                skip_prepare_empty += 1
                continue
            need_ind = ["ma_fast", "ma_slow", "ma_long", "ma_vol", "ma_fast_prev", "ma_slow_prev", "ma_long_prev", "ma_vol_prev"]
            if any((c not in df.columns) for c in need_ind):
                skip_missing_indicators += 1
                continue
            ts_ran = False
            ts_used_cache = False
            ts_has_data = False
            if int(params_for_indicators.use_tushare_features) > 0 and pro is not None:
                ts_ran = True
                t2 = time.time()
                df_ts, ts_used_cache = load_or_fetch_tushare_features(
                    cache_dir=cache_dir,
                    market=int(s.market),
                    code=str(s.code),
                    start_date=str(pd.to_datetime(min_date).strftime("%Y%m%d")),
                    end_date=str(pd.to_datetime(max_date).strftime("%Y%m%d")),
                    strict_range=bool(strict_range),
                    refresh=bool(ts_refresh_cache),
                    sleep_s=float(ts_fetch_sleep),
                    ts_timeout_s=float(ts_timeout_s or 0.0),
                    cache_read_timeout_s=float(cache_read_timeout_s or 0.0),
                )
                t3 = time.time()
                if bool(debug) and (t3 - t2) >= 3.0:
                    print(
                        f"DEBUG: slow_tsfeat market={int(s.market)} code={str(s.code).zfill(6)} cache={int(bool(ts_used_cache))} elapsed_s={(t3 - t2):.2f}",
                        flush=True,
                    )
                if df_ts is not None and not df_ts.empty:
                    df = df.merge(df_ts, on="datetime", how="left")
                    ts_has_data = True
                else:
                    for c in ["turnover_rate", "volume_ratio", "net_mf_amount", "up_limit", "down_limit"]:
                        if c not in df.columns:
                            df[c] = np.nan
            else:
                for c in ["turnover_rate", "volume_ratio", "net_mf_amount", "up_limit", "down_limit"]:
                    if c not in df.columns:
                        df[c] = np.nan

            chip_ran = False
            chip_used_cache = False
            chip_has_data = False
            if int(params_for_indicators.use_chip_features) > 0 and pro is not None:
                chip_ran = True
                t4 = time.time()
                df_chip, chip_used_cache = load_or_fetch_cyq_perf(
                    cache_dir=cache_dir,
                    market=int(s.market),
                    code=str(s.code),
                    start_date=str(pd.to_datetime(min_date).strftime("%Y%m%d")),
                    end_date=str(pd.to_datetime(max_date).strftime("%Y%m%d")),
                    strict_range=bool(strict_range),
                    refresh=bool(chip_refresh_cache),
                    sleep_s=float(chip_fetch_sleep),
                    ts_timeout_s=float(ts_timeout_s or 0.0),
                    cache_read_timeout_s=float(cache_read_timeout_s or 0.0),
                )
                t5 = time.time()
                if bool(debug) and (t5 - t4) >= 3.0:
                    print(
                        f"DEBUG: slow_cyqperf market={int(s.market)} code={str(s.code).zfill(6)} cache={int(bool(chip_used_cache))} elapsed_s={(t5 - t4):.2f}",
                        flush=True,
                    )
                if df_chip is not None and not df_chip.empty:
                    df = df.merge(df_chip, on="datetime", how="left")
                    chip_has_data = True
                else:
                    for c in ["chip_weight_avg", "chip_winner_rate", "chip_band", "chip_cost_5pct", "chip_cost_50pct", "chip_cost_95pct"]:
                        if c not in df.columns:
                            df[c] = np.nan
            else:
                for c in ["chip_weight_avg", "chip_winner_rate", "chip_band", "chip_cost_5pct", "chip_cost_50pct", "chip_cost_95pct"]:
                    if c not in df.columns:
                        df[c] = np.nan

            w = df["chip_weight_avg"].astype(float) if "chip_weight_avg" in df.columns else pd.Series(np.nan, index=df.index)
            close_s = df["close"].astype(float)
            df["chip_pos"] = np.where((w > 0) & np.isfinite(w) & np.isfinite(close_s), close_s / w - 1.0, np.nan)

            df = df.dropna(subset=["datetime", "open", "close", "high", "low", "vol", "ma_fast", "ma_slow", "ma_long", "ma_vol"]).reset_index(
                drop=True
            )
            if len(df) < 260:
                skip_too_short_after_dropna += 1
                continue
            if bool(ts_ran):
                if bool(ts_used_cache):
                    ts_cache_hit += 1
                else:
                    ts_cache_miss += 1
                if bool(ts_has_data):
                    ts_ok += 1
                else:
                    ts_empty += 1
            if bool(chip_ran):
                if bool(chip_used_cache):
                    chip_cache_hit += 1
                else:
                    chip_cache_miss += 1
                if bool(chip_has_data):
                    chip_ok += 1
                else:
                    chip_empty += 1
            td = pd.to_datetime(df["datetime"]).dt.strftime("%Y-%m-%d").astype(str).values
            if "amount" not in df.columns:
                df["amount"] = df["close"].astype(float).values * df["vol"].astype(float).values
            try:
                ts_cover_turnover_sum += float(np.isfinite(df["turnover_rate"].astype(float).values).mean())
                ts_cover_vr_sum += float(np.isfinite(df["volume_ratio"].astype(float).values).mean())
                ts_cover_nmf_sum += float(np.isfinite(df["net_mf_amount"].astype(float).values).mean())
                ts_cover_ul_sum += float(np.isfinite(df["up_limit"].astype(float).values).mean())
                ts_cover_dl_sum += float(np.isfinite(df["down_limit"].astype(float).values).mean())
            except Exception:
                ts_fail_merge += 1
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
                    chip_weight_avg=df["chip_weight_avg"].astype(float).values,
                    chip_winner_rate=df["chip_winner_rate"].astype(float).values,
                    chip_band=df["chip_band"].astype(float).values,
                    chip_pos=df["chip_pos"].astype(float).values,
                )
            )
    if bool(debug) and int(params_for_indicators.use_tushare_features) > 0:
        denom = max(1, len(prepared))
        print(
            "DEBUG: tsfeat_summary "
            f"prepared={len(prepared)} ok={ts_ok} empty={ts_empty} fail_merge={ts_fail_merge} "
            f"cover(turnover/vr/nmf/ul/dl)={ts_cover_turnover_sum/denom:.3f}/{ts_cover_vr_sum/denom:.3f}/{ts_cover_nmf_sum/denom:.3f}/{ts_cover_ul_sum/denom:.3f}/{ts_cover_dl_sum/denom:.3f}"
        )
    if bool(debug):
        print(
            f"DEBUG: cache_summary daily_hit={daily_cache_hit} daily_miss={daily_cache_miss} "
            f"ts_hit={ts_cache_hit} ts_miss={ts_cache_miss} chip_hit={chip_cache_hit} chip_miss={chip_cache_miss}"
        )
        print(
            "DEBUG: preload_skips "
            f"daily_empty={skip_daily_empty} prepare_empty={skip_prepare_empty} missing_ind={skip_missing_indicators} too_short_after_dropna={skip_too_short_after_dropna}",
            flush=True,
        )
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
    max_abs_worst_mae: float,
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
    worst_mae = float(m.get("worst_mae", 0.0))
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
        penalty += (abs(float(min_ret)) - float(max_abs_min_ret)) * 250.0
        feasible = 0.0
    if float(max_abs_avg_loss_ret) > 0.0 and abs(float(avg_loss_ret)) > float(max_abs_avg_loss_ret):
        penalty += (abs(float(avg_loss_ret)) - float(max_abs_avg_loss_ret)) * 120.0
        feasible = 0.0
    if float(max_abs_worst_mae) > 0.0 and abs(float(worst_mae)) > float(max_abs_worst_mae):
        penalty += (abs(float(worst_mae)) - float(max_abs_worst_mae)) * 120.0
        feasible = 0.0

    base_score = float(m.get("score", -1e9))
    m["score"] = base_score - float(penalty)
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
        "use_chip_features": float(p.use_chip_features),
        "min_turnover_rate": float(p.min_turnover_rate),
        "max_turnover_rate": float(p.max_turnover_rate),
        "min_volume_ratio": float(p.min_volume_ratio),
        "min_net_mf_amount": float(p.min_net_mf_amount),
        "min_net_mf_ratio": float(p.min_net_mf_ratio),
        "min_winner_rate": float(p.min_winner_rate),
        "max_winner_rate": float(p.max_winner_rate),
        "min_chip_pos": float(p.min_chip_pos),
        "max_chip_band": float(p.max_chip_band),
    }


def _params_from_args(args: argparse.Namespace) -> StrategyParams:
    return StrategyParams(
        pullback_min=float(args.pullback_min),
        pullback_max=float(args.pullback_max),
        vol_contract_ratio=float(args.vol_contract_ratio),
        take_profit_pct=float(args.take_profit_pct),
        stop_loss_pct=float(args.stop_loss_pct),
        trail_stop_pct=float(args.trail_stop_pct),
        breakeven_after_pct=float(args.breakeven_after_pct),
        exit_on_ma_fast_break=int(args.exit_on_ma_fast_break),
        max_hold_days=int(args.max_hold_days),
        max_gap_up_pct=float(args.max_gap_up_pct),
        trend_ma_fast=int(args.trend_ma_fast),
        trend_ma_slow=int(args.trend_ma_slow),
        trend_ma_long=int(args.trend_ma_long),
        use_tushare_features=1 if bool(args.use_tushare_features) else 0,
        use_chip_features=1 if bool(args.use_chip_features) else 0,
        min_turnover_rate=float(args.turnover_min),
        max_turnover_rate=float(args.turnover_max),
        min_volume_ratio=float(args.vr_min),
        min_net_mf_amount=float(args.netmf_min),
        min_net_mf_ratio=float(args.netmf_ratio_min),
        min_winner_rate=float(args.winner_min),
        max_winner_rate=float(args.winner_max),
        min_chip_pos=float(args.chip_pos_min),
        max_chip_band=float(args.chip_band_max),
    )


def _try_load_params_from_topk_csv(path: str) -> Optional[StrategyParams]:
    p = str(path or "").strip()
    if not p or (not os.path.exists(p)):
        return None
    try:
        df = pd.read_csv(p)
        if df is None or df.empty:
            return None
        row = df.sort_values(["score"], ascending=False).iloc[0].to_dict()
        return StrategyParams(
            pullback_min=float(row.get("pullback_min")),
            pullback_max=float(row.get("pullback_max")),
            vol_contract_ratio=float(row.get("vol_contract_ratio")),
            take_profit_pct=float(row.get("take_profit_pct")),
            stop_loss_pct=float(row.get("stop_loss_pct")),
            trail_stop_pct=float(row.get("trail_stop_pct")),
            breakeven_after_pct=float(row.get("breakeven_after_pct")),
            exit_on_ma_fast_break=int(float(row.get("exit_on_ma_fast_break"))),
            max_hold_days=int(float(row.get("max_hold_days"))),
            max_gap_up_pct=float(row.get("max_gap_up_pct")),
            trend_ma_fast=int(float(row.get("trend_ma_fast"))),
            trend_ma_slow=int(float(row.get("trend_ma_slow"))),
            trend_ma_long=int(float(row.get("trend_ma_long"))),
            use_tushare_features=int(float(row.get("use_tushare_features", 0.0))),
            use_chip_features=int(float(row.get("use_chip_features", 0.0))),
            min_turnover_rate=float(row.get("min_turnover_rate", 0.0)),
            max_turnover_rate=float(row.get("max_turnover_rate", 0.0)),
            min_volume_ratio=float(row.get("min_volume_ratio", 0.0)),
            min_net_mf_amount=float(row.get("min_net_mf_amount", 0.0)),
            min_net_mf_ratio=float(row.get("min_net_mf_ratio", 0.0)),
            min_winner_rate=float(row.get("min_winner_rate", 0.0)),
            max_winner_rate=float(row.get("max_winner_rate", 0.0)),
            min_chip_pos=float(row.get("min_chip_pos", 0.0)),
            max_chip_band=float(row.get("max_chip_band", 0.0)),
        )
    except Exception:
        return None


def scan_candidates(prepared: List[PreparedStockData], params: StrategyParams, limit: int) -> pd.DataFrame:
    rows = []
    for d in prepared:
        n = len(d.trade_date)
        if n < 260:
            continue
        i = int(n - 1)
        if i <= 0:
            continue
        if not _signal_ok_at(i=i, data=d, params=params):
            continue
        pullback = float(d.low[i]) / float(d.ma_fast[i]) - 1.0 if float(d.ma_fast[i]) > 0 else np.nan
        vol_ratio = float(d.vol[i]) / float(d.ma_vol_prev[i]) if float(d.ma_vol_prev[i]) > 0 else np.nan
        tr = float(d.turnover_rate[i]) if np.isfinite(d.turnover_rate[i]) else np.nan
        vr = float(d.volume_ratio[i]) if np.isfinite(d.volume_ratio[i]) else np.nan
        nmf = float(d.net_mf_amount[i]) if np.isfinite(d.net_mf_amount[i]) else np.nan
        wr = float(d.chip_winner_rate[i]) if np.isfinite(d.chip_winner_rate[i]) else np.nan
        chip_pos = float(d.chip_pos[i]) if np.isfinite(d.chip_pos[i]) else np.nan
        chip_band = float(d.chip_band[i]) if np.isfinite(d.chip_band[i]) else np.nan
        pb = float(pullback) if np.isfinite(pullback) else 0.0
        vr_vol = float(vol_ratio) if np.isfinite(vol_ratio) else 1.0
        close_ma = (float(d.close[i]) / float(d.ma_fast[i]) - 1.0) if float(d.ma_fast[i]) > 0 else 0.0
        score = (
            close_ma * 50.0
            - abs(pb) * 120.0
            - abs(vr_vol - 0.8) * 30.0
            + (float(nmf) / 200.0 if np.isfinite(nmf) else 0.0)
            + (float(vr) * 5.0 if np.isfinite(vr) else 0.0)
            + ((float(wr) - 10.0) * 0.25 if np.isfinite(wr) else 0.0)
            - (abs(float(chip_pos)) * 60.0 if np.isfinite(chip_pos) else 0.0)
            - (float(chip_band) * 20.0 if np.isfinite(chip_band) else 0.0)
        )
        reason = f"趋势>MA 回踩={pb:.3%} 缩量比={vr_vol:.2f} 胜率={wr:.1f}% 成本偏离={chip_pos:.2%} 集中度={chip_band:.2f}"
        rows.append(
            {
                "symbol": d.stock.code,
                "name": d.stock.name,
                "market": int(d.stock.market),
                "signal_date": str(d.trade_date[i]),
                "score": float(score),
                "reason": str(reason),
                "close": float(d.close[i]),
                "ma_fast": float(d.ma_fast[i]),
                "ma_slow": float(d.ma_slow[i]),
                "pullback": float(pullback),
                "vol_ratio": float(vol_ratio),
                "turnover_rate": float(tr) if np.isfinite(tr) else np.nan,
                "volume_ratio": float(vr) if np.isfinite(vr) else np.nan,
                "net_mf_amount": float(nmf) if np.isfinite(nmf) else np.nan,
                "chip_winner_rate": float(wr) if np.isfinite(wr) else np.nan,
                "chip_pos": float(chip_pos) if np.isfinite(chip_pos) else np.nan,
                "chip_band": float(chip_band) if np.isfinite(chip_band) else np.nan,
            }
        )
    df = pd.DataFrame(rows)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.sort_values(["score"], ascending=False).reset_index(drop=True)
    if int(limit) > 0:
        df = df.head(int(limit)).copy()
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="均衡短持策略：顺势回踩，低持有周期/低回撤/高胜率/尽量高收益")
    parser.add_argument("--mode", type=str, default=os.getenv("MODE", "backtest"), choices=["backtest", "optimize", "scan"])
    parser.add_argument("--exclude-st", action="store_true")

    parser.add_argument("--start-year", type=int, default=2020)
    parser.add_argument("--end-year", type=int, default=pd.Timestamp.today().year - 1)
    parser.add_argument("--max-stocks", type=int, default=300)
    parser.add_argument("--cache-dir", type=str, default=os.path.join("backend", "scripts", "赚钱的策略", "_cache_daily"))
    parser.add_argument("--refresh-cache", action="store_true")
    parser.add_argument("--fetch-sleep", type=float, default=0.0)
    parser.add_argument("--cache-strict-range", action="store_true")
    parser.add_argument("--progress-every", type=int, default=50)
    parser.add_argument("--fetch-timeout-s", type=float, default=25.0)
    parser.add_argument("--ts-timeout-s", type=float, default=60.0)
    parser.add_argument("--cache-read-timeout-s", type=float, default=8.0)
    parser.add_argument("--warmup-cache-only", action="store_true")

    parser.add_argument("--use-tushare-features", action="store_true")
    parser.add_argument("--ts-refresh-cache", action="store_true")
    parser.add_argument("--ts-fetch-sleep", type=float, default=0.0)
    parser.add_argument("--turnover-min", type=float, default=0.0)
    parser.add_argument("--turnover-max", type=float, default=0.0)
    parser.add_argument("--vr-min", type=float, default=0.0)
    parser.add_argument("--netmf-min", type=float, default=0.0)
    parser.add_argument("--netmf-ratio-min", type=float, default=0.0)

    parser.add_argument("--use-chip-features", action="store_true")
    parser.add_argument("--chip-refresh-cache", action="store_true")
    parser.add_argument("--chip-fetch-sleep", type=float, default=0.0)
    parser.add_argument("--winner-min", type=float, default=0.0)
    parser.add_argument("--winner-max", type=float, default=0.0)
    parser.add_argument("--chip-pos-min", type=float, default=0.0)
    parser.add_argument("--chip-band-max", type=float, default=0.0)

    parser.add_argument("--pullback-min", type=float, default=-0.03)
    parser.add_argument("--pullback-max", type=float, default=0.01)
    parser.add_argument("--vol-contract-ratio", type=float, default=0.75)
    parser.add_argument("--take-profit-pct", type=float, default=0.012)
    parser.add_argument("--stop-loss-pct", type=float, default=0.02)
    parser.add_argument("--trail-stop-pct", type=float, default=0.012)
    parser.add_argument("--breakeven-after-pct", type=float, default=0.01)
    parser.add_argument("--exit-on-ma-fast-break", type=int, default=1)
    parser.add_argument("--max-hold-days", type=int, default=3)
    parser.add_argument("--max-gap-up-pct", type=float, default=0.015)
    parser.add_argument("--trend-ma-fast", type=int, default=20)
    parser.add_argument("--trend-ma-slow", type=int, default=60)
    parser.add_argument("--trend-ma-long", type=int, default=120)

    parser.add_argument("--buy-fee-bps", type=float, default=3.0)
    parser.add_argument("--sell-fee-bps", type=float, default=13.0)

    parser.add_argument("--max-iters", type=int, default=3)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--out", type=str, default="")
    parser.add_argument("--tp-min", type=float, default=0.008)
    parser.add_argument("--tp-max", type=float, default=0.03)
    parser.add_argument("--hold-min", type=int, default=1)
    parser.add_argument("--hold-max", type=int, default=5)
    parser.add_argument("--min-trades", type=int, default=200)
    parser.add_argument("--target-win-rate", type=float, default=0.0)
    parser.add_argument("--min-profit-factor", type=float, default=0.0)
    parser.add_argument("--max-abs-min-ret", type=float, default=6.0)
    parser.add_argument("--max-abs-avg-loss-ret", type=float, default=2.8)
    parser.add_argument("--max-abs-worst-mae", type=float, default=5.0)
    parser.add_argument("--params-from-topk", type=str, default="")

    parser.add_argument("--scan-limit", type=int, default=200)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--debug-sample-stocks", type=int, default=30)
    parser.add_argument("--debug-max-checks-per-stock", type=int, default=4000)
    args = parser.parse_args()

    global _TS_DEBUG
    _TS_DEBUG = bool(args.debug)

    if int(args.end_year) < int(args.start_year):
        raise SystemExit("end-year 必须 >= start-year")

    params = _try_load_params_from_topk_csv(args.params_from_topk) or _params_from_args(args)

    warmup_days = 260
    if str(args.mode).lower() == "scan":
        start_year = pd.Timestamp.today().year - 2
        end_year = pd.Timestamp.today().year
        min_date = (pd.Timestamp.today() - pd.Timedelta(days=warmup_days + 370)).strftime("%Y-%m-%d")
        max_date = (pd.Timestamp.today() + pd.Timedelta(days=10)).strftime("%Y-%m-%d")
    else:
        start_year = int(args.start_year)
        end_year = int(args.end_year)
        min_date = (pd.Timestamp(year=start_year, month=1, day=1) - pd.Timedelta(days=warmup_days + 30)).strftime("%Y-%m-%d")
        max_date = (pd.Timestamp(year=end_year, month=12, day=31) + pd.Timedelta(days=10)).strftime("%Y-%m-%d")

    buy_cost_rate = _cost_rate_from_bps(float(args.buy_fee_bps))
    sell_cost_rate = _cost_rate_from_bps(float(args.sell_fee_bps))

    exclude_st = bool(args.exclude_st)
    active = _load_active_codes_from_tushare(exclude_st=exclude_st)
    stocks: List[StockDef] = []

    with tdx:
        connected_endpoint()
        for s in _iter_all_a_share_defs(exclude_st=exclude_st):
            if active is not None:
                if _ts_code(s.market, s.code) not in active:
                    continue
            stocks.append(s)
            if int(args.max_stocks) > 0 and len(stocks) >= int(args.max_stocks):
                break

    if not stocks:
        print("股票池为空")
        return

    print("=" * 60)
    print("均衡短持策略")
    print(f"mode={args.mode} tdx_endpoint={connected_endpoint()}")
    print(f"stocks={len(stocks)} cache_dir={args.cache_dir} refresh_cache={bool(args.refresh_cache)}")
    print(
        f"use_tushare_features={bool(args.use_tushare_features)} use_chip_features={bool(args.use_chip_features)} pro={'ok' if pro is not None else 'none'}"
    )
    print(pd.Series(format_params(params)).to_string())
    print("=" * 60)

    prepared = preload_stock_data(
        stocks=stocks,
        cache_dir=str(args.cache_dir),
        refresh_cache=bool(args.refresh_cache),
        fetch_sleep=float(args.fetch_sleep),
        ts_refresh_cache=bool(args.ts_refresh_cache),
        ts_fetch_sleep=float(args.ts_fetch_sleep),
        chip_refresh_cache=bool(args.chip_refresh_cache),
        chip_fetch_sleep=float(args.chip_fetch_sleep),
        min_date=str(min_date),
        max_date=str(max_date),
        params_for_indicators=params,
        strict_range=bool(args.cache_strict_range),
        progress_every=int(args.progress_every),
        fetch_timeout_s=float(args.fetch_timeout_s),
        ts_timeout_s=float(args.ts_timeout_s),
        cache_read_timeout_s=float(args.cache_read_timeout_s),
        debug=bool(args.debug),
    )
    if bool(args.warmup_cache_only):
        print(f"cache_warmup_finished prepared={len(prepared)}", flush=True)
        return
    if not prepared:
        print("股票数据为空，无法继续")
        return
    if bool(args.debug):
        _debug_diagnose_prepared(
            prepared=prepared,
            params=params,
            start_year=int(args.start_year),
            end_year=int(args.end_year),
            sample_stocks=int(args.debug_sample_stocks),
            max_checks_per_stock=int(args.debug_max_checks_per_stock),
        )

    if str(args.mode).lower() == "scan":
        df = scan_candidates(prepared=prepared, params=params, limit=int(args.scan_limit))
        ts = time.strftime("%Y%m%d_%H%M%S")
        out_base = (args.out.strip() or os.path.join(_script_dir, f"均衡短持_scan_{ts}")).strip()
        if df is None or df.empty:
            print("无候选")
            return
        df.to_csv(out_base + "_candidates.csv", index=False, encoding="utf-8-sig")
        print(f"输出: {out_base + '_candidates.csv'} rows={len(df)}")
        return

    if str(args.mode).lower() == "backtest":
        m, df_trades = evaluate_params(
            params=params,
            prepared=prepared,
            start_year=int(args.start_year),
            end_year=int(args.end_year),
            buy_cost_rate=buy_cost_rate,
            sell_cost_rate=sell_cost_rate,
            min_trades=0,
            target_win_rate=0.0,
            min_profit_factor=0.0,
            max_abs_min_ret=0.0,
            max_abs_avg_loss_ret=0.0,
            max_abs_worst_mae=0.0,
        )
        print(pd.Series(m).to_string())
        ts = time.strftime("%Y%m%d_%H%M%S")
        out_base = (args.out.strip() or os.path.join(_script_dir, f"均衡短持_backtest_{ts}")).strip()
        if df_trades is not None and not df_trades.empty:
            df_trades = df_trades.sort_values(["entry_date", "symbol"], ascending=[True, True]).reset_index(drop=True)
            df_trades.to_csv(out_base + "_trades.csv", index=False, encoding="utf-8-sig")
            print(f"输出: {out_base + '_trades.csv'} rows={len(df_trades)}")
        return

    best_params: Optional[StrategyParams] = None
    best_score = None
    history_rows = []

    max_iters = int(args.max_iters)
    if max_iters <= 0:
        t0 = time.perf_counter()
        m, _ = evaluate_params(
            params=params,
            prepared=prepared,
            start_year=int(args.start_year),
            end_year=int(args.end_year),
            buy_cost_rate=buy_cost_rate,
            sell_cost_rate=sell_cost_rate,
            min_trades=int(args.min_trades),
            target_win_rate=float(args.target_win_rate),
            min_profit_factor=float(args.min_profit_factor),
            max_abs_min_ret=float(args.max_abs_min_ret),
            max_abs_avg_loss_ret=float(args.max_abs_avg_loss_ret),
            max_abs_worst_mae=float(args.max_abs_worst_mae),
        )
        elapsed = time.perf_counter() - t0
        row = {"iter": 0, "idx": 1, "elapsed_s": round(elapsed, 2)}
        row.update(format_params(params))
        row.update({k: float(v) for k, v in m.items()})
        history_rows.append(row)
        best_params = params
        best_score = float(m.get("score", -1e9))
    else:
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
                use_tushare_features=1 if bool(args.use_tushare_features) else 0,
                use_chip_features=1 if bool(args.use_chip_features) else 0,
                min_turnover_rate=float(args.turnover_min),
                max_turnover_rate=float(args.turnover_max),
                min_volume_ratio=float(args.vr_min),
                min_net_mf_amount=float(args.netmf_min),
                min_net_mf_ratio=float(args.netmf_ratio_min),
                min_winner_rate=float(args.winner_min),
                max_winner_rate=float(args.winner_max),
                min_chip_pos=float(args.chip_pos_min),
                max_chip_band=float(args.chip_band_max),
            )
            print("-" * 60)
            print(f"第{it+1}轮参数搜索：候选={len(grid)} refine={refine}")

            iter_results = []
            for idx, p in enumerate(grid, start=1):
                t0 = time.perf_counter()
                m, _ = evaluate_params(
                    params=p,
                    prepared=prepared,
                    start_year=int(args.start_year),
                    end_year=int(args.end_year),
                    buy_cost_rate=buy_cost_rate,
                    sell_cost_rate=sell_cost_rate,
                    min_trades=int(args.min_trades),
                    target_win_rate=float(args.target_win_rate),
                    min_profit_factor=float(args.min_profit_factor),
                    max_abs_min_ret=float(args.max_abs_min_ret),
                    max_abs_avg_loss_ret=float(args.max_abs_avg_loss_ret),
                    max_abs_worst_mae=float(args.max_abs_worst_mae),
                )
                elapsed = time.perf_counter() - t0
                row = {"iter": it + 1, "idx": idx, "elapsed_s": round(elapsed, 2)}
                row.update(format_params(p))
                row.update({k: float(v) for k, v in m.items()})
                iter_results.append((float(m.get("score", -1e9)), p, row))
                if idx % 10 == 0:
                    print(
                        f"进度: {idx}/{len(grid)} score={row['score']:.2f} feasible={row.get('feasible', 0.0):.0f} trades={row['trades']:.0f} win_rate={row['win_rate']:.2f} pf={row['profit_factor']:.2f} min_ret={row['min_ret']:.2f} worst_mae={row.get('worst_mae', 0.0):.2f}"
                    )

            iter_results.sort(key=lambda x: x[0], reverse=True)
            best_feasible = next((x for x in iter_results if float(x[2].get("feasible", 0.0)) > 0.0), None)
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
                "avg_loss_ret",
                "worst_mae",
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

            if refine and prev_center is not None and top1_params == prev_center:
                print("收敛：最优参数在细化轮次保持不变")
                break
            best_params = top1_params

    print("=" * 60)
    print("最终最优参数：")
    print(pd.Series(format_params(best_params)).to_string() if best_params else "无")
    print(f"best_score={best_score}")

    ts = time.strftime("%Y%m%d_%H%M%S")
    out_base = (args.out.strip() or os.path.join(_script_dir, f"均衡短持_opt_{ts}")).strip()
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
        start_year=int(args.start_year),
        end_year=int(args.end_year),
        buy_cost_rate=buy_cost_rate,
        sell_cost_rate=sell_cost_rate,
        min_trades=0,
        target_win_rate=0.0,
        min_profit_factor=0.0,
        max_abs_min_ret=0.0,
        max_abs_avg_loss_ret=0.0,
        max_abs_worst_mae=0.0,
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
