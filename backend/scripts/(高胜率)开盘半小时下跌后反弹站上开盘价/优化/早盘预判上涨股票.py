"""
全市场扫描：开盘前 30 分钟先下跌，随后反弹并稳稳站上开盘价（强主力扫货信号）
10:20–10:40 之间跑一遍
对结果标的进行排查：先看所属板块情况，再看当前量比

不加任何参数的默认行为：
- trade_date=今天，asof_time=当前时间，markets=全市场；不剔除 ST；不限制扫描数量
- 实时模式（trade_date=今天）：先按快照做预筛（默认 pct_from_open>=0.2%），再逐只拉取当日 1 分钟K到 asof_time
- 信号（默认阈值）：09:30–10:00 先出现回撤（low 相对开盘<=-0.4% 且 09:59 收盘相对开盘<=-0.05%），随后在 75 分钟内站上开盘价（>+0.1%）并在最后 10 分钟持续站稳（允许回踩 0.03%）
- 近两日“深跌后快速反弹”加分项默认开启：扫描昨天/前天的 1 分钟K，找 10 分钟内深跌(>=3%)后接着 10 分钟快速反弹(>=2%)且回补>=0.6 的记录；昨日权重 1.0、前日权重 0.6；加分=recent_total*1.2
- 输出：默认写到脚本同目录 CSV，按 score=pct_from_open+bonus_points 综合得分排序，输出 Top200

典型用法（盘中任意时刻运行，asof_time 默认取当前时间）：
python3 "/Users/huangchuanjian/workspace/my_projects/ai_watch_stock/backend/scripts/(高胜率)开盘半小时下跌后反弹站上开盘价/优化/早盘预判上涨股票.py"

更严格/更快：
python3 "/Users/huangchuanjian/workspace/my_projects/ai_watch_stock/backend/scripts/(高胜率)开盘半小时下跌后反弹站上开盘价/优化/早盘预判上涨股票.py" --prefilter-pct-from-open 0.8 --max-stocks 2000
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

try:
    from utils.tushare_client import pro
    _tushare_available = True
except Exception:
    _tushare_available = False

from utils.pytdx_client import tdx, connected_endpoint
from utils.stock_codes import get_all_a_share_codes

_chip_data_cache: Dict[str, pd.DataFrame] = {}


def _get_cached_chip_data(ts_code: str, trade_date: str) -> Optional[pd.DataFrame]:
    return _chip_data_cache.get(f"{ts_code}_{trade_date}")


def _set_cached_chip_data(ts_code: str, trade_date: str, df: pd.DataFrame) -> None:
    _chip_data_cache[f"{ts_code}_{trade_date}"] = df


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


def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _trade_date_default() -> str:
    return datetime.now().strftime("%Y%m%d")


def _asof_time_default_hhmm() -> str:
    return datetime.now().strftime("%H:%M")


def _resolve_chip_trade_date(base_date: str) -> str:
    if not _tushare_available:
        return str(base_date or "").strip() or datetime.now().strftime("%Y%m%d")
    base_date = str(base_date or "").strip()
    if not base_date:
        base_date = datetime.now().strftime("%Y%m%d")
    try:
        end_dt = datetime.strptime(base_date, "%Y%m%d")
    except Exception:
        return base_date
    start_dt = end_dt - timedelta(days=120)
    cal = pro.trade_cal(
        exchange="SSE",
        start_date=start_dt.strftime("%Y%m%d"),
        end_date=base_date,
        fields="cal_date,is_open",
    )
    if cal is None or cal.empty:
        return base_date
    cal = cal[cal["is_open"] == 1]
    if cal.empty:
        return base_date
    open_dates = sorted(cal["cal_date"].astype(str).unique())
    if not open_dates:
        return base_date
    open_dates = [d for d in open_dates if d <= base_date]
    if not open_dates:
        return base_date
    resolved = open_dates[-1]
    now = datetime.now()
    today = now.strftime("%Y%m%d")
    if base_date == today and resolved == today and now.time().strftime("%H%M") < "1630":
        if len(open_dates) >= 2:
            resolved = open_dates[-2]
    return resolved


def _resolve_daily_basic_trade_date(base_date: str) -> str:
    if not _tushare_available:
        return str(base_date or "").strip() or datetime.now().strftime("%Y%m%d")
    base_date = str(base_date or "").strip()
    if not base_date:
        base_date = datetime.now().strftime("%Y%m%d")
    try:
        end_dt = datetime.strptime(base_date, "%Y%m%d")
    except Exception:
        return base_date
    start_dt = end_dt - timedelta(days=120)
    cal = pro.trade_cal(
        exchange="SSE",
        start_date=start_dt.strftime("%Y%m%d"),
        end_date=base_date,
        fields="cal_date,is_open",
    )
    if cal is None or cal.empty:
        return base_date
    cal = cal[cal["is_open"] == 1]
    if cal.empty:
        return base_date
    open_dates = sorted(cal["cal_date"].astype(str).unique())
    if not open_dates:
        return base_date
    open_dates = [d for d in open_dates if d <= base_date]
    if not open_dates:
        return base_date
    resolved = open_dates[-1]
    now = datetime.now()
    today = now.strftime("%Y%m%d")
    if base_date == today and resolved == today and now.time().strftime("%H%M") < "1630":
        if len(open_dates) >= 2:
            resolved = open_dates[-2]
    for d in reversed(open_dates[-20:]):
        try:
            chk = pro.daily_basic(trade_date=d, fields="ts_code,circ_mv,total_mv")
        except Exception:
            chk = None
        if chk is not None and not chk.empty:
            resolved = d
            break
    return resolved


def _calc_drop_pct_by_mv(
    base_pct: float,
    circ_mv: float,
    mv_min: float,
    mv_max: float,
    mult_min: float,
    mult_max: float,
) -> float:
    base_pct = float(base_pct)
    mv = float(circ_mv or 0.0)
    if mv <= 0:
        return base_pct
    mv_yi = mv / 1e4
    if mv_max <= mv_min:
        return base_pct * float(mult_min)
    ratio = (mv_yi - float(mv_min)) / (float(mv_max) - float(mv_min))
    ratio = min(1.0, max(0.0, ratio))
    mult = float(mult_min) + ratio * (float(mult_max) - float(mult_min))
    return base_pct * mult


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
    except Exception:
        return None

    probe_count = max(50, min(int(probe_count), 800))
    probe_step = int(probe_count)
    max_probe_start = int(240 * 40)
    base = 0
    while base <= max_probe_start:
        try:
            bars = tdx_.get_security_bars(8, int(probe_market), str(probe_code).zfill(6), int(base), int(probe_step))
        except Exception:
            bars = []
        df = tdx_.to_df(bars) if bars else pd.DataFrame()
        if df is None or df.empty or "datetime" not in df.columns:
            base += probe_step
            continue
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



def _detect_signal(
    df_1m: pd.DataFrame,
    trade_date: str,
    first30_drop_pct: float,
    first30_close_below_open_pct: float,
    min_rebound_pct: float,
    cross_above_open_pct: float,
    max_cross_minutes: int,
    hold_tolerance_pct: float,
    min_hold_minutes: int,
    enable_after_cross_support: bool,
    min_after_cross_up_dn_vol_ratio: float,
    max_after_cross_down_vol_share: float,
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
    if min_close_after_cross < hold_line:
        return None

    if bool(enable_after_cross_support):
        if "vol" not in df_1m.columns:
            return None
        ac = after_cross.copy()
        ac["_o"] = pd.to_numeric(ac["open"], errors="coerce")
        ac["_c"] = pd.to_numeric(ac["close"], errors="coerce")
        ac["_v"] = pd.to_numeric(ac["vol"], errors="coerce")
        ac = ac.dropna(subset=["_o", "_c", "_v"])
        ac = ac[ac["_v"] > 0].copy()
        if ac.empty:
            return None
        up = ac[ac["_c"] >= ac["_o"]]
        dn = ac[ac["_c"] < ac["_o"]]
        up_vol = float(pd.to_numeric(up["_v"], errors="coerce").sum() or 0.0)
        dn_vol = float(pd.to_numeric(dn["_v"], errors="coerce").sum() or 0.0)
        denom = up_vol + dn_vol
        if denom <= 0:
            return None
        up_dn_ratio = up_vol / (dn_vol + 1e-12)
        dn_share = dn_vol / (denom + 1e-12)
        if float(min_after_cross_up_dn_vol_ratio) > 0 and up_dn_ratio < float(min_after_cross_up_dn_vol_ratio):
            return None
        if 0 < float(max_after_cross_down_vol_share) < 1 and dn_share > float(max_after_cross_down_vol_share):
            return None

    last_close = float(pd.to_numeric(df_1m["close"].iloc[-1], errors="coerce") or 0.0)
    if last_close <= 0:
        return None
    pct_from_open = (last_close - open_px) / open_px * 100.0
    if pct_from_open < float(min_rebound_pct):
        return None

    min_hold_minutes = max(5, int(min_hold_minutes))
    tail = df_1m.iloc[-min(min_hold_minutes, len(df_1m)) :].copy()
    if tail.empty:
        return None
    if float(pd.to_numeric(tail["close"], errors="coerce").min() or float("nan")) < hold_line:
        return None

    return {
        "open": open_px,
        "low_0930_1000": low30,
        "low_0930_1000_pct": low30_pct,
        "close_0959": close30,
        "close_0959_pct": close30_pct,
        "cross_time": cross_time.strftime("%H:%M"),
        "cross_minutes": cross_minutes,
        "last_close": last_close,
        "pct_from_open": pct_from_open,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date", default=_trade_date_default(), help="交易日 YYYYMMDD（默认今天）")
    parser.add_argument("--asof-time", default=_asof_time_default_hhmm(), help="截至时间 HH:MM（默认当前）")
    parser.add_argument("--markets", default="all", help="all/sz/sh")
    parser.add_argument("--exclude-st", action="store_true", help="剔除名称包含 ST 的标的")
    parser.add_argument("--max-stocks", type=int, default=0, help="最多扫描多少只（0=不限制）")

    parser.add_argument("--prefilter-pct-from-open", type=float, default=0.2, help="快照预筛：当前价相对开盘涨幅下限(%%)")
    parser.add_argument("--first30-drop-pct", type=float, default=0.4, help="前30分钟最低价相对开盘的跌幅下限(%%)")
    parser.add_argument("--first30-close-below-open-pct", type=float, default=0.05, help="前30分钟结束时(≈09:59)收盘低于开盘的跌幅下限(%%)")
    parser.add_argument("--first30-drop-pct-mode", type=str, default="cap", help="前30分钟回撤阈值模式：fixed/cap，默认cap")
    parser.add_argument("--cap-mv-min", type=float, default=50.0, help="市值缩放下限(亿)")
    parser.add_argument("--cap-mv-max", type=float, default=1000.0, help="市值缩放上限(亿)")
    parser.add_argument("--cap-mult-min", type=float, default=1.0, help="小市值倍率")
    parser.add_argument("--cap-mult-max", type=float, default=2.0, help="大市值倍率")
    parser.add_argument("--min-rebound-pct", type=float, default=0.9, help="截至时刻相对开盘涨幅下限(%%)")
    parser.add_argument("--cross-above-open-pct", type=float, default=0.1, help="站上开盘价的阈值(%%)")
    parser.add_argument("--max-cross-minutes", type=int, default=75, help="必须在开盘后多少分钟内站上开盘价")
    parser.add_argument("--hold-tolerance-pct", type=float, default=0.03, help="站上后允许回踩开盘价的容忍度(%%)")
    parser.add_argument("--min-hold-minutes", type=int, default=10, help="最近 N 分钟需要持续站稳开盘价")
    parser.add_argument("--enable-after-cross-support", action="store_true", help="启用站上后承接过滤")
    parser.add_argument("--min-after-cross-up-dn-vol-ratio", type=float, default=1.5, help="站上后上涨分钟量/下跌分钟量下限")
    parser.add_argument("--max-after-cross-down-vol-share", type=float, default=0.3, help="站上后下跌分钟量占比上限(0-1)")

    parser.add_argument("--disable-recent-deepdrop-rebound", action="store_true", help="关闭近两日“深跌后快速反弹”加分项（默认开启）")
    parser.add_argument("--recent-drop-window-minutes", type=int, default=10, help="深跌识别窗口(分钟)")
    parser.add_argument("--recent-rebound-window-minutes", type=int, default=10, help="反弹识别窗口(分钟)")
    parser.add_argument("--recent-deep-drop-pct", type=float, default=3.0, help="深跌阈值：窗口内回撤下限(%%)")
    parser.add_argument("--recent-rebound-min-pct", type=float, default=2.0, help="反弹阈值：窗口内反弹下限(%%)")
    parser.add_argument("--recent-recover-ratio", type=float, default=0.6, help="反弹回补比例下限(0-1)")
    parser.add_argument("--recent-weight-yesterday", type=float, default=1.0, help="昨日权重")
    parser.add_argument("--recent-weight-daybefore", type=float, default=0.6, help="前日权重")
    parser.add_argument("--recent-bonus-points", type=float, default=1.2, help="近两日反弹强度的加分系数（按百分点叠加到涨幅）")

    parser.add_argument("--quote-chunk-size", type=int, default=80, help="快照请求分块大小")
    parser.add_argument("--quote-sleep-s", type=float, default=0.02, help="快照分块间隔(秒)")
    parser.add_argument("--bars-max-total", type=int, default=320, help="分钟K最多拉取条数（不足会导致信号缺失）")
    parser.add_argument("--bars-step", type=int, default=200, help="分钟K单次拉取条数")
    parser.add_argument("--per-stock-sleep-s", type=float, default=0.0, help="每只股票分钟K拉取后休眠(秒)")

    parser.add_argument("--topk", type=int, default=200, help="输出TopK（按综合得分排序）")
    parser.add_argument("--output-csv", default="", help="输出 CSV 路径（默认脚本同目录）")

    parser.add_argument("--enable-chip-concentrated", action="store_true", help="启用筹码聚集筛选（需要tushare）")
    parser.add_argument("--chip-concentration-threshold", type=float, default=15.0, help="筹码集中度阈值（%%），默认15%%")
    parser.add_argument("--min-winner-rate", type=float, default=40.0, help="最小胜率阈值（%%），默认40%%")
    args = parser.parse_args()

    trade_date = str(args.trade_date).strip()
    asof_time = str(args.asof_time).strip()
    markets = set(_parse_markets(args.markets))
    today = datetime.now().strftime("%Y%m%d")

    out_path = str(args.output_csv or "").strip()
    if not out_path:
        out_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f"开盘半小时下跌后反弹站上开盘价_{trade_date}_{_now_ts()}.csv",
        )

    enable_chip_concentrated = bool(args.enable_chip_concentrated)
    chip_concentration_threshold = float(args.chip_concentration_threshold)
    min_winner_rate = float(args.min_winner_rate)
    first30_drop_pct_mode = str(args.first30_drop_pct_mode or "").strip().lower()
    cap_mv_min = float(args.cap_mv_min)
    cap_mv_max = float(args.cap_mv_max)
    cap_mult_min = float(args.cap_mult_min)
    cap_mult_max = float(args.cap_mult_max)

    with tdx:
        ep = connected_endpoint()
        if ep is not None:
            print(f"{_now_ts()} pytdx 已连接: {ep[0]}:{ep[1]}", flush=True)

        enable_recent_bonus = not bool(args.disable_recent_deepdrop_rebound)
        recent_trade_dates: List[str] = []
        recent_offsets: Dict[str, int] = {}
        if enable_recent_bonus:
            recent_trade_dates, recent_offsets = _resolve_recent_trade_dates_with_offsets(
                tdx,
                base_trade_date=str(trade_date),
                want=2,
                asof_time="15:00",
                probe_market=0,
                probe_code="000001",
            )

        df_codes = get_all_a_share_codes()
        if df_codes is None or df_codes.empty:
            print(f"{_now_ts()} 股票列表为空", flush=True)
            return 2

        df_codes = df_codes[df_codes["market"].isin(list(markets))].copy()
        if bool(args.exclude_st) and "name" in df_codes.columns:
            df_codes = df_codes[~df_codes["name"].astype(str).str.upper().str.contains("ST", na=False)].copy()
        if int(args.max_stocks) > 0:
            df_codes = df_codes.head(int(args.max_stocks)).copy()

        print(f"{_now_ts()} 股票池: {len(df_codes)}", flush=True)

        mv_map: Dict[str, float] = {}
        if first30_drop_pct_mode == "cap":
            if not _tushare_available:
                print(f"{_now_ts()} 警告：tushare 不可用，回撤阈值退化为 fixed", flush=True)
            else:
                mv_trade_date = _resolve_daily_basic_trade_date(trade_date)
                if mv_trade_date != trade_date:
                    print(f"{_now_ts()} 市值基准日已调整: {trade_date} -> {mv_trade_date}", flush=True)
                mv_df = pro.daily_basic(trade_date=mv_trade_date, fields="ts_code,circ_mv,total_mv")
                if mv_df is None or mv_df.empty:
                    print(f"{_now_ts()} 市值数据为空，回撤阈值退化为 fixed", flush=True)
                else:
                    for _, r in mv_df.iterrows():
                        ts_code = str(r.get("ts_code") or "").strip()
                        circ_mv = float(r.get("circ_mv") or 0.0)
                        total_mv = float(r.get("total_mv") or 0.0)
                        mv = circ_mv if circ_mv > 0 else total_mv
                        if ts_code and mv > 0:
                            mv_map[ts_code] = mv

        start_offset_hint: Optional[int] = None
        if trade_date != today:
            start_offset_hint = _compute_start_offset_by_probe(tdx, trade_date=trade_date, asof_time=asof_time)
            if start_offset_hint is None:
                print(
                    f"{_now_ts()} 历史模式定位失败：probe 无法覆盖目标时刻（trade_date={trade_date}, asof_time={asof_time}）",
                    flush=True,
                )
            else:
                print(
                    f"{_now_ts()} 历史模式定位：start_offset_hint={start_offset_hint}（trade_date={trade_date}, asof_time={asof_time}）",
                    flush=True,
                )
            df_quotes = df_codes.copy()
            print(f"{_now_ts()} 历史模式：跳过实时快照预筛，候选={len(df_quotes)}", flush=True)
        else:
            df_quotes = _quotes_snapshot_df(
                tdx,
                df_codes=df_codes,
                chunk_size=int(args.quote_chunk_size),
                sleep_s=float(args.quote_sleep_s),
            )
            if df_quotes is None or df_quotes.empty:
                print(f"{_now_ts()} 快照为空，无法预筛", flush=True)
                return 2

            df_quotes = df_quotes.dropna(subset=["open", "price", "pct_from_open"])
            df_quotes = df_quotes[(df_quotes["open"] > 0) & (df_quotes["price"] > 0)]
            df_quotes = df_quotes[df_quotes["pct_from_open"] >= float(args.prefilter_pct_from_open)].copy()
            df_quotes = df_quotes.sort_values("pct_from_open", ascending=False).reset_index(drop=True)

            print(
                f"{_now_ts()} 预筛后候选: {len(df_quotes)} (pct_from_open>={float(args.prefilter_pct_from_open):.2f}%)",
                flush=True,
            )

        # 筹码聚集筛选
        chip_signal_map: Dict[str, Dict] = {}
        if enable_chip_concentrated:
            if not _tushare_available:
                print(f"{_now_ts()} 警告：tushare 不可用，跳过筹码聚集筛选", flush=True)
            else:
                chip_trade_date = _resolve_chip_trade_date(trade_date)
                if chip_trade_date != trade_date:
                    print(
                        f"{_now_ts()} 筹码基准日已调整: {trade_date} -> {chip_trade_date}",
                        flush=True,
                    )
                chip_before = len(df_quotes)
                chip_passed = []
                for i, r in df_quotes.iterrows():
                    market = int(r["market"])
                    code = str(r["code"]).zfill(6)
                    price = float(r["price"])
                    ts_code = f"{code}.SZ" if market == 0 else f"{code}.SH"
                    
                    chip_signal = _check_chip_concentration(
                        ts_code=ts_code,
                        trade_date=chip_trade_date,
                        current_price=price,
                        chip_concentration_threshold=chip_concentration_threshold,
                        min_winner_rate=min_winner_rate,
                    )
                    
                    if chip_signal is not None:
                        chip_passed.append(r)
                        chip_signal_map[ts_code] = chip_signal
                    
                    if (i + 1) % 200 == 0:
                        print(f"{_now_ts()} 筹码筛选进度: {i+1}/{len(df_quotes)} 已通过: {len(chip_passed)}", flush=True)
                
                if chip_passed:
                    df_quotes = pd.DataFrame(chip_passed).reset_index(drop=True)
                    chip_after = len(df_quotes)
                    chip_drop = chip_before - chip_after
                    print(f"{_now_ts()} 筹码筛选后候选: {chip_after} (筛掉 {chip_drop}, 通过率 {chip_after/max(1, chip_before)*100:.2f}%)", flush=True)
                else:
                    print(f"{_now_ts()} 无股票通过筹码聚集筛选", flush=True)
                    return 0

        rows: List[Dict] = []
        miss_1m = 0
        miss_1m_samples: List[str] = []
        miss_1m_reason_cnt: Dict[str, int] = {}
        for i, r in df_quotes.iterrows():
            market = int(r["market"])
            code = str(r["code"]).zfill(6)
            name = str(r.get("name") or "").strip()
            df_1m = _fetch_intraday_1m_bars(
                tdx,
                market=market,
                code=code,
                trade_date=trade_date,
                asof_time=asof_time,
                max_total=int(args.bars_max_total),
                step=int(args.bars_step),
                start_offset_hint=start_offset_hint,
            )
            if df_1m is None or df_1m.empty:
                miss_1m += 1
                if len(miss_1m_samples) < 10:
                    ts_code = f"{code}.SZ" if market == 0 else f"{code}.SH"
                    miss_1m_samples.append(ts_code)
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
                if (i + 1) % 200 == 0:
                    print(f"{_now_ts()} 进度: {i+1}/{len(df_quotes)} 命中: {len(rows)} 缺分钟K: {miss_1m}", flush=True)
                continue

            if trade_date != today:
                try:
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

            sig = _detect_signal(
                df_1m=df_1m,
                trade_date=trade_date,
                first30_drop_pct=_calc_drop_pct_by_mv(
                    base_pct=float(args.first30_drop_pct),
                    circ_mv=mv_map.get(f"{code}.SZ" if market == 0 else f"{code}.SH"),
                    mv_min=cap_mv_min,
                    mv_max=cap_mv_max,
                    mult_min=cap_mult_min,
                    mult_max=cap_mult_max,
                )
                if first30_drop_pct_mode == "cap" and mv_map
                else float(args.first30_drop_pct),
                first30_close_below_open_pct=float(args.first30_close_below_open_pct),
                min_rebound_pct=float(args.min_rebound_pct),
                cross_above_open_pct=float(args.cross_above_open_pct),
                max_cross_minutes=int(args.max_cross_minutes),
                hold_tolerance_pct=float(args.hold_tolerance_pct),
                min_hold_minutes=int(args.min_hold_minutes),
                enable_after_cross_support=bool(args.enable_after_cross_support),
                min_after_cross_up_dn_vol_ratio=float(args.min_after_cross_up_dn_vol_ratio),
                max_after_cross_down_vol_share=float(args.max_after_cross_down_vol_share),
            )
            if sig is not None:
                recent_total = 0.0
                recent_y = 0.0
                recent_p = 0.0
                if enable_recent_bonus and recent_trade_dates and recent_offsets:
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
                
                # 检查筹码聚集信号
                chip_signal = None
                if enable_chip_concentrated and _tushare_available:
                    ts_code = f"{code}.SZ" if market == 0 else f"{code}.SH"
                    chip_signal = chip_signal_map.get(ts_code)
                    if chip_signal is None:
                        chip_signal = _check_chip_concentration(
                            ts_code=ts_code,
                            trade_date=trade_date,
                            current_price=float(sig["last_close"]),
                            chip_concentration_threshold=chip_concentration_threshold,
                            min_winner_rate=min_winner_rate,
                        )
                    if chip_signal is None:
                        if float(args.per_stock_sleep_s) > 0:
                            time.sleep(float(args.per_stock_sleep_s))
                        if (i + 1) % 200 == 0:
                            print(f"{_now_ts()} 进度: {i+1}/{len(df_quotes)} 命中: {len(rows)} 缺分钟K: {miss_1m}", flush=True)
                        continue
                
                row_data = {
                    "ts_code": f"{code}.SZ" if market == 0 else f"{code}.SH",
                    "name": name,
                    "trade_date": trade_date,
                    "asof_time": asof_time,
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
                    "reason": f"前30分钟回撤{sig['low_0930_1000_pct']:.2f}%，随后站上开盘价并稳住",
                }
                
                # 添加筹码聚集相关字段
                if chip_signal:
                    row_data.update({
                        "chip_concentration": chip_signal["chip_concentration"],
                        "winner_rate": chip_signal["winner_rate"],
                        "price_position_pct": chip_signal["price_position_pct"],
                        "cost_5pct": chip_signal["cost_5pct"],
                        "cost_95pct": chip_signal["cost_95pct"],
                        "weight_avg": chip_signal["weight_avg"],
                    })
                
                rows.append(row_data)

            if float(args.per_stock_sleep_s) > 0:
                time.sleep(float(args.per_stock_sleep_s))

            if (i + 1) % 200 == 0:
                print(f"{_now_ts()} 进度: {i+1}/{len(df_quotes)} 命中: {len(rows)} 缺分钟K: {miss_1m}", flush=True)

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
