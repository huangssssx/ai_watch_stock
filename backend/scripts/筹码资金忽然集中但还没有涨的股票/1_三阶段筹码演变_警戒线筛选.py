import argparse
import os
import sys
import time
import traceback
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd


def _project_root() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(here, "..", "..", ".."))


def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _as_yyyymmdd(d: datetime) -> str:
    return d.strftime("%Y%m%d")


def _safe_call(fn, sleep_s: float, what: str) -> Optional[pd.DataFrame]:
    try:
        df = fn()
        if sleep_s and sleep_s > 0:
            time.sleep(float(sleep_s))
        if df is None:
            print(f"{_now_ts()} {what} 返回 None", flush=True)
            return None
        return df
    except Exception as e:
        print(f"{_now_ts()} {what} 异常: {type(e).__name__}:{e}", flush=True)
        if sleep_s and sleep_s > 0:
            time.sleep(float(sleep_s))
        return None


def _pick_last_trade_date(pro, end_date: str) -> str:
    start = _as_yyyymmdd(datetime.strptime(end_date, "%Y%m%d") - timedelta(days=60))
    df = pro.trade_cal(exchange="SSE", start_date=start, end_date=end_date, fields="cal_date,is_open")
    if df is None or df.empty:
        raise RuntimeError("trade_cal 返回为空，无法确定最近交易日")
    df = df[df["is_open"].astype(str) == "1"].copy()
    if df.empty:
        raise RuntimeError("trade_cal 无开市日期，无法确定最近交易日")
    dates = sorted(df["cal_date"].astype(str).tolist())
    return str(dates[-1])


def _load_trade_dates(pro, end_date: str, lookback: int) -> list[str]:
    start = _as_yyyymmdd(datetime.strptime(end_date, "%Y%m%d") - timedelta(days=int(lookback) * 3))
    df = pro.trade_cal(exchange="SSE", start_date=start, end_date=end_date, fields="cal_date,is_open")
    if df is None or df.empty:
        raise RuntimeError("trade_cal 返回为空，无法获取交易日序列")
    df = df[df["is_open"].astype(str) == "1"].copy()
    dates = sorted(df["cal_date"].astype(str).tolist())
    if len(dates) < int(lookback):
        return dates
    return dates[-int(lookback) :]


def _pick_last_chip_trade_date(pro, probe_ts_code: str, end_date: str, sleep_s: float) -> str:
    td = _pick_last_trade_date(pro, end_date=end_date)
    dates = _load_trade_dates(pro, end_date=td, lookback=30)
    dates = list(reversed(dates))
    fields = "ts_code,trade_date,cost_5pct,cost_95pct,weight_avg,winner_rate"
    for d in dates:
        df = _safe_call(
            lambda: pro.cyq_perf(ts_code=str(probe_ts_code), trade_date=str(d), fields=fields),
            sleep_s=sleep_s,
            what=f"cyq_perf_probe({probe_ts_code},{d})",
        )
        if df is None:
            continue
        if not df.empty:
            return str(d)
    raise RuntimeError("近30个交易日均未获取到筹码数据，无法确定 T 日")


def _load_stock_pool(pro, exclude_st: bool, min_list_days: int) -> pd.DataFrame:
    df = pro.stock_basic(exchange="", list_status="L", fields="ts_code,symbol,name,list_date")
    if df is None or df.empty:
        raise RuntimeError("stock_basic 返回为空，无法构建股票池")
    df = df.dropna(subset=["ts_code", "symbol"]).copy()
    df["ts_code"] = df["ts_code"].astype(str).str.strip()
    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["name"] = df.get("name", "").astype(str).str.strip()
    df["list_date"] = df.get("list_date", "").astype(str).str.strip()
    if exclude_st:
        df = df[~df["name"].str.contains("ST", na=False)].copy()
    today = datetime.now().date()
    df["list_dt"] = pd.to_datetime(df["list_date"], format="%Y%m%d", errors="coerce")
    min_dt = today - timedelta(days=int(min_list_days))
    df = df[(df["list_dt"].isna()) | (df["list_dt"].dt.date <= min_dt)].copy()
    df = df.reset_index(drop=True)
    return df


def _load_chip_cache(cache_file: str) -> pd.DataFrame:
    if not cache_file:
        return pd.DataFrame()
    if not os.path.exists(cache_file):
        return pd.DataFrame()
    try:
        df = pd.read_csv(cache_file, dtype={"trade_date": str, "ts_code": str})
        if df is None or df.empty:
            return pd.DataFrame()
        keep = [c for c in ["ts_code", "trade_date", "cost_5pct", "cost_95pct", "weight_avg", "winner_rate"] if c in df.columns]
        if not keep:
            return pd.DataFrame()
        df = df[keep].copy()
        df["ts_code"] = df["ts_code"].astype(str).str.strip()
        df["trade_date"] = df["trade_date"].astype(str).str.strip()
        df = df[df["trade_date"].str.len() == 8].copy()
        df = df.drop_duplicates(subset=["ts_code", "trade_date"], keep="last").reset_index(drop=True)
        return df
    except Exception:
        return pd.DataFrame()


def _append_chip_cache(cache_file: str, df_new: pd.DataFrame):
    if not cache_file:
        return
    if df_new is None or df_new.empty:
        return
    cols = ["ts_code", "trade_date", "cost_5pct", "cost_95pct", "weight_avg", "winner_rate"]
    for c in cols:
        if c not in df_new.columns:
            return
    out = df_new[cols].copy()
    out["ts_code"] = out["ts_code"].astype(str).str.strip()
    out["trade_date"] = out["trade_date"].astype(str).str.strip()
    out = out[out["trade_date"].str.len() == 8].copy()
    if out.empty:
        return
    mode = "a" if os.path.exists(cache_file) else "w"
    header = not os.path.exists(cache_file)
    out.to_csv(cache_file, mode=mode, header=header, index=False)


def _fetch_chip_one(pro, ts_code: str, start_date: str, end_date: str, sleep_s: float) -> pd.DataFrame:
    fields = "ts_code,trade_date,cost_5pct,cost_95pct,weight_avg,winner_rate"
    df = _safe_call(
        lambda: pro.cyq_perf(ts_code=str(ts_code), start_date=str(start_date), end_date=str(end_date), fields=fields),
        sleep_s=sleep_s,
        what=f"cyq_perf({ts_code},{start_date}-{end_date})",
    )
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["ts_code"] = df["ts_code"].astype(str).str.strip()
    df["trade_date"] = df["trade_date"].astype(str).str.strip()
    df = df[df["trade_date"].str.len() == 8].copy()
    df = df.drop_duplicates(subset=["ts_code", "trade_date"], keep="last").reset_index(drop=True)
    return df


def _lin_slope(y: np.ndarray) -> float:
    if y is None:
        return float("nan")
    y = np.asarray(y, dtype=float)
    n = int(y.size)
    if n < 2:
        return float("nan")
    x = np.arange(n, dtype=float)
    x_mean = float(x.mean())
    y_mean = float(y.mean())
    denom = float(np.sum((x - x_mean) ** 2))
    if denom <= 0:
        return float("nan")
    numer = float(np.sum((x - x_mean) * (y - y_mean)))
    return numer / denom


def _calc_chip_band_series(chip_df: pd.DataFrame) -> pd.DataFrame:
    if chip_df is None or chip_df.empty:
        return pd.DataFrame()
    df = chip_df.copy()
    df["trade_date"] = df["trade_date"].astype(str).str.strip()
    df = df[df["trade_date"].str.len() == 8].copy()
    df["date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["date"]).copy()
    for c in ["cost_5pct", "cost_95pct", "weight_avg", "winner_rate"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["cost_5pct", "cost_95pct", "weight_avg"]).copy()
    df = df.sort_values("date").reset_index(drop=True)
    w = df["weight_avg"].to_numpy(dtype=float)
    band = (df["cost_95pct"].to_numpy(dtype=float) - df["cost_5pct"].to_numpy(dtype=float)) / np.where(w > 0, w, np.nan)
    df["chip_band"] = pd.to_numeric(pd.Series(band), errors="coerce")
    df["chip_band_smooth"] = df["chip_band"].rolling(3, min_periods=1).mean()
    df["date_str"] = df["date"].dt.strftime("%Y%m%d")
    return df[["ts_code", "date", "date_str", "trade_date", "chip_band", "chip_band_smooth", "winner_rate"]].copy()


def _recent_concentration_trend(
    df_band: pd.DataFrame,
    anchor_peak_date: str,
    min_days: int,
    lookback_days: int,
    max_days: int,
    min_drop_pct: float,
    min_down_ratio: float,
    min_slope: float,
    start_min_global_q: float,
) -> tuple[bool, dict]:
    if df_band is None or df_band.empty:
        return False, {}
    if "chip_band_smooth" not in df_band.columns:
        return False, {}
    x = pd.to_numeric(df_band["chip_band_smooth"], errors="coerce").to_numpy(dtype=float)
    d = df_band["date_str"].astype(str).to_numpy()
    mask = np.isfinite(x)
    x = x[mask]
    d = d[mask]
    n = int(len(x))
    k = max(0, int(min_days))
    if k <= 0:
        return True, {}
    min_seg = max(2, k)
    if n < min_seg + 1:
        return False, {}
    max_k = max(0, int(max_days))

    i_peak = -1
    apd = str(anchor_peak_date or "").strip()
    if apd and apd in set(d.tolist()):
        idx = np.where(d == apd)[0]
        if idx.size > 0:
            i_peak = int(idx[-1])
    if i_peak < 0:
        i_peak = int(np.argmax(x[: max(1, n - min_seg)]))
    if i_peak >= n - min_seg:
        return False, {}

    lb = max(0, int(lookback_days))
    lb = min(lb, n)
    search_start = max(i_peak + 1, n - lb - 1) if lb > 0 else i_peak + 1
    search_end = n - min_seg
    if search_start >= search_end:
        return False, {}

    best_j = -1
    best_meta = None
    for j in range(int(search_start), int(search_end) + 1):
        if j <= i_peak or j >= n - 1:
            continue
        if max_k > 0 and (n - j) > max_k:
            continue
        if n - j >= 3:
            is_local_peak = bool(x[j] >= x[j - 1] and x[j] >= x[j + 1])
            if not is_local_peak:
                continue
        seg = x[j:]
        if seg.size < min_seg:
            continue
        start_v = float(seg[0])
        end_v = float(seg[-1])
        if not (np.isfinite(start_v) and np.isfinite(end_v) and start_v > 0):
            continue
        slope = _lin_slope(seg)
        if not np.isfinite(slope) or slope >= -abs(float(min_slope)):
            continue
        diffs = np.diff(seg)
        down_ratio = float(np.mean(diffs < 0)) if diffs.size > 0 else 0.0
        if down_ratio < float(min_down_ratio):
            continue
        drop_pct = (start_v - end_v) / start_v
        if drop_pct < float(min_drop_pct):
            continue
        start_q = float(np.mean(x <= start_v))
        if np.isfinite(start_q) and float(start_min_global_q) > 0:
            if start_q < float(start_min_global_q):
                continue
        end_q = float(np.mean(x <= end_v))

        meta = {
            "rc_anchor_peak_date": str(d[i_peak]),
            "rc_anchor_peak_band": float(x[i_peak]),
            "rc_days": int(n - j),
            "rc_start_date": str(d[j]),
            "rc_end_date": str(d[-1]),
            "rc_start_band": float(start_v),
            "rc_end_band": float(end_v),
            "rc_drop_pct": float(drop_pct),
            "rc_down_ratio": float(down_ratio),
            "rc_slope": float(slope),
            "rc_start_global_q": float(start_q) if np.isfinite(start_q) else float("nan"),
            "rc_end_global_q": float(end_q) if np.isfinite(end_q) else float("nan"),
        }

        if j > best_j:
            best_j = j
            best_meta = meta

    if best_meta is not None:
        return True, best_meta
    return False, {}


def _match_three_stage(
    df_band: pd.DataFrame,
    min_total_days: int,
    min_stage3_days: int,
    stage1_gap_pct: float,
    stage3_min_drop_pct: float,
    stage3_min_down_ratio: float,
    stage3_min_slope: float,
    warn_abs_max: float,
    warn_global_q: float,
    warn_post_q: float,
) -> Optional[dict]:
    if df_band is None or df_band.empty:
        return None
    if "chip_band_smooth" not in df_band.columns:
        return None
    x = pd.to_numeric(df_band["chip_band_smooth"], errors="coerce").to_numpy(dtype=float)
    d = df_band["date_str"].astype(str).to_numpy()
    mask = np.isfinite(x)
    x = x[mask]
    d = d[mask]
    n = int(len(x))
    if n < int(min_total_days):
        return None

    stage3_days = int(min_stage3_days)
    if n <= stage3_days + 5:
        return None
    peak_end = n - stage3_days - 1
    if peak_end <= 10:
        return None
    i2 = int(np.argmax(x[: peak_end + 1]))
    if i2 <= 5 or i2 >= n - stage3_days:
        return None

    pre = x[:i2]
    post = x[i2 + 1 :]
    if len(pre) < 10 or len(post) < stage3_days:
        return None

    pre_p60 = float(np.nanpercentile(pre, 60))
    peak = float(x[i2])
    if not np.isfinite(pre_p60) or pre_p60 <= 0:
        return None
    if not (peak >= pre_p60 * (1.0 + float(stage1_gap_pct))):
        return None

    post_recent = post[-stage3_days:]
    if len(post_recent) < stage3_days:
        return None
    post_slope = _lin_slope(post_recent)
    if not np.isfinite(post_slope) or post_slope >= -abs(float(stage3_min_slope)):
        return None

    diffs = np.diff(post_recent)
    down_ratio = float(np.mean(diffs < 0)) if diffs.size > 0 else 0.0
    if down_ratio < float(stage3_min_down_ratio):
        return None

    start_v = float(post_recent[0])
    end_v = float(post_recent[-1])
    if not (np.isfinite(start_v) and np.isfinite(end_v) and start_v > 0):
        return None
    drop_pct = (start_v - end_v) / start_v
    if drop_pct < float(stage3_min_drop_pct):
        return None

    t_band = float(x[-1])
    if not np.isfinite(t_band) or t_band <= 0:
        return None

    global_q = float(np.mean(x <= t_band))
    post_q = float(np.mean(post <= t_band)) if post.size > 0 else global_q

    warn_hit = False
    if float(warn_abs_max) > 0 and t_band <= float(warn_abs_max):
        warn_hit = True
    if global_q <= float(warn_global_q):
        warn_hit = True
    if post_q <= float(warn_post_q):
        warn_hit = True
    if not warn_hit:
        return None

    score = drop_pct * 60.0 + down_ratio * 20.0 + (max(0.0, (float(warn_global_q) - global_q)) / max(1e-6, float(warn_global_q))) * 20.0
    return {
        "t_date": str(d[-1]),
        "t_band": t_band,
        "t_band_global_q": global_q,
        "t_band_post_q": post_q,
        "stage2_date": str(d[i2]),
        "stage2_band": peak,
        "stage3_days": stage3_days,
        "stage3_slope": float(post_slope),
        "stage3_down_ratio": down_ratio,
        "stage3_drop_pct": drop_pct,
        "score": float(score),
    }


def _fetch_daily_close(pro, ts_code: str, start_date: str, end_date: str, sleep_s: float) -> pd.DataFrame:
    fields = "ts_code,trade_date,close,pct_chg"
    df = _safe_call(
        lambda: pro.daily(ts_code=str(ts_code), start_date=str(start_date), end_date=str(end_date), fields=fields),
        sleep_s=sleep_s,
        what=f"daily({ts_code},{start_date}-{end_date})",
    )
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["trade_date"] = df["trade_date"].astype(str).str.strip()
    df = df[df["trade_date"].str.len() == 8].copy()
    for c in ["close", "pct_chg"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["trade_date", "close"]).copy()
    df = df.sort_values("trade_date").reset_index(drop=True)
    return df


def _not_raised_filter(daily_df: pd.DataFrame, t_date: str, max_ret_10d: float, max_pct_chg_t: float) -> tuple[bool, dict]:
    if daily_df is None or daily_df.empty:
        return False, {}
    df = daily_df.copy()
    df = df[df["trade_date"].astype(str) <= str(t_date)].copy()
    if df.empty:
        return False, {}
    df = df.tail(15).copy()
    df = df.dropna(subset=["close"]).copy()
    if df.empty:
        return False, {}
    t_close = float(df["close"].iloc[-1])
    t_pct_chg = float(df["pct_chg"].iloc[-1]) if "pct_chg" in df.columns and np.isfinite(df["pct_chg"].iloc[-1]) else float("nan")
    ret_10d = float("nan")
    if len(df) >= 11:
        base = float(df["close"].iloc[-11])
        if base > 0 and np.isfinite(t_close):
            ret_10d = t_close / base - 1.0
    ok = True
    if np.isfinite(ret_10d) and float(max_ret_10d) > 0:
        if ret_10d > float(max_ret_10d):
            ok = False
    if np.isfinite(t_pct_chg) and float(max_pct_chg_t) > 0:
        if t_pct_chg > float(max_pct_chg_t) * 100.0:
            ok = False
    return ok, {"t_close": t_close, "t_pct_chg": t_pct_chg, "ret_10d": ret_10d}


def _rc_not_already_risen_filter(
    daily_df: pd.DataFrame,
    rc_start_date: str,
    t_date: str,
    max_ret: float,
    max_up_days: int,
) -> tuple[bool, dict]:
    if daily_df is None or daily_df.empty:
        return False, {}
    rsd = str(rc_start_date or "").strip()
    if not rsd or len(rsd) != 8:
        return True, {}
    df = daily_df.copy()
    df["trade_date"] = df["trade_date"].astype(str).str.strip()
    df = df[(df["trade_date"] >= rsd) & (df["trade_date"] <= str(t_date))].copy()
    df = df.dropna(subset=["trade_date", "close"]).copy()
    if df.empty:
        return False, {}
    df = df.sort_values("trade_date").reset_index(drop=True)

    start_close = float(df["close"].iloc[0])
    t_close = float(df["close"].iloc[-1])
    rc_ret = float("nan")
    if start_close > 0 and np.isfinite(t_close):
        rc_ret = t_close / start_close - 1.0

    up_days = 0
    if "pct_chg" in df.columns:
        pct = pd.to_numeric(df["pct_chg"], errors="coerce").fillna(0).to_numpy(dtype=float)
        up_days = int(np.sum(pct > 0))

    ok = True
    if np.isfinite(rc_ret) and float(max_ret) > 0:
        if rc_ret > float(max_ret):
            ok = False
    if int(max_up_days) >= 0:
        if up_days > int(max_up_days):
            ok = False

    meta = {
        "rc_start_close": float(start_close),
        "rc_t_close": float(t_close),
        "rc_ret": float(rc_ret),
        "rc_up_days": int(up_days),
    }
    return ok, meta


def _price_wave_early_filter(
    daily_df: pd.DataFrame,
    stage2_date: str,
    t_date: str,
    peak_window_days: int,
    min_dd: float,
    min_rebound: float,
    max_rebound: float,
    min_rebound_days: int,
    last_slope_min: float,
    t_over_peak_max: float,
) -> tuple[bool, dict]:
    if daily_df is None or daily_df.empty:
        return False, {}
    df = daily_df.copy()
    df["trade_date"] = df["trade_date"].astype(str).str.strip()
    df = df[(df["trade_date"] >= str(stage2_date)) & (df["trade_date"] <= str(t_date))].copy()
    df = df.dropna(subset=["trade_date", "close"]).copy()
    if df.empty or len(df) < 12:
        return False, {}
    df = df.sort_values("trade_date").reset_index(drop=True)

    closes = pd.to_numeric(df["close"], errors="coerce").to_numpy(dtype=float)
    if closes.size < 12 or not np.isfinite(closes).any():
        return False, {}

    pw = int(peak_window_days)
    if pw <= 0:
        pw = 30
    pw = min(pw, int(closes.size))
    if pw < 5:
        return False, {}
    peak_idx = int(np.nanargmax(closes[:pw]))
    if peak_idx >= closes.size - 3:
        return False, {}
    peak_close = float(closes[peak_idx])
    if not np.isfinite(peak_close) or peak_close <= 0:
        return False, {}

    after = closes[peak_idx + 1 :]
    if after.size < 3:
        return False, {}
    low_rel = int(np.nanargmin(after))
    low_idx = peak_idx + 1 + low_rel
    if low_idx >= closes.size - 2:
        return False, {}
    low_close = float(closes[low_idx])
    if not np.isfinite(low_close) or low_close <= 0:
        return False, {}
    low_date = str(df["trade_date"].iloc[low_idx])

    dd_pct = 1.0 - low_close / peak_close
    if not np.isfinite(dd_pct) or dd_pct < float(min_dd):
        return False, {}

    t_close = float(closes[-1])
    rebound_pct = t_close / low_close - 1.0
    if not np.isfinite(rebound_pct):
        return False, {}
    if rebound_pct < float(min_rebound) or rebound_pct > float(max_rebound):
        return False, {}

    t_over_peak = t_close / peak_close - 1.0
    if np.isfinite(t_over_peak) and t_over_peak > float(t_over_peak_max):
        return False, {}

    rebound_days = int(closes.size - 1 - low_idx)
    if rebound_days < int(min_rebound_days):
        return False, {}

    win = min(5, int(closes.size))
    last_slope = _lin_slope(closes[-win:])
    if np.isfinite(last_slope) and last_slope < float(last_slope_min):
        return False, {}

    return True, {
        "pw_peak_date": str(df["trade_date"].iloc[peak_idx]),
        "pw_peak_close": peak_close,
        "pw_low_date": low_date,
        "pw_low_close": low_close,
        "pw_dd_pct": float(dd_pct),
        "pw_rebound_pct": float(rebound_pct),
        "pw_rebound_days": rebound_days,
        "pw_last_slope": float(last_slope) if np.isfinite(last_slope) else float("nan"),
        "pw_t_over_peak": float(t_over_peak) if np.isfinite(t_over_peak) else float("nan"),
    }


def _fetch_moneyflow_and_basic(pro, ts_codes: list[str], trade_date: str, sleep_s: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not ts_codes:
        return pd.DataFrame(), pd.DataFrame()
    batch_size = 200
    frames_mf = []
    for i in range(0, len(ts_codes), batch_size):
        batch = ts_codes[i : i + batch_size]
        df = _safe_call(
            lambda: pro.moneyflow_dc(ts_code=",".join(batch), trade_date=str(trade_date)),
            sleep_s=sleep_s,
            what=f"moneyflow_dc({i//batch_size+1})",
        )
        if df is not None and not df.empty:
            frames_mf.append(df)
    mf = pd.concat(frames_mf, ignore_index=True) if frames_mf else pd.DataFrame()

    fields = "ts_code,trade_date,turnover_rate,volume_ratio,circ_mv,total_mv"
    basic = _safe_call(
        lambda: pro.daily_basic(trade_date=str(trade_date), fields=fields),
        sleep_s=sleep_s,
        what=f"daily_basic({trade_date})",
    )
    if basic is None:
        basic = pd.DataFrame()
    return mf, basic


def _fetch_moneyflow_dc_for_dates(pro, ts_codes: list[str], trade_dates: list[str], sleep_s: float) -> pd.DataFrame:
    if not ts_codes or not trade_dates:
        return pd.DataFrame()
    batch_size = 200
    frames = []
    fields = "ts_code,trade_date,net_amount,net_amount_rate,buy_elg_amount,buy_lg_amount"
    for d in trade_dates:
        for i in range(0, len(ts_codes), batch_size):
            batch = ts_codes[i : i + batch_size]
            df = _safe_call(
                lambda: pro.moneyflow_dc(ts_code=",".join(batch), trade_date=str(d), fields=fields),
                sleep_s=sleep_s,
                what=f"moneyflow_dc_series({d},{i//batch_size+1})",
            )
            if df is not None and not df.empty:
                frames.append(df)
    if not frames:
        return pd.DataFrame()
    df_all = pd.concat(frames, ignore_index=True)
    df_all = df_all.copy()
    df_all["ts_code"] = df_all["ts_code"].astype(str).str.strip()
    df_all["trade_date"] = df_all["trade_date"].astype(str).str.strip()
    df_all = df_all[(df_all["ts_code"].isin(set([str(x).strip() for x in ts_codes]))) & (df_all["trade_date"].isin(set([str(x).strip() for x in trade_dates])))].copy()
    for c in ["net_amount", "net_amount_rate", "buy_elg_amount", "buy_lg_amount"]:
        if c in df_all.columns:
            df_all[c] = pd.to_numeric(df_all[c], errors="coerce")
    df_all = df_all.drop_duplicates(subset=["ts_code", "trade_date"], keep="last").reset_index(drop=True)
    return df_all


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--end-date", default=_as_yyyymmdd(datetime.now()))
    parser.add_argument("--lookback", type=int, default=260)
    parser.add_argument("--min-total-days", type=int, default=80)
    parser.add_argument("--min-stage3-days", type=int, default=12)
    parser.add_argument("--stage1-gap-pct", type=float, default=0.20)
    parser.add_argument("--stage3-min-drop-pct", type=float, default=0.18)
    parser.add_argument("--stage3-min-down-ratio", type=float, default=0.55)
    parser.add_argument("--stage3-min-slope", type=float, default=0.002)
    parser.add_argument("--warn-abs-max", type=float, default=0.10)
    parser.add_argument("--warn-global-q", type=float, default=0.20)
    parser.add_argument("--warn-post-q", type=float, default=0.35)
    parser.add_argument("--exclude-st", action="store_true", default=True)
    parser.add_argument("--min-list-days", type=int, default=60)
    parser.add_argument("--max-stocks", type=int, default=0)
    parser.add_argument("--ts-sleep-s", type=float, default=0.12)
    parser.add_argument("--cache", default="")
    parser.add_argument("--probe-ts-code", default="000001.SZ")
    parser.add_argument("--rise-filter", dest="use_rise_filter", action="store_true")
    parser.add_argument("--no-rise-filter", dest="use_rise_filter", action="store_false")
    parser.set_defaults(use_rise_filter=False)
    parser.add_argument("--max-ret-10d", type=float, default=0.12)
    parser.add_argument("--max-pct-chg-t", type=float, default=0.06)
    parser.add_argument("--require-net-inflow", action="store_true", default=False)
    parser.add_argument("--ignite-days", type=int, default=3)
    parser.add_argument("--recent-conc", dest="require_recent_conc", action="store_true")
    parser.add_argument("--no-recent-conc", dest="require_recent_conc", action="store_false")
    parser.set_defaults(require_recent_conc=True)
    parser.add_argument("--recent-conc-days", type=int, default=2)
    parser.add_argument("--recent-conc-lookback", type=int, default=25)
    parser.add_argument("--recent-conc-max-days", type=int, default=2)
    parser.add_argument("--recent-conc-min-drop", type=float, default=0.06)
    parser.add_argument("--recent-conc-min-down-ratio", type=float, default=0.60)
    parser.add_argument("--recent-conc-min-slope", type=float, default=0.001)
    parser.add_argument("--recent-start-min-global-q", type=float, default=0.35)
    parser.add_argument("--rc-price-filter", dest="use_rc_price_filter", action="store_true")
    parser.add_argument("--no-rc-price-filter", dest="use_rc_price_filter", action="store_false")
    parser.set_defaults(use_rc_price_filter=True)
    parser.add_argument("--rc-max-ret", type=float, default=0.10)
    parser.add_argument("--rc-max-up-days", type=int, default=4)
    parser.add_argument("--price-wave", dest="require_price_wave", action="store_true")
    parser.add_argument("--no-price-wave", dest="require_price_wave", action="store_false")
    parser.set_defaults(require_price_wave=True)
    parser.add_argument("--pw-min-dd", type=float, default=0.08)
    parser.add_argument("--pw-min-rebound", type=float, default=0.03)
    parser.add_argument("--pw-max-rebound", type=float, default=0.18)
    parser.add_argument("--pw-min-rebound-days", type=int, default=3)
    parser.add_argument("--pw-last-slope-min", type=float, default=0.0)
    parser.add_argument("--pw-peak-window-days", type=int, default=30)
    parser.add_argument("--pw-t-over-peak-max", type=float, default=0.03)
    parser.add_argument("--out", default="")
    args = parser.parse_args()
    args.recent_conc_max_days = min(int(getattr(args, "recent_conc_max_days", 0) or 0), 2)
    if int(getattr(args, "recent_conc_days", 0) or 0) > int(args.recent_conc_max_days):
        args.recent_conc_days = int(args.recent_conc_max_days)

    root = _project_root()
    if root not in sys.path:
        sys.path.insert(0, root)
    from backend.utils.tushare_client import pro

    end_date = str(args.end_date).strip()
    lookback = int(args.lookback)
    sleep_s = float(args.ts_sleep_s)

    print(f"{_now_ts()} 开始：三阶段筹码演变筛选", flush=True)
    print(f"{_now_ts()} end_date={end_date} lookback={lookback} ts_sleep_s={sleep_s}", flush=True)

    t_date = _pick_last_chip_trade_date(pro, probe_ts_code=str(args.probe_ts_code), end_date=end_date, sleep_s=sleep_s)
    t_dates = _load_trade_dates(pro, end_date=t_date, lookback=lookback)
    if not t_dates:
        raise SystemExit("交易日序列为空，无法继续")
    start_date = t_dates[0]
    print(f"{_now_ts()} T={t_date} 回溯起点={start_date} 交易日数={len(t_dates)}", flush=True)

    if args.cache:
        cache_file = str(args.cache).strip()
    else:
        cache_dir = os.path.join(root, "backend", "data")
        os.makedirs(cache_dir, exist_ok=True)
        cache_file = os.path.join(cache_dir, f"all_chip_data_{datetime.now().strftime('%Y%m%d')}.csv")
    cache_df = _load_chip_cache(cache_file)
    print(f"{_now_ts()} cache_file={cache_file} cache_rows={len(cache_df)}", flush=True)

    pool = _load_stock_pool(pro, exclude_st=bool(args.exclude_st), min_list_days=int(args.min_list_days))
    if int(args.max_stocks) > 0:
        pool = pool.head(int(args.max_stocks)).copy()
    print(f"{_now_ts()} 股票池={len(pool)} exclude_st={int(bool(args.exclude_st))}", flush=True)

    rows = []
    checked = 0
    matched = 0

    for _, r in pool.iterrows():
        ts_code = str(r["ts_code"]).strip()
        name = str(r.get("name", "")).strip()
        checked += 1
        if checked % 200 == 0:
            print(f"{_now_ts()} 进度 {checked}/{len(pool)} 命中={matched}", flush=True)

        chip_df = pd.DataFrame()
        if cache_df is not None and not cache_df.empty:
            chip_df = cache_df[(cache_df["ts_code"] == ts_code) & (cache_df["trade_date"] >= start_date) & (cache_df["trade_date"] <= t_date)].copy()
        if chip_df is None or chip_df.empty or (chip_df["trade_date"].nunique() < int(args.min_total_days)):
            fetched = _fetch_chip_one(pro, ts_code=ts_code, start_date=start_date, end_date=t_date, sleep_s=sleep_s)
            if fetched is not None and not fetched.empty:
                _append_chip_cache(cache_file, fetched)
                if cache_df is None or cache_df.empty:
                    cache_df = fetched.copy()
                else:
                    cache_df = pd.concat([cache_df, fetched], ignore_index=True)
                    cache_df = cache_df.drop_duplicates(subset=["ts_code", "trade_date"], keep="last").reset_index(drop=True)
                chip_df = fetched.copy()

        if chip_df is None or chip_df.empty:
            continue

        df_band = _calc_chip_band_series(chip_df)
        m = _match_three_stage(
            df_band=df_band,
            min_total_days=int(args.min_total_days),
            min_stage3_days=int(args.min_stage3_days),
            stage1_gap_pct=float(args.stage1_gap_pct),
            stage3_min_drop_pct=float(args.stage3_min_drop_pct),
            stage3_min_down_ratio=float(args.stage3_min_down_ratio),
            stage3_min_slope=float(args.stage3_min_slope),
            warn_abs_max=float(args.warn_abs_max),
            warn_global_q=float(args.warn_global_q),
            warn_post_q=float(args.warn_post_q),
        )
        if m is None:
            continue

        if bool(args.require_recent_conc):
            ok, rc = _recent_concentration_trend(
                df_band=df_band,
                anchor_peak_date=str(m.get("stage2_date") or ""),
                min_days=int(args.recent_conc_days),
                lookback_days=int(args.recent_conc_lookback),
                max_days=int(args.recent_conc_max_days),
                min_drop_pct=float(args.recent_conc_min_drop),
                min_down_ratio=float(args.recent_conc_min_down_ratio),
                min_slope=float(args.recent_conc_min_slope),
                start_min_global_q=float(args.recent_start_min_global_q),
            )
            if not ok:
                continue
            m.update(rc)

        need_daily = bool(args.use_rise_filter) or bool(args.require_price_wave) or bool(args.use_rc_price_filter)
        if need_daily:
            daily_start = str(m.get("stage2_date") or start_date)
            if bool(args.use_rc_price_filter):
                rsd = str(m.get("rc_start_date") or "").strip()
                if rsd and len(rsd) == 8:
                    daily_start = min(daily_start, rsd)
            daily_df = _fetch_daily_close(pro, ts_code=ts_code, start_date=daily_start, end_date=t_date, sleep_s=sleep_s)
        else:
            daily_df = pd.DataFrame()

        extra = {}
        if bool(args.use_rc_price_filter):
            ok, extra0 = _rc_not_already_risen_filter(
                daily_df=daily_df,
                rc_start_date=str(m.get("rc_start_date") or ""),
                t_date=t_date,
                max_ret=float(args.rc_max_ret),
                max_up_days=int(args.rc_max_up_days),
            )
            if not ok:
                continue
            extra.update(extra0)

        if bool(args.use_rise_filter):
            ok, extra1 = _not_raised_filter(
                daily_df=daily_df,
                t_date=t_date,
                max_ret_10d=float(args.max_ret_10d),
                max_pct_chg_t=float(args.max_pct_chg_t),
            )
            if not ok:
                continue
            extra.update(extra1)

        if bool(args.require_price_wave):
            ok, extra2 = _price_wave_early_filter(
                daily_df=daily_df,
                stage2_date=str(m.get("stage2_date") or start_date),
                t_date=t_date,
                peak_window_days=int(args.pw_peak_window_days),
                min_dd=float(args.pw_min_dd),
                min_rebound=float(args.pw_min_rebound),
                max_rebound=float(args.pw_max_rebound),
                min_rebound_days=int(args.pw_min_rebound_days),
                last_slope_min=float(args.pw_last_slope_min),
                t_over_peak_max=float(args.pw_t_over_peak_max),
            )
            if not ok:
                continue
            extra.update(extra2)

        matched += 1
        row = {
            "ts_code": ts_code,
            "name": name,
            **m,
            **extra,
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    if df is None or df.empty:
        print(f"{_now_ts()} 完成：0 条命中", flush=True)
        out = str(args.out).strip()
        if not out:
            out = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"三阶段筹码演变_0_{t_date}_{datetime.now().strftime('%H%M%S')}.csv")
        pd.DataFrame().to_csv(out, index=False, encoding="utf-8-sig")
        print(f"{_now_ts()} 输出: {out}", flush=True)
        return

    df = df.sort_values(["score"], ascending=False).reset_index(drop=True)
    if len(df) > 400:
        df = df.head(400).copy()

    ts_codes = df["ts_code"].astype(str).tolist()
    ignite_days = max(0, int(args.ignite_days))
    if ignite_days > 0:
        if len(t_dates) < ignite_days:
            ignite_dates = list(t_dates)
        else:
            ignite_dates = list(t_dates[-ignite_days:])
        mf_series = _fetch_moneyflow_dc_for_dates(pro, ts_codes=ts_codes, trade_dates=ignite_dates, sleep_s=sleep_s)
        if mf_series is None or mf_series.empty:
            print(f"{_now_ts()} 点火器过滤：moneyflow_dc 返回为空，无法判断连续净流入", flush=True)
        else:
            piv = mf_series.pivot_table(index="ts_code", columns="trade_date", values="net_amount", aggfunc="last")
            for d in ignite_dates:
                if str(d) not in piv.columns:
                    piv[str(d)] = np.nan
            flags = [(pd.to_numeric(piv[str(d)], errors="coerce").fillna(0) > 0) for d in ignite_dates]
            all_in = flags[0]
            for f in flags[1:]:
                all_in = all_in & f
            piv = piv.reset_index()
            piv["ignite_all_inflow"] = all_in.to_numpy()
            piv["ignite_inflow_days"] = sum([f.astype(int) for f in flags]).to_numpy()
            for d in ignite_dates:
                piv[f"ignite_net_amount_{d}"] = pd.to_numeric(piv.get(str(d), np.nan), errors="coerce")
            keep_cols = ["ts_code", "ignite_all_inflow", "ignite_inflow_days"] + [f"ignite_net_amount_{d}" for d in ignite_dates]
            piv2 = piv[keep_cols].copy()
            df = df.merge(piv2, on="ts_code", how="left")
            before_n = len(df)
            df = df[pd.to_numeric(df["ignite_all_inflow"], errors="coerce").fillna(0).astype(int) == 1].copy()
            df = df.reset_index(drop=True)
            print(f"{_now_ts()} 点火器过滤：连续{len(ignite_dates)}日主力净流入>0 {before_n}->{len(df)}", flush=True)

    mf, basic = _fetch_moneyflow_and_basic(pro, ts_codes=ts_codes, trade_date=t_date, sleep_s=sleep_s)
    if mf is not None and not mf.empty:
        keep = [c for c in ["ts_code", "net_amount", "net_amount_rate"] if c in mf.columns]
        mf2 = mf[keep].copy() if keep else pd.DataFrame()
        if not mf2.empty:
            mf2["ts_code"] = mf2["ts_code"].astype(str).str.strip()
            mf2 = mf2.drop_duplicates(subset=["ts_code"], keep="last")
            df = df.merge(mf2, on="ts_code", how="left")
    if basic is not None and not basic.empty:
        keep = [c for c in ["ts_code", "turnover_rate", "volume_ratio", "circ_mv", "total_mv"] if c in basic.columns]
        b2 = basic[keep].copy() if keep else pd.DataFrame()
        if not b2.empty:
            b2["ts_code"] = b2["ts_code"].astype(str).str.strip()
            b2 = b2.drop_duplicates(subset=["ts_code"], keep="last")
            df = df.merge(b2, on="ts_code", how="left")

    if bool(args.require_net_inflow) and "net_amount" in df.columns:
        df = df[pd.to_numeric(df["net_amount"], errors="coerce").fillna(0) > 0].copy()
        df = df.reset_index(drop=True)

    out = str(args.out).strip()
    if not out:
        out = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"三阶段筹码演变_{len(df)}_{t_date}_{datetime.now().strftime('%H%M%S')}.csv")
    df.to_csv(out, index=False, encoding="utf-8-sig")

    show_cols = [
        "ts_code",
        "name",
        "score",
        "t_date",
        "t_band",
        "t_band_global_q",
        "stage2_date",
        "stage2_band",
        "stage3_drop_pct",
        "stage3_down_ratio",
        "ignite_inflow_days",
        "rc_anchor_peak_date",
        "rc_anchor_peak_band",
        "rc_days",
        "rc_start_date",
        "rc_drop_pct",
        "rc_down_ratio",
        "rc_slope",
        "rc_start_global_q",
        "rc_ret",
        "rc_up_days",
        "pw_dd_pct",
        "pw_rebound_pct",
        "pw_rebound_days",
        "pw_t_over_peak",
        "ret_10d",
        "t_pct_chg",
        "net_amount",
        "net_amount_rate",
        "turnover_rate",
        "volume_ratio",
    ]
    if "ignite_inflow_days" in df.columns:
        ignite_cols = [c for c in df.columns if c.startswith("ignite_net_amount_")]
        ignite_cols = sorted(ignite_cols)[-5:]
        show_cols.extend(ignite_cols)
    show_cols = [c for c in show_cols if c in df.columns]
    print(f"{_now_ts()} 完成：checked={checked} 命中={len(df)} 输出={out}", flush=True)
    print(df[show_cols].head(20).to_string(index=False), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"{_now_ts()} 脚本异常: {type(e).__name__}:{e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise
