import argparse
import os
import sys
import time
import traceback
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
from scipy import stats


def _project_root() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    # 脚本在 backend/scripts/筹码/ 下，需要往上跳三级才能到达项目根目录
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
    return dates[-int(lookback):]


def _pick_last_chip_trade_date(pro, end_date: str, sleep_s: float) -> str:
    td = _pick_last_trade_date(pro, end_date=end_date)
    dates = _load_trade_dates(pro, end_date=td, lookback=30)
    dates = list(reversed(dates))
    fields = "ts_code,trade_date,cost_5pct,cost_95pct,weight_avg,winner_rate"
    for d in dates:
        df = _safe_call(
            lambda: pro.cyq_perf(ts_code="000001.SZ", trade_date=str(d), fields=fields),
            sleep_s=sleep_s,
            what=f"cyq_perf_probe({d})",
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


def _calc_chip_band(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    result = df.copy()
    result["trade_date"] = result["trade_date"].astype(str).str.strip()
    result = result[result["trade_date"].str.len() == 8].copy()
    for c in ["cost_5pct", "cost_95pct", "weight_avg", "winner_rate"]:
        if c in result.columns:
            result[c] = pd.to_numeric(result[c], errors="coerce")
    result = result.dropna(subset=["cost_5pct", "cost_95pct", "weight_avg"]).copy()
    result = result.sort_values("trade_date").reset_index(drop=True)
    w = result["weight_avg"].to_numpy(dtype=float)
    band = (result["cost_95pct"].to_numpy(dtype=float) - result["cost_5pct"].to_numpy(dtype=float)) / np.where(w > 0, w, np.nan)
    result["chip_band"] = pd.to_numeric(pd.Series(band), errors="coerce")
    return result


def _calc_mann_kendall_trend(band_series: np.ndarray) -> tuple[float, float, float]:
    if band_series is None or len(band_series) < 3:
        return float("nan"), float("nan"), float("nan")
    x = np.arange(len(band_series))
    y = np.asarray(band_series, dtype=float)
    mask = np.isfinite(y)
    if np.sum(mask) < 3:
        return float("nan"), float("nan"), float("nan")
    x, y = x[mask], y[mask]
    tau, p_value = stats.kendalltau(x, y)
    if not np.isfinite(tau):
        return float("nan"), float("nan"), float("nan")
    change_rate = float("nan")
    if len(y) >= 2 and y[0] > 0:
        change_rate = (y[-1] - y[0]) / y[0]
    return float(tau), float(p_value), float(change_rate)


def _fetch_daily_close(pro, ts_code: str, trade_date: str, sleep_s: float) -> Optional[float]:
    fields = "ts_code,trade_date,close"
    df = _safe_call(
        lambda: pro.daily(ts_code=str(ts_code), trade_date=str(trade_date), fields=fields),
        sleep_s=sleep_s,
        what=f"daily({ts_code},{trade_date})",
    )
    if df is None or df.empty:
        return None
    if "close" not in df.columns:
        return None
    close = pd.to_numeric(df["close"].iloc[-1], errors="coerce")
    return float(close) if np.isfinite(close) else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--end-date", default=_as_yyyymmdd(datetime.now()))
    parser.add_argument("--lookback", type=int, default=60)
    parser.add_argument("--min-band", type=float, default=0.10)
    parser.add_argument("--max-band", type=float, default=0.20)
    parser.add_argument("--trend-lookback", type=int, default=20)
    parser.add_argument("--trend-min-tau", type=float, default=-0.3)
    parser.add_argument("--trend-max-pvalue", type=float, default=0.05)
    parser.add_argument("--no-trend-filter", action="store_true")
    parser.add_argument("--exclude-st", action="store_true", default=True)
    parser.add_argument("--min-list-days", type=int, default=60)
    parser.add_argument("--max-stocks", type=int, default=0)
    parser.add_argument("--ts-sleep-s", type=float, default=0.12)
    parser.add_argument("--out", default="")
    args = parser.parse_args()

    root = _project_root()
    if root not in sys.path:
        sys.path.insert(0, root)
    from backend.utils.tushare_client import pro

    end_date = str(args.end_date).strip()
    lookback = int(args.lookback)
    sleep_s = float(args.ts_sleep_s)

    print(f"{_now_ts()} 开始：筹码带宽扫描 + Mann-Kendall 趋势检验", flush=True)
    print(f"{_now_ts()} end_date={end_date} lookback={lookback}", flush=True)
    print(f"{_now_ts()} min_band={args.min_band} max_band={args.max_band}", flush=True)
    print(f"{_now_ts()} trend_lookback={args.trend_lookback} min_tau={args.trend_min_tau} max_pvalue={args.trend_max_pvalue}", flush=True)

    t_date = _pick_last_chip_trade_date(pro, end_date=end_date, sleep_s=sleep_s)
    t_dates = _load_trade_dates(pro, end_date=t_date, lookback=lookback)
    if not t_dates:
        raise SystemExit("交易日序列为空，无法继续")
    start_date = t_dates[0]
    print(f"{_now_ts()} T={t_date} 回溯起点={start_date} 交易日数={len(t_dates)}", flush=True)

    cache_dir = os.path.join(root, "backend", "data")
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = os.path.join(cache_dir, "chip_band_cache.csv")
    cache_df = _load_chip_cache(cache_file)
    print(f"{_now_ts()} cache_file={cache_file} cache_rows={len(cache_df)}", flush=True)

    pool = _load_stock_pool(pro, exclude_st=bool(args.exclude_st), min_list_days=int(args.min_list_days))
    if int(args.max_stocks) > 0:
        pool = pool.head(int(args.max_stocks)).copy()
    print(f"{_now_ts()} 股票池={len(pool)} exclude_st={int(bool(args.exclude_st))}", flush=True)

    rows = []
    checked = 0
    matched = 0

    trend_lookback = int(args.trend_lookback)
    min_tau = float(args.trend_min_tau)
    max_pvalue = float(args.trend_max_pvalue)
    use_trend_filter = not bool(args.no_trend_filter)

    for _, r in pool.iterrows():
        ts_code = str(r["ts_code"]).strip()
        name = str(r.get("name", "")).strip()
        checked += 1
        if checked % 200 == 0:
            print(f"{_now_ts()} 进度 {checked}/{len(pool)} 命中={matched}", flush=True)

        chip_df = pd.DataFrame()
        if cache_df is not None and not cache_df.empty:
            chip_df = cache_df[(cache_df["ts_code"] == ts_code) & (cache_df["trade_date"] >= start_date) & (cache_df["trade_date"] <= t_date)].copy()
        if chip_df is None or chip_df.empty or (chip_df["trade_date"].nunique() < max(20, trend_lookback)):
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

        df_band = _calc_chip_band(chip_df)
        if df_band.empty:
            continue

        if df_band["trade_date"].nunique() < max(20, trend_lookback):
            continue

        latest = df_band.iloc[-1]
        t_band = float(latest["chip_band"]) if np.isfinite(float(latest["chip_band"])) else float("nan")
        if not np.isfinite(t_band):
            continue

        if t_band < float(args.min_band) or t_band > float(args.max_band):
            continue

        if use_trend_filter and trend_lookback >= 3:
            recent_df = df_band.tail(trend_lookback)
            band_series = pd.to_numeric(recent_df["chip_band"], errors="coerce").to_numpy(dtype=float)
            tau, pvalue, change_rate = _calc_mann_kendall_trend(band_series)
            if not np.isfinite(tau) or tau > min_tau:
                continue
            if not np.isfinite(pvalue) or pvalue > max_pvalue:
                continue
        else:
            tau, pvalue, change_rate = float("nan"), float("nan"), float("nan")
            if use_trend_filter:
                recent_df = df_band.tail(trend_lookback) if trend_lookback >= 3 else df_band
                band_series = pd.to_numeric(recent_df["chip_band"], errors="coerce").to_numpy(dtype=float)
                tau, pvalue, change_rate = _calc_mann_kendall_trend(band_series)

        close = _fetch_daily_close(pro, ts_code=ts_code, trade_date=t_date, sleep_s=sleep_s)

        matched += 1
        row = {
            "ts_code": ts_code,
            "name": name,
            "chip_band": round(t_band, 4),
            "close": close,
            "cost_5pct": latest.get("cost_5pct"),
            "cost_95pct": latest.get("cost_95pct"),
            "weight_avg": latest.get("weight_avg"),
            "winner_rate": latest.get("winner_rate"),
            "trend_tau": round(tau, 4) if np.isfinite(tau) else None,
            "trend_pvalue": round(pvalue, 4) if np.isfinite(pvalue) else None,
            "trend_change_rate": round(change_rate, 4) if np.isfinite(change_rate) else None,
            "trade_date": t_date,
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    if df is None or df.empty:
        print(f"{_now_ts()} 完成：0 条命中", flush=True)
        out = str(args.out).strip()
        if not out:
            out = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"筹码聚集度扫描_0_{t_date}_{datetime.now().strftime('%H%M%S')}.csv")
        pd.DataFrame().to_csv(out, index=False, encoding="utf-8-sig")
        print(f"{_now_ts()} 输出: {out}", flush=True)
        return

    df = df.sort_values(["chip_band"], ascending=True).reset_index(drop=True)

    out = str(args.out).strip()
    if not out:
        out = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"筹码聚集度扫描_{len(df)}_{t_date}_{datetime.now().strftime('%H%M%S')}.csv")
    df.to_csv(out, index=False, encoding="utf-8-sig")

    show_cols = ["ts_code", "name", "chip_band", "close", "cost_5pct", "cost_95pct", "weight_avg", "winner_rate", "trend_tau", "trend_pvalue", "trend_change_rate"]
    show_cols = [c for c in show_cols if c in df.columns]
    print(f"{_now_ts()} 完成：checked={checked} 命中={len(df)} 输出={out}", flush=True)
    print(df[show_cols].head(30).to_string(index=False), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"{_now_ts()} 脚本异常: {type(e).__name__}:{e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise
