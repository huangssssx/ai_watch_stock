import os
import sys
import time
import traceback
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from utils.stock_codes import get_all_a_share_codes
from utils.chips import ChipProxyParams, get_chip_concentration_proxy
from utils.pytdx_client import tdx

# 可通过环境变量调整脚本行为（直接在命令前写 VAR=VALUE 即可）：
# - MAX_WORKERS：并发线程数（默认 8）。越大通常越快，但更容易触发数据源异常/网络抖动。
#   示例：MAX_WORKERS=1 python3 scripts/筹码忽然集中.py  -> 单线程更稳但更慢
#         MAX_WORKERS=12 python3 scripts/筹码忽然集中.py -> 更快但更可能出现 “BAD RESPONSE ...”
# - MAX_STOCKS：只跑前 N 只股票（默认 0=全市场）。用于快速调参/验证。
#   示例：MAX_STOCKS=200 python3 scripts/筹码忽然集中.py -> 只筛前 200 只，分钟级验证
# - MAX_RESULTS：最多保留多少条入选结果（默认 300），避免结果过大影响前端展示/存储。
# - CHIP_WINDOW：筹码代理指标的回看窗口（默认 60 个交易日）。越大越“看长期”，拐点更少更稳。
# - CHIP_SMOOTH：集中度序列的平滑窗口（默认 5）。越大越平滑，越不容易被 1 天噪声触发。
# - HIST_N：用于计算“自适应阈值”的历史样本长度（默认 120）。越大阈值越稳，但更慢且对新股不友好。
# - LOOKBACK_DAYS：单股拉取日线覆盖的自然日天数（默认 220），需要覆盖 window+hist_n 的计算。
# - DELTA_FLOOR：集中度“突然下降”的保底阈值（默认 -0.12，表示较昨日下降 12% 以上才算突然，按未平滑值计算）。
# - DELTA_SMOOTH_FLOOR：平滑集中度“突然下降”的保底阈值（默认 -0.05）。用于过滤“未平滑在跌但平滑没怎么动”的伪拐点。
# - ABS_DROP_FLOOR：集中度“绝对值下降”保底阈值（默认 0=不启用）。例如 0.003 表示今日比昨日至少下降 0.003。
# - Z_THR：鲁棒 z-score 阈值（默认 -2.0）。越接近 0 越宽松，越小越严格。
# - Z_THR_RAW：未平滑集中度的鲁棒 z-score 阈值（默认等于 Z_THR）。用于要求“原始值”也处于显著低位。
# - FETCH_RETRIES：单股拉取失败时重试次数（默认 2）。提高成功率但会增加耗时。


def _to_date_ymd(d: pd.Timestamp) -> str:
    return pd.Timestamp(d).strftime("%Y-%m-%d")


def _robust_z(x: float, hist: np.ndarray) -> Optional[float]:
    a = np.asarray(hist, dtype=float)
    a = a[np.isfinite(a)]
    if len(a) < 20 or not np.isfinite(x):
        return None
    med = float(np.median(a))
    mad = float(np.median(np.abs(a - med)))
    if mad > 0:
        return float((x - med) / (1.4826 * mad))
    std = float(np.std(a))
    if std > 0:
        return float((x - float(np.mean(a))) / std)
    return None


def _analyze_one(
    code: str,
    name: str,
    last_date: pd.Timestamp,
    *,
    window: int,
    smooth: int,
    hist_n: int,
    lookback_days: int,
    delta_floor: float,
    z_thr: float,
) -> Optional[dict]:
    # lookback_days：拉取日线覆盖的自然日天数（不是交易日天数）
    # 目的：确保有足够的历史样本计算：CHIP_WINDOW（计算滚动分布）+ HIST_N（计算自适应阈值）。
    start_date = _to_date_ymd(last_date - pd.Timedelta(days=int(lookback_days)))
    end_date = _to_date_ymd(last_date)
    # FETCH_RETRIES：单股拉取失败重试次数。提高成功率但会增加耗时。
    retries = int(os.getenv("FETCH_RETRIES", "2") or "2")
    df_proxy = None
    for attempt in range(max(1, retries)):
        try:
            df_proxy = get_chip_concentration_proxy(
                code=code,
                start_date=start_date,
                end_date=end_date,
                params=ChipProxyParams(window=int(window), smooth=int(smooth)),
                as_df=True,
            )
            break
        except Exception:
            df_proxy = None
            time.sleep(0.05 * (attempt + 1))
    if df_proxy is None or df_proxy.empty:
        return None

    dfp = df_proxy.copy()
    dfp["date"] = pd.to_datetime(dfp["date"], errors="coerce").dt.normalize()
    dfp = dfp.dropna(subset=["date"]).sort_values("date")
    col_smooth = "proxy_70_concentration_smooth"
    col_raw = "proxy_70_concentration"
    if col_smooth not in dfp.columns or col_raw not in dfp.columns:
        return None
    dfp[col_smooth] = pd.to_numeric(dfp[col_smooth], errors="coerce")
    dfp[col_raw] = pd.to_numeric(dfp[col_raw], errors="coerce")
    dfp = dfp.dropna(subset=[col_smooth, col_raw])
    if dfp.empty:
        return None

    df_last = dfp[dfp["date"] <= last_date].tail(2)
    if len(df_last) < 2:
        return None
    row_prev = df_last.iloc[0]
    row_t = df_last.iloc[1]

    date_prev = pd.Timestamp(row_prev["date"]).normalize()
    date_t = pd.Timestamp(row_t["date"]).normalize()
    if (date_t - date_prev).days > 7:
        return None

    x_t_smooth = float(row_t[col_smooth])
    x_prev_smooth = float(row_prev[col_smooth])
    x_t_raw = float(row_t[col_raw])
    x_prev_raw = float(row_prev[col_raw])
    if (
        not np.isfinite(x_t_smooth)
        or not np.isfinite(x_prev_smooth)
        or x_prev_smooth <= 0
        or not np.isfinite(x_t_raw)
        or not np.isfinite(x_prev_raw)
        or x_prev_raw <= 0
    ):
        return None
    delta = float((x_t_raw - x_prev_raw) / x_prev_raw)
    delta_smooth = float((x_t_smooth - x_prev_smooth) / x_prev_smooth)

    df_hist = dfp[dfp["date"] < date_prev].tail(int(hist_n))
    if df_hist is None or df_hist.empty:
        return None
    hist_smooth = df_hist[col_smooth].to_numpy(dtype=float)
    hist_smooth = hist_smooth[np.isfinite(hist_smooth)]
    hist_raw = df_hist[col_raw].to_numpy(dtype=float)
    hist_raw = hist_raw[np.isfinite(hist_raw)]
    if len(hist_smooth) < max(30, int(hist_n) // 3) or len(hist_raw) < max(30, int(hist_n) // 3):
        return None

    q10_smooth = float(np.quantile(hist_smooth, 0.10))
    q10_raw = float(np.quantile(hist_raw, 0.10))
    percentile_smooth = float(np.mean(hist_smooth <= x_t_smooth)) * 100.0
    percentile_raw = float(np.mean(hist_raw <= x_t_raw)) * 100.0

    dh = pd.Series(df_hist[col_raw].to_numpy(dtype=float)).pct_change().dropna()
    dh = dh[np.isfinite(dh)]
    q05_delta = float(dh.quantile(0.05)) if len(dh) >= 20 else np.nan
    delta_thr = float(min(q05_delta, float(delta_floor))) if np.isfinite(q05_delta) else float(delta_floor)

    z_smooth = _robust_z(x_t_smooth, hist_smooth)
    z_raw = _robust_z(x_t_raw, hist_raw)
    if z_smooth is None or not np.isfinite(z_smooth) or z_raw is None or not np.isfinite(z_raw):
        return None

    abs_drop_floor = float(os.getenv("ABS_DROP_FLOOR", "0") or "0")
    if abs_drop_floor > 0 and (x_prev_raw - x_t_raw) < abs_drop_floor:
        return None

    delta_smooth_floor = float(os.getenv("DELTA_SMOOTH_FLOOR", "-0.05") or "-0.05")
    if delta_smooth > float(delta_smooth_floor):
        return None

    z_thr_raw = float(os.getenv("Z_THR_RAW", str(z_thr)) or str(z_thr))
    if not (
        x_t_smooth <= q10_smooth
        and x_t_raw <= q10_raw
        and delta <= delta_thr
        and delta_smooth <= float(delta_smooth_floor)
        and z_smooth <= float(z_thr)
        and z_raw <= float(z_thr_raw)
    ):
        return None

    score = float(
        (-z_smooth) * 30.0
        + (-z_raw) * 30.0
        + (-delta) * 80.0
        + (-delta_smooth) * 40.0
        + (10.0 - min(10.0, percentile_smooth / 10.0)) * 2.0
    )
    reason = f"集中度突降 p{percentile_smooth:.0f}/p{percentile_raw:.0f} Δ{delta:.1%}/{delta_smooth:.1%} z{z_smooth:.2f}/{z_raw:.2f}"
    return {
        "symbol": str(code).zfill(6),
        "name": str(name),
        "date": _to_date_ymd(date_t),
        "score": score,
        "reason": reason,
        "conc_today": x_t_raw,
        "conc_prev": x_prev_raw,
        "conc_today_smooth": x_t_smooth,
        "conc_prev_smooth": x_prev_smooth,
        "delta": delta,
        "delta_smooth": delta_smooth,
        "percentile_smooth": percentile_smooth,
        "percentile_raw": percentile_raw,
        "z_smooth": z_smooth,
        "z_raw": z_raw,
        "q10_smooth": q10_smooth,
        "q10_raw": q10_raw,
        "delta_thr": delta_thr,
        "z_thr": float(z_thr),
        "z_thr_raw": float(z_thr_raw),
        "delta_smooth_floor": float(delta_smooth_floor),
        "window": int(window),
        "smooth": int(smooth),
        "hist_n": int(hist_n),
    }


def _get_recent_open_dates(k: int = 2, today: Optional[pd.Timestamp] = None) -> list[pd.Timestamp]:
    base = pd.Timestamp(today) if today is not None else pd.Timestamp.today()
    base = base.normalize()

    for ip, port in [("180.153.18.170", 7709), ("101.227.73.20", 7709)]:
        try:
            tdx.configure(ip, port)
            bars = tdx.get_security_bars(9, 0, "000001", 0, 120) or []
            df = pd.DataFrame(bars)
            if df is None or df.empty or "datetime" not in df.columns:
                continue
            ds = pd.to_datetime(df["datetime"].astype(str).str.slice(0, 10), errors="coerce").dropna().dt.normalize()
            ds = ds[ds <= base].drop_duplicates().sort_values()
            if len(ds) >= k:
                return [pd.Timestamp(x).normalize() for x in ds.tolist()[-k:]]
        except Exception:
            continue

    out: list[pd.Timestamp] = []
    d = base
    for _ in range(30):
        if d.weekday() < 5:
            out.append(d)
            if len(out) >= k:
                break
        d = (d - pd.Timedelta(days=1)).normalize()
    return list(reversed(out))


def _load_stock_pool(limit: Optional[int] = None) -> pd.DataFrame:
    df_codes = get_all_a_share_codes()
    if df_codes is None or df_codes.empty:
        return pd.DataFrame(columns=["market", "code", "name"])
    df_codes = df_codes.copy()
    df_codes["code"] = df_codes["code"].astype(str).str.zfill(6)
    df_codes["name"] = df_codes.get("name", "").astype(str)
    if limit is not None:
        df_codes = df_codes.head(int(limit))
    return df_codes[["market", "code", "name"]].reset_index(drop=True)


def main() -> pd.DataFrame:
    print("开始运行：全市场筛选（筹码忽然集中）")
    t0 = time.perf_counter()
    open_dates = _get_recent_open_dates(k=2)
    if len(open_dates) < 2:
        print("无法确定最近两个交易日，结束。")
        return pd.DataFrame()
    last_date = open_dates[-1]
    prev_date = open_dates[-2]
    print(f"最近交易日: {last_date:%Y-%m-%d}，上一交易日: {prev_date:%Y-%m-%d}")

    max_stocks = int(os.getenv("MAX_STOCKS", "0") or "0")
    # MAX_STOCKS：只跑前 N 只股票，方便快速验证与调参；0 表示全市场。
    df_codes = _load_stock_pool(limit=max_stocks if max_stocks > 0 else None)
    print(f"股票池数量: {len(df_codes)}")

    # CHIP_WINDOW：集中度计算的滚动窗口长度（交易日）；越大越稳、越不敏感。
    window = int(os.getenv("CHIP_WINDOW", "60") or "60")
    # CHIP_SMOOTH：对集中度序列做均值平滑的窗口长度；越大越不容易被 1 日噪声触发。
    smooth = int(os.getenv("CHIP_SMOOTH", "5") or "5")
    # HIST_N：用于计算“自适应阈值”（历史分位、历史 delta 分位、历史 MAD）的样本长度。
    hist_n = int(os.getenv("HIST_N", "120") or "120")
    # LOOKBACK_DAYS：单股日线拉取覆盖的自然日天数（需足够覆盖 window+hist_n）。
    lookback_days = int(os.getenv("LOOKBACK_DAYS", "220") or "220")
    # DELTA_FLOOR：集中度“突然下降”的保底阈值（负数）。例如 -0.12 表示较昨日下降 ≥12% 才算突然。
    delta_floor = float(os.getenv("DELTA_FLOOR", "-0.12") or "-0.12")
    # Z_THR：鲁棒 z-score 阈值。比如 -2.0 表示当日显著低于自身历史中位数（约 2 个鲁棒标准差）。
    z_thr = float(os.getenv("Z_THR", "-2.0") or "-2.0")
    # MAX_WORKERS：并发线程数。提高会加速，但也会放大网络抖动/数据源限制导致的失败概率。
    max_workers = int(os.getenv("MAX_WORKERS", "8") or "8")

    rows: list[dict] = []
    items = list(df_codes[["code", "name"]].itertuples(index=False, name=None))
    total = len(items)
    print(f"开始扫描：0/{total}")
    if max_workers <= 1:
        for i, (code, name) in enumerate(items, 1):
            if i % 200 == 0:
                print(f"已扫描了：{i}/{total}")
            try:
                r = _analyze_one(
                    code=code,
                    name=name,
                    last_date=last_date,
                    window=window,
                    smooth=smooth,
                    hist_n=hist_n,
                    lookback_days=lookback_days,
                    delta_floor=delta_floor,
                    z_thr=z_thr,
                )
            except Exception:
                r = None
            if r is not None:
                rows.append(r)
    else:
        done_n = 0
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            fut_map = {
                pool.submit(
                    _analyze_one,
                    code,
                    name,
                    last_date,
                    window=window,
                    smooth=smooth,
                    hist_n=hist_n,
                    lookback_days=lookback_days,
                    delta_floor=delta_floor,
                    z_thr=z_thr,
                ): (code, name)
                for code, name in items
            }
            for fut in as_completed(fut_map):
                done_n += 1
                if done_n % 200 == 0:
                    print(f"已扫描了：{done_n}/{total}")
                try:
                    r = fut.result()
                except Exception:
                    r = None
                if r is not None:
                    rows.append(r)
        print(f"已扫描了：{total}/{total}")

    if not rows:
        print("无入选标的。")
        print(f"完成：总耗时 {time.perf_counter() - t0:.2f}s")
        return pd.DataFrame(columns=["symbol", "name", "date", "score", "reason"])

    df_out = pd.DataFrame(rows)
    df_out = df_out.sort_values(["score", "delta"], ascending=[False, True]).reset_index(drop=True)
    # MAX_RESULTS：最多保留多少条结果，避免结果过大导致存储/前端渲染压力。
    max_results = int(os.getenv("MAX_RESULTS", "300") or "300")
    df_out = df_out.head(max_results)
    print(f"入选数量: {len(df_out)}")
    print(df_out.head(50).to_string(index=False))
    print(f"完成：总耗时 {time.perf_counter() - t0:.2f}s")
    return df_out


if __name__ == "__main__":
    try:
        df = main()
    except Exception as e:
        print("脚本异常:", str(e))
        print(traceback.format_exc())
        df = pd.DataFrame()
