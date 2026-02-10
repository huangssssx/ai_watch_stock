from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Iterable

import numpy as np
import pandas as pd
import threading

from utils.pytdx_client import tdx

_best_endpoint_cache: Optional[tuple[str, int]] = None
_best_endpoint_cached_at: float = 0.0
_tdx_request_lock = threading.Lock()


def _infer_market(code: str) -> int:
    code = str(code or "").zfill(6)
    if code.startswith(("600", "601", "603", "605", "688")):
        return 1
    return 0


def _iter_stock_endpoints(limit: int = 15) -> list[tuple[str, int]]:
    out: list[tuple[str, int]] = []
    seen: set[tuple[str, int]] = set()

    for ep in [("180.153.18.170", 7709), ("101.227.73.20", 7709)]:
        if ep in seen:
            continue
        seen.add(ep)
        out.append(ep)
    try:
        from pytdx.util import best_ip as _best_ip
    except Exception:
        _best_ip = None
    items = getattr(_best_ip, "stock_ip", None) if _best_ip is not None else None
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            ip = item.get("ip")
            port = item.get("port")
            if not isinstance(ip, str) or not isinstance(port, int):
                continue
            ep = (ip, port)
            if ep in seen:
                continue
            seen.add(ep)
            out.append(ep)
            if len(out) >= max(1, int(limit)):
                break
    return out[: max(1, int(limit))]


def _safe_vwap_and_volume_shares(amount: float, volume_raw: float, close_price: float) -> tuple[Optional[float], float]:
    try:
        vol = float(volume_raw)
    except Exception:
        return None, 0.0
    if not np.isfinite(vol) or vol <= 0:
        return None, 0.0

    try:
        amt = float(amount)
    except Exception:
        amt = np.nan
    if not np.isfinite(amt) or amt <= 0:
        return None, vol

    raw_vwap = amt / vol

    cp = float(close_price) if close_price is not None and np.isfinite(close_price) else None
    if cp is not None and cp > 0:
        ratio = raw_vwap / cp
        if 80.0 < ratio < 120.0:
            return raw_vwap / 100.0, vol * 100.0
        if 0.8 < ratio < 1.2:
            return raw_vwap, vol

    if cp is not None and cp > 0 and raw_vwap > cp * 50:
        return raw_vwap / 100.0, vol * 100.0
    return raw_vwap, vol


def _weighted_quantile(values: np.ndarray, weights: np.ndarray, q: float) -> float:
    order = np.argsort(values)
    v = values[order]
    w = weights[order]
    w = np.where(np.isfinite(w) & (w > 0), w, 0.0)
    total = float(np.sum(w))
    if total <= 0:
        return float(v[-1])
    cum = np.cumsum(w) / total
    idx = int(np.searchsorted(cum, q, side="left"))
    if idx < 0:
        idx = 0
    if idx >= len(v):
        idx = len(v) - 1
    return float(v[idx])


def _fetch_daily_bars(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    market = _infer_market(code)
    code = str(code or "").zfill(6)
    start_dt = pd.to_datetime(start_date, errors="coerce")
    end_dt = pd.to_datetime(end_date, errors="coerce")
    if pd.isna(start_dt) or pd.isna(end_dt):
        raise ValueError(f"非法日期: start_date={start_date}, end_date={end_date}")
    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt

    step = 700
    endpoints_tried: list[tuple[str, int]] = []
    endpoints = _iter_stock_endpoints()
    frames: list[pd.DataFrame] = []
    last_err: Optional[Exception] = None
    for ip, port in endpoints:
        endpoints_tried.append((ip, port))
        local_frames: list[pd.DataFrame] = []
        start_offset = 0
        try:
            while True:
                with _tdx_request_lock:
                    tdx.configure(ip, port)
                    bars = tdx.get_security_bars(9, market, code, int(start_offset), int(step))
                if not bars:
                    break
                df = pd.DataFrame(bars)
                if df is None or df.empty:
                    break
                if "datetime" in df.columns:
                    df = df.sort_values("datetime")
                local_frames.append(df)

                dt_col = pd.to_datetime(df["datetime"].astype(str).str.slice(0, 10), errors="coerce")
                min_dt = dt_col.min()
                if pd.isna(min_dt) or min_dt <= start_dt:
                    break
                start_offset += step
                if start_offset > 20000:
                    break

            frames = local_frames
            last_err = None
            break
        except Exception as e:
            last_err = e
            frames = []

    if not frames:
        if last_err is not None:
            raise RuntimeError(f"pytdx 拉取失败: code={code}, tried_endpoints={endpoints_tried[:10]}") from last_err
        return pd.DataFrame()

    out = pd.concat(frames, axis=0, ignore_index=True)
    out["date"] = pd.to_datetime(out["datetime"].astype(str).str.slice(0, 10), errors="coerce")
    out = out.dropna(subset=["date"]).drop_duplicates(subset=["date"], keep="last")
    out = out.sort_values("date").reset_index(drop=True)
    out = out[(out["date"] >= start_dt) & (out["date"] <= end_dt)].reset_index(drop=True)
    return out


def _calc_proxy_concentration(df_daily: pd.DataFrame, window: int, smooth: int) -> pd.DataFrame:
    df = df_daily.copy()
    close = pd.to_numeric(df.get("close", np.nan), errors="coerce")
    amount = pd.to_numeric(df.get("amount", np.nan), errors="coerce")
    vol = pd.to_numeric(df.get("vol", np.nan), errors="coerce")

    vwap_list: list[Optional[float]] = []
    w_list: list[float] = []
    for a, v, c in zip(amount.tolist(), vol.tolist(), close.tolist()):
        vwap, w = _safe_vwap_and_volume_shares(a, v, c)
        vwap_list.append(vwap)
        w_list.append(w)
    df["vwap"] = pd.to_numeric(pd.Series(vwap_list), errors="coerce")
    df["vol_shares"] = pd.to_numeric(pd.Series(w_list), errors="coerce").fillna(0.0)

    proxy_90: list[Optional[float]] = []
    proxy_70: list[Optional[float]] = []
    avg_cost: list[Optional[float]] = []
    for i in range(len(df)):
        j0 = max(0, i - window + 1)
        sub = df.iloc[j0 : i + 1]
        vals = sub["vwap"].to_numpy(dtype=float)
        wts = sub["vol_shares"].to_numpy(dtype=float)
        mask = np.isfinite(vals) & np.isfinite(wts) & (wts > 0)
        vals = vals[mask]
        wts = wts[mask]
        if len(vals) < max(10, window // 3):
            proxy_90.append(None)
            proxy_70.append(None)
            avg_cost.append(None)
            continue

        w_mean = float(np.sum(vals * wts) / np.sum(wts))
        q05 = _weighted_quantile(vals, wts, 0.05)
        q95 = _weighted_quantile(vals, wts, 0.95)
        q15 = _weighted_quantile(vals, wts, 0.15)
        q85 = _weighted_quantile(vals, wts, 0.85)
        avg_cost.append(w_mean)
        proxy_90.append((q95 - q05) / w_mean if w_mean > 0 else None)
        proxy_70.append((q85 - q15) / w_mean if w_mean > 0 else None)

    df["proxy_avg_cost"] = avg_cost
    df["proxy_90_concentration"] = proxy_90
    df["proxy_70_concentration"] = proxy_70

    smooth_n = max(1, int(smooth))
    df["proxy_90_concentration_smooth"] = (
        pd.to_numeric(df["proxy_90_concentration"], errors="coerce").rolling(smooth_n, min_periods=1).mean()
    )
    df["proxy_70_concentration_smooth"] = (
        pd.to_numeric(df["proxy_70_concentration"], errors="coerce").rolling(smooth_n, min_periods=1).mean()
    )
    return df[
        [
            "date",
            "proxy_avg_cost",
            "proxy_90_concentration",
            "proxy_70_concentration",
            "proxy_90_concentration_smooth",
            "proxy_70_concentration_smooth",
        ]
    ].copy()


@dataclass(frozen=True)
class ChipProxyParams:
    window: int = 60
    smooth: int = 5


def get_chip_concentration_proxy(
    code: str,
    start_date: str,
    end_date: str,
    params: ChipProxyParams | None = None,
    as_df: bool = False,
) -> list[dict] | pd.DataFrame:
    p = params or ChipProxyParams()
    df_daily = _fetch_daily_bars(code=code, start_date=start_date, end_date=end_date)
    if df_daily.empty:
        out_df = pd.DataFrame(
            columns=[
                "date",
                "proxy_avg_cost",
                "proxy_90_concentration",
                "proxy_70_concentration",
                "proxy_90_concentration_smooth",
                "proxy_70_concentration_smooth",
            ]
        )
        return out_df if as_df else []

    out_df = _calc_proxy_concentration(df_daily, window=int(p.window), smooth=int(p.smooth))
    if as_df:
        return out_df
    out_df = out_df.copy()
    out_df["date"] = out_df["date"].dt.strftime("%Y-%m-%d")
    return out_df.to_dict(orient="records")


def _parse_args(argv: Iterable[str]) -> dict:
    args = list(argv)
    if len(args) < 4:
        raise SystemExit("用法: python -m utils.chips <code> <start_date> <end_date> [window] [smooth]")
    code = args[1]
    start_date = args[2]
    end_date = args[3]
    window = int(args[4]) if len(args) >= 5 else 60
    smooth = int(args[5]) if len(args) >= 6 else 5
    return {"code": code, "start_date": start_date, "end_date": end_date, "window": window, "smooth": smooth}


if __name__ == "__main__":
    import sys

    a = _parse_args(sys.argv)
    df = get_chip_concentration_proxy(
        a["code"],
        a["start_date"],
        a["end_date"],
        params=ChipProxyParams(window=a["window"], smooth=a["smooth"]),
        as_df=True,
    )
    print(df.tail(20).to_string(index=False))
