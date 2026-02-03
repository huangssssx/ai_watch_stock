import akshare as ak
import pandas as pd
import numpy as np
from typing import Any, Dict, List, Optional, Tuple

def _normalize_sina_symbol(symbol: str) -> str:
    s = str(symbol).strip().lower()
    if s.startswith(("sh", "sz", "bj")):
        return s
    if s.startswith(("6", "688")):
        return "sh" + s
    if s.startswith(("0", "2", "3")):
        return "sz" + s
    return s

def get_a_minute_data(symbol: str, period: str = "1", adjust: str = "", start_date: str = None, end_date: str = None) -> pd.DataFrame:
    code = str(symbol).strip()
    df = pd.DataFrame()
    try:
        if start_date or end_date:
            df = ak.stock_zh_a_hist_min_em(symbol=code, start_date=start_date, end_date=end_date, period=period, adjust=adjust)
        else:
            df = ak.stock_zh_a_hist_min_em(symbol=code, period=period, adjust=adjust)
    except Exception as e:
        df = pd.DataFrame()
        print(f"Warning: Failed to fetch minute data for {symbol} with period {period} and adjust {adjust}. Error: {e}")
    if df is not None and not df.empty:
        out = df.copy()
        for col in ["开盘", "收盘", "最高", "最低", "成交量", "成交额", "均价"]:
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce")
        if "时间" in out.columns:
            out["时间"] = out["时间"].astype(str)
        return out
    sina_code = _normalize_sina_symbol(code)
    try:
        sdf = ak.stock_zh_a_minute(symbol=sina_code, period=period, adjust=adjust)
    except Exception:
        sdf = pd.DataFrame()
    if sdf is None or sdf.empty:
        return pd.DataFrame()
    out = sdf.copy()
    time_col = None
    for c in ["时间", "datetime", "日期", "date", "time", "day"]:
        if c in out.columns:
            time_col = c
            break
    if time_col:
        out.rename(columns={time_col: "时间"}, inplace=True)
    out.rename(columns={
        "open": "开盘",
        "close": "收盘",
        "high": "最高",
        "low": "最低",
        "volume": "成交量",
        "amount": "成交额",
    }, inplace=True)
    if "均价" not in out.columns:
        if "收盘" in out.columns:
            out["均价"] = out["收盘"]
        else:
            out["均价"] = np.nan
    for col in ["开盘", "收盘", "最高", "最低", "成交量", "成交额", "均价"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    if "时间" in out.columns:
        out["时间"] = out["时间"].astype(str)
    return out

def get_a_minute_data_with_error(
    symbol: str,
    period: str = "1",
    adjust: str = "",
    start_date: str = None,
    end_date: str = None,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    code = str(symbol).strip()
    errors: List[Dict[str, str]] = []

    try:
        if start_date or end_date:
            df = ak.stock_zh_a_hist_min_em(
                symbol=code,
                start_date=start_date,
                end_date=end_date,
                period=period,
                adjust=adjust,
            )
        else:
            df = ak.stock_zh_a_hist_min_em(symbol=code, period=period, adjust=adjust)
    except Exception as e:
        df = pd.DataFrame()
        errors.append({"source": "stock_zh_a_hist_min_em", "error": str(e)})

    if df is not None and not df.empty:
        out = df.copy()
        for col in ["开盘", "收盘", "最高", "最低", "成交量", "成交额", "均价"]:
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce")
        if "时间" in out.columns:
            out["时间"] = out["时间"].astype(str)
        return out, {"source": "stock_zh_a_hist_min_em", "errors": errors}

    sina_code = _normalize_sina_symbol(code)
    try:
        sdf = ak.stock_zh_a_minute(symbol=sina_code, period=period, adjust=adjust)
    except Exception as e:
        sdf = pd.DataFrame()
        errors.append({"source": "stock_zh_a_minute", "error": str(e)})

    if sdf is None or sdf.empty:
        return pd.DataFrame(), {"source": None, "errors": errors}

    out = sdf.copy()
    time_col: Optional[str] = None
    for c in ["时间", "datetime", "日期", "date", "time", "day"]:
        if c in out.columns:
            time_col = c
            break
    if time_col:
        out.rename(columns={time_col: "时间"}, inplace=True)
    out.rename(
        columns={
            "open": "开盘",
            "close": "收盘",
            "high": "最高",
            "low": "最低",
            "volume": "成交量",
            "amount": "成交额",
        },
        inplace=True,
    )
    if "均价" not in out.columns:
        if "收盘" in out.columns:
            out["均价"] = out["收盘"]
        else:
            out["均价"] = np.nan
    for col in ["开盘", "收盘", "最高", "最低", "成交量", "成交额", "均价"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    if "时间" in out.columns:
        out["时间"] = out["时间"].astype(str)
    return out, {"source": "stock_zh_a_minute", "errors": errors}
