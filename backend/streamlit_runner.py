import os
import sys
_backend_dir = os.path.dirname(os.path.abspath(__file__))
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)
from pymr_compat import ensure_py_mini_racer
ensure_py_mini_racer()
import pandas as pd
import datetime
import math

codes = ["600746"]


def _to_num(x):
    return pd.to_numeric(x, errors="coerce")


def _clamp(x, lo=0.0, hi=1.0):
    try:
        v = float(x)
    except Exception:
        return 0.0
    if v != v:
        return 0.0
    return max(lo, min(hi, v))


def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - signal_line
    return macd_line, signal_line, hist


def _score_to_prob(score_0_1: float) -> float:
    z = (score_0_1 - 0.55) * 6.0
    return 100.0 / (1.0 + math.exp(-z))


def _normalize_code(code: str) -> str:
    s = str(code).strip()
    if s.startswith("sh") or s.startswith("sz"):
        s = s[2:]
    return s.zfill(6)


def _get_name(code6: str):
    try:
        info = ak.stock_individual_info_em(symbol=code6)
        if info is not None and not info.empty:
            if "item" in info.columns and "value" in info.columns:
                m = dict(zip(info["item"].astype(str), info["value"].astype(str)))
                v = m.get("股票简称") or m.get("证券简称") or m.get("股票名称")
                if v:
                    return str(v)
    except Exception:
        pass
    return ""


def _calc_one(code: str) -> dict:
    code6 = _normalize_code(code)
    hist = ak.stock_zh_a_hist(symbol=code6, period="daily", adjust="qfq")
    if hist is None or hist.empty:
        return {"symbol": code6, "name": _get_name(code6), "error": "no_hist"}

    df = hist.copy()
    rename = {
        "日期": "date",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "amount",
        "涨跌幅": "pct_chg",
    }
    for k, v in rename.items():
        if k in df.columns:
            df.rename(columns={k: v}, inplace=True)

    if "date" in df.columns:
        df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
        df = df[df["date_dt"].notna()].sort_values("date_dt").reset_index(drop=True)
    else:
        return {"symbol": code6, "name": _get_name(code6), "error": "missing_date"}

    for c in ["open", "close", "high", "low", "volume", "amount", "pct_chg"]:
        if c in df.columns:
            df[c] = _to_num(df[c])

    if len(df) < 80:
        return {"symbol": code6, "name": _get_name(code6), "error": f"hist_too_short:{len(df)}"}

    close = df["close"]
    open_ = df.get("open", close)
    high = df.get("high", close)
    low = df.get("low", close)
    vol = df.get("volume", pd.Series([None] * len(df)))

    rsi14 = _rsi(close, 14)
    macd_line, signal_line, histv = _macd(close)

    low60 = low.rolling(60, min_periods=10).min()
    near_low_ratio = (close - low60) / low60

    ret20 = close / close.shift(20) - 1.0

    vol_ma5 = vol.rolling(5, min_periods=3).mean()
    vol_ma20 = vol.rolling(20, min_periods=10).mean()
    vol_ratio = vol_ma5 / vol_ma20

    ma20 = close.rolling(20, min_periods=10).mean()
    ma20_dist = (close - ma20) / ma20

    rng = (high - low).replace(0, pd.NA)
    lower_shadow = (pd.concat([open_, close], axis=1).min(axis=1) - low) / rng

    i = len(df) - 1

    s_rsi = _clamp((35.0 - float(rsi14.iloc[i])) / 20.0)
    s_near_low = _clamp((0.08 - float(near_low_ratio.iloc[i])) / 0.08)
    s_dd = _clamp((-float(ret20.iloc[i])) / 0.25)

    vr = float(vol_ratio.iloc[i]) if i < len(vol_ratio) else float("nan")
    s_vol = _clamp((0.75 - vr) / 0.45)

    hist_now = float(histv.iloc[i])
    hist_prev = float(histv.iloc[i - 1])
    macd_now = float(macd_line.iloc[i])
    s_macd = 1.0 if (hist_now > hist_prev and macd_now < 0) else (0.6 if hist_now > hist_prev else (0.25 if macd_now < 0 else 0.0))

    ls = float(lower_shadow.iloc[i]) if i < len(lower_shadow) else float("nan")
    s_pin = _clamp((ls - 0.35) / 0.35)

    mad = float(ma20_dist.iloc[i])
    s_ma = _clamp((0.02 - abs(mad)) / 0.02)

    weights = {
        "rsi": 0.25,
        "near_low": 0.20,
        "drawdown": 0.15,
        "vol_dry": 0.10,
        "macd": 0.15,
        "pin": 0.10,
        "ma": 0.05,
    }

    score = (
        weights["rsi"] * s_rsi
        + weights["near_low"] * s_near_low
        + weights["drawdown"] * s_dd
        + weights["vol_dry"] * s_vol
        + weights["macd"] * s_macd
        + weights["pin"] * s_pin
        + weights["ma"] * s_ma
    )

    prob = _score_to_prob(score)

    sig = []
    if float(rsi14.iloc[i]) <= 30:
        sig.append("RSI<=30")
    if float(near_low_ratio.iloc[i]) <= 0.08:
        sig.append("距60日低点<=8%")
    if float(ret20.iloc[i]) <= -0.10:
        sig.append("20日跌幅>=10%")
    if vr == vr and vr <= 0.75:
        sig.append("量能收缩")
    if hist_now > hist_prev:
        sig.append("MACD柱走高")
    if ls == ls and ls >= 0.55:
        sig.append("长下影")
    if abs(mad) <= 0.02:
        sig.append("贴近MA20")

    return {
        "symbol": code6,
        "name": _get_name(code6),
        "date": df["date"].iloc[i],
        "close": float(close.iloc[i]),
        "rsi14": float(rsi14.iloc[i]),
        "near_low_60d": float(near_low_ratio.iloc[i]),
        "ret20": float(ret20.iloc[i]),
        "vol_ratio_5_20": float(vr) if vr == vr else None,
        "macd": float(macd_now),
        "macd_hist": float(hist_now),
        "lower_shadow": float(ls) if ls == ls else None,
        "score_0_1": float(score),
        "bottom_prob": float(round(prob, 2)),
        "signals": " | ".join(sig),
        "error": "",
    }


rows = []
for c in codes:
    if c is None:
        continue
    cs = str(c).strip()
    if not cs:
        continue
    try:
        rows.append(_calc_one(cs))
    except Exception as e:
        rows.append({"symbol": _normalize_code(cs), "name": _get_name(_normalize_code(cs)), "error": str(e)})

df = pd.DataFrame(rows)
if not df.empty and "bottom_prob" in df.columns:
    df = df.sort_values(["bottom_prob"], ascending=False, na_position="last").reset_index(drop=True)

print("codes=", codes)
print("rows=", len(df))
