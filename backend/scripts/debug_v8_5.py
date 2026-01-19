
import akshare as ak
import pandas as pd
import numpy as np
import datetime
import talib
from scipy.signal import argrelextrema
import warnings

warnings.filterwarnings('ignore')

# 调试配置
DEBUG_SYMBOL = "600519" # 茅台作为基准
RECENT_VOLUME_DAYS = 5
VOLUME_BASE_DAYS = 400
CAP_SMALL = 100 * 1e8
CAP_LARGE = 500 * 1e8
MAX_SCORE_ALPHA = 4.0
MAX_SCORE_DRAGON = 3.0

def _kalman_filter_1d(values: pd.Series, q: float = 1e-5, r_scale: float = 0.20):
    v = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    if v.size == 0: return values
    first_finite_idx = int(np.argmax(np.isfinite(v))) if np.isfinite(v).any() else None
    if first_finite_idx is None: return values
    dv = np.diff(v[np.isfinite(v)])
    base_var = float(np.nanvar(dv)) if dv.size else 0.1
    r = max(1e-9, r_scale * base_var)
    x = float(v[first_finite_idx])
    p = 1.0
    out = np.empty_like(v, dtype=float)
    out[:] = np.nan
    for i in range(first_finite_idx, v.size):
        if np.isfinite(v[i]):
            p = p + q
            k = p / (p + r)
            x = x + k * (v[i] - x)
            p = (1.0 - k) * p
            out[i] = x
    return pd.Series(out, index=values.index)

def calculate_alpha54(open_s, high_s, low_s, close_s):
    o = pd.to_numeric(open_s, errors="coerce")
    h = pd.to_numeric(high_s, errors="coerce")
    l = pd.to_numeric(low_s, errors="coerce")
    c = pd.to_numeric(close_s, errors="coerce")
    denom = (l - h).replace(0, -0.01)
    term1 = (l - c) / denom
    term2 = (o / c) ** 5
    alpha = -1.0 * term1 * term2
    curr = alpha.iloc[-1]
    hist = alpha.iloc[-60:].dropna()
    if len(hist) < 30: return 0, 0
    rank = (hist <= curr).mean()
    score = 0
    if rank > 0.5:
        score = ((rank - 0.5) / 0.5) * MAX_SCORE_ALPHA
    return score, rank

def calculate_long_term_vwap(amount, volume, current_price):
    if len(amount) < 300: return 0, 0, 0
    amt_300 = amount.iloc[-300:].sum()
    vol_300 = volume.iloc[-300:].sum()
    vwap_300 = amt_300 / (vol_300 + 1e-9)
    dist_pct = (current_price - vwap_300) / vwap_300
    score = 0
    if -0.15 <= dist_pct <= 0.05:
        score = 2
    return score, vwap_300, dist_pct

def check_overhead_supply(close, volume, amount, current_price):
    if len(close) < 250: return False
    vol_20 = volume.rolling(20).sum()
    amt_20 = amount.rolling(20).sum()
    vwap_20 = amt_20 / (vol_20 + 1e-9)
    current_vwap = vwap_20.iloc[-1]
    
    print(f"   [Debug] Price: {current_price}, VWAP20: {current_vwap:.2f}")
    if current_price < current_vwap * 0.90: 
        print("   [Debug] ❌ Failed: Price < 0.9 * VWAP20 (Short Term Trend Bad)")
        return False
        
    high_52w = close.rolling(250).max().iloc[-1]
    drawdown = (high_52w - current_price) / high_52w
    print(f"   [Debug] Drawdown: {drawdown:.2f}")
    if drawdown <= 0.20:
        print("   [Debug] ❌ Failed: Drawdown <= 0.20 (Not Deep Enough)")
        return False
        
    return True

print(f"🔍 Debugging v8.5 Logic on {DEBUG_SYMBOL}...")

end_date = datetime.datetime.now()
start_date = end_date - datetime.timedelta(days=VOLUME_BASE_DAYS + 60)
start_date_str = start_date.strftime("%Y%m%d")
end_date_str = end_date.strftime("%Y%m%d")

try:
    df_hist = ak.stock_zh_a_hist(symbol=DEBUG_SYMBOL, period="daily", start_date=start_date_str, end_date=end_date_str, adjust="qfq")
    if df_hist is None or df_hist.empty:
        print("No data")
        exit()
        
    close = pd.to_numeric(df_hist["收盘"], errors="coerce")
    open_ = pd.to_numeric(df_hist["开盘"], errors="coerce")
    high = pd.to_numeric(df_hist["最高"], errors="coerce")
    low = pd.to_numeric(df_hist["最低"], errors="coerce")
    volume = pd.to_numeric(df_hist["成交量"], errors="coerce")
    amount = pd.to_numeric(df_hist["成交额"], errors="coerce")
    current_price = close.iloc[-1]
    
    print(f"\n1. Overhead Supply Check:")
    res_supply = check_overhead_supply(close, volume, amount, current_price)
    print(f"   -> Result: {res_supply}")
    
    print(f"\n2. Trend Protect Check:")
    ma60 = close.rolling(60).mean()
    ma60_val = ma60.iloc[-1]
    print(f"   [Debug] Price: {current_price}, MA60: {ma60_val:.2f}")
    if current_price < ma60_val * 0.85:
        print("   [Debug] ❌ Failed: Price < MA60 * 0.85 (Trend Broken)")
    else:
        print("   [Debug] ✅ Passed: Trend Protect")
        
    print(f"\n3. Alpha#54 Check:")
    a_score, a_rank = calculate_alpha54(open_, high, low, close)
    print(f"   [Debug] Alpha Rank: {a_rank:.2f}, Score: {a_score:.2f}")
    
    print(f"\n4. VWAP Support Check:")
    v_score, v_val, v_dist = calculate_long_term_vwap(amount, volume, current_price)
    print(f"   [Debug] VWAP300: {v_val:.2f}, Dist: {v_dist:.2%}, Score: {v_score}")

except Exception as e:
    print(f"Error: {e}")
