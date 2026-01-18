import talib
import numpy as np
import pandas as pd
import akshare as ak
import datetime

# --- 1. 动态波动率画像 (Dynamic Volatility Profile) ---
def get_volatility_profile_rolling(df, window=60):
    """
    Calculate rolling volatility profile for backtesting.
    Returns a DataFrame with columns: 'vol_type', 'chop_thresh', 'vol_mul', 'kama_slow_T'
    """
    high = df["high"].values
    low = df["low"].values
    close = df["close"].values
    
    # NATR
    natr = talib.NATR(high, low, close, timeperiod=14)
    natr_s = pd.Series(natr, index=df.index)
    
    # Rolling average NATR
    avg_natr = natr_s.rolling(window=window, min_periods=1).mean()
    
    # Define thresholds
    params = pd.DataFrame(index=df.index)
    params['type'] = 'NORMAL'
    params['chop_thresh'] = 61.8
    params['vol_mul'] = 1.5
    params['kama_slow_T'] = 20
    params['rsi_limit'] = 75 # Default RSI limit
    params['ignore_weakening'] = False # Default respect weakening
    params['kama_fast_T'] = 8 # Default fast KAMA
    params['use_ma60_filter'] = False
    
    # High Beta (NATR > 3.5)
    mask_high = avg_natr > 3.5
    params.loc[mask_high, 'type'] = 'HIGH_BETA'
    params.loc[mask_high, 'chop_thresh'] = 50.0
    params.loc[mask_high, 'vol_mul'] = 2.0
    params.loc[mask_high, 'kama_slow_T'] = 30
    params.loc[mask_high, 'rsi_limit'] = 75
    params.loc[mask_high, 'kama_fast_T'] = 5
    
    # Low Beta (NATR < 2.0) - Banks/Utilities
    mask_low = avg_natr < 2.0
    params.loc[mask_low, 'type'] = 'LOW_BETA'
    params.loc[mask_low, 'chop_thresh'] = 65.0
    params.loc[mask_low, 'vol_mul'] = 1.2
    params.loc[mask_low, 'kama_slow_T'] = 25
    params.loc[mask_low, 'rsi_limit'] = 80
    params.loc[mask_low, 'ignore_weakening'] = True
    params.loc[mask_low, 'kama_fast_T'] = 12
    params.loc[mask_low, 'use_ma60_filter'] = True # Must break MA60 to confirm trend reversal
    
    return params

# --- 2. 周线趋势 (Weekly Momentum) ---
def get_weekly_status_rolling(df):
    """
    Calculate weekly status mapped back to daily.
    Returns Series with values: 'UP', 'DOWN', 'WEAKENING', 'NEUTRAL'
    """
    df_w = df.resample("W").agg({
        "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"
    }).dropna()
    
    if len(df_w) < 26:
        return pd.Series("NEUTRAL", index=df.index)
        
    close_w = df_w["close"].values
    macd, signal, hist = talib.MACD(close_w, fastperiod=12, slowperiod=26, signalperiod=9)
    
    status_w = []
    for i in range(len(close_w)):
        c_macd = macd[i]
        c_sig = signal[i]
        c_hist = hist[i]
        p_hist = hist[i-1] if i > 0 else 0
        
        if np.isnan(c_macd) or np.isnan(c_sig):
            status_w.append("NEUTRAL")
            continue
            
        if c_macd < c_sig:
            status_w.append("DOWN")
        elif (c_macd > c_sig) and (c_hist < p_hist * 0.8) and (c_hist > 0):
            status_w.append("WEAKENING")
        else:
            status_w.append("UP")
            
    status_series_w = pd.Series(status_w, index=df_w.index)
    status_daily = status_series_w.reindex(df.index, method='ffill').fillna("NEUTRAL")
    
    return status_daily

# --- 3. Main Backtest Logic ---
def run_backtest_strategy(df):
    """
    Runs the v4.0 strategy on the entire dataframe.
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    
    # 1. Volatility Profile
    profile = get_volatility_profile_rolling(df)
    
    # 2. Indicators
    close = df["close"].values
    high = df["high"].values
    low = df["low"].values
    volume = df["volume"].values.astype(float)
    
    # Pre-calculate possible KAMA lines
    kama_slow_20 = talib.KAMA(close, timeperiod=20)
    kama_slow_25 = talib.KAMA(close, timeperiod=25)
    kama_slow_30 = talib.KAMA(close, timeperiod=30)
    
    kama_fast_5 = talib.KAMA(close, timeperiod=5)
    kama_fast_8 = talib.KAMA(close, timeperiod=8)
    kama_fast_12 = talib.KAMA(close, timeperiod=12)
    
    ma20 = talib.SMA(close, timeperiod=20)
    ma60 = talib.SMA(close, timeperiod=60) # New: MA60 for Low Beta
    atr = talib.ATR(high, low, close, timeperiod=14)
    rsi = talib.RSI(close, timeperiod=14)
    vol_ma5 = talib.SMA(volume, timeperiod=5)
    
    # CHOP
    tr1 = talib.TRANGE(high, low, close)
    sum_tr = pd.Series(tr1).rolling(14).sum()
    range_hl = pd.Series(high).rolling(14).max() - pd.Series(low).rolling(14).min()
    range_hl = range_hl.replace(0, np.nan)
    chop = 100 * np.log10(sum_tr / range_hl) / np.log10(14)
    
    weekly_status = get_weekly_status_rolling(df)
    
    results = pd.DataFrame(index=df.index)
    results['signal'] = 'SAFE'
    results['reason'] = ''
    results['price'] = close
    
    for i in range(60, len(df)):
        idx = df.index[i]
        
        # Profile Parameters
        p_type = profile.loc[idx, 'type']
        p_chop_thresh = profile.loc[idx, 'chop_thresh']
        p_vol_mul = profile.loc[idx, 'vol_mul']
        p_kama_slow_T = profile.loc[idx, 'kama_slow_T']
        p_rsi_limit = profile.loc[idx, 'rsi_limit']
        p_ignore_weak = profile.loc[idx, 'ignore_weakening']
        p_kama_fast_T = profile.loc[idx, 'kama_fast_T']
        p_use_ma60 = profile.loc[idx, 'use_ma60_filter']
        
        # Select KAMA Slow
        if p_kama_slow_T == 30:
            curr_kama_slow = kama_slow_30[i]
        elif p_kama_slow_T == 25:
            curr_kama_slow = kama_slow_25[i]
        else:
            curr_kama_slow = kama_slow_20[i]
            
        # Select KAMA Fast
        if p_kama_fast_T == 5:
            curr_kama_fast = kama_fast_5[i]
        elif p_kama_fast_T == 12:
            curr_kama_fast = kama_fast_12[i]
        else:
            curr_kama_fast = kama_fast_8[i]
            
        # Current Data
        curr_close = close[i]
        prev_close = close[i-1]
        prev_low = low[i-1]
        curr_vol = volume[i]
        curr_vol_ma5 = vol_ma5[i]
        curr_rsi = rsi[i]
        curr_chop = chop.iloc[i]
        curr_weekly = weekly_status.iloc[i]
        
        danger_reasons = []
        warning_reasons = []
        
        # --- LOGIC CORE ---
        
        # A. Low CHOP Breakout (Black Swan)
        if (curr_chop < 38.2) and (curr_close < curr_kama_slow):
            danger_reasons.append("Low_CHOP_Breakout")
            
        # B. Gap Down
        curr_open = df["open"].iloc[i]
        if (curr_open < prev_low * 0.995) and (curr_close < prev_low):
            danger_reasons.append("Gap_Down")
            
        # C. Zhaban Trap
        day_high = high[i]
        day_high_pct = (day_high - prev_close) / prev_close
        drawdown = (day_high - curr_close) / day_high
        if (day_high_pct > 0.05) and (drawdown > 0.04):
            danger_reasons.append("Zhaban_Trap")
            
        # D. Trend Breakdown
        # Filter: If Low Beta, must also break MA60
        trend_broken = (curr_close < curr_kama_slow)
        
        # Low Beta Filter
        if trend_broken and p_use_ma60:
            if curr_close > ma60[i]:
                trend_broken = False
        
        # High Beta Filter: If RSI is still strong (>60), don't sell on first dip
        if trend_broken and (p_type == 'HIGH_BETA') and (curr_rsi > 60):
            trend_broken = False
                
        if trend_broken:
            if curr_weekly == "DOWN":
                # Filter in choppy market
                if (curr_chop < p_chop_thresh) or (curr_vol > p_vol_mul * curr_vol_ma5):
                    danger_reasons.append("Trend_Break_Weekly_Down")
            elif curr_weekly == "WEAKENING":
                # If Low Beta, ignore Weakening, wait for Dead Cross
                if not p_ignore_weak:
                    danger_reasons.append("Trend_Break_Weekly_Weak")
                
        # E. Limit Up Trap
        prev_pct = (close[i-1] - close[i-2]) / close[i-2]
        if (prev_pct > 0.09) and (curr_close < prev_close):
            danger_reasons.append("Limit_Up_Trap")
            
        # F. Churning
        if (curr_vol > 2.0 * curr_vol_ma5) and (abs(curr_close - prev_close)/prev_close < 0.01):
            warning_reasons.append("Churning")
            
        # G. Overheat
        if (curr_rsi > p_rsi_limit) and (curr_close < curr_kama_fast):
            warning_reasons.append("RSI_Overheat")
            
        # Output
        if danger_reasons:
            results.loc[idx, 'signal'] = 'STRONG_SELL'
            results.loc[idx, 'reason'] = "+".join(danger_reasons)
        elif warning_reasons:
            results.loc[idx, 'signal'] = 'SELL'
            results.loc[idx, 'reason'] = "+".join(warning_reasons)
            
    return results
