import sqlite3
import os

db_path = 'backend/stock_watch.db'

# Use a raw string for the code to avoid escape character issues
# Changes made:
# 1. Removed top-level imports that might be failing in the sandbox scope if not executed
# 2. Moved imports INSIDE functions or the main execution block to ensure they are available in local scope
v4_code = r'''# ==============================================================================
# 掉头向下监控 (Downturn Monitor) - v4.0 A股实战终极版
# ------------------------------------------------------------------------------
# 核心升级点：
# 1. 动态波动率画像 (无硬编码板块)
# 2. 周线动能衰竭 (MACD红柱缩短先行报警)
# 3. 陷阱监控 (日内炸板 + 跳空低开)
# 4. 黑天鹅捕捉 (Low CHOP Breakout)
# ==============================================================================

# 确保在执行上下文中引入依赖
import talib
import numpy as np
import pandas as pd
import akshare as ak
import datetime

# --- 1. 实盘数据获取 (Real-time Data) ---
def get_realtime_data_merged(symbol_input):
    """
    获取“历史日线 + 实时快照”拼接后的完整 DataFrame
    """
    # 显式引入依赖，防止闭包问题
    import pandas as pd
    import akshare as ak
    import datetime
    
    try:
        # 1. 获取历史日线 (截止到昨天)
        # 强制北京时间 (UTC+8)
        now_cn = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
        
        yesterday = (now_cn - datetime.timedelta(days=1)).strftime("%Y%m%d")
        start_date = (now_cn - datetime.timedelta(days=400)).strftime("%Y%m%d")
        
        # 处理 symbol 格式
        symbol_code = symbol_input
        if symbol_input.startswith(("sh", "sz", "bj")):
            symbol_code = symbol_input[2:]
            
        # 拉取历史
        df_hist = ak.stock_zh_a_hist(symbol=symbol_code, period="daily", start_date=start_date, end_date=yesterday, adjust="qfq")
        if df_hist is None or df_hist.empty:
            return pd.DataFrame()
            
        df_hist = df_hist.rename(columns={
            "日期": "date", "开盘": "open", "收盘": "close", 
            "最高": "high", "最低": "low", "成交量": "volume"
        })
        df_hist["date"] = pd.to_datetime(df_hist["date"])
        
        # 2. 获取实时快照 (Spot Data) - 仅在交易时间有效
        # 注意：若盘后运行，spot数据可能已停留在15:00，也是有效的
        try:
            spot_df = ak.stock_zh_a_spot_em()
            current_row = spot_df[spot_df["代码"] == symbol_code]
            
            if not current_row.empty:
                # 3. 构造今日的 Bar
                today_date = pd.to_datetime(now_cn.date())
                
                today_bar = pd.DataFrame({
                    "date": [today_date],
                    "open": [float(current_row.iloc[0]["今开"])],
                    "high": [float(current_row.iloc[0]["最高"])],
                    "low": [float(current_row.iloc[0]["最低"])],
                    "close": [float(current_row.iloc[0]["最新价"])],
                    "volume": [float(current_row.iloc[0]["成交量"])]
                })
                
                # 4. 拼接
                df_merged = pd.concat([df_hist, today_bar], ignore_index=True)
                # 去重 (防止 akshare 历史数据已包含今日)
                df_merged = df_merged.sort_values("date").drop_duplicates(subset=["date"], keep="last")
                return df_merged
        except:
            pass
            
        return df_hist

    except Exception as e:
        # 使用 str(e) 而不是 print 以防 print 不可用
        return pd.DataFrame()

# --- 2. 动态波动率画像 ---
def get_volatility_profile_rolling(df, window=60):
    import talib
    import pandas as pd
    
    high = df["high"].values
    low = df["low"].values
    close = df["close"].values
    
    natr = talib.NATR(high, low, close, timeperiod=14)
    natr_s = pd.Series(natr, index=df.index)
    avg_natr = natr_s.rolling(window=window, min_periods=1).mean()
    
    params = pd.DataFrame(index=df.index)
    # Add avg_natr to params
    params['avg_natr'] = avg_natr
    
    params['type'] = 'NORMAL'
    params['chop_thresh'] = 61.8
    params['vol_mul'] = 1.5
    params['kama_slow_T'] = 20
    params['rsi_limit'] = 75
    params['ignore_weakening'] = False
    params['kama_fast_T'] = 8
    params['use_ma60_filter'] = False
    
    # High Beta (NATR > 3.5)
    mask_high = avg_natr > 3.5
    params.loc[mask_high, 'type'] = 'HIGH_BETA'
    params.loc[mask_high, 'chop_thresh'] = 50.0
    params.loc[mask_high, 'vol_mul'] = 2.0
    params.loc[mask_high, 'kama_slow_T'] = 30
    params.loc[mask_high, 'rsi_limit'] = 75
    params.loc[mask_high, 'kama_fast_T'] = 5
    
    # Low Beta (NATR < 2.0)
    mask_low = avg_natr < 2.0
    params.loc[mask_low, 'type'] = 'LOW_BETA'
    params.loc[mask_low, 'chop_thresh'] = 65.0
    params.loc[mask_low, 'vol_mul'] = 1.2
    params.loc[mask_low, 'kama_slow_T'] = 25
    params.loc[mask_low, 'rsi_limit'] = 80
    params.loc[mask_low, 'ignore_weakening'] = True
    params.loc[mask_low, 'kama_fast_T'] = 12
    params.loc[mask_low, 'use_ma60_filter'] = True
    
    return params

# --- 3. 周线趋势 ---
def get_weekly_status_rolling(df):
    import talib
    import pandas as pd
    import numpy as np
    
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

# --- Main Logic ---
triggered = False
signal = "WAIT"
message = "监控中..."

try:
    # 再次确保主逻辑中有依赖
    import talib
    import pandas as pd
    import numpy as np
    
    # 1. 获取数据
    # 'symbol' 变量由外部环境提供
    df = get_realtime_data_merged(symbol)
    
    if df is None or len(df) < 60:
        message = "数据不足 (需 > 60 天)"
    else:
        # 2. 计算指标
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        
        close = df["close"].values
        high = df["high"].values
        low = df["low"].values
        volume = df["volume"].values.astype(float)
        
        # Profile
        profile_df = get_volatility_profile_rolling(df)
        p = profile_df.iloc[-1] # Take last row parameters
        
        # Indicators
        kama_slow = talib.KAMA(close, timeperiod=int(p['kama_slow_T']))
        kama_fast = talib.KAMA(close, timeperiod=int(p['kama_fast_T']))
        ma60 = talib.SMA(close, timeperiod=60)
        atr = talib.ATR(high, low, close, timeperiod=14)
        rsi = talib.RSI(close, timeperiod=14)
        vol_ma5 = talib.SMA(volume, timeperiod=5)
        
        # CHOP
        tr1 = talib.TRANGE(high, low, close)
        sum_tr = pd.Series(tr1).rolling(14).sum()
        range_hl = pd.Series(high).rolling(14).max() - pd.Series(low).rolling(14).min()
        range_hl = range_hl.replace(0, np.nan)
        chop = 100 * np.log10(sum_tr / range_hl) / np.log10(14)
        
        weekly_status_s = get_weekly_status_rolling(df)
        
        # 3. 获取当前切片 (Last Row)
        curr_close = close[-1]
        prev_close = close[-2]
        prev_low = low[-2]
        curr_vol = volume[-1]
        curr_vol_ma5 = vol_ma5[-1]
        curr_rsi = rsi[-1]
        curr_chop = chop.iloc[-1]
        curr_weekly = weekly_status_s.iloc[-1]
        
        curr_kama_slow = kama_slow[-1]
        curr_kama_fast = kama_fast[-1]
        curr_ma60 = ma60[-1]
        
        danger_reasons = []
        warning_reasons = []
        
        # --- LOGIC CORE v4.0 ---
        
        # A. Low CHOP Breakout (Black Swan)
        if (curr_chop < 38.2) and (curr_close < curr_kama_slow):
            danger_reasons.append("Low_CHOP_Breakout")
            
        # B. Gap Down
        curr_open = df["open"].iloc[-1]
        if (curr_open < prev_low * 0.995) and (curr_close < prev_low):
            danger_reasons.append("Gap_Down")
            
        # C. Zhaban Trap
        day_high = high[-1]
        day_high_pct = (day_high - prev_close) / prev_close
        drawdown = (day_high - curr_close) / day_high
        if (day_high_pct > 0.05) and (drawdown > 0.04):
            danger_reasons.append("Zhaban_Trap")
            
        # D. Trend Breakdown
        trend_broken = (curr_close < curr_kama_slow)
        
        # Low Beta Filter: Must also break MA60
        if trend_broken and p['use_ma60_filter']:
            if curr_close > curr_ma60:
                trend_broken = False
        
        # High Beta Filter: If RSI > 60, don't sell
        if trend_broken and (p['type'] == 'HIGH_BETA') and (curr_rsi > 60):
            trend_broken = False
            
        if trend_broken:
            if curr_weekly == "DOWN":
                if (curr_chop < p['chop_thresh']) or (curr_vol > p['vol_mul'] * curr_vol_ma5):
                    danger_reasons.append("Trend_Break_Weekly_Down")
            elif curr_weekly == "WEAKENING":
                if not p['ignore_weakening']:
                    danger_reasons.append("Trend_Break_Weekly_Weak")
                    
        # E. Limit Up Trap
        prev_pct = (close[-2] - close[-3]) / close[-3] if len(close) > 2 else 0
        if (prev_pct > 0.09) and (curr_close < prev_close):
            danger_reasons.append("Limit_Up_Trap")
            
        # F. Churning
        if (curr_vol > 2.0 * curr_vol_ma5) and (abs(curr_close - prev_close)/prev_close < 0.01):
            warning_reasons.append("Churning")
            
        # G. Overheat
        if (curr_rsi > p['rsi_limit']) and (curr_close < curr_kama_fast):
            warning_reasons.append("RSI_Overheat")
            
        # Output Construction
        if danger_reasons:
            triggered = True
            signal = "STRONG_SELL"
            message = f"💀【危险|{p['type']}】{' + '.join(danger_reasons)}"
        elif warning_reasons:
            triggered = True
            signal = "SELL"
            message = f"📉【预警|{p['type']}】{' + '.join(warning_reasons)}"
        else:
            triggered = False
            signal = "SAFE"
            # Access avg_natr from parameters dataframe row 'p'
            message = f"✅【安好|{p['type']}】趋势:{curr_weekly} | 波动:{p['avg_natr']:.1f}"

except Exception as e:
    triggered = False
    signal = "WAIT"
    message = f"Script Error: {str(e)}"
    # print(f"Error: {e}")
'''

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("UPDATE rule_scripts SET code = ? WHERE name = '掉头向下监控'", (v4_code,))
    conn.commit()
    print("Database updated successfully.")
except Exception as e:
    print(f"DB Error: {e}")
finally:
    if conn:
        conn.close()
