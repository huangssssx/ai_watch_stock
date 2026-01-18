import akshare as ak
import pandas as pd
import numpy as np
import datetime
import sys
import os
import talib

# Add current directory to path so we can import the strategy
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from downturn_strategy_v4 import run_backtest_strategy

# --- OUT-OF-SAMPLE TEST SET ---
STOCKS = {
    # High Beta (Tech/Growth)
    "002371": "BeiFangHuaChuang (Semi)",
    "002594": "BYD (Auto/Battery)",
    "601138": "GongYeFuLian (AI Server)",
    "002230": "KeDaXunFei (AI)",
    
    # Low Beta (Value/Div)
    "601088": "ZhongGuoShenHua (Coal)",
    "600900": "ChangJiangDianLi (Hydro)",
    "600036": "ZhaoShangYinHang (Bank)",
    
    # Cyclical/Others
    "601899": "ZiJinKuangYe (Mining)",
    "600276": "HengRuiYiYao (Pharma)",
    "600048": "BaoLiFaZhan (Real Estate)"
}

START_DATE = "20230101"
END_DATE = "20250101"

def fetch_data(symbol):
    print(f"Fetching data for {symbol}...")
    try:
        df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=START_DATE, end_date=END_DATE, adjust="qfq")
        if df is None or df.empty:
            return None
        df = df.rename(columns={
            "日期": "date", "开盘": "open", "收盘": "close", 
            "最高": "high", "最低": "low", "成交量": "volume"
        })
        return df
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None

def evaluate_signals(results_df, df_raw):
    """
    Evaluate the effectiveness of signals.
    Dynamic threshold: 1.5 * ATR(14) at signal time.
    """
    # Calculate ATR for evaluation
    high = df_raw["high"].values
    low = df_raw["low"].values
    close = df_raw["close"].values
    atr = talib.ATR(high, low, close, timeperiod=14)
    atr_s = pd.Series(atr, index=results_df.index)
    
    signals = results_df[results_df['signal'].isin(['STRONG_SELL', 'SELL'])].copy()
    
    stats = {
        "STRONG_SELL": {"total": 0, "good": 0, "bad": 0, "neutral": 0},
        "SELL": {"total": 0, "good": 0, "bad": 0, "neutral": 0}
    }
    
    if len(signals) == 0:
        return stats
        
    prices = results_df['price']
    
    last_signal_idx = -999
    
    for date, row in signals.iterrows():
        sig_type = row['signal']
        
        try:
            loc = results_df.index.get_loc(date)
            
            # Cooldown: Ignore consecutive signals within 5 days
            if loc - last_signal_idx < 5:
                continue
                
            last_signal_idx = loc
            stats[sig_type]["total"] += 1
            
            if loc + 5 >= len(results_df):
                continue
            
            # Dynamic Target
            curr_atr = atr_s.loc[date]
            target_drop = 1.5 * curr_atr
            target_drop_pct = target_drop / row['price']
            
            # Limit minimum drop to 2% (to avoid noise) and max to 5%
            target_drop_pct = max(0.02, min(target_drop_pct, 0.05))
            
            future_prices = prices.iloc[loc+1 : loc+6]
            entry_price = row['price']
            
            min_price = future_prices.min()
            max_price = future_prices.max()
            
            max_drawdown = (min_price - entry_price) / entry_price
            max_gain = (max_price - entry_price) / entry_price
            
            # Good: Dropped more than target
            if max_drawdown < -target_drop_pct:
                stats[sig_type]["good"] += 1
            # Bad: Rose more than target
            elif max_gain > target_drop_pct:
                stats[sig_type]["bad"] += 1
            else:
                stats[sig_type]["neutral"] += 1
                
        except Exception as e:
            continue
            
    return stats

def main():
    print("Starting Out-of-Sample Backtest (Random Selection)...")
    summary = []
    
    for symbol, name in STOCKS.items():
        df = fetch_data(symbol)
        if df is None or df.empty:
            print(f"Skipping {name} (No Data)")
            continue
            
        print(f"Running strategy for {name} ({len(df)} bars)...")
        results = run_backtest_strategy(df)
        
        metrics = evaluate_signals(results, df)
        
        # Calculate precision
        ss_total = metrics["STRONG_SELL"]["total"]
        ss_prec = metrics["STRONG_SELL"]["good"] / ss_total if ss_total > 0 else 0
        
        s_total = metrics["SELL"]["total"]
        s_prec = metrics["SELL"]["good"] / s_total if s_total > 0 else 0
        
        print(f"--- {name} ---")
        print(f"STRONG_SELL: {ss_total} | Good: {metrics['STRONG_SELL']['good']} ({ss_prec:.1%})")
        
        summary.append({
            "name": name,
            "SS_Total": ss_total,
            "SS_Good": metrics['STRONG_SELL']['good'],
            "SS_Prec": ss_prec
        })
        
    print("\n=== FINAL GENERALIZATION REPORT ===")
    summary_df = pd.DataFrame(summary)
    print(summary_df)
    
    # Calculate Average Precision
    if not summary_df.empty:
        avg_prec = summary_df["SS_Prec"].mean()
        print(f"\nAverage Precision across all sectors: {avg_prec:.1%}")

if __name__ == "__main__":
    main()
