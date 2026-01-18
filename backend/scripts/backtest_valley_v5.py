
import akshare as ak
import pandas as pd
import numpy as np
import datetime
import time

# V5 Strategy Parameters
RSI_PERIOD = 14
RSI_OVERSOLD_THRESHOLD = 35 # Optimized from 50 to 35
RSI_LOOKBACK = 5 # Check if RSI was < 35 in last 5 days
MA_SHORT = 5
MA_LONG = 60
VOL_MA = 5

def calculate_indicators(df):
    close = pd.to_numeric(df["收盘"], errors="coerce")
    open_price = pd.to_numeric(df["开盘"], errors="coerce")
    high = pd.to_numeric(df["最高"], errors="coerce")
    low = pd.to_numeric(df["最低"], errors="coerce")
    vol = pd.to_numeric(df["成交量"], errors="coerce")
    
    # MA
    ma5 = close.rolling(window=MA_SHORT).mean()
    ma60 = close.rolling(window=MA_LONG).mean()
    
    # Volume MA
    vol_ma5 = vol.rolling(window=VOL_MA).mean()
    
    # MACD (12, 26, 9)
    exp1 = close.ewm(span=12, adjust=False).mean()
    exp2 = close.ewm(span=26, adjust=False).mean()
    macd = exp1 - exp2
    signal_line = macd.ewm(span=9, adjust=False).mean()
    hist = (macd - signal_line) * 2
    
    # RSI (Wilder's Smoothing)
    delta = close.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ma_up = up.ewm(com=RSI_PERIOD-1, adjust=False).mean()
    ma_down = down.ewm(com=RSI_PERIOD-1, adjust=False).mean()
    rsi = 100 - (100 / (1 + ma_up / ma_down))
    
    # Bollinger Bands (20, 2)
    bb_mid = close.rolling(window=20).mean()
    bb_std = close.rolling(window=20).std()
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std
    
    return pd.DataFrame({
        "close": close,
        "open": open_price,
        "high": high,
        "low": low,
        "vol": vol,
        "ma5": ma5,
        "ma60": ma60,
        "vol_ma5": vol_ma5,
        "macd": macd,
        "signal": signal_line,
        "hist": hist,
        "rsi": rsi,
        "bb_lower": bb_lower,
        "date": df["日期"]
    })

def run_backtest(symbol, params=None, start_date="20230101", end_date="20241231"):
    if params is None:
        params = {
            "RSI_THRESHOLD": 35,
            "VOL_RATIO": 1.0,
            "REQUIRE_BOTH_SIGNALS": False,
            "USE_BOLLINGER": False,
            "STOP_LOSS_LOOKBACK": 1
        }

    # print(f"Testing {symbol}...")
    try:
        # Handle symbol format for akshare
        symbol_code = symbol
        if symbol.startswith(("sh", "sz", "bj")):
            symbol_code = symbol[2:]
            
        df = ak.stock_zh_a_hist(symbol=symbol_code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
        if df is None or df.empty:
            # print(f"No data for {symbol}")
            return []
            
        data = calculate_indicators(df)
        trades = []
        
        # Iterate starting from enough data points
        start_idx = 60 
        
        for i in range(start_idx, len(data)):
            curr = data.iloc[i]
            prev = data.iloc[i-1]
            
            # --- V5 Logic ---
            
            # 1. Position
            rsi_window = data["rsi"].iloc[i-RSI_LOOKBACK+1 : i+1]
            is_oversold = rsi_window.min() < params["RSI_THRESHOLD"]
            
            is_below_trend = curr["close"] < curr["ma60"]
            
            # Bollinger Check
            is_bb_support = True
            if params.get("USE_BOLLINGER", False):
                # Check if Low touched Lower Band in last 3 days
                touched = False
                for k in range(3):
                    if data.iloc[i-k]["low"] <= data.iloc[i-k]["bb_lower"]:
                        touched = True
                        break
                is_bb_support = touched

            # 2. Breakout
            is_solid_breakout = (curr["close"] > curr["ma5"]) and (curr["close"] > curr["open"])
            is_ma5_turning = curr["ma5"] >= prev["ma5"]
            
            # 3. Volume
            is_volume_confirmed = curr["vol"] > (prev["vol_ma5"] * params["VOL_RATIO"])
            
            # 4. Indicators Confirmation
            macd_golden_cross = (prev["macd"] < prev["signal"]) and (curr["macd"] > curr["signal"])
            rsi_rebound = (prev["rsi"] < 40) and (curr["rsi"] > prev["rsi"]) 
            
            if params["REQUIRE_BOTH_SIGNALS"]:
                is_indicator_confirmed = macd_golden_cross and rsi_rebound
            else:
                is_indicator_confirmed = macd_golden_cross or rsi_rebound
            
            if is_oversold and is_below_trend and is_solid_breakout and is_ma5_turning and is_volume_confirmed and is_indicator_confirmed and is_bb_support:
                # Triggered!
                entry_price = curr["close"]
                
                # Stop Loss: Min of last N days low
                sl_lookback = params.get("STOP_LOSS_LOOKBACK", 1)
                stop_loss = data["low"].iloc[i-sl_lookback+1 : i+1].min()
                
                entry_date = curr["date"]
                
                # Check outcome in next 10 days
                outcome = "HOLD"
                pnl = 0.0
                days_held = 0
                
                for j in range(1, 11):
                    if i + j >= len(data):
                        break
                    
                    future = data.iloc[i+j]
                    days_held = j
                    
                    # Check Stop Loss
                    if future["low"] < stop_loss:
                        outcome = "LOSS"
                        pnl = (stop_loss - entry_price) / entry_price
                        break
                    
                    # Check Take Profit
                    if (future["high"] - entry_price) / entry_price > 0.10:
                        outcome = "WIN"
                        pnl = 0.10 
                        break
                        
                    # End of period
                    if j == 10:
                        outcome = "TIME_EXIT"
                        pnl = (future["close"] - entry_price) / entry_price
                
                trades.append({
                    "symbol": symbol,
                    "date": entry_date,
                    "entry": entry_price,
                    "outcome": outcome,
                    "pnl": pnl,
                    "config": str(params)
                })
                
        return trades

    except Exception as e:
        print(f"Error testing {symbol}: {e}")
        return []

def main():
    symbols = [
        "600519", "300750", "601127", "002415", 
        "600030", "002594", "601919", "000001"
    ]
    
    configs = [
        {"RSI_THRESHOLD": 35, "VOL_RATIO": 1.0, "REQUIRE_BOTH_SIGNALS": False, "USE_BOLLINGER": False, "STOP_LOSS_LOOKBACK": 1}, # Baseline
        {"RSI_THRESHOLD": 30, "VOL_RATIO": 1.0, "REQUIRE_BOTH_SIGNALS": False, "USE_BOLLINGER": False, "STOP_LOSS_LOOKBACK": 1}, # Strict RSI
        {"RSI_THRESHOLD": 35, "VOL_RATIO": 1.0, "REQUIRE_BOTH_SIGNALS": False, "USE_BOLLINGER": True, "STOP_LOSS_LOOKBACK": 1},  # Bollinger
        {"RSI_THRESHOLD": 35, "VOL_RATIO": 1.0, "REQUIRE_BOTH_SIGNALS": False, "USE_BOLLINGER": False, "STOP_LOSS_LOOKBACK": 3}, # Loose Stop
        {"RSI_THRESHOLD": 30, "VOL_RATIO": 1.0, "REQUIRE_BOTH_SIGNALS": False, "USE_BOLLINGER": True, "STOP_LOSS_LOOKBACK": 3},  # Combo
    ]
    
    print("Starting Grid Search Optimization...")
    
    for config in configs:
        all_trades = []
        print(f"\nTesting Config: {config}")
        for sym in symbols:
            trades = run_backtest(sym, params=config)
            all_trades.extend(trades)
            # time.sleep(0.1)
            
        if not all_trades:
            print("  No trades.")
            continue
            
        df = pd.DataFrame(all_trades)
        win_rate = len(df[df['pnl'] > 0]) / len(df)
        avg_pnl = df['pnl'].mean()
        trade_count = len(df)
        
        print(f"  Trades: {trade_count}")
        print(f"  Win Rate: {win_rate:.2%}")
        print(f"  Avg PnL: {avg_pnl:.2%}")

if __name__ == "__main__":
    main()
