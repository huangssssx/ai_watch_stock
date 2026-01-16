
import akshare as ak
import pandas as pd
import numpy as np
import talib
import datetime
import time

# ==============================================================================
# 策略核心逻辑 (封装版 v3.0 - A股深度优化)
# ==============================================================================
class DownturnStrategy:
    def __init__(self):
        self.triggered = False
        self.signal = "WAIT"
        self.reasons = []

    def calculate_poc(self, df, window=20, bins=20):
        """Approximate Volume Point of Control (POC) for last 'window' days"""
        subset = df.iloc[-window:]
        if subset.empty: return 0
        
        # Create price bins based on High/Low range
        price_min = subset["low"].min()
        price_max = subset["high"].max()
        if price_min == price_max: return price_min
        
        # Histogram of Volume by Price
        # We attribute volume to the mean of O/C/H/L or just Close
        # A simple approx: use (High+Low+Close)/3 as the 'price' for that day's volume
        typical_price = (subset["high"] + subset["low"] + subset["close"]) / 3
        
        # Bin the volume
        hist, bin_edges = np.histogram(typical_price, bins=bins, range=(price_min, price_max), weights=subset["volume"])
        
        # Find bin with max volume
        max_idx = np.argmax(hist)
        poc_price = (bin_edges[max_idx] + bin_edges[max_idx+1]) / 2
        return poc_price

    def get_weekly_trend(self, df):
        """Get Weekly MACD Trend"""
        # Resample to weekly
        # Ensure 'date' is datetime
        df_w = df.copy()
        df_w["date"] = pd.to_datetime(df_w["date"])
        df_w.set_index("date", inplace=True)
        
        # Resample: Take last close, max high, min low
        weekly = df_w.resample("W").agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum"
        }).dropna()
        
        if len(weekly) < 26:
            return "NEUTRAL"
            
        close_w = weekly["close"].values
        macd, signal, hist = talib.MACD(close_w, fastperiod=12, slowperiod=26, signalperiod=9)
        
        # Trend is DOWN if MACD < Signal (Dead Cross State)
        # Histogram slope is good but noisy. Let's use position relative to signal line.
        if macd[-1] < signal[-1]:
            return "DOWN"
        elif macd[-1] > signal[-1]:
            return "UP"
        else:
            return "NEUTRAL"

    def process(self, history_df, index_df=None):
        """
        输入: 截止到当日的历史数据 DataFrame
        输出: (Signal, Reasons)
        """
        if len(history_df) < 60:
            return "WAIT", []

        # --- Data Preparation ---
        close = history_df["close"].values
        high = history_df["high"].values
        low = history_df["low"].values
        open_p = history_df["open"].values
        volume = history_df["volume"].values.astype(float)
        
        # 1. Adaptive Moving Averages (KAMA)
        # KAMA defaults: period=30 (ER), fast=2, slow=30 in TA-Lib? 
        # We'll use KAMA(10) as Fast and KAMA(20) as Slow (Adaptive replacement for MA20)
        kama_fast = talib.KAMA(close, timeperiod=10)
        kama_slow = talib.KAMA(close, timeperiod=20) 
        
        # 2. Basic Indicators
        ma5 = talib.SMA(close, timeperiod=5)
        ma20 = talib.SMA(close, timeperiod=20)
        atr = talib.ATR(high, low, close, timeperiod=14)
        rsi = talib.RSI(close, timeperiod=14)
        
        # 3. Behavioral Alpha Factors
        # Alpha 1: Volume Spike * Body Intensity
        # Vol / MA_Vol_5 * (Close - Open) / (High - Low)
        vol_ma5 = talib.SMA(volume, timeperiod=5)
        
        # Avoid division by zero
        hl_range = (high - low)
        hl_range[hl_range == 0] = 0.01
        
        body_strength = (close - open_p) / hl_range
        vol_ratio = np.zeros_like(volume)
        # Handle nan in vol_ma5
        valid_idx = np.where(vol_ma5 > 0)
        vol_ratio[valid_idx] = volume[valid_idx] / vol_ma5[valid_idx]
        
        alpha_vol_body = vol_ratio * body_strength
        
        # Alpha 2: RSI Reversal * Low Close
        # (100 - RSI) * (High - Close) / (High - Low)
        alpha_rsi_rev = (100 - rsi) * ((high - close) / hl_range)
        
        # 4. Weekly Trend (Triple Screen - Screen 1)
        weekly_trend = self.get_weekly_trend(history_df)
        
        # 5. Market Context (Index Trend)
        # Assuming index_df is passed and aligned or we just check last known state
        is_market_weak = False
        if index_df is not None and not index_df.empty:
            # Simple check: Index < Index MA20
            idx_close = index_df["close"].values
            idx_ma20 = talib.SMA(idx_close, timeperiod=20)
            if len(idx_close) > 0 and idx_close[-1] < idx_ma20[-1]:
                is_market_weak = True

        # 6. POC (Volume Profile)
        poc_price = self.calculate_poc(history_df, window=20)

        # --- Current Values ---
        curr_price = close[-1]
        prev_price = close[-2]
        curr_kama_fast = kama_fast[-1]
        curr_kama_slow = kama_slow[-1]
        curr_ma20 = ma20[-1]
        curr_atr = atr[-1]
        curr_rsi = rsi[-1]
        curr_vol = volume[-1]
        curr_vol_ma5 = vol_ma5[-1]
        
        curr_alpha_vb = alpha_vol_body[-1] # High +ve means Strong Up, High -ve means Strong Down
        curr_alpha_rsi = alpha_rsi_rev[-1] # High means Reversal likely
        
        bias20 = (curr_price - curr_ma20) / curr_ma20 if curr_ma20 != 0 else 0
        
        # --- Logic Decision Tree (A-share Optimized) ---
        
        danger_reasons = []
        warning_reasons = []
        info_reasons = []
        
        # --------------------------------------------------------
        # Logic 1: STRONG_SELL (Trend Reversal)
        # --------------------------------------------------------
        # Condition A: Break KAMA Slow + Weekly Trend Down
        # The "Double Confirmation" - Event Driven (CrossUnder)
        is_cross_under_kama = (curr_price < curr_kama_slow) and (prev_price >= kama_slow[-2])
        if is_cross_under_kama and (weekly_trend == "DOWN"):
             danger_reasons.append("跌破KAMA慢线+周线向下")
             
        # Condition B: Limit Up Trap (Exploding Board)
        # Check if Prev Close was Limit Up (approx > 9.5% gain)
        prev_pct = (close[-2] - close[-3]) / close[-3] if len(close) > 2 else 0
        is_prev_limit_up = (prev_pct > 0.095)
        # Today: Low Close (Green), Heavy Volume
        if is_prev_limit_up:
            if (curr_price < close[-2]) and (curr_vol > 1.2 * curr_vol_ma5):
                 danger_reasons.append("涨停次日放量杀跌(诱多)")
                 
        # Condition C: Market Width Collapse (Index Weak + Stock Break MA20)
        # If market is weak, strict MA20 break is fatal
        # Event Driven (CrossUnder)
        is_cross_under_ma20 = (curr_price < curr_ma20) and (prev_price >= ma20[-2])
        if is_market_weak and is_cross_under_ma20:
             danger_reasons.append("弱势市场跌破生命线")
             
        # --------------------------------------------------------
        # Logic 2: SELL (Profit Protection)
        # --------------------------------------------------------
        # Condition A: Extreme Overheat + Alpha Reversal
        # RSI > 75 AND Alpha RSI Rev High (> 20 approx?)
        # Or just Price < KAMA_Fast + ATR buffer in Overheat
        is_overheat = (curr_rsi > 75) or (bias20 > 0.15)
        
        if is_overheat:
            stop_price = curr_kama_fast - (0.5 * curr_atr) # Tighter stop in overheat
            if curr_price < stop_price:
                 warning_reasons.append(f"过热期跌破KAMA快线(止盈)")
                 
        # Condition B: High Volume Stagnation (Churning)
        # Alpha Vol Body: High Volume but Small Body (Abs value low)
        # This alpha formula: Vol/MA * BodyLen. 
        # If Vol is high (e.g. 2x), but Price change is small, BodyLen is small. 
        # Wait, the formula provided was Vol/MA * (Close-Open)/(High-Low). 
        # Churning means High-Low is big (maybe) but Close-Open is small.
        # Let's stick to the report's "Vol Spike * Body Intensity". 
        # If this value is near 0 BUT Vol is High -> Indecision? 
        # Actually, let's use the report's "Alpha Rev RSI" for exhaustion.
        if (curr_alpha_rsi > 50) and (curr_rsi > 70): # Heuristic threshold
             warning_reasons.append("Alpha因子示警:顶部衰竭")

        # --------------------------------------------------------
        # Logic 3: WAIT (Technical Correction)
        # --------------------------------------------------------
        # Drop below MA5/KAMA_Fast, BUT above POC and Low Volume
        is_drop = (curr_price < curr_kama_fast) or (curr_price < ma5[-1])
        is_supported = (curr_price > poc_price) and (curr_price > curr_kama_slow)
        is_shrink_vol = (curr_vol < 1.0 * curr_vol_ma5)
        
        if is_drop and is_supported and is_shrink_vol and not danger_reasons and not warning_reasons:
            return "WAIT", [f"缩量回踩POC({poc_price:.2f})支撑有效"]

        # --------------------------------------------------------
        # Final Output
        # --------------------------------------------------------
        if danger_reasons:
            return "STRONG_SELL", danger_reasons
        elif warning_reasons:
            return "SELL", warning_reasons
        elif info_reasons:
            return "OBSERVE", info_reasons
        else:
            return "SAFE", []

# ==============================================================================
# 回测引擎 (Updated)
# ==============================================================================
class Backtester:
    def __init__(self, symbol_code, start_date, end_date):
        self.symbol = symbol_code
        self.start_date = start_date
        self.end_date = end_date
        self.strategy = DownturnStrategy()
        self.df = None
        self.index_df = None
        self.results = []

    def fetch_data(self):
        print(f"Fetching {self.symbol}...")
        try:
            # Handle symbol format
            code = self.symbol
            if code.startswith(("sh", "sz", "bj")):
                code = code[2:]
                
            df = ak.stock_zh_a_hist(symbol=code, period="daily", 
                                    start_date=self.start_date, end_date=self.end_date, adjust="qfq")
            if df is None or df.empty:
                print(f"No data found for {code}.")
                return False
            
            df = df.rename(columns={
                "日期": "date", "开盘": "open", "收盘": "close", 
                "最高": "high", "最低": "low", "成交量": "volume"
            })
            # Ensure date is datetime
            df["date"] = pd.to_datetime(df["date"])
            
            cols = ["open", "close", "high", "low", "volume"]
            for col in cols:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            
            self.df = df
            
            # Fetch Index Data (CSI 300 Proxy)
            try:
                print("Fetching Index (sh000300) context...")
                index_df = ak.stock_zh_index_daily(symbol="sh000300")
                # Ensure date is datetime
                index_df["date"] = pd.to_datetime(index_df["date"])
                
                # Filter by date range (ensure types match for comparison)
                s_date = pd.to_datetime(self.start_date)
                e_date = pd.to_datetime(self.end_date)
                
                self.index_df = index_df[(index_df["date"] >= s_date) & (index_df["date"] <= e_date)].reset_index(drop=True)
            except:
                print("Index fetch failed, proceeding without market context.")
                self.index_df = pd.DataFrame()
                
            return True
        except Exception as e:
            print(f"Error: {e}")
            return False

    def run(self):
        if self.df is None: return
        
        total_days = len(self.df)
        print(f"Running backtest on {total_days} days...")
        
        # Start from day 60
        for i in range(60, total_days):
            history = self.df.iloc[:i+1]
            current_date = self.df.iloc[i]["date"]
            
            # Slice Index DF to current date
            curr_index_df = None
            if self.index_df is not None and not self.index_df.empty:
                # Assuming dates align or close enough. 
                # Find index row with same date
                mask = self.index_df["date"] <= current_date
                curr_index_df = self.index_df[mask]
            
            signal, reasons = self.strategy.process(history, curr_index_df)
            
            if signal in ["STRONG_SELL", "SELL"]:
                current_price = self.df.iloc[i]["close"]
                future_days = [5, 10, 20]
                returns = {}
                for d in future_days:
                    if i + d < total_days:
                        future_price = self.df.iloc[i+d]["close"]
                        ret = (future_price - current_price) / current_price
                        returns[f"day_{d}"] = ret
                    else:
                        returns[f"day_{d}"] = None
                
                self.results.append({
                    "date": current_date,
                    "price": current_price,
                    "signal": signal,
                    "reasons": " | ".join(reasons),
                    "ret_5d": returns.get("day_5"),
                    "ret_10d": returns.get("day_10"),
                    "ret_20d": returns.get("day_20")
                })

    def report(self):
        print(f"\n=== Report for {self.symbol} ===")
        if not self.results:
            print("No signals triggered.")
            return

        df_res = pd.DataFrame(self.results)
        
        for sig_type in ["STRONG_SELL", "SELL"]:
            subset = df_res[df_res["signal"] == sig_type]
            if subset.empty: continue
            
            print(f"\n--- {sig_type} Analysis ({len(subset)}) ---")
            
            wins_5d = subset[subset["ret_5d"] < 0]
            win_rate_5d = len(wins_5d) / len(subset) if len(subset) > 0 else 0
            
            print(f"5-Day Win Rate (Price Drop): {win_rate_5d*100:.1f}%")
            if len(subset) > 0:
                print(f"Avg 5-Day Return: {subset['ret_5d'].mean()*100:.2f}%")
                print(f"Avg 20-Day Return: {subset['ret_20d'].mean()*100:.2f}%")
            
            print("Top Successful Calls (Biggest Drops):")
            print(subset.sort_values("ret_20d").head(3)[["date", "reasons", "ret_20d"]])
            
            print("False Alarms (Price Rose > 3%):")
            print(subset[subset["ret_5d"] > 0.03][["date", "reasons", "ret_5d"]].head(3))

# ==============================================================================
# 执行入口
# ==============================================================================
if __name__ == "__main__":
    # Settings
    end_dt = datetime.datetime.now().strftime("%Y%m%d")
    start_dt = (datetime.datetime.now() - datetime.timedelta(days=730)).strftime("%Y%m%d") # 2 years
    
    # 1. Index (CSI 300)
    print("\n>>> Testing Index (CSI 300) <<<")
    bt1 = Backtester("000300", start_dt, end_dt) # Index symbol might need adjustment for akshare
    # akshare index usually sh000300
    bt1.symbol = "000300" 
    # NOTE: ak.stock_zh_a_hist is for individual stocks. 
    # For index, we use stock_zh_index_daily or similar. 
    # To keep it simple and consistent, let's use a proxy ETF for index: 510300 (Huatai-PineBridge CSI 300 ETF)
    bt1 = Backtester("sh510300", start_dt, end_dt) 
    if bt1.fetch_data():
        bt1.run()
        bt1.report()
        
    # 2. Volatile Stock (CATL 300750)
    print("\n>>> Testing Volatile (CATL) <<<")
    bt2 = Backtester("sz300750", start_dt, end_dt)
    if bt2.fetch_data():
        bt2.run()
        bt2.report()
        
    # 3. Stable Stock (Yangtze Power 600900)
    print("\n>>> Testing Stable (Yangtze Power) <<<")
    bt3 = Backtester("sh600900", start_dt, end_dt)
    if bt3.fetch_data():
        bt3.run()
        bt3.report()
