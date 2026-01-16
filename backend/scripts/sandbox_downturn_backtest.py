
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
    def __init__(self, symbol_code):
        self.symbol = symbol_code
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

    def calculate_chop(self, df, window=14):
        """Calculate Choppiness Index (CHOP)"""
        try:
            if len(df) < window + 1: return 50.0
            
            high = df["high"]
            low = df["low"]
            close = df["close"]
            
            tr1 = talib.TRANGE(high.values, low.values, close.values)
            tr1_s = pd.Series(tr1, index=df.index)
            
            sum_tr = tr1_s.rolling(window=window).sum()
            max_hi = high.rolling(window=window).max()
            min_lo = low.rolling(window=window).min()
            
            range_hl = max_hi - min_lo
            range_hl = range_hl.replace(0, np.nan)
            
            chop = 100 * np.log10(sum_tr / range_hl) / np.log10(window)
            val = chop.iloc[-1]
            return 50.0 if np.isnan(val) else val
        except:
            return 50.0

    def process(self, history_df, index_df=None, use_chop=True, override_params=None):
        """
        输入: 截止到当日的历史数据 DataFrame
        输出: (Signal, Reasons)
        """
        if len(history_df) < 60:
            return "WAIT", []
        
        # --- Sector Adaptive Config ---
        SECTOR_MAP = {
            # High Beta
            "sz300750": "HIGH_BETA", # 宁德时代
            "sz300059": "HIGH_BETA", # 东方财富
            "sz300308": "HIGH_BETA", # 中际旭创
            "sh601138": "HIGH_BETA", # 工业富联
            "sz000063": "HIGH_BETA", # 中兴通讯
            "sz002475": "HIGH_BETA", # 立讯精密
            "sz002594": "HIGH_BETA", # 比亚迪
            "sh600104": "HIGH_BETA", # 上汽集团
            
            # Low Beta / Defensive
            "sh600900": "LOW_BETA",  # 长江电力
            "sh600036": "LOW_BETA",  # 招商银行
            "sh601398": "LOW_BETA",  # 工商银行
            "sh601857": "LOW_BETA",  # 中国石油
            "sh601318": "LOW_BETA",  # 中国平安
            
            # Stable Growth
            "sh600519": "STABLE",    # 贵州茅台
            "sh600887": "STABLE",    # 伊利股份
            "sh600030": "STABLE",    # 中信证券
            "sh601899": "STABLE",    # 紫金矿业
            "sh600309": "STABLE",    # 万华化学
            "sh600031": "STABLE",    # 三一重工
        }
        
        SECTOR_PARAMS = {
            "HIGH_BETA": {
                "chop_threshold": 55.0, 
                "vol_multiplier": 2.0, 
                "kama_slow_period": 25,
            },
            "LOW_BETA": {
                "chop_threshold": 65.0, 
                "vol_multiplier": 1.2, 
                "kama_slow_period": 20,
            },
            "STABLE": {
                "chop_threshold": 61.8, 
                "vol_multiplier": 1.5, 
                "kama_slow_period": 20,
            },
            "DEFAULT": {
                "chop_threshold": 61.8,
                "vol_multiplier": 1.5,
                "kama_slow_period": 20,
            }
        }
        
        # Determine Sector
        if override_params:
            params = override_params
        else:
            full_symbol = self.symbol if self.symbol.startswith(("sh", "sz")) else ("sh" if self.symbol.startswith("6") else "sz") + self.symbol
            sector_type = SECTOR_MAP.get(full_symbol, "DEFAULT")
            params = SECTOR_PARAMS.get(sector_type, SECTOR_PARAMS["DEFAULT"])

        # --- Data Preparation ---
        close = history_df["close"].values
        high = history_df["high"].values
        low = history_df["low"].values
        open_p = history_df["open"].values
        volume = history_df["volume"].values.astype(float)
        
        # 1. Adaptive Moving Averages (KAMA)
        kama_fast = talib.KAMA(close, timeperiod=10)
        kama_slow = talib.KAMA(close, timeperiod=params["kama_slow_period"]) 
        
        # 2. Basic Indicators
        ma5 = talib.SMA(close, timeperiod=5)
        ma20 = talib.SMA(close, timeperiod=20)
        atr = talib.ATR(high, low, close, timeperiod=14)
        rsi = talib.RSI(close, timeperiod=14)
        
        # 3. Behavioral Alpha Factors
        vol_ma5 = talib.SMA(volume, timeperiod=5)
        
        hl_range = (high - low)
        hl_range[hl_range == 0] = 0.01
        
        body_strength = (close - open_p) / hl_range
        vol_ratio = np.zeros_like(volume)
        valid_idx = np.where(vol_ma5 > 0)
        vol_ratio[valid_idx] = volume[valid_idx] / vol_ma5[valid_idx]
        
        alpha_vol_body = vol_ratio * body_strength
        alpha_rsi_rev = (100 - rsi) * ((high - close) / hl_range)
        
        # 4. Weekly Trend
        weekly_trend = self.get_weekly_trend(history_df)
        
        # 5. Market Context
        is_market_weak = False
        if index_df is not None and not index_df.empty:
            idx_close = index_df["close"].values
            idx_ma20 = talib.SMA(idx_close, timeperiod=20)
            if len(idx_close) > 0 and idx_close[-1] < idx_ma20[-1]:
                is_market_weak = True

        # 6. POC & CHOP
        poc_price = self.calculate_poc(history_df, window=20)
        chop_val = self.calculate_chop(history_df, window=14)
        is_choppy = chop_val > params["chop_threshold"]

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
        
        curr_alpha_rsi = alpha_rsi_rev[-1]
        
        bias20 = (curr_price - curr_ma20) / curr_ma20 if curr_ma20 != 0 else 0
        
        # --- Logic Decision Tree ---
        
        danger_reasons = []
        warning_reasons = []
        info_reasons = []
        
        # Logic 1: STRONG_SELL
        
        # A. Break KAMA Slow + Weekly Trend Down
        is_cross_under_kama = (curr_price < curr_kama_slow) and (prev_price >= kama_slow[-2])
        if is_cross_under_kama and (weekly_trend == "DOWN"):
             if use_chop and is_choppy:
                 if curr_vol > params["vol_multiplier"] * curr_vol_ma5:
                     danger_reasons.append(f"震荡区放量(>{params['vol_multiplier']}x)跌破KAMA")
             else:
                 danger_reasons.append("跌破KAMA慢线+周线向下")
             
        # B. Limit Up Trap
        prev_pct = (close[-2] - close[-3]) / close[-3] if len(close) > 2 else 0
        is_prev_limit_up = (prev_pct > 0.095)
        if is_prev_limit_up:
            if (curr_price < close[-2]) and (curr_vol > 1.2 * curr_vol_ma5):
                 danger_reasons.append("涨停次日放量杀跌(诱多)")
                 
        # C. Market Width Collapse
        is_cross_under_ma20 = (curr_price < curr_ma20) and (prev_price >= ma20[-2])
        if is_market_weak and is_cross_under_ma20:
             if use_chop and is_choppy:
                 pass # Skip in choppy market unless volume confirms (simplified: just skip)
             else:
                 danger_reasons.append("弱势市场跌破生命线")
             
        # Logic 2: SELL
        
        # A. Overheat
        is_overheat = (curr_rsi > 75) or (bias20 > 0.15)
        if is_overheat:
            stop_price = curr_kama_fast - (0.5 * curr_atr)
            if curr_price < stop_price:
                 warning_reasons.append(f"过热期跌破KAMA快线(止盈)")
                 
        # B. Exhaustion
        if (curr_alpha_rsi > 50) and (curr_rsi > 70):
             warning_reasons.append("Alpha因子示警:顶部衰竭")

        # Logic 3: WAIT
        is_drop = (curr_price < curr_kama_fast) or (curr_price < ma5[-1])
        is_supported = (curr_price > poc_price) and (curr_price > curr_kama_slow)
        is_shrink_vol = (curr_vol < 1.0 * curr_vol_ma5)
        
        if is_drop and is_supported and is_shrink_vol and not danger_reasons and not warning_reasons:
            return "WAIT", [f"缩量回踩POC({poc_price:.2f})支撑有效"]

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
        self.strategy = DownturnStrategy(symbol_code)
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

    def run(self, use_chop=True, override_params=None):
        if self.df is None: return
        
        self.results = [] # Clear previous results
        total_days = len(self.df)
        if override_params is None:
             print(f"Running backtest (CHOP={use_chop}) on {total_days} days...")
        
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
            
            signal, reasons = self.strategy.process(history, curr_index_df, use_chop=use_chop, override_params=override_params)
            
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
    
    # 15 只全市场代表性个股
    targets = [
        # --- High Beta ---
        {"code": "sz300750", "name": "宁德时代 (新能)"},
        {"code": "sz300059", "name": "东方财富 (券商)"},
        {"code": "sz300308", "name": "中际旭创 (AI)"},
        {"code": "sh601138", "name": "工业富联 (算力)"},
        {"code": "sz000063", "name": "中兴通讯 (通信)"},
        {"code": "sz002594", "name": "比亚迪 (汽车)"},
        
        # --- Stable Growth ---
        {"code": "sh600519", "name": "贵州茅台 (白酒)"},
        {"code": "sh600887", "name": "伊利股份 (乳业)"},
        {"code": "sh601899", "name": "紫金矿业 (有色)"},
        {"code": "sh600309", "name": "万华化学 (化工)"},
        {"code": "sh600030", "name": "中信证券 (非银)"},

        # --- Low Beta / Defensive ---
        {"code": "sh600900", "name": "长江电力 (水电)"},
        {"code": "sh600036", "name": "招商银行 (银行)"},
        {"code": "sh601398", "name": "工商银行 (大行)"},
        {"code": "sh601857", "name": "中国石油 (能源)"},
    ]

    print(f"=== 全市场自适应策略回测 (Total: {len(targets)}) ===")
    print(f"Time Range: {start_dt} to {end_dt}")
    
    summary = []

    for t in targets:
        symbol = t["code"]
        name = t["name"]
        print(f"\n>>> Testing {name} [{symbol}] <<<")
        
        bt = Backtester(symbol, start_dt, end_dt)
        if bt.fetch_data():
            # Run Optimized Strategy with Adaptive Params
            bt.run(use_chop=True)
            
            # Collect Stats
            strong_sells = [r for r in bt.results if r['signal'] == 'STRONG_SELL']
            count = len(strong_sells)
            
            win_rate = 0
            avg_ret_5d = 0
            avg_ret_20d = 0
            
            if count > 0:
                wins = [r for r in strong_sells if r['ret_5d'] is not None and r['ret_5d'] < 0]
                win_rate = len(wins) / count
                
                rets_5d = [r['ret_5d'] for r in strong_sells if r['ret_5d'] is not None]
                if rets_5d: avg_ret_5d = sum(rets_5d) / len(rets_5d)
                
                rets_20d = [r['ret_20d'] for r in strong_sells if r['ret_20d'] is not None]
                if rets_20d: avg_ret_20d = sum(rets_20d) / len(rets_20d)
            
            summary.append({
                "name": name,
                "count": count,
                "win_rate": win_rate,
                "ret_5d": avg_ret_5d,
                "ret_20d": avg_ret_20d
            })
            
            # Simplified report output to save space
            if count > 0:
                 print(f"   Signals: {count}, WR(5d): {win_rate*100:.1f}%, AvgRet(20d): {avg_ret_20d*100:.2f}%")
        else:
            print(f"Failed to fetch data for {name}")

    print("\n\n" + "="*80)
    print("ALL STOCKS BACKTEST SUMMARY (Sector Adaptive Strategy)")
    print("="*80)
    print(f"{'Stock':<20} | {'Count':<5} | {'WinRate(5d)':<12} | {'AvgRet(5d)':<12} | {'AvgRet(20d)':<12}")
    print("-" * 95)
    for s in summary:
        print(f"{s['name']:<20} | {s['count']:<5} | {s['win_rate']*100:5.1f}%      | {s['ret_5d']*100:6.2f}%      | {s['ret_20d']*100:6.2f}%")
    print("="*80)

