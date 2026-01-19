import akshare as ak
import pandas as pd
import numpy as np
import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
BACKTEST_DAYS = 30 
SLIPPAGE_ENTRY = 0.01
SECTOR_POOL = ["半导体", "软件开发", "汽车整车", "证券", "酿酒行业", "光伏设备", "消费电子", "通信设备"]
MAX_WORKERS = 4

# --- Helper Functions (Same as V5) ---

def get_trade_dates(days=60):
    try:
        tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()
        dates = tool_trade_date_hist_sina_df['trade_date'].tolist()
        today = datetime.date.today()
        valid_dates = [str(d) for d in dates if d <= today]
        return valid_dates[-days:]
    except Exception as e:
        print(f"Error fetching trade dates: {e}")
        return []

def fetch_sector_components(sector):
    try:
        cons = ak.stock_board_industry_cons_em(symbol=sector)
        return cons[['代码', '名称']].values.tolist() if not cons.empty else []
    except:
        return []

def get_daily_data(symbol, start_date, end_date):
    try:
        df = ak.stock_zh_a_hist(symbol=symbol, start_date=start_date, end_date=end_date, adjust="qfq")
        if df.empty: return None
        df['日期'] = df['日期'].astype(str)
        return df
    except:
        return None

def get_minute_snapshot(symbol, date_str):
    try:
        start_dt = f"{date_str} 09:30:00"
        end_dt = f"{date_str} 15:00:00"
        df = ak.stock_zh_a_hist_min_em(symbol=symbol, start_date=start_dt, end_date=end_dt, period='1', adjust='qfq')
        if df is None or df.empty: return None
        
        target_time = f"{date_str} 10:00:00"
        mask_morning = (df['时间'] <= target_time)
        morning_df = df[mask_morning]
        if morning_df.empty: return None
        
        acc_vol = morning_df['成交量'].sum()
        snapshot = morning_df.iloc[-1]
        vwap = (morning_df['收盘'] * morning_df['成交量']).sum() / acc_vol if acc_vol > 0 else snapshot['收盘']
        
        return {
            "price_1000": snapshot['收盘'],
            "open_0930": morning_df.iloc[0]['开盘'],
            "vwap_1000": vwap,
            "acc_vol_1000": acc_vol,
            "time": snapshot['时间']
        }
    except:
        return None

def get_rpp(close, high_60, low_60):
    if high_60 == low_60: return 0.5
    return (close - low_60) / (high_60 - low_60)

# --- Core Logic ---

def run_comparison():
    print("⚔️  Strategy Comparison: V1 (Baseline) vs V2 (Optimized) ⚔️")
    dates = get_trade_dates(BACKTEST_DAYS + 70)
    if len(dates) < 70:
        print("Not enough dates.")
        return

    test_dates = dates[-BACKTEST_DAYS:]
    print(f"Period: {test_dates[0]} to {test_dates[-1]}")
    
    # Universe
    universe = {}
    for sector in SECTOR_POOL:
        stocks = fetch_sector_components(sector)
        for code, name in stocks:
            universe[code] = name
            
    # Sample if too large
    MAX_UNIVERSE = 200
    if len(universe) > MAX_UNIVERSE:
        import random
        keys = list(universe.keys())
        sampled_keys = random.sample(keys, MAX_UNIVERSE)
        universe = {k: universe[k] for k in sampled_keys}
    
    print(f"Universe: {len(universe)} stocks")
    
    # Pre-fetch Daily
    daily_cache = {}
    print("Fetching Daily Data...")
    start_str = dates[0].replace("-","")
    end_str = dates[-1].replace("-","")
    
    def fetch_job(code):
        return code, get_daily_data(code, start_str, end_str)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_job, code): code for code in universe}
        for future in as_completed(futures):
            code, df = future.result()
            if df is not None:
                daily_cache[code] = df

    v1_trades = []
    v2_trades = []
    v2_1_trades = []
    v2_details = [] # Store details for autopsy

    for i, t_date in enumerate(test_dates):
        if i >= len(test_dates) - 1: break
        t_next = test_dates[i+1]
        
        print(f"[{t_date}] Processing...", end="\r")
        
        # We need to iterate ALL stocks to screen for both strategies
        # To save time, we do a loose pre-filter based on daily data (T-1)
        
        candidates = []
        for code, df in daily_cache.items():
            mask_prev = df['日期'] < t_date
            df_prev = df[mask_prev]
            if df_prev.empty or len(df_prev) < 61: continue
            
            last_row = df_prev.iloc[-1]
            close_prev = last_row['收盘']
            vol_prev = last_row['成交量']
            
            # 60-day metrics
            window_60 = df_prev.tail(60)
            high_60 = window_60['最高'].max()
            low_60 = window_60['最低'].min()
            
            # V1: 60d Chg < 25%
            # Calc 60d change: (Close_T-1 - Close_T-61) / Close_T-61
            close_60 = window_60.iloc[0]['收盘']
            pct_60 = (close_prev - close_60) / close_60 * 100
            
            # V2: RPP < 0.4 & Price > MA20
            rpp = get_rpp(close_prev, high_60, low_60)
            ma20 = window_60['收盘'].tail(20).mean()
            
            candidates.append({
                "code": code,
                "name": universe[code],
                "close_prev": close_prev,
                "vol_prev": vol_prev,
                "pct_60": pct_60,
                "rpp": rpp,
                "ma20": ma20
            })
            
        # For Minute Data Check, we select potential candidates for BOTH
        # V1 Potential: pct_60 < 25
        # V2 Potential: rpp < 0.4 & close > ma20
        
        check_list = [c for c in candidates if (c['pct_60'] < 25) or (c['rpp'] < 0.4 and c['close_prev'] > c['ma20'])]
        # Limit checks
        check_list = check_list[:30] 
        
        for cand in check_list:
            if "ST" in cand['name']: continue
            
            snap = get_minute_snapshot(cand['code'], t_date)
            if not snap: continue
            
            current_price = snap['price_1000']
            pct_chg = (current_price - cand['close_prev']) / cand['close_prev'] * 100
            
            # --- V1 Strategy Logic (Baseline) ---
            # 1. 1% < Chg < 5%
            # 2. 60d Chg < 25%
            # 3. VR > 1.5 (Approx by Vol > 20% prev day)
            v1_hit = False
            if (1.0 < pct_chg < 5.0) and \
               (cand['pct_60'] < 25.0) and \
               (cand['pct_60'] > -30.0) and \
               (snap['acc_vol_1000'] > cand['vol_prev'] * 0.15): # Loose VR proxy
                v1_hit = True
            
            # --- V2 Strategy Logic (New) ---
            # 1. RPP < 0.4
            # 2. Price > MA20 (Trend)
            # 3. Price > VWAP & Price > Open (Intraday Strength)
            # 4. Vol > 25% prev day (Stronger Volume)
            # 5. Not Limit Up
            v2_hit = False
            vwap_dev = (current_price - snap['vwap_1000']) / snap['vwap_1000']
            
            if (cand['rpp'] < 0.4) and \
               (cand['close_prev'] > cand['ma20']) and \
               (current_price < cand['close_prev'] * 1.09) and \
               (snap['acc_vol_1000'] > cand['vol_prev'] * 0.25) and \
               (snap['price_1000'] > snap['open_0930']) and \
               (vwap_dev > 0): # Price > VWAP
                v2_hit = True

            # --- V2.1 Strategy Logic (Optimized) ---
            # Added: VWAP Deviation Cap < 1.5%
            v2_1_hit = False
            if v2_hit and (vwap_dev < 0.015):
                 v2_1_hit = True
            
            # Calculate Result (T+1 Open Exit)
            buy_price = current_price * (1 + SLIPPAGE_ENTRY)
            
            df_curr = daily_cache[cand['code']]
            row_next = df_curr[df_curr['日期'] == t_next]
            if row_next.empty:
                exit_price = buy_price
            else:
                exit_price = row_next.iloc[0]['开盘']
            
            pnl = (exit_price - buy_price) / buy_price
            
            if v1_hit:
                v1_trades.append(pnl)
            if v2_hit:
                v2_trades.append(pnl)
            if v2_1_hit:
                v2_1_trades.append(pnl)
                v2_details.append({
                    "date": t_date,
                    "code": cand['code'],
                    "name": cand['name'],
                    "entry_price": buy_price,
                    "exit_price": exit_price,
                    "pnl": pnl,
                    "rpp": cand['rpp'],
                    "vwap_gap": vwap_dev * 100
                })

    # --- Report ---
    print("\n" + "="*65)
    print(f"{'Metric':<15} | {'V1 (Old)':<15} | {'V2 (New)':<15} | {'V2.1 (Opt)':<15}")
    print("-" * 65)
    
    def calc_stats(trades):
        if not trades: return 0, 0, 0
        win = len([x for x in trades if x > 0])
        total = len(trades)
        avg = sum(trades) / total
        return total, win/total, avg

    n1, w1, p1 = calc_stats(v1_trades)
    n2, w2, p2 = calc_stats(v2_trades)
    n3, w3, p3 = calc_stats(v2_1_trades)
    
    print(f"{'Trades':<15} | {n1:<15} | {n2:<15} | {n3:<15}")
    print(f"{'Win Rate':<15} | {w1*100:.2f}%{'':<9} | {w2*100:.2f}%{'':<9} | {w3*100:.2f}%")
    print(f"{'Avg PnL':<15} | {p1*100:.2f}%{'':<9} | {p2*100:.2f}%{'':<9} | {p3*100:.2f}%")
    print("="*65)

    if v2_details:
        print("\n🔍 V2.1 Trades Autopsy:")
        for t in v2_details:
            print(f"[{t['date']}] {t['name']} ({t['code']}) | PnL: {t['pnl']*100:.2f}%")
            print(f"  Entry: {t['entry_price']:.2f} | Exit: {t['exit_price']:.2f}")
            print(f"  RPP: {t['rpp']:.2f} | Price vs VWAP: +{t['vwap_gap']:.2f}%")
            print("-" * 30)

if __name__ == "__main__":
    run_comparison()
