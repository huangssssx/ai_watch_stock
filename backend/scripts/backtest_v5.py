import akshare as ak
import pandas as pd
import numpy as np
import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
BACKTEST_DAYS = 20  # Reduced for demo speed
SLIPPAGE_ENTRY = 0.01
# Focus on active sectors to ensure we find "hot" stocks
SECTOR_POOL = ["半导体", "软件开发", "汽车整车", "证券", "酿酒行业", "光伏设备", "消费电子", "通信设备"]
MAX_WORKERS = 4

# --- Helper Functions ---

def get_trade_dates(days=60):
    """Get recent trading dates."""
    try:
        tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()
        dates = tool_trade_date_hist_sina_df['trade_date'].tolist()
        # Filter dates up to today
        today = datetime.date.today()
        valid_dates = [str(d) for d in dates if d <= today]
        return valid_dates[-days:]
    except Exception as e:
        print(f"Error fetching trade dates: {e}")
        return []

def fetch_sector_components(sector):
    """Get stock list for a sector."""
    try:
        cons = ak.stock_board_industry_cons_em(symbol=sector)
        return cons[['代码', '名称']].values.tolist() if not cons.empty else []
    except:
        return []

def get_daily_data(symbol, start_date, end_date):
    """Fetch daily data for Layer 1."""
    try:
        df = ak.stock_zh_a_hist(symbol=symbol, start_date=start_date, end_date=end_date, adjust="qfq")
        if df.empty: return None
        # Ensure '日期' is string for comparison
        df['日期'] = df['日期'].astype(str)
        return df
    except:
        return None

def get_minute_snapshot(symbol, date_str):
    """
    Fetch minute data for Layer 2.
    Target: 09:30 - 10:00 on specific date.
    Returns: 10:00 row (Close, VWAP, Vol_Accum)
    """
    try:
        # AKShare minute data often returns recent data. 
        # Note: ak.stock_zh_a_hist_min_em usually supports recent period.
        # We need to construct the datetime string carefully.
        
        # Use a wide range to ensure we capture the day
        start_dt = f"{date_str} 09:30:00"
        end_dt = f"{date_str} 15:00:00"
        
        df = ak.stock_zh_a_hist_min_em(symbol=symbol, start_date=start_dt, end_date=end_dt, period='1', adjust='qfq')
        if df is None or df.empty: return None
        
        # Filter for the specific date and time range
        target_time = f"{date_str} 10:00:00"
        
        # Accumulate volume up to 10:00
        mask_morning = (df['时间'] <= target_time)
        morning_df = df[mask_morning]
        
        if morning_df.empty: return None
        
        acc_vol = morning_df['成交量'].sum()
        
        # Get 10:00 snapshot (or the last minute before 10:00 if missing)
        snapshot = morning_df.iloc[-1]
        
        # Calculate VWAP for the morning session
        # VWAP = Sum(Price * Vol) / Sum(Vol)
        vwap = (morning_df['收盘'] * morning_df['成交量']).sum() / acc_vol if acc_vol > 0 else snapshot['收盘']
        
        return {
            "price_1000": snapshot['收盘'],
            "open_0930": morning_df.iloc[0]['开盘'],
            "vwap_1000": vwap,
            "acc_vol_1000": acc_vol,
            "time": snapshot['时间']
        }
    except Exception as e:
        return None

# --- Core Logic ---

def run_backtest():
    print("Initializing Backtest V5.0...")
    dates = get_trade_dates(BACKTEST_DAYS + 70) # Need extra for 60d lookback
    if len(dates) < 70:
        print("Not enough dates available.")
        return

    test_dates = dates[-BACKTEST_DAYS:]
    print(f"Testing Period: {test_dates[0]} to {test_dates[-1]}")
    
    # 1. Build Universe
    universe = {} # symbol -> name
    for sector in SECTOR_POOL:
        stocks = fetch_sector_components(sector)
        for code, name in stocks:
            universe[code] = name
            
    # Limit universe size for demo performance if too large
    MAX_UNIVERSE = 300
    if len(universe) > MAX_UNIVERSE:
        # Random sample to keep it fast
        import random
        keys = list(universe.keys())
        sampled_keys = random.sample(keys, MAX_UNIVERSE)
        universe = {k: universe[k] for k in sampled_keys}
    
    print(f"Universe Size: {len(universe)} stocks (Sampled from {len(SECTOR_POOL)} sectors).")
    
    results = []
    daily_cache = {} 
    
    print("Pre-fetching daily data...")
    start_str = dates[0].replace("-","")
    end_str = dates[-1].replace("-","")
    
    def fetch_job(code):
        return code, get_daily_data(code, start_str, end_str)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_job, code): code for code in universe}
        count = 0
        for future in as_completed(futures):
            code, df = future.result()
            if df is not None:
                daily_cache[code] = df
            count += 1
            if count % 50 == 0: print(f"  Fetched {count}/{len(universe)}")
    
    print("Daily data ready. Starting Simulation...")

    for i, t_date in enumerate(test_dates):
        if i >= len(test_dates) - 1: break
        
        t_next = test_dates[i+1]
        print(f"\n[{t_date}] Layer 1 Screening...", end="")
        
        # Layer 1: T-1 Screening
        candidates = []
        
        for code, df in daily_cache.items():
            # Get data up to T-1
            mask_prev = df['日期'] < t_date
            df_prev = df[mask_prev]
            if df_prev.empty or len(df_prev) < 61: continue
            
            last_row = df_prev.iloc[-1]
            
            # 1. RPP Filter (Relative Position)
            window_60 = df_prev.tail(60)
            high_60 = window_60['最高'].max()
            low_60 = window_60['最低'].min()
            close = last_row['收盘']
            
            if high_60 == low_60: continue
            rpp = (close - low_60) / (high_60 - low_60)
            
            if rpp >= 0.4: continue # Must be low
            
            # 2. Trend Filter (Price > MA20)
            ma20 = window_60['收盘'].tail(20).mean()
            if close < ma20: continue
            
            candidates.append({
                "code": code,
                "name": universe[code],
                "vol_prev": last_row['成交量'],
                "close_prev": close,
                "rpp": rpp
            })
            
        print(f" Found {len(candidates)} candidates.")
        if not candidates: continue
        
        # Layer 2: T-0 10:00 Validation
        # Sort by RPP to pick "Lowest of the Low"
        candidates.sort(key=lambda x: x['rpp'])
        check_list = candidates[:15] # Only check top 15 to save API calls
        
        trades = []
        
        # print(f"  Checking Layer 2 (Minute Data) for {len(check_list)} stocks...")
        for cand in check_list:
            # 0. ST Filter
            if "ST" in cand['name'] or "退" in cand['name']: continue

            # Fetch Minute Data
            snap = get_minute_snapshot(cand['code'], t_date)
            if not snap: continue
            
            # Logic Checks
            
            # 1. Liquidity Trap: Price < 1.09 * PrevClose
            if snap['price_1000'] >= cand['close_prev'] * 1.09:
                continue
                
            # 2. Volume Trigger: Vol > 25% of Yesterday (Increased from 20%)
            if snap['acc_vol_1000'] < cand['vol_prev'] * 0.25:
                continue
                
            # 3. Price Confirmation
            if snap['open_0930'] <= cand['close_prev']: # Gap Up Required
                 continue

            if snap['price_1000'] <= snap['open_0930']: # Must be red to green or green
                continue
                
            if snap['price_1000'] <= snap['vwap_1000']: # Must be above VWAP
                continue
                
            # EXECUTE BUY
            buy_price = snap['price_1000'] * (1 + SLIPPAGE_ENTRY)
            
            # GET EXIT PRICE (T+1 Open)
            df_curr = daily_cache[cand['code']]
            row_next = df_curr[df_curr['日期'] == t_next]
            
            if row_next.empty:
                # If T+1 data missing (e.g. suspension), assume exit at buy_price (flat)
                exit_price = buy_price 
            else:
                exit_price = row_next.iloc[0]['开盘']
                
            pnl = (exit_price - buy_price) / buy_price
            
            trades.append({
                "date": t_date,
                "code": cand['code'],
                "name": cand['name'],
                "buy_price": buy_price,
                "exit_price": exit_price,
                "pnl": pnl
            })
            time.sleep(0.1) # Be nice to API
            
        results.extend(trades)
        if trades:
            print(f"  -> Executed {len(trades)} trades. Best: {max([t['pnl'] for t in trades])*100:.2f}%")
        else:
            print("  -> No trades triggered.")

    # --- Summary ---
    if not results:
        print("No trades executed in the period.")
        return

    df_res = pd.DataFrame(results)
    total_trades = len(df_res)
    win_trades = len(df_res[df_res['pnl'] > 0])
    win_rate = win_trades / total_trades
    avg_pnl = df_res['pnl'].mean()
    
    print("\n" + "="*40)
    print(f"BACKTEST RESULTS (V5.0) - {BACKTEST_DAYS} Days")
    print("="*40)
    print(f"Total Trades: {total_trades}")
    print(f"Win Rate:     {win_rate*100:.2f}%")
    print(f"Avg PnL:      {avg_pnl*100:.2f}% (incl. 1% slippage)")
    print(f"Max Win:      {df_res['pnl'].max()*100:.2f}%")
    print(f"Max Loss:     {df_res['pnl'].min()*100:.2f}%")
    print("="*40)
    
    # Show last few trades
    print("\nRecent Trades:")
    print(df_res[['date', 'name', 'pnl']].tail(5).to_string())

if __name__ == "__main__":
    run_backtest()
