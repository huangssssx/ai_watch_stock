import datetime
import itertools
import pandas as pd
import numpy as np
from sandbox_downturn_backtest import Backtester

# --- Tuning Targets ---
# 1. High Beta (券商: 东方财富 sz300059)
# 2. Low Beta (银行: 工商银行 sh601398)

TARGETS = [
    {"code": "sz300059", "name": "东方财富", "sector": "HIGH_BETA"},
    {"code": "sh601398", "name": "工商银行", "sector": "LOW_BETA"}
]

# --- Parameter Grid ---
PARAM_GRID = {
    "HIGH_BETA": {
        "chop_threshold": [50.0, 55.0, 58.0],     # Try stricter
        "vol_multiplier": [2.0, 2.5, 3.0],        # Try higher volume confirmation
        "kama_slow_period": [20, 25, 30]          # Try smoother trend
    },
    "LOW_BETA": {
        "chop_threshold": [60.0, 65.0, 70.0],     # Try looser
        "vol_multiplier": [1.2, 1.5, 1.8],        # Try standard
        "kama_slow_period": [20, 30, 40, 60]      # Try much slower (Quarterly)
    }
}

def calculate_score(win_rate, avg_ret, trade_count):
    # Score = WinRate * 0.6 + AvgRet * 0.4
    # Penalize low trade count (< 3)
    if trade_count < 3: return -1.0
    
    # WinRate is 0-1
    # AvgRet: we want negative return (price drop). So bigger drop is better.
    # Let's normalize AvgRet. Assume -10% is good (1.0). 0% is bad (0.0).
    # ret_score = min(abs(avg_ret) * 10, 1.0) # Cap at 1.0 for -10%
    
    # Actually, we want to maximize WinRate primarily (Precision)
    # But if return is positive (loss), score should be low.
    
    if avg_ret > 0: # Losing strategy on average
        return 0.0
        
    ret_score = min(abs(avg_ret) * 5, 1.0) # -20% gives 1.0 score
    
    return (win_rate * 0.7) + (ret_score * 0.3)

def run_tuning():
    end_dt = datetime.datetime.now().strftime("%Y%m%d")
    start_dt = (datetime.datetime.now() - datetime.timedelta(days=730)).strftime("%Y%m%d")

    print(f"=== Starting Auto-Tuning (Target: 80% Win Rate) ===")
    
    best_params_global = {}

    for t in TARGETS:
        symbol = t["code"]
        name = t["name"]
        sector = t["sector"]
        print(f"\n>>> Tuning {name} ({sector}) <<<")
        
        # 1. Prepare Data
        bt = Backtester(symbol, start_dt, end_dt)
        if not bt.fetch_data():
            print("Fetch failed, skipping.")
            continue
            
        # 2. Generate Grid
        grid = PARAM_GRID[sector]
        keys = grid.keys()
        combinations = list(itertools.product(*grid.values()))
        
        best_score = -100
        best_params = None
        best_metrics = None
        
        print(f"Testing {len(combinations)} combinations...")
        
        for values in combinations:
            params = dict(zip(keys, values))
            
            # Run Backtest
            bt.run(use_chop=True, override_params=params)
            
            # Calculate Metrics
            strong_sells = [r for r in bt.results if r['signal'] == 'STRONG_SELL']
            count = len(strong_sells)
            
            if count == 0:
                continue
                
            wins = [r for r in strong_sells if r['ret_5d'] is not None and r['ret_5d'] < 0]
            win_rate = len(wins) / count
            
            rets_20d = [r['ret_20d'] for r in strong_sells if r['ret_20d'] is not None]
            avg_ret_20d = sum(rets_20d) / len(rets_20d) if rets_20d else 0
            
            score = calculate_score(win_rate, avg_ret_20d, count)
            
            # Print significant improvements
            if win_rate >= 0.7:
                 print(f"  [Found High WR] Params: {params} | WR: {win_rate*100:.1f}% | Ret20d: {avg_ret_20d*100:.1f}% | Count: {count}")
            
            if score > best_score:
                best_score = score
                best_params = params
                best_metrics = {"wr": win_rate, "ret": avg_ret_20d, "count": count}
        
        print(f"--- Best Result for {name} ---")
        print(f"Params: {best_params}")
        if best_metrics:
            print(f"WinRate: {best_metrics['wr']*100:.1f}%")
            print(f"AvgRet20d: {best_metrics['ret']*100:.2f}%")
            print(f"Count: {best_metrics['count']}")
            
        best_params_global[sector] = best_params

    return best_params_global

if __name__ == "__main__":
    run_tuning()
