import efinance as ef
import pandas as pd
import traceback

def probe(name, fn):
    print(f"\n--- Probing {name} ---")
    try:
        res = fn()
        if isinstance(res, pd.DataFrame):
            print(f"Shape: {res.shape}")
            print(f"Columns: {res.columns.tolist()}")
            print(res.head(2).to_string())
        else:
            print(f"Result type: {type(res)}")
            print(res)
    except Exception as e:
        print(f"❌ FAILED: {e}")
        traceback.print_exc()

# 1. Realtime Quotes
probe("ef.stock.get_realtime_quotes()", lambda: ef.stock.get_realtime_quotes())

# 2. History Data
probe("ef.stock.get_quote_history(['600519'])", lambda: ef.stock.get_quote_history(['600519']))

# 3. Industry List?
# efinance doesn't have explicit industry list function in top level usually, 
# but let's check if we can get it via get_realtime_quotes with specific type?
# Actually ef.stock.get_realtime_quotes() returns ALL stocks. 
# Maybe we can use it to infer sectors if '板块' column exists?
# Let's check columns of get_realtime_quotes.

# 4. Sector Fund Flow?
# ef.stock.get_today_bill() might have individual flow.
# ef.stock.get_all_company_performance()?

# Let's just check the columns of realtime quotes first, it might contain industry info.
