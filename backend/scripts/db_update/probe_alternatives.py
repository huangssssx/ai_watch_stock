import efinance as ef
import qstock as qs
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

# 1. Realtime Quotes (efinance)
probe("ef.stock.get_realtime_quotes()", lambda: ef.stock.get_realtime_quotes())

# 2. History Data (qstock)
probe("qs.get_data('600519')", lambda: qs.get_data('600519', start='20240101', end='20240110'))

# 3. Industry List (qstock)
probe("qs.industry_list()", lambda: qs.industry_list())

# 4. Industry Member (qstock)
probe("qs.industry_member('半导体')", lambda: qs.industry_member('半导体'))

# 5. Sector Fund Flow (qstock)
probe("qs.realtime_money_flow('行业')", lambda: qs.realtime_money_flow('行业'))

# 6. Index Realtime (qstock)
probe("qs.realtime_data(market='index')", lambda: qs.realtime_data(market='index'))
