import akshare as ak
import pandas as pd

def check(name, fn):
    try:
        print(f"Checking {name}...")
        df = fn()
        print(f"Count: {len(df)}")
        print(f"Columns: {df.columns.tolist()[:5]} ...")
        if "市盈率-动态" in df.columns:
            print("Has PE-Dynamic")
        else:
            print("MISSING PE-Dynamic")
    except Exception as e:
        print(f"Error {name}: {e}")

check("SH Main", ak.stock_sh_a_spot_em)
check("SZ Main", ak.stock_sz_a_spot_em)
check("KC (STAR)", ak.stock_kc_a_spot_em)
check("CY (GEM)", ak.stock_cy_a_spot_em)
check("BJ", ak.stock_bj_a_spot_em)
