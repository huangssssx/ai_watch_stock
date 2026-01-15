import akshare as ak
try:
    print("Testing sina spot...")
    df = ak.stock_zh_index_spot_sina()
    print(df.head())
except Exception as e:
    print(f"Sina Failed: {e}")

try:
    print("Testing EM spot default...")
    df = ak.stock_zh_index_spot_em() # No args
    print(df.head())
except Exception as e:
    print(f"EM Default Failed: {e}")
