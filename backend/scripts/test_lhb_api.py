
import akshare as ak
import pandas as pd

try:
    symbol = "000001" # 平安银行
    print(f"Testing stock_lhb_stock_detail_date_em for {symbol}...")
    df = ak.stock_lhb_stock_detail_date_em(symbol=symbol)
    print("Success!")
    print(df.head())
    print("\nColumns:", df.columns.tolist())
except Exception as e:
    print(f"Error: {e}")
