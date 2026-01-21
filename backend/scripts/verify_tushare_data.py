
import tushare as ts
import pandas as pd
import datetime
import os

# Init
token = os.getenv("TUSHARE_TOKEN")
if not token:
    raise SystemExit("环境变量 TUSHARE_TOKEN 未设置")
ts.set_token(token)
pro = ts.pro_api()
pro._DataApi__http_url = os.getenv("TUSHARE_API_URL", "http://5k1a.xiximiao.com/dataapi")

print("--- Tushare Data Verification ---")

# 1. Realtime Quotes
print("\n[1] Realtime Quotes (600519)")
try:
    df = ts.get_realtime_quotes(['600519'])
    if not df.empty:
        print(df[['code', 'name', 'price', 'volume', 'amount']].to_string(index=False))
    else:
        print("Empty realtime data")
except Exception as e:
    print(f"Error: {e}")

# 2. Daily History
print("\n[2] Daily History (600519, Last 5 Days)")
try:
    today = datetime.date.today().strftime('%Y%m%d')
    start = (datetime.date.today() - datetime.timedelta(days=10)).strftime('%Y%m%d')
    df = pro.daily(ts_code='600519.SH', start_date=start, end_date=today)
    if not df.empty:
        print(df[['trade_date', 'close', 'vol', 'amount']].head().to_string(index=False))
    else:
        print("Empty daily data")
except Exception as e:
    print(f"Error: {e}")

# 3. Trade Cal
print("\n[3] Trade Calendar")
try:
    df = pro.trade_cal(start_date=today, end_date=today)
    print(df)
except Exception as e:
    print(f"Error: {e}")
