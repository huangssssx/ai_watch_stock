
import tushare as ts
import pandas as pd
import datetime

# Init
token = '4501928450004005131'
ts.set_token(token)
pro = ts.pro_api()
pro._DataApi__http_url = 'http://5k1a.xiximiao.com/dataapi'

print("获取行情数据 (Tushare Daily)...")

# Use Daily data (Yesterday)
today = datetime.date.today().strftime('%Y%m%d')
start = (datetime.date.today() - datetime.timedelta(days=5)).strftime('%Y%m%d')
cal = pro.trade_cal(is_open='1', end_date=today)
last_trade_date = cal.iloc[-1]['cal_date']

print(f"Fetch Date: {last_trade_date}")
df_spot = pro.daily(trade_date=last_trade_date)

if df_spot.empty:
    print("No data found.")
    exit()

# Get Names
df_basic = pro.stock_basic(fields='ts_code,name')
df_spot = pd.merge(df_spot, df_basic, on='ts_code')

print(f"\n总股票数: {len(df_spot)}")
print(f"\n列名: {df_spot.columns.tolist()}")
print(f"\n前5行数据:")
print(df_spot.head())

# Rename for compatibility with print statements if needed, or update prints
# Tushare: amount (千元) -> Convert to Yuan for comparison
df_spot['成交额'] = df_spot['amount'] * 1000 
df_spot['成交量'] = df_spot['vol'] * 100 # Hand -> Share (approx)

print(f"\n成交额列的基本信息:")
print(f"  成交额数据类型: {df_spot['成交额'].dtype}")
print(f"  成交额前10个值:")
print(df_spot['成交额'].head(10))
print(f"\n成交额统计:")
print(df_spot['成交额'].describe())

print(f"\n过滤ST股票前:")
print(f"  总数: {len(df_spot)}")
df_spot = df_spot[~df_spot['name'].str.contains("ST|退")]
print(f"  过滤ST后: {len(df_spot)}")

print(f"\n检查成交额>100000000 (1亿) 的股票数:")
count = (df_spot['成交额'] > 100000000).sum()
print(f"  符合条件的股票数: {count}")

if count == 0:
    print("\n⚠️ 没有股票的成交额>1亿！")
    print(f"\n成交额的最大值: {df_spot['成交额'].max()}")
    print(f"成交额的前20大值:")
    print(df_spot.nlargest(20, '成交额')[['ts_code', 'name', '成交额', '成交量']])
