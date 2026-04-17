import sys
import os

here = os.path.abspath(os.path.dirname(__file__))
project_root = os.path.abspath(os.path.join(here, "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import tushare as ts
from backend.utils.tushare_client import pro

print("=== 1. pro.daily() 按日期批量 ===")
df = pro.daily(trade_date="20260410", fields="ts_code,trade_date,open,close,high,low,pct_chg,vol,amount")
print(f"行数: {len(df)}")
print(df.head(3).to_string())
print()

print("=== 2. pro.daily_basic() 换手率/市值 ===")
df2 = pro.daily_basic(trade_date="20260410", fields="ts_code,trade_date,turnover_rate,total_mv,circ_mv")
print(f"行数: {len(df2)}")
print(df2.head(3).to_string())
print()

print("=== 3. ts.pro_bar() 周线 ===")
df3 = ts.pro_bar(ts_code="000001.SZ", asset="E", freq="W", start_date="20260101", end_date="20260410")
if df3 is not None and len(df3) > 0:
    print(f"行数: {len(df3)}, 列: {list(df3.columns)}")
    print(df3.tail(5).to_string())
else:
    print("周线数据为空!")
print()

print("=== 4. pro.index_daily() 大盘指数 ===")
df4 = pro.index_daily(ts_code="000001.SH", start_date="20260301", end_date="20260410", fields="ts_code,trade_date,close,pct_chg")
print(f"行数: {len(df4)}")
print(df4.tail(5).to_string())
print()

print("=== 5. pro.daily() 按股票取120天 ===")
df5 = pro.daily(ts_code="000001.SZ", start_date="20251101", end_date="20260410", fields="trade_date,open,close,high,low,pct_chg,vol,amount")
print(f"行数: {len(df5)}")
print(df5.head(3).to_string())
print()

print("=== 6. 验证 vol/amount 单位 ===")
row = df5.iloc[-1]
print(f"最近一天: vol={row['vol']}, amount={row['amount']}, close={row['close']}")
raw_vwap = row["amount"] / row["vol"]
vwap_per_share = row["amount"] * 1000 / (row["vol"] * 100)
print(f"amount/vol = {raw_vwap:.2f} (千元/手)")
print(f"amount*1000/(vol*100) = {vwap_per_share:.2f} (元/股, 应约等于股价)")
print()

print("=== 7. pro.weekly() 周线替代方案 ===")
df7 = pro.weekly(ts_code="000001.SZ", start_date="20260101", end_date="20260410", fields="ts_code,trade_date,open,close,high,low,vol,amount")
if df7 is not None and len(df7) > 0:
    print(f"行数: {len(df7)}, 列: {list(df7.columns)}")
    print(df7.tail(5).to_string())
else:
    print("pro.weekly() 数据为空!")

print()
print("=== 全部接口测试完成 ===")
