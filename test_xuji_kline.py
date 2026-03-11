import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backend'))

from utils.pytdx_client import tdx
import pandas as pd
from datetime import datetime, timedelta

# 获取许继电器的1分钟K线数据
code = "000400"
market = 0  # 0=深圳
trade_date = "20240311"

print(f"获取 {code} 许继电器的1分钟K线数据...")
data = tdx.get_security_bars(8, market, code, 0, 200)
if data is None or len(data) == 0:
    print("无法获取K线数据")
    exit(1)

# 转换为DataFrame
df_1m = pd.DataFrame(data)
df_1m['datetime'] = pd.to_datetime(df_1m['datetime'])

print(f"数据列: {df_1m.columns.tolist()}")
print(f"数据时间范围: {df_1m['datetime'].min()} 到 {df_1m['datetime'].max()}")
print(f"总数据行数: {len(df_1m)}")

# 筛选指定日期的数据
df_1m = df_1m[df_1m['datetime'].dt.strftime('%Y%m%d') == trade_date]

if df_1m.empty:
    print(f"没有找到指定日期 {trade_date} 的数据")
    print(f"尝试获取最近的数据...")
    # 显示最近的数据
    recent_data = df_1m.head(10)
    print(f"最近10条数据:")
    print(recent_data[['datetime', 'open', 'close', 'high', 'low', 'vol']])
else:
    print(f"指定日期数据行数: {len(df_1m)}")
    print(f"前10条数据:")
    print(df_1m.head(10)[['datetime', 'open', 'close', 'high', 'low', 'vol']])