import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backend'))

from utils.pytdx_client import tdx
import pandas as pd
from datetime import datetime, timedelta

# 获取许继电器的1分钟K线数据
code = "000400"
market = 0  # 0=深圳
trade_date = "20260311"

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
    print("没有找到指定日期的数据")
    print(f"数据前几行:\n{df_1m.head()}")
    exit(1)

print(f"指定日期数据行数: {len(df_1m)}")

# 计算开盘价
open_px = float(df_1m["open"].iloc[0])
print(f"\n开盘价: {open_px:.2f}")

# 计算前30分钟数据
first30_end = pd.to_datetime(trade_date, format="%Y%m%d") + pd.Timedelta(hours=10, minutes=0)
first30 = df_1m[df_1m["datetime"] < first30_end].copy()

print(f"\n前30分钟数据 (09:30-10:00):")
print(f"数据行数: {len(first30)}")
if len(first30) > 0:
    print(f"最低价: {first30['low'].min():.2f} (跌幅: {(first30['low'].min() - open_px) / open_px * 100:.2f}%)")
    print(f"10:00收盘价: {first30['close'].iloc[-1]:.2f} (跌幅: {(first30['close'].iloc[-1] - open_px) / open_px * 100:.2f}%)")
else:
    print("前30分钟数据为空")

# 显示所有数据
print(f"\n所有K线数据:")
for i in range(len(df_1m)):
    dt = df_1m.iloc[i]['datetime']
    print(f"{dt.strftime('%H:%M')}: 开盘={df_1m.iloc[i]['open']:.2f}, 最高={df_1m.iloc[i]['high']:.2f}, 最低={df_1m.iloc[i]['low']:.2f}, 收盘={df_1m.iloc[i]['close']:.2f}, 成交量={df_1m.iloc[i]['vol']:.0f}")