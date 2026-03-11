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
df_1m = df_1m[df_1m['datetime'].dt.strftime('%Y%m%d') == trade_date]

if df_1m.empty:
    print("没有找到指定日期的数据")
    print(f"数据列: {df_1m.columns.tolist()}")
    print(f"数据前几行:\n{df_1m.head()}")
    exit(1)

# 计算开盘价
open_px = float(df_1m["open"].iloc[0])
print(f"\n开盘价: {open_px:.2f}")

# 计算前30分钟数据
first30_end = pd.to_datetime(trade_date, format="%Y%m%d") + pd.Timedelta(hours=10, minutes=0)
first30 = df_1m[df_1m["datetime"] < first30_end].copy()

print(f"\n前30分钟数据 (09:30-10:00):")
print(f"最低价: {first30['low'].min():.2f} (跌幅: {(first30['low'].min() - open_px) / open_px * 100:.2f}%)")
print(f"10:00收盘价: {first30['close'].iloc[-1]:.2f} (跌幅: {(first30['close'].iloc[-1] - open_px) / open_px * 100:.2f}%)")

# 计算站上开盘价的时间
post = df_1m[df_1m["datetime"] >= first30_end].copy()
cross_line = open_px * 1.001  # 站上0.1%
cross_mask = pd.to_numeric(post["close"], errors="coerce") >= cross_line

if cross_mask.any():
    cross_pos = int(cross_mask.idxmax())
    cross_time = post.loc[cross_pos, "datetime"]
    print(f"\n站上开盘价时间: {cross_time.strftime('%H:%M')}")
    print(f"站上时价格: {post.loc[cross_pos, 'close']:.2f}")
else:
    print("\n未站上开盘价")

# 显示关键时间点的价格
print(f"\n关键时间点价格:")
for i in range(len(df_1m)):
    dt = df_1m.iloc[i]['datetime']
    if dt.minute in [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]:
        if dt.hour == 9 or dt.hour == 10:
            print(f"{dt.strftime('%H:%M')}: {df_1m.iloc[i]['close']:.2f} (涨幅: {(df_1m.iloc[i]['close'] - open_px) / open_px * 100:.2f}%)")

# 显示成交量数据
if 'vol' in df_1m.columns:
    print(f"\n成交量数据:")
    first30_vol = first30['vol'].mean()
    post_vol = post['vol'].mean()
    print(f"前30分钟平均成交量: {first30_vol:.0f}")
    print(f"站上后平均成交量: {post_vol:.0f}")
    print(f"成交量比: {post_vol / first30_vol if first30_vol > 0 else 0:.2f}")