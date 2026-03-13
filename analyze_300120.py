import sys
import os
sys.path.insert(0, '/Users/huangchuanjian/workspace/my_projects/ai_watch_stock')

from utils.pytdx_client import tdx, connected_endpoint
import pandas as pd
from datetime import datetime

# 连接TDX
print(f"{datetime.now().strftime('%Y%m%d_%H%M%S')} 连接TDX...")
connected_endpoint()

# 获取日线数据
df_daily, _ = _fetch_daily_bars(market=0, code="300120", count=200)
print(f"日线数据: {len(df_daily)}条")

# 获取月线数据
df_monthly, _ = _fetch_monthly_bars(market=0, code="300120", count=24)
print(f"月线数据: {len(df_monthly)}条")

# 分析形态
print("\n=== 经纬辉开(300120)形态分析 ===")
print(f"最新收盘价: {df_daily['close'].iloc[-1]:.2f}")
print(f"60日均线: {df_daily['close'].rolling(60).mean().iloc[-1]:.2f}")
print(f"年线位置: {(df_daily['close'].iloc[-1] - df_daily['low'].tail(250).min()) / (df_daily['high'].tail(250).max() - df_daily['low'].tail(250).min()):.2%}")

# 保存数据
df_daily.to_csv('/Users/huangchuanjian/workspace/my_projects/ai_watch_stock/300120_daily.csv', index=False)
df_monthly.to_csv('/Users/huangchuanjian/workspace/my_projects/ai_watch_stock/300120_monthly.csv', index=False)
print("\n数据已保存到CSV文件")