import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backend'))

from utils.pytdx_client import tdx
import pandas as pd

# 测试获取大盘指数的1分钟K线数据，尝试获取更多数据
trade_date = "20260311"

# 测试上证指数
print(f"测试获取上证指数 (000001) 的1分钟K线数据，尝试获取更多数据...")
market_sh = 1
code_sh = "000001"

# 尝试获取更多数据
data_sh = tdx.get_security_bars(8, market_sh, code_sh, 0, 500)
if data_sh is None or len(data_sh) == 0:
    print(f"无法获取上证指数K线数据")
else:
    df_sh = pd.DataFrame(data_sh)
    df_sh['datetime'] = pd.to_datetime(df_sh['datetime'], errors='coerce')
    df_sh = df_sh.dropna(subset=['datetime'])
    
    print(f"总数据行数: {len(df_sh)}")
    print(f"数据时间范围: {df_sh['datetime'].min()} 到 {df_sh['datetime'].max()}")
    
    # 筛选指定日期的数据
    df_sh_filtered = df_sh[df_sh['datetime'].dt.strftime('%Y%m%d') == trade_date]
    
    if not df_sh_filtered.empty:
        print(f"指定日期数据行数: {len(df_sh_filtered)}")
        print(f"指定日期数据时间范围: {df_sh_filtered['datetime'].min()} 到 {df_sh_filtered['datetime'].max()}")
        
        # 检查是否有09:30-10:00的数据
        open_start = pd.to_datetime(trade_date, format="%Y%m%d") + pd.Timedelta(hours=9, minutes=30)
        first30_end = pd.to_datetime(trade_date, format="%Y%m%d") + pd.Timedelta(hours=10, minutes=0)
        
        first30 = df_sh_filtered[df_sh_filtered["datetime"] < first30_end].copy()
        print(f"\n前30分钟数据行数: {len(first30)}")
        
        if len(first30) > 0:
            print(f"前30分钟数据:")
            print(first30[['datetime', 'open', 'close', 'high', 'low', 'vol']])
        else:
            print(f"没有前30分钟数据")
            
            # 检查最早的数据时间
            earliest_time = df_sh_filtered['datetime'].min()
            print(f"最早的数据时间: {earliest_time}")
            
            # 检查是否有09:30之前的数据
            before_open = df_sh_filtered[df_sh_filtered["datetime"] < open_start]
            print(f"开盘前的数据行数: {len(before_open)}")
            
            if len(before_open) > 0:
                print(f"开盘前的数据:")
                print(before_open[['datetime', 'open', 'close', 'high', 'low', 'vol']])
    else:
        print(f"没有找到指定日期 {trade_date} 的数据")