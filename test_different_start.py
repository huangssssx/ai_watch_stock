import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backend'))

from utils.pytdx_client import tdx
import pandas as pd

# 测试使用不同的 start 参数获取大盘指数的1分钟K线数据
trade_date = "20260311"

# 测试上证指数
print(f"测试使用不同的 start 参数获取上证指数 (000001) 的1分钟K线数据...")
market_sh = 1
code_sh = "000001"

# 尝试不同的 start 参数
for start in [0, 100, 200, 300, 400, 500]:
    print(f"\n{'='*60}")
    print(f"start={start}")
    
    data_sh = tdx.get_index_bars(8, market_sh, code_sh, start, 100)
    if data_sh is None or len(data_sh) == 0:
        print(f"无法获取上证指数K线数据")
        continue
    
    df_sh = pd.DataFrame(data_sh)
    df_sh['datetime'] = pd.to_datetime(df_sh['datetime'], errors='coerce')
    df_sh = df_sh.dropna(subset=['datetime'])
    
    print(f"数据时间范围: {df_sh['datetime'].min()} 到 {df_sh['datetime'].max()}")
    print(f"总数据行数: {len(df_sh)}")
    
    # 筛选指定日期的数据
    df_sh_filtered = df_sh[df_sh['datetime'].dt.strftime('%Y%m%d') == trade_date]
    
    if not df_sh_filtered.empty:
        print(f"指定日期数据行数: {len(df_sh_filtered)}")
        print(f"指定日期数据时间范围: {df_sh_filtered['datetime'].min()} 到 {df_sh_filtered['datetime'].max()}")
        
        # 计算前30分钟数据
        open_start = pd.to_datetime(trade_date, format="%Y%m%d") + pd.Timedelta(hours=9, minutes=30)
        first30_end = pd.to_datetime(trade_date, format="%Y%m%d") + pd.Timedelta(hours=10, minutes=0)
        
        first30 = df_sh_filtered[df_sh_filtered["datetime"] < first30_end].copy()
        print(f"前30分钟数据行数: {len(first30)}")
        
        if len(first30) > 0:
            print(f"前30分钟数据:")
            print(first30[['datetime', 'open', 'close', 'high', 'low', 'vol']])
            
            if len(first30) >= 25:
                print(f"✓ 前30分钟数据充足，有{len(first30)}条")
                break