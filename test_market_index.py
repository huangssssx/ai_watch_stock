import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backend'))

from utils.pytdx_client import tdx
import pandas as pd

# 测试获取大盘指数的1分钟K线数据
trade_date = "20260311"
asof_time = "10:25"

# 测试上证指数
print(f"测试获取上证指数 (000001) 的1分钟K线数据...")
market_sh = 1
code_sh = "000001"

data_sh = tdx.get_security_bars(8, market_sh, code_sh, 0, 200)
if data_sh is None or len(data_sh) == 0:
    print(f"无法获取上证指数K线数据")
else:
    print(f"原始数据示例:")
    print(data_sh[:5])
    
    df_sh = pd.DataFrame(data_sh)
    print(f"\nDataFrame列: {df_sh.columns.tolist()}")
    print(f"datetime列前10个值:")
    print(df_sh['datetime'].head(10))
    
    try:
        df_sh['datetime'] = pd.to_datetime(df_sh['datetime'], errors='coerce')
    except Exception as e:
        print(f"转换datetime失败: {e}")
        df_sh['datetime'] = pd.to_datetime(df_sh['datetime'], errors='coerce')
    
    print(f"\n数据时间范围: {df_sh['datetime'].min()} 到 {df_sh['datetime'].max()}")
    print(f"总数据行数: {len(df_sh)}")
    
    # 筛选指定日期的数据
    df_sh_filtered = df_sh[df_sh['datetime'].dt.strftime('%Y%m%d') == trade_date]
    if not df_sh_filtered.empty:
        print(f"指定日期数据行数: {len(df_sh_filtered)}")
        print(f"前10条数据:")
        print(df_sh_filtered.head(10)[['datetime', 'open', 'close', 'high', 'low', 'vol']])
    else:
        print(f"没有找到指定日期 {trade_date} 的数据")

print(f"\n{'='*60}\n")

# 测试深证成指
print(f"测试获取深证成指 (399001) 的1分钟K线数据...")
market_sz = 0
code_sz = "399001"

data_sz = tdx.get_security_bars(8, market_sz, code_sz, 0, 200)
if data_sz is None or len(data_sz) == 0:
    print(f"无法获取深证成指K线数据")
else:
    df_sz = pd.DataFrame(data_sz)
    df_sz['datetime'] = pd.to_datetime(df_sz['datetime'])
    print(f"深证成指数据获取成功:")
    print(f"数据列: {df_sz.columns.tolist()}")
    print(f"数据时间范围: {df_sz['datetime'].min()} 到 {df_sz['datetime'].max()}")
    print(f"总数据行数: {len(df_sz)}")
    
    # 筛选指定日期的数据
    df_sz_filtered = df_sz[df_sz['datetime'].dt.strftime('%Y%m%d') == trade_date]
    if not df_sz_filtered.empty:
        print(f"指定日期数据行数: {len(df_sz_filtered)}")
        print(f"前10条数据:")
        print(df_sz_filtered.head(10)[['datetime', 'open', 'close', 'high', 'low', 'vol']])
    else:
        print(f"没有找到指定日期 {trade_date} 的数据")