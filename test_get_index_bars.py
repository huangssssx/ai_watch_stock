import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backend'))

from utils.pytdx_client import tdx
import pandas as pd

# 测试使用 get_index_bars 获取大盘指数的1分钟K线数据
trade_date = "20260311"

# 测试上证指数
print(f"测试使用 get_index_bars 获取上证指数 (000001) 的1分钟K线数据...")
market_sh = 1
code_sh = "000001"

# 使用 get_index_bars 获取指数数据
data_sh = tdx.get_index_bars(8, market_sh, code_sh, 0, 200)
if data_sh is None or len(data_sh) == 0:
    print(f"无法获取上证指数K线数据")
else:
    print(f"原始数据示例:")
    print(data_sh[:5])
    
    df_sh = pd.DataFrame(data_sh)
    print(f"\nDataFrame列: {df_sh.columns.tolist()}")
    print(f"datetime列前10个值:")
    print(df_sh['datetime'].head(10))
    
    df_sh['datetime'] = pd.to_datetime(df_sh['datetime'], errors='coerce')
    df_sh = df_sh.dropna(subset=['datetime'])
    
    print(f"\n数据时间范围: {df_sh['datetime'].min()} 到 {df_sh['datetime'].max()}")
    print(f"总数据行数: {len(df_sh)}")
    
    # 筛选指定日期的数据
    df_sh_filtered = df_sh[df_sh['datetime'].dt.strftime('%Y%m%d') == trade_date]
    
    if not df_sh_filtered.empty:
        print(f"指定日期数据行数: {len(df_sh_filtered)}")
        print(f"指定日期数据时间范围: {df_sh_filtered['datetime'].min()} 到 {df_sh_filtered['datetime'].max()}")
        
        # 计算前30分钟数据
        open_start = pd.to_datetime(trade_date, format="%Y%m%d") + pd.Timedelta(hours=9, minutes=30)
        first30_end = pd.to_datetime(trade_date, format="%Y%m%d") + pd.Timedelta(hours=10, minutes=0)
        
        print(f"\n开盘时间: {open_start}")
        print(f"前30分钟结束时间: {first30_end}")
        
        first30 = df_sh_filtered[df_sh_filtered["datetime"] < first30_end].copy()
        print(f"\n前30分钟数据行数: {len(first30)}")
        
        if len(first30) > 0:
            print(f"前30分钟数据:")
            print(first30[['datetime', 'open', 'close', 'high', 'low', 'vol']])
            
            if len(first30) >= 25:
                print(f"\n✓ 前30分钟数据充足，有{len(first30)}条")
                
                # 检查开盘价
                open_px = float(df_sh_filtered["open"].iloc[0] or 0.0)
                print(f"开盘价: {open_px}")
                
                if open_px > 0:
                    # 检查最低价
                    low30 = float(pd.to_numeric(first30.get("low", pd.Series(dtype=float)), errors="coerce").min() or float("nan"))
                    if not (low30 > 0):
                        low30 = float(pd.to_numeric(first30["close"], errors="coerce").min() or float("nan"))
                    
                    print(f"前30分钟最低价: {low30}")
                    
                    if low30 > 0:
                        # 计算跌幅
                        low30_pct = (low30 - open_px) / open_px * 100.0
                        print(f"前30分钟跌幅: {low30_pct:.2f}%")
                        
                        # 检查最新收盘价
                        last_close = float(df_sh_filtered["close"].iloc[-1] or 0.0)
                        print(f"最新收盘价: {last_close}")
                        
                        if last_close > 0:
                            rebound_pct = ((last_close - low30) / low30 * 100.0)
                            print(f"反弹幅度: {rebound_pct:.2f}%")
                            
                            cross_open = last_close >= open_px
                            print(f"是否站上开盘价: {cross_open}")
                            
                            if cross_open:
                                cross_mask = pd.to_numeric(df_sh_filtered["close"], errors="coerce") >= open_px
                                if cross_mask.any():
                                    cross_pos = int(cross_mask.idxmax())
                                    cross_time = df_sh_filtered.loc[cross_pos, "datetime"].to_pydatetime()
                                    cross_time_str = cross_time.strftime("%H:%M")
                                    print(f"站上开盘价时间: {cross_time_str}")
            else:
                print(f"\n✗ 前30分钟数据不足25条，只有{len(first30)}条")
    else:
        print(f"没有找到指定日期 {trade_date} 的数据")