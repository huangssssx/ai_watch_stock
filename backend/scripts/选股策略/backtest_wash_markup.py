
import sys
import os
import akshare as ak
import pandas as pd
import numpy as np
import datetime
import time

# 添加当前目录到 path 以便导入
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from 洗盘_拉升_策略_dev import analyze_df, process_data

def run_backtest(symbols, start_date, end_date):
    """
    简易回测框架
    """
    print(f"Starting backtest from {start_date} to {end_date} for {len(symbols)} stocks...")
    
    total_signals = 0
    wash_end_signals = 0
    markup_signals = 0
    
    results = []
    
    for symbol in symbols:
        try:
            print(f"Processing {symbol}...")
            # 获取数据
            df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
            if df.empty or len(df) < 60:
                continue
                
            # 处理数据
            df = process_data(df)
            
            # 运行策略逻辑
            res = analyze_df(df)
            
            # 统计信号效果 (N日后收益)
            # 信号列: signal_wash_end, signal_markup_start
            
            # 为了计算未来收益，我们需要 shift 收益率
            # future_return_5d: (Close_t+5 - Close_t) / Close_t
            res['future_close_5'] = res['收盘'].shift(-5)
            res['ret_5d'] = (res['future_close_5'] - res['收盘']) / res['收盘']
            
            res['future_close_10'] = res['收盘'].shift(-10)
            res['ret_10d'] = (res['future_close_10'] - res['收盘']) / res['收盘']
            
            # 提取信号触发点
            wash_signals = res[res['signal_wash_end']].copy()
            markup_signals_df = res[res['signal_markup_start']].copy()
            
            if not wash_signals.empty:
                wash_end_signals += len(wash_signals)
                for date, row in wash_signals.iterrows():
                    results.append({
                        'symbol': symbol,
                        'date': row['日期'],
                        'type': 'WASH_END',
                        'ret_5d': row['ret_5d'],
                        'ret_10d': row['ret_10d']
                    })
            
            if not markup_signals_df.empty:
                markup_signals += len(markup_signals_df)
                for date, row in markup_signals_df.iterrows():
                    results.append({
                        'symbol': symbol,
                        'date': row['日期'],
                        'type': 'MARKUP_START',
                        'ret_5d': row['ret_5d'],
                        'ret_10d': row['ret_10d']
                    })
                    
            time.sleep(0.5) # 防止请求过快
            
        except Exception as e:
            print(f"Error processing {symbol}: {e}")
            
    # 汇总结果
    df_res = pd.DataFrame(results)
    
    if df_res.empty:
        print("No signals triggered.")
        return
        
    print("\n" + "="*50)
    print(f"Total Signals: {len(df_res)}")
    print(f"Wash End Signals: {wash_end_signals}")
    print(f"Markup Start Signals: {markup_signals}")
    print("="*50)
    
    # 分组统计
    print("\nPerformance by Signal Type:")
    print(df_res.groupby('type')[['ret_5d', 'ret_10d']].describe())
    
    # 胜率 (5日收益 > 0)
    df_res['win_5d'] = df_res['ret_5d'] > 0
    print("\nWin Rate (5-day > 0):")
    print(df_res.groupby('type')['win_5d'].mean())

if __name__ == "__main__":
    # 测试样本：包含一些近期热门股和随机股
    test_symbols = [
        "600519", "000858", # 白酒
        "300750", "601138", # 新能源/科技 (宁德时代, 工业富联)
        "002230", "002415", # 科技 (科大讯飞, 海康威视)
        "601919", "600030", # 传统 (中远海控, 中信证券)
        "000063", "300059", # 通信/金融 (中兴通讯, 东方财富)
        "603259", "603986", # 医药/其他 (药明康德, 兆易创新)
        "600418", "002049"  # 汽车 (江淮汽车, 紫光国微)
    ]
    
    # 设置回测时间：最近 6 个月
    end = datetime.datetime.now().strftime("%Y%m%d")
    start = (datetime.datetime.now() - datetime.timedelta(days=180)).strftime("%Y%m%d")
    
    run_backtest(test_symbols, start, end)
