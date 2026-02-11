#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
下载全市场股票筹码数据脚本
功能：调用 tushare pro.cyq_perf 接口，获取全市场股票的历史筹码数据并保存到本地 CSV 文件
"""

import os
import sys
import time
import pandas as pd
from datetime import datetime, timedelta

# 添加项目根目录到 Python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from backend.utils.tushare_client import pro

# 检查 pro 对象是否初始化成功
if pro is None:
    print("错误：Tushare 客户端初始化失败，无法继续")
    sys.exit(1)


def get_all_stock_codes():
    """
    获取全市场股票代码列表
    """
    print("正在获取全市场股票代码...")
    # 获取所有股票代码
    df = pro.stock_basic(exchange='', list_status='L', fields='ts_code, name')
    print(f"共获取到 {len(df)} 只股票")
    return df


def get_stock_chip_data(ts_code, start_date, end_date):
    """
    获取单只股票的筹码数据
    :param ts_code: 股票代码
    :param start_date: 开始日期
    :param end_date: 结束日期
    :return: 筹码数据 DataFrame
    """
    try:
        # 调用 cyq_perf 接口获取筹码数据
        df = pro.cyq_perf(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date
        )
        return df
    except Exception as e:
        print(f"获取 {ts_code} 筹码数据失败: {e}")
        return pd.DataFrame()


def main():
    """
    主函数：下载全市场筹码数据
    """
    # 设置日期范围
    end_date = datetime.now().strftime('%Y%m%d')
    # 设置开始日期为一年前，可根据需要调整
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
    
    print(f"开始下载全市场筹码数据...")
    print(f"日期范围: {start_date} 到 {end_date}")
    
    # 获取全市场股票代码
    stock_list = get_all_stock_codes()
    
    # 限制处理的股票数量，以便快速测试
    # 如需处理全市场股票，注释掉下面两行
    # test_limit = 10
    # stock_list = stock_list.head(test_limit)
    # print(f"为了测试，仅处理前 {test_limit} 只股票")
    
    # 定义输出文件路径
    output_file = f"筹码数据_{start_date}_{end_date}.csv"
    
    # 检查文件是否存在，不存在则创建并写入表头
    if not os.path.exists(output_file):
        # 创建一个空的 DataFrame 并写入表头
        empty_df = pd.DataFrame(columns=['ts_code', 'trade_date', 'close', 'cyq_low', 'cyq_high', 
                                        'cyq_zhong', 'cyq_perf10', 'cyq_perf20', 'cyq_perf30', 
                                        'cyq_perf60', 'cyq_perf90', 'cyq_perf120', 'cyq_perf150', 
                                        'cyq_perf180'])
        empty_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"已创建输出文件: {output_file}")
    
    # 遍历所有股票
    total_stocks = len(stock_list)
    success_count = 0
    fail_count = 0
    
    for i, (_, stock) in enumerate(stock_list.iterrows()):
        ts_code = stock['ts_code']
        name = stock['name']
        
        print(f"\n处理第 {i+1}/{total_stocks} 只股票: {ts_code} {name}")
        
        try:
            # 获取该股票的筹码数据
            chip_data = get_stock_chip_data(ts_code, start_date, end_date)
            
            if not chip_data.empty:
                print(f"  成功获取 {len(chip_data)} 条数据")
                # 追加数据到 CSV 文件
                chip_data.to_csv(output_file, mode='a', index=False, header=False, encoding='utf-8-sig')
                success_count += 1
            else:
                print(f"  未获取到数据")
                fail_count += 1
        except Exception as e:
            print(f"  处理失败: {e}")
            fail_count += 1
        
        # 添加延时，避免触发 API 调用频率限制
        time.sleep(0.1)
    
    # 打印统计信息
    print(f"\n下载完成！")
    print(f"成功处理: {success_count} 只股票")
    print(f"失败处理: {fail_count} 只股票")
    print(f"数据已保存到: {output_file}")
    
    # 检查文件大小
    if os.path.exists(output_file):
        file_size = os.path.getsize(output_file) / (1024 * 1024)
        print(f"文件大小: {file_size:.2f} MB")


if __name__ == "__main__":
    main()