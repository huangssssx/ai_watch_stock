#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筹码忽然集中选股脚本
使用tushare的筹码接口，在全市场中找出筹码忽然集中的疑似要涨的股票
"""

import os
import sys
import time
import pandas as pd
import os
from datetime import datetime, timedelta
# 添加项目根目录到路径，以便导入模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# 导入tushare client
from backend.utils.tushare_client import pro

# 全局变量存储缓存数据
cache_data = None
cache_file_path = None

def get_trading_date(n_days_ago=0):
    """
    获取最近的交易日
    :param n_days_ago: 多少天前的交易日
    :return: 交易日期字符串，格式为YYYYMMDD
    """
    today = datetime.now()
    # 先尝试获取今天的日期，如果不是交易日则往前推
    for i in range(30):  # 最多往前推30天
        target_date = today - timedelta(days=i + n_days_ago)
        date_str = target_date.strftime('%Y%m%d')
        try:
            # 使用trade_cal接口检查是否为交易日
            cal_df = pro.trade_cal(exchange='SSE', start_date=date_str, end_date=date_str)
            if not cal_df.empty and cal_df.iloc[0]['is_open'] == 1:
                return date_str
        except Exception as e:
            pass
    raise Exception("无法获取有效的交易日")


def get_trading_dates(n_days=10):
    """
    获取最近的n个交易日
    :param n_days: 需要的交易日数量
    :return: 交易日期字符串列表，格式为YYYYMMDD
    """
    dates = []
    for i in range(n_days * 3):  # 最多尝试3倍天数
        date_str = get_trading_date(i)
        if date_str not in dates:
            dates.append(date_str)
        if len(dates) >= n_days:
            break
    return dates


def get_all_stock_codes():
    """
    获取全市场股票代码列表
    :return: 包含ts_code和name的DataFrame
    """
    try:
        # 获取当前所有正常上市交易的股票列表
        df = pro.stock_basic(
            exchange='',
            list_status='L',
            fields='ts_code,name'
        )
        print(f"获取到 {len(df)} 只股票")
        return df
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        return pd.DataFrame()


def get_chip_data(ts_code, start_date, end_date):
    """
    获取指定股票的筹码数据
    :param ts_code: 股票代码
    :param start_date: 开始日期
    :param end_date: 结束日期
    :return: 筹码数据DataFrame
    """
    global cache_data, cache_file_path
    # 缓存文件路径 - 单个文件存储所有股票数据
    cache_dir = "../data"
    os.makedirs(cache_dir, exist_ok=True)
    
    # 生成缓存文件名，包含日期范围
    today = datetime.now().strftime('%Y%m%d')
    cache_file = f"{cache_dir}/all_chip_data_{today}.csv"
    
    # 检查是否需要加载缓存数据
    if cache_data is None or cache_file_path != cache_file:
        if os.path.exists(cache_file):
            try:
                # 从缓存文件中读取所有数据
                all_chip_df = pd.read_csv(cache_file)
                # 确保trade_date列是字符串类型
                all_chip_df['trade_date'] = all_chip_df['trade_date'].astype(str)
                cache_data = all_chip_df
                cache_file_path = cache_file
                print(f"缓存文件已加载到内存: {cache_file}")
            except Exception as e:
                print(f"读取缓存文件失败: {e}")
                cache_data = pd.DataFrame()
                cache_file_path = cache_file
        else:
            cache_data = pd.DataFrame()
            cache_file_path = cache_file
    
    # 筛选当前股票的数据
    chip_df = pd.DataFrame()
    if not cache_data.empty and 'ts_code' in cache_data.columns:
        chip_df = cache_data[
            (cache_data['ts_code'] == ts_code) &
            (cache_data['trade_date'] >= start_date) &
            (cache_data['trade_date'] <= end_date)
        ]
    if not chip_df.empty:
        print(f"从缓存读取{ts_code}筹码数据")
        return chip_df
    
    try:
        # 调用tushare接口获取筹码数据
        print(f"获取{ts_code}筹码数据")
        chip_df = pro.cyq_perf(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date
        )
        
        # 保存到缓存文件和内存
        if not chip_df.empty:
            # 检查缓存文件是否存在
            if os.path.exists(cache_file):
                # 追加模式写入
                chip_df.to_csv(cache_file, mode='a', header=False, index=False)
            else:
                # 新建文件写入
                chip_df.to_csv(cache_file, index=False)
            # 更新内存缓存
            if cache_data.empty:
                cache_data = chip_df
            else:
                cache_data = pd.concat([cache_data, chip_df], ignore_index=True)
            print(f"{ts_code}筹码数据已保存到缓存")
        
        return chip_df
    except Exception as e:
        print(f"获取{ts_code}筹码数据失败: {e}")
        return pd.DataFrame()


def get_money_flow_data(ts_codes, trade_date):
    """获取股票资金流向数据"""
    try:
        # 确保trade_date是字符串类型
        trade_date_str = str(trade_date)
        # 分批获取数据，每次最多200只股票
        batch_size = 200
        result = []
        
        for i in range(0, len(ts_codes), batch_size):
            batch_codes = ts_codes[i:i+batch_size]
            df = pro.moneyflow_dc(ts_code=','.join(batch_codes), trade_date=trade_date_str)
            result.append(df)
            time.sleep(0.5)  # 避免请求过于频繁
        
        if result:
            return pd.concat(result, ignore_index=True)
        return pd.DataFrame()
    except Exception as e:
        print(f"获取资金流向数据失败: {e}")
        return pd.DataFrame()


def get_volume_data(ts_codes, trade_date, days=5):
    """获取股票成交量数据，计算成交量放大情况"""
    try:
        # 确保trade_date是字符串类型
        trade_date_str = str(trade_date)
        # 计算开始日期
        end_date = trade_date_str
        start_date = (datetime.strptime(trade_date_str, '%Y%m%d') - timedelta(days=days)).strftime('%Y%m%d')
        
        # 分批获取数据
        batch_size = 200
        result = []
        
        for i in range(0, len(ts_codes), batch_size):
            batch_codes = ts_codes[i:i+batch_size]
            df = pro.daily(ts_code=','.join(batch_codes), start_date=start_date, end_date=end_date)
            result.append(df)
            time.sleep(0.5)  # 避免请求过于频繁
        
        if result:
            return pd.concat(result, ignore_index=True)
        return pd.DataFrame()
    except Exception as e:
        print(f"获取成交量数据失败: {e}")
        return pd.DataFrame()


# 策略配置对象
STRATEGIES = {
    "稳健型": {
        "name": "稳健型",
        "description": "适合长周期横盘蓄势的股票，风险较低",
        "params": {
            "cost_range_slope_threshold": -0.01,  # 成本范围斜率阈值（温和）
            "cost_range_change_rate_threshold": -5,  # 成本范围变化率阈值
            "winner_rate_slope_threshold": 0.1,  # 胜率斜率阈值（温和）
            "winner_rate_change_rate_threshold": 10,  # 胜率变化率阈值
            "weight_avg_change_rate_threshold": 15,  # 加权平均成本变化率阈值
            "min_cost_range_percent": 5,  # 最小成本范围百分比（相对于股价）
            "max_cost_range_percent": 30,  # 最大成本范围百分比（相对于股价）
            "min_winner_rate": 50,  # 最小胜率
            "max_weight_avg_diff_percent": 10  # 股价与加权平均成本差异百分比
        }
    },
    "激进型": {
        "name": "激进型",
        "description": "适合即将爆发的股票，捕捉启动瞬间",
        "params": {
            "cost_range_slope_threshold": -0.05,  # 成本范围斜率阈值（陡峭）
            "cost_range_change_rate_threshold": -10,  # 成本范围变化率阈值
            "winner_rate_slope_threshold": 0.5,  # 胜率斜率阈值（陡峭）
            "winner_rate_change_rate_threshold": 50,  # 胜率变化率阈值
            "weight_avg_change_rate_threshold": 20,  # 加权平均成本变化率阈值
            "min_cost_range_percent": 3,  # 最小成本范围百分比（相对于股价）
            "max_cost_range_percent": 20,  # 最大成本范围百分比（相对于股价）
            "min_winner_rate": 60,  # 最小胜率
            "max_weight_avg_diff_percent": 15  # 股价与加权平均成本差异百分比
        }
    },
    "平衡型": {
        "name": "平衡型",
        "description": "平衡风险和收益，适合大多数市场环境",
        "params": {
            "cost_range_slope_threshold": -0.02,  # 成本范围斜率阈值
            "cost_range_change_rate_threshold": -7,  # 成本范围变化率阈值
            "winner_rate_slope_threshold": 0.2,  # 胜率斜率阈值
            "winner_rate_change_rate_threshold": 20,  # 胜率变化率阈值
            "weight_avg_change_rate_threshold": 15,  # 加权平均成本变化率阈值
            "min_cost_range_percent": 4,  # 最小成本范围百分比（相对于股价）
            "max_cost_range_percent": 25,  # 最大成本范围百分比（相对于股价）
            "min_winner_rate": 55,  # 最小胜率
            "max_weight_avg_diff_percent": 12  # 股价与加权平均成本差异百分比
        }
    },
    "暴力型": {
        "name": "暴力型",
        "description": "捕捉暴力连板和V型反转的股票，风险较高",
        "params": {
            "cost_range_slope_threshold": -0.1,  # 成本范围斜率阈值（非常陡峭）
            "cost_range_change_rate_threshold": -15,  # 成本范围变化率阈值
            "winner_rate_slope_threshold": 1.0,  # 胜率斜率阈值（非常陡峭）
            "winner_rate_change_rate_threshold": 100,  # 胜率变化率阈值
            "weight_avg_change_rate_threshold": 25,  # 加权平均成本变化率阈值
            "min_cost_range_percent": 2,  # 最小成本范围百分比（相对于股价）
            "max_cost_range_percent": 15,  # 最大成本范围百分比（相对于股价）
            "min_winner_rate": 70,  # 最小胜率
            "max_weight_avg_diff_percent": 20  # 股价与加权平均成本差异百分比
        }
    }
}

def is_chip_concentrated(chip_df, strategy_name="平衡型"):
    """
    分析筹码是否集中
    
    Args:
        chip_df: 筹码数据DataFrame
        strategy_name: 策略名称，可选值：稳健型、激进型、平衡型、暴力型
    
    Returns:
        tuple: (是否集中, 相关指标)
    """
    # 获取策略参数
    strategy = STRATEGIES.get(strategy_name, STRATEGIES["平衡型"])
    params = strategy["params"]
    
    # 确保数据按日期降序排列
    chip_df = chip_df.sort_values('trade_date', ascending=False)
    
    # 只使用最近的20个交易日数据
    recent_data = chip_df.head(20)
    
    # 计算成本范围（95分位成本 - 5分位成本）
    # 使用.loc避免SettingWithCopyWarning
    recent_data = recent_data.copy()
    recent_data.loc[:, 'cost_range'] = recent_data['cost_95pct'] - recent_data['cost_5pct']
    
    # 计算趋势（使用线性回归的斜率）
    from scipy import stats
    import numpy as np
    
    # 准备数据进行线性回归
    x = np.arange(len(recent_data))[::-1]  # 时间序列，从早到晚
    
    # 成本范围趋势
    slope_cost_range, _, _, _, _ = stats.linregress(x, recent_data['cost_range'])
    cost_range_trend = "下降" if slope_cost_range < 0 else "上升"
    
    # 胜率趋势
    slope_winner_rate, _, _, _, _ = stats.linregress(x, recent_data['winner_rate'])
    winner_rate_trend = "上升" if slope_winner_rate > 0 else "下降"
    
    # 加权平均成本趋势
    slope_weight_avg, _, _, _, _ = stats.linregress(x, recent_data['weight_avg'])
    weight_avg_trend = "上升" if slope_weight_avg > 0 else "下降"
    
    # 计算变化率
    first_cost_range = recent_data['cost_range'].iloc[-1]  # 最早的数据
    last_cost_range = recent_data['cost_range'].iloc[0]   # 最新的数据
    cost_range_change = last_cost_range - first_cost_range
    cost_range_change_rate = (cost_range_change / first_cost_range) * 100 if first_cost_range != 0 else 0
    
    first_winner_rate = recent_data['winner_rate'].iloc[-1]
    last_winner_rate = recent_data['winner_rate'].iloc[0]
    winner_rate_change = last_winner_rate - first_winner_rate
    winner_rate_change_rate = (winner_rate_change / first_winner_rate) * 100 if first_winner_rate != 0 else 0
    
    first_weight_avg = recent_data['weight_avg'].iloc[-1]
    last_weight_avg = recent_data['weight_avg'].iloc[0]
    weight_avg_change = last_weight_avg - first_weight_avg
    weight_avg_change_rate = (weight_avg_change / first_weight_avg) * 100 if first_weight_avg != 0 else 0
    
    # 分析期间
    start_date = recent_data['trade_date'].iloc[-1]
    end_date = recent_data['trade_date'].iloc[0]
    
    # 获取最新股价（使用95分位成本作为近似）
    latest_price = recent_data['cost_95pct'].iloc[0]
    # 计算成本范围占股价的百分比
    cost_range_percent = (last_cost_range / latest_price) * 100 if latest_price != 0 else 0
    
    # 计算股价与加权平均成本的差异百分比
    weight_avg_diff_percent = ((latest_price - last_weight_avg) / last_weight_avg) * 100 if last_weight_avg != 0 else 0
    
    # 筹码集中的条件判断
    is_concentrated = (
        # 成本范围趋势为下降（筹码集中）
        slope_cost_range < params["cost_range_slope_threshold"] and 
        # 成本范围变化率小于阈值（有所集中）
        cost_range_change_rate < params["cost_range_change_rate_threshold"] and 
        # 胜率趋势为上升（市场情绪向好）
        slope_winner_rate > params["winner_rate_slope_threshold"] and 
        # 胜率变化率大于阈值（有所向好）
        winner_rate_change_rate > params["winner_rate_change_rate_threshold"] and 
        # 加权平均成本相对稳定（变化率小于阈值）
        abs(weight_avg_change_rate) < params["weight_avg_change_rate_threshold"] and 
        # 成本范围占股价的百分比在合理范围内
        params["min_cost_range_percent"] < cost_range_percent < params["max_cost_range_percent"] and 
        # 胜率达到一定水平
        last_winner_rate > params["min_winner_rate"] and 
        # 股价与加权平均成本的差异在合理范围内
        abs(weight_avg_diff_percent) < params["max_weight_avg_diff_percent"]
    )
    
    # 返回相关指标
    indicators = {
        'ts_code': recent_data['ts_code'].iloc[0],
        'strategy': strategy['name'],
        'cost_range_start': first_cost_range,
        'cost_range_end': last_cost_range,
        'cost_range_change': cost_range_change,
        'cost_range_change_rate': cost_range_change_rate,
        'cost_range_slope': slope_cost_range,
        'cost_range_trend': cost_range_trend,
        'cost_range_percent': cost_range_percent,
        'winner_rate_start': first_winner_rate,
        'winner_rate_end': last_winner_rate,
        'winner_rate_change': winner_rate_change,
        'winner_rate_change_rate': winner_rate_change_rate,
        'winner_rate_slope': slope_winner_rate,
        'winner_rate_trend': winner_rate_trend,
        'weight_avg_start': first_weight_avg,
        'weight_avg_end': last_weight_avg,
        'weight_avg_change': weight_avg_change,
        'weight_avg_change_rate': weight_avg_change_rate,
        'weight_avg_slope': slope_weight_avg,
        'weight_avg_trend': weight_avg_trend,
        'weight_avg_diff_percent': weight_avg_diff_percent,
        'start_date': start_date,
        'end_date': end_date,
        'latest_price': latest_price
    }
    
    return is_concentrated, indicators


def main():
    """
    主函数
    """
    print("开始执行筹码忽然集中选股策略...")
    
    # 选择策略
    strategy_name = "平衡型"  # 可切换为：稳健型、激进型、平衡型、暴力型
    print(f"使用策略: {STRATEGIES[strategy_name]['name']} - {STRATEGIES[strategy_name]['description']}")
    
    # 获取最近的20个交易日
    trading_dates = get_trading_dates(20)
    print(f"最近的20个交易日: {trading_dates}")
    
    # 使用最近的20个交易日
    latest_date = trading_dates[0]
    # 获取更早的日期作为开始日期，确保能获取到足够的数据
    start_date = trading_dates[-1]  # 最早的交易日
    print(f"使用的日期范围: 开始 {start_date}, 结束 {latest_date}")
    
    # 获取全市场股票代码
    print("获取全市场股票代码...")
    stock_list = get_all_stock_codes()
    if stock_list.empty:
        print("没有获取到股票列表，退出")
        return
    print(f"共获取到 {len(stock_list)} 只股票")
    
    # 存储符合条件的股票
    concentrated_stocks = []
    
    # 遍历股票列表，获取筹码数据并分析
    total = len(stock_list)
    # 为了测试，先只分析前10只股票
    test_mode = False  # 设置为True以启用测试模式，只分析前10只股票
    max_stocks = 10 if test_mode else total
    
    for i, row in stock_list.iterrows():
        if i >= max_stocks:
            break
            
        ts_code = row['ts_code']
        name = row['name']
        
        print(f"分析 {i+1}/{max_stocks}: {ts_code} {name}")
        
        # 获取筹码数据
        chip_df = get_chip_data(ts_code, start_date, latest_date)
        if chip_df.empty:
            print(f"  筹码数据为空，跳过")
            continue
        
        # 分析筹码是否集中
        is_concentrated, indicators = is_chip_concentrated(chip_df, strategy_name)
        if is_concentrated:
            indicators['stock_name'] = name
            concentrated_stocks.append(indicators)
            print(f"发现筹码集中趋势股票: {ts_code} {name}")
    
    # 保存结果到CSV文件
    if concentrated_stocks:
        result_df = pd.DataFrame(concentrated_stocks)
        # 按成本范围变化率排序（变化率越小，筹码集中程度越高）
        result_df = result_df.sort_values('cost_range_change_rate')
        
        # 生成带日期和策略的文件名
        output_file = f"筹码忽然集中_{strategy_name}_{latest_date}.csv"
        result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n分析完成，共发现 {len(concentrated_stocks)} 只筹码集中趋势的股票")
        print(f"结果已保存到 {output_file}")
        
        # 打印前5只股票的简要信息
        print("\n前5只筹码集中趋势最明显的股票:")
        for i, row in result_df.head(5).iterrows():
            print(f"{i+1}. {row['ts_code']} {row.get('stock_name', '')}")
            print(f"   成本范围变化率: {row['cost_range_change_rate']:.2f}%, 胜率变化率: {row['winner_rate_change_rate']:.2f}%")
            print()
        
        # 二次筛选：寻找启动标的
        print("\n开始二次筛选：寻找启动标的")
        
        # 筛选条件
        print("\n筛选条件：")
        print("1. 成本范围变化率 < -10% （筹码集中程度更高）")
        print("2. 胜率变化率 > 30% （市场情绪向好更明显）")
        print("3. 股价与加权平均成本差异 < 5% （主力未大幅获利，拉抬动力足）")
        
        # 应用筛选条件
        filtered_df = result_df[
            (result_df['cost_range_change_rate'] < -10) &  # 成本范围变化率 < -10%
            (result_df['winner_rate_change_rate'] > 30) &  # 胜率变化率 > 30%
            (result_df['weight_avg_diff_percent'] < 5)      # 股价与加权平均成本差异 < 5%
        ]
        
        # 打印筛选后的数据行数
        print(f"\n筛选后数据行数：{len(filtered_df)}")
        
        # 按成本范围变化率降序排序（负值越小，集中程度越高）
        filtered_df = filtered_df.sort_values(by='cost_range_change_rate', ascending=True)
        
        # 重置索引
        filtered_df = filtered_df.reset_index(drop=True)
        
        # 获取最近的交易日
        latest_trading_date = latest_date
        print(f"\n使用最近交易日数据：{latest_trading_date}")
        
        # 如果有筛选结果，进行资金流向和成交量核查
        if len(filtered_df) > 0:
            # 提取股票代码列表
            stock_codes = filtered_df['ts_code'].tolist()
            print(f"\n正在获取 {len(stock_codes)} 只股票的资金流向和成交量数据...")
            
            # 获取资金流向数据
            money_flow_df = get_money_flow_data(stock_codes, latest_trading_date)
            
            # 获取成交量数据
            volume_df = get_volume_data(stock_codes, latest_trading_date)
            
            # 合并数据
            if not money_flow_df.empty:
                filtered_df = filtered_df.merge(money_flow_df[['ts_code', 'net_amount', 'net_amount_rate']], on='ts_code', how='left')
            else:
                filtered_df['net_amount'] = 0
                filtered_df['net_amount_rate'] = 0
            
            # 计算成交量放大情况
            if not volume_df.empty:
                # 计算每只股票的平均成交量和当日成交量
                volume_stats = volume_df.groupby('ts_code').agg(
                    avg_volume=('vol', 'mean'),
                    latest_volume=('vol', 'last')
                ).reset_index()
                # 计算成交量放大倍数
                volume_stats['volume_ratio'] = volume_stats['latest_volume'] / volume_stats['avg_volume']
                # 合并到主数据框
                filtered_df = filtered_df.merge(volume_stats[['ts_code', 'volume_ratio', 'latest_volume']], on='ts_code', how='left')
            else:
                filtered_df['volume_ratio'] = 1.0
                filtered_df['latest_volume'] = 0
            
            # 应用资金流向和成交量筛选条件
            print("\n应用资金流向和成交量筛选条件：")
            print("1. 主力净流入 > 0 （资金流入）")
            print("2. 成交量放大倍数 > 1.2 （有量）")
            
            final_df = filtered_df[
                (filtered_df['net_amount'] > 0) &  # 主力净流入
                (filtered_df['volume_ratio'] > 1.2)  # 成交量放大
            ]
            
            # 按主力净流入和成交量放大倍数排序
            final_df = final_df.sort_values(by=['net_amount', 'volume_ratio'], ascending=[False, False])
            final_df = final_df.reset_index(drop=True)
            
            # 保存最终结果
            output_file_launch = f"筹码集中启动标的_{strategy_name}_{latest_date}.csv"
            print(f"\n正在保存最终筛选结果到：{output_file_launch}")
            final_df.to_csv(output_file_launch, index=False, encoding='utf-8-sig')
            
            # 打印前10行结果
            print("\n最终筛选结果前10行：")
            print(final_df.head(10))
            
            # 打印总结
            print(f"\n筛选完成！共筛选出 {len(final_df)} 只符合条件的股票。")
            print(f"结果已保存到：{output_file_launch}")
            
            # 打印资金流向和成交量统计
            if not final_df.empty:
                print("\n资金流向和成交量统计：")
                print(f"平均主力净流入：{final_df['net_amount'].mean():.2f} 万元")
                print(f"平均成交量放大倍数：{final_df['volume_ratio'].mean():.2f} 倍")
        else:
            # 保存空结果
            output_file_launch = f"筹码集中启动标的_{strategy_name}_{latest_date}.csv"
            print(f"\n正在保存筛选结果到：{output_file_launch}")
            filtered_df.to_csv(output_file_launch, index=False, encoding='utf-8-sig')
            
            # 打印总结
            print(f"\n筛选完成！共筛选出 {len(filtered_df)} 只符合条件的股票。")
            print(f"结果已保存到：{output_file_launch}")
    else:
        print("\n未发现筹码集中趋势的股票")
        
        # 保存空结果
        output_file_launch = f"筹码集中启动标的_{strategy_name}_{latest_date}.csv"
        pd.DataFrame().to_csv(output_file_launch, index=False, encoding='utf-8-sig')
        print(f"\n筛选完成！共筛选出 0 只符合条件的股票。")
        print(f"结果已保存到：{output_file_launch}")
    



if __name__ == "__main__":
    main()