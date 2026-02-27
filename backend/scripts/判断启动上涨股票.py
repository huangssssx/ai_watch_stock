#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
判断最近一个交易日启动，下个交易日会涨的股票

脚本目标：
- 从全市场 A 股中，筛选出最近一个交易日启动的股票
- 基于多种指标综合判断，预测这些股票在下个交易日是否会上涨
- 输出最终的候选股票列表

数据来源：
- tushare API：获取股票基本信息、日线数据、资金流向数据

判断维度：
1. 成交量：启动日放量明显
2. 价格走势：启动日涨幅较大且收在高位
3. 资金流向：主力资金净流入
4. 技术指标：MACD、KDJ等指标配合
5. 筹码分布：筹码集中且上方压力小
"""

import os
import sys
import time
import pandas as pd
from datetime import datetime, timedelta

# 添加项目根目录到路径，以便导入模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# 导入tushare client
from backend.utils.tushare_client import pro

# 策略配置
STRATEGY_CONFIG = {
    # 成交量放大倍数阈值
    "volume_ratio_min": 1.5,
    # 启动日涨幅阈值（百分比）
    "price_change_min": 3.0,
    # 主力资金净流入阈值（万元）
    "net_amount_min": 1000,
    # 主力资金净流入率阈值（百分比）
    "net_amount_rate_min": 0.5,
    # MACD金叉阈值
    "macd_golden_cross": True,
    # KDJ金叉阈值
    "kdj_golden_cross": True,
    # 启动日收盘价相对于最高价的比例阈值
    "close_to_high_ratio_min": 0.95,
    # 启动日相对于前5日均价的涨幅阈值（百分比）
    "price_to_ma5_ratio_min": 3.0
}

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

def calculate_macd(df, fast_period=12, slow_period=26, signal_period=9):
    """
    计算MACD指标
    :param df: 包含close价格的DataFrame
    :param fast_period: 快线周期
    :param slow_period: 慢线周期
    :param signal_period: 信号线周期
    :return: 添加了MACD指标的DataFrame
    """
    # 计算EMA
    df['ema_fast'] = df['close'].ewm(span=fast_period, adjust=False).mean()
    df['ema_slow'] = df['close'].ewm(span=slow_period, adjust=False).mean()
    
    # 计算DIF和DEA
    df['dif'] = df['ema_fast'] - df['ema_slow']
    df['dea'] = df['dif'].ewm(span=signal_period, adjust=False).mean()
    
    # 计算MACD柱状图
    df['macd_hist'] = 2 * (df['dif'] - df['dea'])
    
    return df

def calculate_kdj(df, n=9, m1=3, m2=3):
    """
    计算KDJ指标
    :param df: 包含high、low、close价格的DataFrame
    :param n: KDJ周期
    :param m1: 第一个平滑系数
    :param m2: 第二个平滑系数
    :return: 添加了KDJ指标的DataFrame
    """
    # 计算RSV
    df['low_n'] = df['low'].rolling(window=n).min()
    df['high_n'] = df['high'].rolling(window=n).max()
    df['rsv'] = (df['close'] - df['low_n']) / (df['high_n'] - df['low_n']) * 100
    
    # 计算K、D、J值
    df['k'] = df['rsv'].ewm(alpha=1/m1, adjust=False).mean()
    df['d'] = df['k'].ewm(alpha=1/m2, adjust=False).mean()
    df['j'] = 3 * df['k'] - 2 * df['d']
    
    return df

def is_macd_golden_cross(df):
    """
    判断是否MACD金叉
    :param df: 包含MACD指标的DataFrame
    :return: 是否金叉
    """
    if len(df) < 2:
        return False
    
    # 金叉条件：DIF从下往上穿过DEA，且MACD柱状图由负转正
    return (df['dif'].iloc[-2] < df['dea'].iloc[-2]) and \
           (df['dif'].iloc[-1] > df['dea'].iloc[-1]) and \
           (df['macd_hist'].iloc[-2] < 0) and \
           (df['macd_hist'].iloc[-1] > 0)

def is_kdj_golden_cross(df):
    """
    判断是否KDJ金叉
    :param df: 包含KDJ指标的DataFrame
    :return: 是否金叉
    """
    if len(df) < 2:
        return False
    
    # 金叉条件：K值从下往上穿过D值
    return (df['k'].iloc[-2] < df['d'].iloc[-2]) and \
           (df['k'].iloc[-1] > df['d'].iloc[-1])

def get_stock_data_batch(ts_codes, start_date, end_date, batch_size=50):
    """
    批量获取股票日线数据
    :param ts_codes: 股票代码列表
    :param start_date: 开始日期
    :param end_date: 结束日期
    :param batch_size: 每批获取的股票数量
    :return: 日线数据DataFrame
    """
    all_data = []
    
    # 分批获取数据
    for i in range(0, len(ts_codes), batch_size):
        batch_codes = ts_codes[i:i+batch_size]
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                # 将股票代码列表转换为逗号分隔的字符串
                codes_str = ','.join(batch_codes)
                
                df = pro.daily(
                    ts_code=codes_str,
                    start_date=start_date,
                    end_date=end_date,
                    fields='ts_code,trade_date,open,high,low,close,vol,amount,change,pct_chg'
                )
                
                if not df.empty:
                    all_data.append(df)
                    print(f"  成功获取 {len(df['ts_code'].unique())} 只股票的数据")
                
                # 添加请求间隔，避免请求过于频繁
                time.sleep(2)
                break
            except Exception as e:
                retry_count += 1
                print(f"批量获取股票数据失败 (重试 {retry_count}/{max_retries}): {e}")
                # 增加重试间隔
                time.sleep(3)
                
                # 如果是最后一次重试，尝试单独获取
                if retry_count == max_retries:
                    print("  尝试单独获取每只股票的数据...")
                    for code in batch_codes:
                        try:
                            df = pro.daily(
                                ts_code=code,
                                start_date=start_date,
                                end_date=end_date,
                                fields='ts_code,trade_date,open,high,low,close,vol,amount,change,pct_chg'
                            )
                            if not df.empty:
                                all_data.append(df)
                                print(f"    成功获取 {code} 的数据")
                            time.sleep(0.8)
                        except Exception as e:
                            print(f"    获取{code}日线数据失败: {e}")
                            time.sleep(0.8)
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        # 按股票代码和日期排序
        combined_df = combined_df.sort_values(['ts_code', 'trade_date'], ascending=[True, True])
        return combined_df
    else:
        return pd.DataFrame()

def get_stock_data(ts_code, start_date, end_date):
    """
    获取指定股票的日线数据
    :param ts_code: 股票代码
    :param start_date: 开始日期
    :param end_date: 结束日期
    :return: 日线数据DataFrame
    """
    try:
        df = pro.daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields='ts_code,trade_date,open,high,low,close,vol,amount,change,pct_chg'
        )
        # 按日期排序
        df = df.sort_values('trade_date', ascending=True)
        # 添加请求间隔，避免请求过于频繁
        time.sleep(0.3)
        return df
    except Exception as e:
        print(f"获取{ts_code}日线数据失败: {e}")
        time.sleep(0.5)
        return pd.DataFrame()

def get_money_flow_data(ts_codes, trade_date):
    """
    获取股票资金流向数据
    :param ts_codes: 股票代码列表
    :param trade_date: 交易日期
    :return: 资金流向数据DataFrame
    """
    try:
        # 分批获取数据，每次最多200只股票
        batch_size = 200
        result = []
        
        for i in range(0, len(ts_codes), batch_size):
            batch_codes = ts_codes[i:i+batch_size]
            df = pro.moneyflow_dc(ts_code=','.join(batch_codes), trade_date=trade_date)
            result.append(df)
            time.sleep(0.5)  # 避免请求过于频繁
        
        if result:
            return pd.concat(result, ignore_index=True)
        return pd.DataFrame()
    except Exception as e:
        print(f"获取资金流向数据失败: {e}")
        return pd.DataFrame()

def is_stock_started(df, config):
    """
    判断股票是否在最近一个交易日启动
    :param df: 包含日线数据的DataFrame
    :param config: 策略配置
    :return: (是否启动, 相关指标)
    """
    if len(df) < 6:  # 需要至少6天数据（5天前的数据用于计算均值）
        return False, {}
    
    # 获取最近一天的数据
    latest_data = df.iloc[-1]
    # 获取前一天的数据
    previous_data = df.iloc[-2]
    # 获取前5天的平均收盘价
    ma5 = df['close'].iloc[-6:-1].mean()
    
    # 计算指标
    indicators = {
        'price_change': latest_data['pct_chg'],
        'volume_ratio': latest_data['vol'] / df['vol'].iloc[-6:-1].mean(),
        'close_to_high_ratio': latest_data['close'] / latest_data['high'],
        'price_to_ma5_ratio': (latest_data['close'] - ma5) / ma5 * 100,
        'is_macd_golden_cross': is_macd_golden_cross(df),
        'is_kdj_golden_cross': is_kdj_golden_cross(df)
    }
    
    # 判断是否启动
    is_started = (
        # 涨幅达到阈值
        indicators['price_change'] >= config['price_change_min'] and
        # 成交量放大达到阈值
        indicators['volume_ratio'] >= config['volume_ratio_min'] and
        # 收盘价接近最高价
        indicators['close_to_high_ratio'] >= config['close_to_high_ratio_min'] and
        # 相对于前5日均价有明显涨幅
        indicators['price_to_ma5_ratio'] >= config['price_to_ma5_ratio_min']
    )
    
    # 如果配置了MACD金叉要求
    if config['macd_golden_cross']:
        is_started = is_started and indicators['is_macd_golden_cross']
    
    # 如果配置了KDJ金叉要求
    if config['kdj_golden_cross']:
        is_started = is_started and indicators['is_kdj_golden_cross']
    
    return is_started, indicators

def predict_next_day_rise(stock_data, money_flow_data, config):
    """
    预测股票在下个交易日是否会上涨
    :param stock_data: 股票日线数据
    :param money_flow_data: 资金流向数据
    :param config: 策略配置
    :return: (是否预测上涨, 预测信心指数)
    """
    if stock_data.empty:
        return False, 0
    
    # 基础信心指数
    confidence = 50
    
    # 获取最近一天的数据
    latest_data = stock_data.iloc[-1]
    
    # 1. 基于涨幅的信心调整
    if latest_data['pct_chg'] > 5:
        confidence += 10
    elif latest_data['pct_chg'] > 3:
        confidence += 5
    
    # 2. 基于成交量的信心调整
    volume_ratio = latest_data['vol'] / stock_data['vol'].iloc[-6:-1].mean()
    if volume_ratio > 3:
        confidence += 15
    elif volume_ratio > 2:
        confidence += 10
    elif volume_ratio > 1.5:
        confidence += 5
    
    # 3. 基于收盘价位置的信心调整
    close_to_high_ratio = latest_data['close'] / latest_data['high']
    if close_to_high_ratio > 0.98:
        confidence += 10
    elif close_to_high_ratio > 0.95:
        confidence += 5
    
    # 4. 基于资金流向的信心调整
    if not money_flow_data.empty:
        mf = money_flow_data.iloc[0]
        if mf['net_amount'] > config['net_amount_min']:
            confidence += 15
        elif mf['net_amount'] > 0:
            confidence += 5
        
        if mf['net_amount_rate'] > config['net_amount_rate_min']:
            confidence += 10
        elif mf['net_amount_rate'] > 0:
            confidence += 5
    
    # 5. 基于技术指标的信心调整
    if is_macd_golden_cross(stock_data):
        confidence += 10
    
    if is_kdj_golden_cross(stock_data):
        confidence += 5
    
    # 6. 基于价格趋势的信心调整
    if len(stock_data) >= 3:
        # 检查是否连续上涨
        if stock_data['pct_chg'].iloc[-3:].mean() > 0:
            confidence += 5
        
        # 检查是否价格创新高
        if latest_data['close'] > stock_data['close'].iloc[-6:-1].max():
            confidence += 10
    
    # 判断是否预测上涨（信心指数大于70）
    is_predicted_rise = confidence >= 70
    
    return is_predicted_rise, confidence

def main():
    """
    主函数
    """
    print("开始执行判断启动上涨股票策略...")
    
    # 获取最近的交易日
    latest_trading_date = get_trading_date()
    print(f"最近的交易日: {latest_trading_date}")
    
    # 获取前6天的交易日（用于计算指标）
    start_date = (datetime.strptime(latest_trading_date, '%Y%m%d') - timedelta(days=10)).strftime('%Y%m%d')
    print(f"数据开始日期: {start_date}")
    
    # 获取全市场股票代码
    print("获取全市场股票代码...")
    stock_list = get_all_stock_codes()
    if stock_list.empty:
        print("没有获取到股票列表，退出")
        return
    print(f"共获取到 {len(stock_list)} 只股票")
    
    # 存储符合条件的股票
    candidate_stocks = []
    
    # 遍历股票列表，分析每只股票
    total = len(stock_list)
    # 全市场筛选
    test_mode = False  # 设置为False以分析所有股票
    max_stocks = 100 if test_mode else total
    
    print(f"分析前 {max_stocks} 只股票...")
    
    # 批量获取股票数据（分批处理）
    print("批量获取股票数据...")
    
    # 定义每批处理的股票数量
    batch_size = 100
    all_batch_data = []
    
    # 分批处理所有股票
    for batch_start in range(0, max_stocks, batch_size):
        batch_end = min(batch_start + batch_size, max_stocks)
        batch_stock_list = stock_list.iloc[batch_start:batch_end]
        batch_stock_codes = batch_stock_list['ts_code'].tolist()
        
        print(f"获取第 {batch_start//batch_size + 1} 批股票数据 ({batch_start+1}-{batch_end}/{max_stocks})...")
        
        # 获取当前批次的股票数据
        batch_data = get_stock_data_batch(batch_stock_codes, start_date, latest_trading_date)
        
        if not batch_data.empty:
            all_batch_data.append(batch_data)
            print(f"  成功获取 {len(batch_data['ts_code'].unique())} 只股票的数据")
        
        # 添加批次间隔，避免请求过于频繁
        time.sleep(2)
    
    # 合并所有批次的数据
    if all_batch_data:
        batch_data = pd.concat(all_batch_data, ignore_index=True)
        print(f"\n成功获取 {len(batch_data['ts_code'].unique())} 只股票的数据")
    else:
        print("没有获取到股票数据，退出")
        return
    
    # 遍历每只股票进行分析
    for i, row in stock_list.iterrows():
        if i >= max_stocks:
            break
            
        ts_code = row['ts_code']
        name = row['name']
        
        if i % 100 == 0:
            print(f"分析 {i+1}/{max_stocks}: {ts_code} {name}")
        
        # 从批量数据中获取当前股票的数据
        stock_data = batch_data[batch_data['ts_code'] == ts_code].copy()
        
        if stock_data.empty:
            continue
        
        # 计算MACD和KDJ指标
        stock_data = calculate_macd(stock_data)
        stock_data = calculate_kdj(stock_data)
        
        # 判断是否启动
        is_started, start_indicators = is_stock_started(stock_data, STRATEGY_CONFIG)
        if not is_started:
            continue
        
        # 获取资金流向数据
        money_flow_data = get_money_flow_data([ts_code], latest_trading_date)
        
        # 预测下个交易日是否会上涨
        is_predicted_rise, confidence = predict_next_day_rise(stock_data, money_flow_data, STRATEGY_CONFIG)
        if not is_predicted_rise:
            continue
        
        # 存储符合条件的股票
        stock_info = {
            'ts_code': ts_code,
            'name': name,
            'latest_trading_date': latest_trading_date,
            'price_change': start_indicators['price_change'],
            'volume_ratio': start_indicators['volume_ratio'],
            'confidence': confidence
        }
        
        # 添加资金流向数据
        if not money_flow_data.empty:
            mf = money_flow_data.iloc[0]
            stock_info['net_amount'] = mf['net_amount']
            stock_info['net_amount_rate'] = mf['net_amount_rate']
        else:
            stock_info['net_amount'] = 0
            stock_info['net_amount_rate'] = 0
        
        candidate_stocks.append(stock_info)
        print(f"  发现候选股票: {ts_code} {name}，信心指数: {confidence:.2f}")
    
    # 保存结果到CSV文件
    if candidate_stocks:
        result_df = pd.DataFrame(candidate_stocks)
        # 按信心指数排序
        result_df = result_df.sort_values('confidence', ascending=False)
        
        # 生成带日期的文件名
        output_file = f"判断启动上涨股票_{latest_trading_date}.csv"
        result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n分析完成，共发现 {len(candidate_stocks)} 只启动且预测下个交易日会涨的股票")
        print(f"结果已保存到 {output_file}")
        
        # 打印前10只股票的简要信息
        print("\n前10只信心指数最高的股票:")
        for i, row in result_df.head(10).iterrows():
            print(f"{i+1}. {row['ts_code']} {row['name']}")
            print(f"   涨幅: {row['price_change']:.2f}%, 量比: {row['volume_ratio']:.2f}, 信心指数: {row['confidence']:.2f}")
            print()
    else:
        print("\n未发现符合条件的股票")
        
        # 保存空结果
        output_file = f"判断启动上涨股票_{latest_trading_date}.csv"
        pd.DataFrame().to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"结果已保存到 {output_file}")

if __name__ == "__main__":
    main()
