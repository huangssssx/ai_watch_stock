# -*- coding: utf-8 -*-
"""
Valley Sniper V5 - Alpha Selection System (Production Ready)
山谷狙击 V5 - 量化选股系统 (实战部署版)

策略核心 (The Alpha):
    1. 动量 (Momentum): 热门板块右侧启动，K线实体阳线，拒绝杂音。
    2. 均值回归 (Mean Reversion): 锁定底部抬高但未暴涨的蓄势区 (-10% ~ 20%)。
    3. 流动性 (Liquidity): 聚焦中盘股 (30亿-500亿)，机构游资共舞。
    4. 健壮性 (Robustness): 分层防御网络异常与数据真空。

执行标准:
    - 运行时间: 交易日 14:45 (确认收盘形态)
    - 数据单位: 元 (CNY) / 百分比数值 (Percentage Value)
"""

import akshare as ak
import pandas as pd
import time
import random
import sys
from functools import wraps

# --- 0. 配置参数 (Configuration) ---
RETRY_CONFIG = {
    'max_retries': 3,
    'initial_delay': 1.0,
    'backoff': 2.0
}

# 核心 Alpha 阈值 (Hard Constraints)
ALPHA_PARAMS = {
    'min_pct_chg': 2.0,      # 最小涨幅 2.0% (过滤随波逐流)
    'max_pct_chg': 6.0,      # 最大涨幅 6.0% (防炸板/透支)
    'shadow_ratio': 0.6,     # 上影线/实体 比例上限 (防避雷针)
    'min_trend_60': -10.0,   # 60日涨幅下限 (防下降通道)
    'max_trend_60': 20.0,    # 60日涨幅上限 (防高位接盘)
    'min_cap': 30 * 10**8,   # 最小流通市值 30亿 (防庄股)
    'max_cap': 500 * 10**8,  # 最大流通市值 500亿 (防大象)
    'min_vr': 1.5,           # 最小量比 (确认资金进场)
    'max_vr': 6.0,           # 最大量比 (防情绪过热)
    'min_turnover': 3.0,     # 最小换手 (确认承接)
    'max_turnover': 15.0     # 最大换手 (防高位出货)
}

# --- 1. 健壮性模块 (Robustness Module) ---

class FatalError(Exception):
    """不可恢复的系统级错误 (如网络瘫痪)"""
    pass

class DataEmptyError(Exception):
    """接口通畅但返回空数据 (如非交易日)"""
    pass

def fetch_market_data_with_retry():
    """
    分层防御的数据获取函数
    Layer 1: 网络重试 (Exponential Backoff)
    Layer 2: 数据完整性校验 (Data Validation)
    """
    max_retries = RETRY_CONFIG['max_retries']
    delay = RETRY_CONFIG['initial_delay']
    
    for i in range(max_retries + 1):
        try:
            print(f"📡 正在获取全市场行情 (尝试 {i+1}/{max_retries+1})...")
            # 获取全市场实时行情
            df = ak.stock_zh_a_spot_em()
            
            # Layer 2: 校验层
            if df is None or df.empty:
                raise DataEmptyError("接口返回数据为空")
                
            # 检查关键字段是否存在 (防止接口变动)
            required_cols = ['代码', '名称', '最新价', '涨跌幅', '最高', '今开', '流通市值', '60日涨跌幅', '量比', '换手率']
            missing = [col for col in required_cols if col not in df.columns]
            if missing:
                raise ValueError(f"缺失关键字段: {missing}")
                
            print(f"✅ 数据获取成功: {len(df)} 条记录")
            return df
            
        except (DataEmptyError, ValueError) as e:
            # 数据逻辑错误，重试可能无效，但为了稳健仍可重试或直接抛出
            # 这里选择直接抛出，因为字段缺失重试通常没用
            print(f"❌ 数据校验失败: {e}")
            if isinstance(e, DataEmptyError) and i < max_retries:
                time.sleep(delay)
                delay *= RETRY_CONFIG['backoff']
                continue
            raise FatalError(f"数据校验未通过: {e}")
            
        except Exception as e:
            # 网络/连接错误，进行指数退避重试
            print(f"⚠️ 网络/接口异常: {e}")
            if i == max_retries:
                raise FatalError(f"重试耗尽，系统终止: {e}")
            
            sleep_time = delay * (1 + random.uniform(-0.1, 0.1)) # Add Jitter
            print(f"⏳ 等待 {sleep_time:.1f}s 后重试...")
            time.sleep(sleep_time)
            delay *= RETRY_CONFIG['backoff']

# --- 2. 策略核心逻辑 (Alpha Logic) ---

def run_valley_sniper(df):
    """
    执行 5 重 Alpha 因子过滤
    Trader's Note: 严格执行，宁缺毋滥。
    """
    print("\n🔍 开始执行 Valley Sniper V5 策略扫描...")
    
    # 0. 数据预处理 (Data Cleaning)
    # 确保数值列类型正确，处理 '-' 或 NaN
    numeric_cols = ['最新价', '涨跌幅', '最高', '今开', '流通市值', '60日涨跌幅', '量比', '换手率']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 剔除无法计算的行 (NaN)
    df.dropna(subset=numeric_cols, inplace=True)
    
    initial_count = len(df)
    
    # 1. 基础清洗 (Basic Filter)
    # 剔除 ST, 退市, 北交所 (8/4/92开头)
    mask_basic = (
        (~df['名称'].str.contains('ST|退')) &
        (~df['代码'].str.match(r'^(8|4|92)'))
    )
    df = df[mask_basic]
    print(f"1️⃣ 基础清洗: {initial_count} -> {len(df)} (剔除ST/北交所)")
    
    # 2. 涨跌幅门槛 (Price Change)
    # Trader's Note: 2% < chg < 6%
    # < 2%: 没启动，随波逐流，浪费时间。
    # > 6%: 接近涨停或炸板风险区，盈亏比下降。
    mask_price = (
        (df['涨跌幅'] > ALPHA_PARAMS['min_pct_chg']) &
        (df['涨跌幅'] < ALPHA_PARAMS['max_pct_chg'])
    )
    df = df[mask_price]
    print(f"2️⃣ 涨跌幅过滤: -> {len(df)} (保留 {ALPHA_PARAMS['min_pct_chg']}% - {ALPHA_PARAMS['max_pct_chg']}%)")
    
    # 3. K线形态 (Candlestick Pattern)
    # Trader's Note: 必须是实体阳线 (Close > Open)。
    # 且上影线不能太长 (High - Close < Entity * 0.6)。
    # 拒绝十字星 (犹豫)，拒绝避雷针 (抛压大)。
    entity = df['最新价'] - df['今开']
    upper_shadow = df['最高'] - df['最新价']
    
    mask_kline = (
        (df['最新价'] > df['今开']) & # 严格阳线
        (upper_shadow < entity * ALPHA_PARAMS['shadow_ratio']) # 上影线约束
    )
    df = df[mask_kline]
    print(f"3️⃣ K线形态: -> {len(df)} (实体阳线 + 短上影)")
    
    # 4. 趋势与位置 (Trend & Position)
    # Trader's Note: -10% < 60日涨幅 < 20%
    # < -10%: 趋势坏了，那是接飞刀。
    # > 20%: 涨多了，空间有限。
    # 我们要找的是“横盘震荡”或“缓慢爬升”的蓄势股。
    mask_trend = (
        (df['60日涨跌幅'] > ALPHA_PARAMS['min_trend_60']) &
        (df['60日涨跌幅'] < ALPHA_PARAMS['max_trend_60'])
    )
    df = df[mask_trend]
    print(f"4️⃣ 趋势位置: -> {len(df)} (60日涨幅 {ALPHA_PARAMS['min_trend_60']}% - {ALPHA_PARAMS['max_trend_60']}%)")
    
    # 5. 资金性质 (Liquidity & Activity)
    # Trader's Note: 
    # 市值 30-500亿: 机构游资战场。
    # 量比 1.5-6.0: 有资金进，但别太疯狂。
    # 换手 3-15%: 活跃承接。
    mask_money = (
        (df['流通市值'] > ALPHA_PARAMS['min_cap']) &
        (df['流通市值'] < ALPHA_PARAMS['max_cap']) &
        (df['量比'] > ALPHA_PARAMS['min_vr']) &
        (df['量比'] < ALPHA_PARAMS['max_vr']) &
        (df['换手率'] > ALPHA_PARAMS['min_turnover']) &
        (df['换手率'] < ALPHA_PARAMS['max_turnover'])
    )
    df = df[mask_money]
    print(f"5️⃣ 资金筛选: -> {len(df)} (市值/量比/换手 Alpha)")
    
    return df

# --- 3. 主程序 (Main) ---

def main():
    print("🚀 Valley Sniper V5 启动...")
    try:
        # Step 1: 获取数据
        df = fetch_market_data_with_retry()
        
        # Step 2: 核心策略
        result_df = run_valley_sniper(df)
        
        # Step 3: 结果展示
        print("\n" + "="*60)
        if result_df.empty:
            # Trader's Note: 空仓也是一种交易。
            print("⚠️ 今日无符合策略标的 (No Alpha Found)")
            print("💡 操盘建议: 空仓观察，不要强行出击。")
        else:
            print(f"🎯 狙击命中: {len(result_df)} 只标的")
            print("="*60)
            
            # 格式化输出
            output_cols = ['代码', '名称', '最新价', '涨跌幅', '流通市值', '60日涨跌幅', '量比', '换手率']
            
            # 简单美化
            display_df = result_df[output_cols].copy()
            display_df['最新价'] = display_df['最新价'].round(2)
            display_df['涨跌幅'] = display_df['涨跌幅'].apply(lambda x: f"{x:.2f}%")
            display_df['流通市值'] = display_df['流通市值'].apply(lambda x: f"{x/10**8:.1f}亿")
            display_df['60日涨跌幅'] = display_df['60日涨跌幅'].apply(lambda x: f"{x:.2f}%")
            display_df['量比'] = display_df['量比'].round(2)
            display_df['换手率'] = display_df['换手率'].apply(lambda x: f"{x:.2f}%")
            
            # 按综合评分排序 (这里简单按量比排序，代表资金强度)
            display_df = display_df.sort_values(by='量比', ascending=False)
            
            print(display_df.to_string(index=False))
            print("\n💡 操盘建议: 重点关注前排个股，结合板块效应决策。")
            
    except FatalError as e:
        print(f"\n❌ 程序终止: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 未知错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
