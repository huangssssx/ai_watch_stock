
import akshare as ak
import pandas as pd
import numpy as np
import datetime
import time

def check_wash_markup(symbol):
    """
    洗盘终结与拉升启动策略 (Dev Version)
    
    逻辑：
    1. 洗盘终结 (Wash End): 
       - 极度缩量 (Liquidity Dry-up)
       - 波动率收敛 (Volatility Compression)
       - 偏度负向极致后回归 (Skewness Reversion)
    2. 拉升启动 (Markup Start):
       - 量能突变 (Volume Spike)
       - 磁吸效应 (Magnet Effect)
       - 动量共振 (Momentum Resonance)
    """
    triggered = False
    signal = "WAIT"
    message = ""
    
    try:
        # 1. 获取日线数据
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime("%Y%m%d")
        
        # 增加重试机制
        for _ in range(3):
            try:
                df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
                if not df.empty:
                    break
            except:
                time.sleep(1)
        
        if df is None or df.empty or len(df) < 60:
            return False, "WAIT", "数据不足"
            
        # 2. 数据清洗与单位防御 (Ratio Check)
        df = process_data(df)
        
        # 3. 计算指标与判定
        result = analyze_df(df)
        
        # 获取最后一天结果
        last_row = result.iloc[-1]
        
        if last_row['signal_wash_end']:
            triggered = True
            signal = "BUY_WASH_END"
            message = f"洗盘终结: 换手{last_row['turnover']:.2f}%(低位), ATR缩量, 偏度{last_row['skew']:.2f}"
        elif last_row['signal_markup_start']:
            triggered = True
            signal = "BUY_MARKUP_START"
            message = f"拉升启动: 量比{last_row['vol_ratio']:.1f}, 磁吸{last_row['magnet']:.4f}, 涨幅{last_row['pct_chg']:.1f}%"
            
    except Exception as e:
        triggered = False
        message = f"Error: {str(e)}"
        
    return triggered, signal, message

def process_data(df):
    """数据预处理与单位修正"""
    last_close = df['收盘'].iloc[-1]
    last_vol = df['成交量'].iloc[-1]
    last_amt = df['成交额'].iloc[-1]
    
    if last_vol > 0:
        raw_vwap = last_amt / last_vol
        ratio = raw_vwap / last_close
        if 80 <= ratio <= 120:
            df['成交量'] = df['成交量'] * 100
    return df

def analyze_df(df):
    """
    核心逻辑计算 (支持向量化回测)
    """
    df = df.copy()
    df['pct_chg'] = df['涨跌幅']
    df['turnover'] = df['换手率']
    
    # 波动率 ATR
    df['tr'] = np.maximum(
        df['最高'] - df['最低'],
        np.maximum(
            abs(df['最高'] - df['收盘'].shift(1)),
            abs(df['最低'] - df['收盘'].shift(1))
        )
    )
    df['atr'] = df['tr'].rolling(window=14).mean()
    df['atr_ma60'] = df['atr'].rolling(60).mean()
    
    # RSI 指标
    delta = df['收盘'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    
    # 偏度 Skewness
    df['skew'] = df['pct_chg'].rolling(window=20).skew()
    
    # 均线与量能
    df['ma5'] = df['收盘'].rolling(window=5).mean()
    df['ma20'] = df['收盘'].rolling(window=20).mean()
    df['vol_ma5'] = df['成交量'].rolling(window=5).mean()
    
    # 洗盘终结与拉升启动 - 综合突破策略 (Cycle 4)
    # 核心思想：寻找“缩量洗盘”后的“放量启动”点
    
    # 1. 定义洗盘状态 (过去 10 天内，至少有 5 天处于极度缩量状态)
    df['turnover_low_q'] = df['turnover'].rolling(60).quantile(0.20)
    df['is_low_turnover'] = df['turnover'] < df['turnover_low_q']
    # 过去10天低换手天数 > 5
    df['wash_days'] = df['is_low_turnover'].rolling(10).sum()
    df['was_washing'] = df['wash_days'] >= 5
    
    # 2. 定义启动信号 (当日)
    # 量能爆发
    df['vol_ratio'] = df['成交量'] / df['vol_ma5'].shift(1) # 对比昨日均量
    df['is_vol_spike'] = df['vol_ratio'] > 2.0
    
    # 价格突破
    df['is_strong_rise'] = df['pct_chg'] > 3.0
    df['is_above_ma20'] = df['收盘'] > df['ma20']
    
    # 3. 组合信号
    # 洗盘刚结束 + 突然放量拉升
    df['signal_wash_end'] = False # 废弃单纯的洗盘指标
    df['signal_markup_start'] = df['was_washing'] & df['is_vol_spike'] & df['is_strong_rise'] & df['is_above_ma20']
    
    return df

if __name__ == "__main__":
    # 测试入口
    symbol = "600519" # 茅台
    print(f"Testing {symbol}...")
    t, s, m = check_wash_markup(symbol)
    print(f"Result: {t}, {s}, {m}")
