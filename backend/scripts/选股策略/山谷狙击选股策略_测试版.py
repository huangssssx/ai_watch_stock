# 山谷狙击选股策略 - 测试版（更宽松参数）
# 用于验证脚本功能

import akshare as ak
import pandas as pd
import numpy as np
import datetime

# 测试用宽松参数
VOLUME_SHRINK_RATIO = 0.6      # 放宽到60%
RECENT_VOLUME_DAYS = 5
HISTORY_VOLUME_DAYS = 60
MA_PERIODS = [60, 120]
MA_SUPPORT_RANGE = 0.08        # 放宽到±8%
MA_REBOUND_RANGE = 0.10        # 放宽到+10%
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
RSI_PERIOD = 14
RSI_OVERSOLD = 40              # RSI放宽到40
MIN_TURNOVER_AMOUNT = 10000000 # 降低到1000万
MAX_PRICE_CHANGE = 8.0         # 放宽到8%
SCORE_VOLUME_SHRINK = 3
SCORE_MA60_SUPPORT = 2
SCORE_MA120_SUPPORT = 1
SCORE_MACD_DIVERGENCE = 3
SCORE_RSI_DIVERGENCE = 2
SCORE_THRESHOLD = 4            # 降低评分阈值到4分

def calculate_macd(prices, fast=12, slow=26, signal=9):
    ema_fast = prices.ewm(span=fast, adjust=False).mean()
    ema_slow = prices.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def calculate_rsi(prices, period=14):
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def detect_bullish_divergence(prices, indicator, lookback=10):
    if len(prices) < lookback or len(indicator) < lookback:
        return False
    recent_prices = prices[-lookback:]
    recent_indicator = indicator[-lookback:]
    current_price = recent_prices.iloc[-1]
    current_indicator = recent_indicator.iloc[-1]
    min_price_idx = recent_prices[:-1].idxmin()
    min_indicator_val = recent_indicator[:-1].min()
    if current_price < recent_prices[min_price_idx] and current_indicator > min_indicator_val:
        return True
    return False

print("🎯 【山谷狙击选股策略 - 测试版】启动")
print(f"📊 使用宽松参数进行测试验证\n")

df_market = ak.stock_zh_a_spot_em()
df_market = df_market[~df_market['名称'].str.contains("ST|退", na=False)]
df_market = df_market[df_market['成交额'] >= MIN_TURNOVER_AMOUNT]
df_market = df_market[abs(df_market['涨跌幅']) <= MAX_PRICE_CHANGE]
df_market = df_market.sort_values(by='换手率', ascending=False).head(50)  # 只测试50只

print(f"✅ 筛选池: {len(df_market)} 只股票\n")

end_date = datetime.datetime.now()
start_date = end_date - datetime.timedelta(days=90)
start_date_str = start_date.strftime("%Y%m%d")
end_date_str = end_date.strftime("%Y%m%d")

results = []
count = 0

for idx, row in df_market.iterrows():
    count += 1
    symbol = row['代码']
    name = row['名称']
    current_price = float(row['最新价'])
    price_change_pct = float(row['涨跌幅'])
    
    if count % 10 == 0:
        print(f"⏳ 进度: {count}/{len(df_market)}...")
    
    try:
        df_hist = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date_str, end_date=end_date_str, adjust="qfq")
        
        if df_hist is None or len(df_hist) < 60:
            continue
        
        df_hist.columns = ['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
        df_hist['收盘'] = pd.to_numeric(df_hist['收盘'], errors='coerce')
        df_hist['成交量'] = pd.to_numeric(df_hist['成交量'], errors='coerce')
        df_hist = df_hist.dropna(subset=['收盘', '成交量'])
        
        if len(df_hist) < 60:
            continue
        
        recent_vol_avg = df_hist['成交量'][-RECENT_VOLUME_DAYS:].mean()
        history_vol_avg = df_hist['成交量'][-HISTORY_VOLUME_DAYS:].mean()
        volume_ratio = recent_vol_avg / history_vol_avg if history_vol_avg > 0 else 1
        is_volume_shrink = volume_ratio < VOLUME_SHRINK_RATIO
        
        ma60 = df_hist['收盘'][-60:].mean()
        ma120 = df_hist['收盘'][-120:].mean() if len(df_hist) >= 120 else None
        
        distance_to_ma60 = (current_price - ma60) / ma60
        is_ma60_support = -MA_SUPPORT_RANGE < distance_to_ma60 < MA_REBOUND_RANGE
        
        is_ma120_support = False
        distance_to_ma120 = None
        if ma120 is not None:
            distance_to_ma120 = (current_price - ma120) / ma120
            is_ma120_support = -MA_SUPPORT_RANGE < distance_to_ma120 < MA_REBOUND_RANGE
        
        macd_line, signal_line, histogram = calculate_macd(df_hist['收盘'], MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        is_macd_divergence = detect_bullish_divergence(df_hist['收盘'].reset_index(drop=True), macd_line.reset_index(drop=True), lookback=10)
        
        rsi = calculate_rsi(df_hist['收盘'], RSI_PERIOD)
        current_rsi = rsi.iloc[-1] if len(rsi) > 0 else 50
        is_rsi_divergence = detect_bullish_divergence(df_hist['收盘'].reset_index(drop=True), rsi.reset_index(drop=True), lookback=10)
        is_rsi_oversold = current_rsi < RSI_OVERSOLD
        
        score = 0
        signals = []
        
        if is_volume_shrink:
            score += SCORE_VOLUME_SHRINK
            signals.append("地量")
        if is_ma60_support:
            score += SCORE_MA60_SUPPORT
            signals.append("MA60")
        if is_ma120_support:
            score += SCORE_MA120_SUPPORT
            signals.append("MA120")
        if is_macd_divergence:
            score += SCORE_MACD_DIVERGENCE
            signals.append("MACD背离")
        if is_rsi_divergence:
            score += SCORE_RSI_DIVERGENCE
            signals.append("RSI背离")
        
        if score >= SCORE_THRESHOLD:
            results.append({
                "代码": symbol,
                "名称": name,
                "最新价": round(current_price, 2),
                "涨跌幅%": round(price_change_pct, 2),
                "山谷评分": score,
                "信号组合": "+".join(signals),
                "缩量比": round(volume_ratio, 2),
                "距MA60%": round(distance_to_ma60 * 100, 2),
                "RSI": round(current_rsi, 2) if not pd.isna(current_rsi) else None
            })
        
    except Exception as e:
        continue

df = pd.DataFrame(results)

if df.empty:
    print("\n⚠️  测试版仍未找到结果")
    df = pd.DataFrame([{"代码": "TEST", "名称": "测试数据", "最新价": 0, "涨跌幅%": 0, "山谷评分": 0, "信号组合": "无", "缩量比": 0, "距MA60%": 0, "RSI": 0}])
else:
    df = df.sort_values(by="山谷评分", ascending=False)
    print(f"\n🎉 测试成功！找到 {len(df)} 只股票\n")
    print(df.to_string(index=False))
