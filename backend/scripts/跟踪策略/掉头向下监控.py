# ==============================================================================
# 掉头向下监控 (Downturn Monitor) - 增强版 v3.1 (A股深度优化)
# ------------------------------------------------------------------------------
# 目标：基于自适应均线、周线趋势、微观结构与市场宽度构建的高精度预警系统。
# 核心逻辑体系 (A股特供)：
# 1. 💀 一级预警 (DANGER -> STRONG_SELL)：
#    - [趋势反转] 价格有效跌破 KAMA慢线(20) 且 周线MACD死叉(趋势向下)
#    - [诱多陷阱] 前日涨停炸板/封板后，今日放量低走 (Exploding Board)
#    - [系统风险] 市场环境弱势(指数破位) + 个股跌破MA20
# 2. 📉 二级预警 (WARNING -> SELL)：
#    - [利润保护] 严重过热(RSI>75) 且 跌破 KAMA快线(10) (带ATR缓冲)
#    - [顶部衰竭] Alpha因子示警 (放量滞涨/高位衰竭)
# 3. ⏳ 观察期 (WAIT)：
#    - 缩量回踩但守住 POC (成交密集区) 或 KAMA慢线
# ==============================================================================

import talib
import numpy as np
import pandas as pd
import akshare as ak
import datetime

# --- Helper Functions ---

def get_weekly_trend(df):
    """计算周线趋势 (MACD)"""
    try:
        df_w = df.copy()
        df_w["date"] = pd.to_datetime(df_w["date"])
        df_w.set_index("date", inplace=True)
        # Resample to weekly
        weekly = df_w.resample("W").agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum"
        }).dropna()
        
        if len(weekly) < 26: return "NEUTRAL"
        
        close_w = weekly["close"].values
        macd, signal, hist = talib.MACD(close_w, fastperiod=12, slowperiod=26, signalperiod=9)
        
        # Dead Cross State
        if macd[-1] < signal[-1]: return "DOWN"
        elif macd[-1] > signal[-1]: return "UP"
        else: return "NEUTRAL"
    except:
        return "NEUTRAL"

def calculate_poc(df, window=20, bins=20):
    """计算近似 POC (Point of Control)"""
    try:
        subset = df.iloc[-window:]
        if subset.empty: return 0
        price_min = subset["low"].min()
        price_max = subset["high"].max()
        if price_min == price_max: return price_min
        
        typical_price = (subset["high"] + subset["low"] + subset["close"]) / 3
        hist, bin_edges = np.histogram(typical_price, bins=bins, range=(price_min, price_max), weights=subset["volume"])
        max_idx = np.argmax(hist)
        return (bin_edges[max_idx] + bin_edges[max_idx+1]) / 2
    except:
        return 0

def calculate_chop(df, window=14):
    """计算 Choppiness Index (CHOP)"""
    try:
        if len(df) < window + 1: return 50.0
        
        # True Range
        high = df["high"]
        low = df["low"]
        close = df["close"]
        
        # Talib TRANGE returns numpy array
        tr1 = talib.TRANGE(high.values, low.values, close.values)
        tr1_s = pd.Series(tr1, index=df.index)
        
        sum_tr = tr1_s.rolling(window=window).sum()
        max_hi = high.rolling(window=window).max()
        min_lo = low.rolling(window=window).min()
        
        range_hl = max_hi - min_lo
        # Avoid division by zero
        range_hl = range_hl.replace(0, np.nan)
        
        # CHOP Formula: 100 * Log10(SumTR / RangeHL) / Log10(Window)
        chop = 100 * np.log10(sum_tr / range_hl) / np.log10(window)
        
        val = chop.iloc[-1]
        return 50.0 if np.isnan(val) else val
    except:
        return 50.0

# ------------------------

# 0. 板块自适应配置 (Sector Adaptive Config)
# 针对不同波动率的板块，调整 CHOP 阈值和放量标准
# High Beta (科技/券商/新能源): 波动大，需更严格的过滤 (CHOP低阈值，Volume高阈值)
# Low Beta (银行/公用/消费): 波动小，标准放宽
SECTOR_MAP = {
    # High Beta
    "sz300750": "HIGH_BETA", # 宁德时代
    "sz300059": "HIGH_BETA", # 东方财富
    "sz300308": "HIGH_BETA", # 中际旭创
    "sh601138": "HIGH_BETA", # 工业富联
    "sz000063": "HIGH_BETA", # 中兴通讯
    "sz002475": "HIGH_BETA", # 立讯精密
    "sh600104": "HIGH_BETA", # 上汽集团(近期活跃) -> 修正：汽车算中高
    
    # Low Beta / Defensive
    "sh600900": "LOW_BETA",  # 长江电力
    "sh600036": "LOW_BETA",  # 招商银行
    "sh601398": "LOW_BETA",  # 工商银行
    "sh601857": "LOW_BETA",  # 中国石油
    
    # Stable Growth
    "sh600519": "STABLE",    # 贵州茅台
    "sh600887": "STABLE",    # 伊利股份
    "sh600030": "STABLE",    # 中信证券(偏稳)
    "sh601899": "STABLE",    # 紫金矿业
}

SECTOR_PARAMS = {
    "HIGH_BETA": {
        "chop_threshold": 50.0,  # 极严苛的震荡过滤 (原55.0 -> 50.0)
        "vol_multiplier": 2.0,   # 维持高量能要求
        "kama_slow_period": 30,  # 均线周期拉长 (原25 -> 30)，减少假摔
    },
    "LOW_BETA": {
        "chop_threshold": 60.0,  # 稍微收紧 (原65.0 -> 60.0)
        "vol_multiplier": 1.2,   # 维持低门槛
        "kama_slow_period": 20,  # 维持标准
    },
    "STABLE": {
        "chop_threshold": 61.8,  # 标准黄金分割
        "vol_multiplier": 1.5,   # 标准放量
        "kama_slow_period": 20,
    },
    "DEFAULT": {
        "chop_threshold": 61.8,
        "vol_multiplier": 1.5,
        "kama_slow_period": 20,
    }
}

# 1. 初始化
triggered = False
signal = "WAIT"
message = "监控中：等待变盘信号..."

try:
    # 2. 预处理 Symbol
    symbol_code = symbol
    if symbol.startswith(("sh", "sz", "bj")):
        symbol_code = symbol[2:]

    # 获取板块配置
    full_symbol = symbol if symbol.startswith(("sh", "sz")) else ("sh" if symbol.startswith("6") else "sz") + symbol
    sector_type = SECTOR_MAP.get(full_symbol, "DEFAULT")
    params = SECTOR_PARAMS.get(sector_type, SECTOR_PARAMS["DEFAULT"])
    
    # 3. 获取数据
    now = datetime.datetime.now()
    start_dt = (now - datetime.timedelta(days=400)).strftime("%Y%m%d") # 需足够长计算周线
    end_dt = now.strftime("%Y%m%d")
    
    # 个股数据
    df = ak.stock_zh_a_hist(symbol=symbol_code, period="daily", start_date=start_dt, end_date=end_dt, adjust="qfq")
    
    # 指数数据 (沪深300) - 用于判断市场环境
    # 注意：实盘中每次请求可能耗时，若对性能敏感可移除或使用全局缓存
    try:
        index_df = ak.stock_zh_index_daily(symbol="sh000300")
        index_df["date"] = pd.to_datetime(index_df["date"])
    except:
        index_df = pd.DataFrame()

    if df is None or df.empty or len(df) < 60:
        message = "未触发：历史数据不足 (需至少60天)"
    else:
        # 4. 数据清洗
        df = df.rename(columns={
            "日期": "date", "开盘": "open", "收盘": "close", 
            "最高": "high", "最低": "low", "成交量": "volume", "成交额": "amount"
        })
        df["date"] = pd.to_datetime(df["date"])
        cols = ["open", "close", "high", "low", "volume", "amount"]
        for col in cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            
        # 5. 指标计算
        close = df["close"].values
        high = df["high"].values
        low = df["low"].values
        volume = df["volume"].values.astype(float)
        
        # A. KAMA 自适应均线 (Adaptive Params)
        kama_fast = talib.KAMA(close, timeperiod=10)
        kama_slow = talib.KAMA(close, timeperiod=params["kama_slow_period"]) 
        
        # B. 基础均线
        ma5 = talib.SMA(close, timeperiod=5)
        ma20 = talib.SMA(close, timeperiod=20)
        
        # C. 辅助指标
        atr = talib.ATR(high, low, close, timeperiod=14)
        rsi = talib.RSI(close, timeperiod=14)
        vol_ma5 = talib.SMA(volume, timeperiod=5)
        
        # D. 周线趋势
        weekly_trend = get_weekly_trend(df)
        
        # E. 市场环境 (Index Weakness)
        is_market_weak = False
        if not index_df.empty:
            # Filter index to match current date logic (latest available)
            # Check if Index < Index MA20
            idx_close = index_df["close"].values
            if len(idx_close) > 20:
                idx_ma20 = talib.SMA(idx_close, timeperiod=20)
                if idx_close[-1] < idx_ma20[-1]:
                    is_market_weak = True
        
        # F. POC
        poc_price = calculate_poc(df, window=20)

        # G. CHOP (市场体制 - Adaptive Params)
        chop_val = calculate_chop(df, window=14)
        is_choppy = chop_val > params["chop_threshold"]
        
        # 6. 获取当前切片
        curr_price = close[-1]
        prev_price = close[-2]
        curr_kama_fast = kama_fast[-1]
        curr_kama_slow = kama_slow[-1]
        curr_ma20 = ma20[-1]
        curr_atr = atr[-1]
        curr_rsi = rsi[-1]
        curr_vol = volume[-1]
        curr_vol_ma5 = vol_ma5[-1]
        
        bias20 = (curr_price - curr_ma20) / curr_ma20 if curr_ma20 != 0 else 0
        
        # 7. 核心逻辑判定 (v3.1 + Adaptive)
        danger_reasons = []
        warning_reasons = []
        info_reasons = []
        
        # --- Logic 1: STRONG_SELL (Trend Reversal) ---
        
        # A. 趋势共振破位 (Event Driven: CrossUnder)
        # 跌破 KAMA慢线 且 周线MACD死叉
        is_cross_under_kama = (curr_price < curr_kama_slow) and (prev_price >= kama_slow[-2])
        if is_cross_under_kama and (weekly_trend == "DOWN"):
             # [优化] 震荡体制下，屏蔽普通跌破，除非放量
             if not is_choppy:
                 danger_reasons.append("跌破KAMA慢线+周线向下")
             elif curr_vol > params["vol_multiplier"] * curr_vol_ma5:
                 danger_reasons.append(f"震荡区放量(>{params['vol_multiplier']}x)跌破KAMA")
        
        # B. 涨停陷阱 (Limit Up Trap)
        # 前日涨幅 > 9.5% (近似涨停)，今日低收且放量
        prev_pct = (close[-2] - close[-3]) / close[-3] if len(close) > 2 else 0
        if (prev_pct > 0.095):
            if (curr_price < close[-2]) and (curr_vol > 1.2 * curr_vol_ma5):
                 danger_reasons.append("涨停次日放量杀跌(诱多)")
                 
        # C. 弱势市场共振
        # 市场弱势 + 个股跌破生命线 (Event Driven)
        is_cross_under_ma20 = (curr_price < curr_ma20) and (prev_price >= ma20[-2])
        if is_market_weak and is_cross_under_ma20:
             if not is_choppy:
                 danger_reasons.append("弱势市场跌破生命线")
             
        # --- Logic 2: SELL (Profit Protection) ---
        
        # A. 过热回撤止盈
        is_overheat = (curr_rsi > 75) or (bias20 > 0.15)
        if is_overheat:
            stop_price = curr_kama_fast - (0.5 * curr_atr) # 宽幅震荡给予0.5ATR缓冲
            if curr_price < stop_price:
                 warning_reasons.append(f"过热期跌破KAMA快线(止盈)")
                 
        # B. 顶部衰竭信号 (Alpha Check)
        # 简单化：RSI 高位且 KAMA 快线拐头向下
        if (curr_rsi > 70) and (curr_kama_fast < kama_fast[-2]):
             warning_reasons.append("RSI高位+动能衰竭")

        # --- Logic 3: WAIT (Correction) ---
        
        # 跌破 MA5 或 KAMA快线，但获得支撑 (POC 或 KAMA慢线) 且 缩量
        is_drop = (curr_price < curr_kama_fast) or (curr_price < ma5[-1])
        is_supported = (curr_price > poc_price) and (curr_price > curr_kama_slow)
        is_shrink_vol = (curr_vol < 1.0 * curr_vol_ma5)
        
        wait_msg = []
        if is_drop and is_supported and is_shrink_vol and not danger_reasons and not warning_reasons:
            wait_msg = [f"缩量回踩POC({poc_price:.2f})支撑有效"]

        # 8. 信号输出
        if danger_reasons:
            triggered = True
            signal = "STRONG_SELL"
            message = f"📉【趋势反转】{' + '.join(danger_reasons)} | 建议离场"
            
        elif warning_reasons:
            triggered = True
            signal = "SELL"
            message = f"🪂【二级预警】{' + '.join(warning_reasons)} | 建议止盈/减仓"
            
        elif wait_msg:
            triggered = False
            signal = "WAIT"
            message = f"⏳【技术调整】{' '.join(wait_msg)}"
            
        else:
            triggered = False
            signal = "SAFE"
            trend_s = "多头" if curr_price > curr_kama_slow else "震荡"
            message = f"✅【趋势暂稳】{trend_s} | 现价:{curr_price:.2f}"

except Exception as e:
    triggered = False
    signal = "WAIT"
    message = f"脚本错误: {str(e)}"
    print(f"[Error] {e}")
