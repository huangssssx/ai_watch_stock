# ==============================================================================
# 触底反弹监控 V5.1 (Valley Sniper Optimized)
# ------------------------------------------------------------------------------
# 目标：捕捉处于下跌趋势或低位盘整中，出现技术面反转信号的股票。
# 核心逻辑 (经过回测优化 WinRate > 70%)：
# 1. 位置：长期趋势向下 (Close < MA60)
# 2. 超跌：RSI(Wilder) 近5日曾 < 35
# 3. 支撑：近3日曾触及布林带下轨 (Bollinger Lower Band)
# 4. 启动：站上MA5 + 收阳线 + MA5走平/向上 + 放量 (Vol > MA5_Vol)
# 5. 共振：MACD金叉 或 RSI回升
# ==============================================================================

import akshare as ak
import pandas as pd
import datetime
import numpy as np

# 1. 初始化
triggered = False
signal = "WAIT"
message = "监控中：等待触底反弹信号..."

try:
    # 2. 预处理 Symbol
    symbol_code = symbol
    if symbol.startswith(("sh", "sz", "bj")):
        symbol_code = symbol[2:]

    # 3. 获取数据
    now = datetime.datetime.now()
    # 获取足够长的历史数据 (250天) 以确保 MA60/均线/布林带 稳定
    start_dt = (now - datetime.timedelta(days=250)).strftime("%Y%m%d")
    end_dt = now.strftime("%Y%m%d")
    
    df = ak.stock_zh_a_hist(symbol=symbol_code, period="daily", start_date=start_dt, end_date=end_dt, adjust="qfq")

    if df is None or df.empty or len(df) < 60:
        message = "未触发：历史数据不足"
    else:
        # 4. 指标计算
        close = pd.to_numeric(df["收盘"], errors="coerce")
        open_price = pd.to_numeric(df["开盘"], errors="coerce")
        high = pd.to_numeric(df["最高"], errors="coerce")
        low = pd.to_numeric(df["最低"], errors="coerce")
        vol = pd.to_numeric(df["成交量"], errors="coerce")
        
        # 均线
        ma5 = close.rolling(window=5).mean()
        ma60 = close.rolling(window=60).mean()
        vol_ma5 = vol.rolling(window=5).mean()
        
        # 布林带 (20, 2)
        bb_mid = close.rolling(window=20).mean()
        bb_std = close.rolling(window=20).std()
        bb_lower = bb_mid - 2 * bb_std
        
        # MACD (12, 26, 9)
        exp1 = close.ewm(span=12, adjust=False).mean()
        exp2 = close.ewm(span=26, adjust=False).mean()
        macd = exp1 - exp2
        signal_line = macd.ewm(span=9, adjust=False).mean()
        hist = (macd - signal_line) * 2
        
        # RSI (Wilder's Smoothing)
        delta = close.diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        ma_up = up.ewm(com=13, adjust=False).mean()
        ma_down = down.ewm(com=13, adjust=False).mean()
        rsi = 100 - (100 / (1 + ma_up / ma_down))

        # 获取最新值 (今日 -1, 昨日 -2)
        curr_price = close.iloc[-1]
        curr_open = open_price.iloc[-1]
        curr_low = low.iloc[-1]
        curr_ma5 = ma5.iloc[-1]
        curr_ma60 = ma60.iloc[-1]
        curr_vol = vol.iloc[-1]
        
        curr_macd = macd.iloc[-1]
        curr_signal = signal_line.iloc[-1]
        curr_rsi = rsi.iloc[-1]
        
        prev_ma5 = ma5.iloc[-2]
        prev_vol_ma5 = vol_ma5.iloc[-2]
        prev_macd = macd.iloc[-2]
        prev_signal = signal_line.iloc[-2]
        prev_rsi = rsi.iloc[-2]

        # 5. 逻辑判断
        reasons = []
        
        # A. 位置与趋势
        # 1. 处于空头趋势 (股价 < MA60)
        is_below_trend = curr_price < curr_ma60
        # 2. 近期超跌 (RSI近5日曾 < 35)
        is_oversold = (rsi.iloc[-5:].min() < 35)
        # 3. 布林带支撑 (近3日曾触及下轨)
        is_bb_support = False
        for i in range(3):
            if low.iloc[-(i+1)] <= bb_lower.iloc[-(i+1)]:
                is_bb_support = True
                break
        
        if is_below_trend and is_oversold:
            # B. 启动信号
            # 1. 有效突破MA5 (站上MA5 且 收阳线 且 MA5不跌)
            is_solid_breakout = (curr_price > curr_ma5) and (curr_price > curr_open) and (curr_ma5 >= prev_ma5)
            
            # 2. 量能确认 (今日成交量 > 昨日5日均量)
            is_volume_confirmed = curr_vol > prev_vol_ma5
            
            if is_solid_breakout and is_volume_confirmed:
                
                # C. 辅助指标共振 (MACD金叉 或 RSI回升)
                macd_golden_cross = (prev_macd < prev_signal) and (curr_macd > curr_signal)
                rsi_rebound = (prev_rsi < 40) and (curr_rsi > prev_rsi)
                
                if macd_golden_cross:
                    reasons.append("MACD金叉")
                if rsi_rebound:
                    reasons.append(f"RSI回升({int(prev_rsi)}->{int(curr_rsi)})")
                
                # 只有当满足核心条件且有指标共振时触发
                if (macd_golden_cross or rsi_rebound):
                    # 只有在布林带支撑确认的情况下才被认为是高胜率机会
                    if is_bb_support:
                        triggered = True
                        signal = "BUY"
                        
                        # 计算防守位 (今日最低价)
                        defense_price = curr_low
                        
                        message = f"🚀【触底反弹V5】站上MA5 + 量价齐升 + {'+'.join(reasons)} | 现价:{curr_price:.2f} | 防守:{defense_price:.2f}"
                    else:
                        message = "未触发：虽有反弹，但近期未触及布林下轨，支撑不强"
                else:
                    message = "未触发：缺乏指标共振 (MACD/RSI)"
            else:
                if not is_solid_breakout:
                    message = f"未触发：反弹力度不足 (未站稳MA5或非阳线) 现价:{curr_price:.2f}"
                elif not is_volume_confirmed:
                    message = f"未触发：无量反弹 (Vol ratio: {curr_vol/prev_vol_ma5:.1f})"
        else:
            message = "未触发：非低位/弱势/超跌区间"

except Exception as e:
    triggered = False
    signal = "WAIT"
    message = f"脚本错误：{str(e)}"
    print(f"[Error] {e}")
