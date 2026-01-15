# ==============================================================================
# 触底反弹监控 (Bottom Rebound Monitor)
# ------------------------------------------------------------------------------
# 目标：捕捉处于下跌趋势或低位盘整中，出现技术面反转信号的股票。
# 核心逻辑：
# 1. 趋势：处于相对低位 (收盘价 < MA60 或 距60日高点跌幅 > 20%)
# 2. 信号：站上MA5 + (MACD金叉 或 RSI底背离/低位回升)
# ==============================================================================

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
    # 获取足够长的历史数据以计算 MA60, MACD, RSI
    start_dt = (now - datetime.timedelta(days=120)).strftime("%Y%m%d")
    end_dt = now.strftime("%Y%m%d")
    
    df = ak.stock_zh_a_hist(symbol=symbol_code, period="daily", start_date=start_dt, end_date=end_dt, adjust="qfq")

    if df is None or df.empty or len(df) < 60:
        message = "未触发：历史数据不足"
    else:
        # 4. 指标计算
        close = pd.to_numeric(df["收盘"], errors="coerce")
        high = pd.to_numeric(df["最高"], errors="coerce")
        low = pd.to_numeric(df["最低"], errors="coerce")
        
        # 均线
        ma5 = close.rolling(window=5).mean()
        ma20 = close.rolling(window=20).mean()
        ma60 = close.rolling(window=60).mean()
        
        # MACD (12, 26, 9)
        exp1 = close.ewm(span=12, adjust=False).mean()
        exp2 = close.ewm(span=26, adjust=False).mean()
        macd = exp1 - exp2
        signal_line = macd.ewm(span=9, adjust=False).mean()
        hist = (macd - signal_line) * 2
        
        # RSI (14)
        delta = close.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))

        # 获取最新值 (今日)
        curr_price = close.iloc[-1]
        curr_ma5 = ma5.iloc[-1]
        curr_ma60 = ma60.iloc[-1]
        curr_macd = macd.iloc[-1]
        curr_signal = signal_line.iloc[-1]
        curr_hist = hist.iloc[-1]
        curr_rsi = rsi.iloc[-1]
        
        # 获取昨日值
        prev_price = close.iloc[-2]
        prev_ma5 = ma5.iloc[-2]
        prev_macd = macd.iloc[-2]
        prev_signal = signal_line.iloc[-2]
        prev_hist = hist.iloc[-2]
        prev_rsi = rsi.iloc[-2]

        # 5. 逻辑判断
        reasons = []
        
        # 条件A：处于弱势/低位 (股价在MA60下方，或者RSI较低)
        # 这里定义为：股价 < MA60 或 RSI < 50 (偏弱区域)
        is_low_position = (curr_price < curr_ma60) or (curr_rsi < 50)
        
        if is_low_position:
            # 条件B：反弹启动 (站上MA5)
            # 今日收盘 > MA5 且 昨日收盘 < MA5 (刚突破) 或 已经站稳(连续2日 > MA5)
            # 简化：今日 > MA5
            is_above_ma5 = curr_price > curr_ma5
            
            if is_above_ma5:
                # 条件C：辅助指标确认
                
                # C1: MACD 金叉 (MACD上穿Signal，或柱状图由负转正/变长)
                macd_golden_cross = (prev_macd < prev_signal) and (curr_macd > curr_signal)
                macd_turning_up = (curr_hist > prev_hist) and (curr_hist > 0) # 柱子增长且为正
                
                # C2: RSI 低位回升
                rsi_rebound = (prev_rsi < 40) and (curr_rsi > prev_rsi)
                
                if macd_golden_cross:
                    reasons.append("MACD金叉")
                elif macd_turning_up:
                    reasons.append("MACD走强")
                    
                if rsi_rebound:
                    reasons.append(f"RSI回升({int(prev_rsi)}->{int(curr_rsi)})")
                
                # 只有当站上MA5 且 有辅助指标确认时，才触发
                if reasons:
                    triggered = True
                    signal = "BUY"
                    message = f"🚀【触底反弹】站上MA5 + {'+'.join(reasons)} | 现价:{curr_price:.2f}"
                else:
                    message = f"未触发：虽站上MA5，但缺乏指标共振 (MACD/RSI)"
            else:
                message = f"未触发：股价仍受压于MA5 ({curr_ma5:.2f})"
        else:
            message = "未触发：非低位/弱势区间，不符合抄底策略"

except Exception as e:
    triggered = False
    signal = "WAIT"
    message = f"脚本错误：{str(e)}"
    print(f"[Error] {e}")
