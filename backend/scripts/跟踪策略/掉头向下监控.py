# ==============================================================================
# 掉头向下监控 (Downturn Monitor)
# ------------------------------------------------------------------------------
# 目标：监测股票是否出现掉头向下的迹象，并区分是“技术性调整”还是“趋势反转/阴跌”。
# 核心逻辑：
# 1. 趋势反转 (Danger)：跌破MA20、放量下跌、或高位MACD死叉 -> STRONG_SELL
# 2. 技术调整 (Warning)：跌破MA5但缩量且守住MA20 -> SELL (减仓/止盈) 或 WAIT (观察)
# ==============================================================================

# 1. 初始化
triggered = False
signal = "WAIT"
message = "监控中：等待变盘信号..."

try:
    # 2. 预处理 Symbol
    symbol_code = symbol
    if symbol.startswith(("sh", "sz", "bj")):
        symbol_code = symbol[2:]

    # 3. 获取数据
    now = datetime.datetime.now()
    start_dt = (now - datetime.timedelta(days=120)).strftime("%Y%m%d")
    end_dt = now.strftime("%Y%m%d")
    
    df = ak.stock_zh_a_hist(symbol=symbol_code, period="daily", start_date=start_dt, end_date=end_dt, adjust="qfq")

    if df is None or df.empty or len(df) < 60:
        message = "未触发：历史数据不足"
    else:
        # 4. 指标计算
        close = pd.to_numeric(df["收盘"], errors="coerce")
        volume = pd.to_numeric(df["成交量"], errors="coerce")
        
        # 均线
        ma5 = close.rolling(window=5).mean()
        ma20 = close.rolling(window=20).mean()
        ma60 = close.rolling(window=60).mean()
        
        # 成交量均线
        vol_ma5 = volume.rolling(window=5).mean()
        
        # MACD
        exp1 = close.ewm(span=12, adjust=False).mean()
        exp2 = close.ewm(span=26, adjust=False).mean()
        macd = exp1 - exp2
        signal_line = macd.ewm(span=9, adjust=False).mean()
        
        # 获取最新数据
        curr_price = close.iloc[-1]
        prev_price = close.iloc[-2]
        curr_vol = volume.iloc[-1]
        curr_vol_ma5 = vol_ma5.iloc[-1]
        
        curr_ma5 = ma5.iloc[-1]
        curr_ma20 = ma20.iloc[-1]
        curr_ma60 = ma60.iloc[-1]
        
        curr_macd = macd.iloc[-1]
        curr_signal = signal_line.iloc[-1]
        prev_macd = macd.iloc[-2]
        prev_signal = signal_line.iloc[-2]
        
        # 5. 逻辑判断
        
        # 前置条件：之前应该是在上涨或高位震荡 (至少价格在MA60之上，或者MA20是向上的)
        # 如果已经是空头排列(价格<MA5<MA20<MA60)，那就是阴跌中
        is_downtrend_already = (curr_price < curr_ma5) and (curr_ma5 < curr_ma20) and (curr_ma20 < curr_ma60)
        
        # 判定 A: 趋势反转/大跌风险 (Strong Sell)
        # A1. 有效跌破MA20 (生命线)
        break_ma20 = (curr_price < curr_ma20) and (prev_price >= curr_ma20)
        # A2. 放量下跌 (跌幅>2% 且 量能 > 1.5倍MA5量)
        pct_change = (curr_price - prev_price) / prev_price
        heavy_volume_drop = (pct_change < -0.02) and (curr_vol > 1.5 * curr_vol_ma5)
        # A3. MACD 高位死叉 (MACD > 0)
        macd_dead_cross = (prev_macd > prev_signal) and (curr_macd < curr_signal) and (curr_macd > 0)
        
        is_danger = break_ma20 or heavy_volume_drop or (macd_dead_cross and curr_price < curr_ma5) or is_downtrend_already
        
        # 判定 B: 技术性调整 (Correction)
        # B1. 跌破MA5
        break_ma5 = (curr_price < curr_ma5)
        # B2. 依然守在MA20之上
        above_ma20 = (curr_price > curr_ma20)
        # B3. 缩量 (量能 < MA5量 或 略大但不超过1.2倍)
        shrinking_volume = (curr_vol < 1.2 * curr_vol_ma5)
        
        is_correction = break_ma5 and above_ma20 and shrinking_volume
        
        reasons = []
        if is_danger:
            if is_downtrend_already:
                reasons.append("已呈空头排列(阴跌)")
            if break_ma20:
                reasons.append("跌破MA20生命线")
            if heavy_volume_drop:
                reasons.append(f"放量杀跌({pct_change*100:.1f}%)")
            if macd_dead_cross:
                reasons.append("MACD高位死叉")
            
            triggered = True
            signal = "STRONG_SELL"
            message = f"📉【趋势反转】{' + '.join(reasons)} | 建议离场"
            
        elif is_correction:
            triggered = True
            signal = "SELL" # 标记为卖出信号，提醒用户注意，或者作为减仓提示
            message = f"⚠️【技术调整】跌破MA5但缩量，MA20({curr_ma20:.2f})仍有支撑 | 建议观察或减仓"
            
        else:
            # 可能是正常波动
            if curr_price < curr_ma5:
                 message = f"未触发：股价在MA5下方但未破位"
            else:
                 message = f"未触发：趋势暂稳 (>{curr_ma5:.2f})"

except Exception as e:
    triggered = False
    signal = "WAIT"
    message = f"脚本错误：{str(e)}"
    print(f"[Error] {e}")
