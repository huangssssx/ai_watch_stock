# 掉头向下预警脚本
# 优先使用 Tushare 接口

triggered = False
signal = "WAIT"
message = "监控中..."

try:
    if pro is None:
        message = "Tushare 未初始化，无法执行"
    else:
        # 1. 转换 Symbol 格式 (例如 sz000001 -> 000001.SZ)
        # 系统传入的 symbol 通常是 "sz000001" 或 "sh600000"
        code = symbol[2:]
        exchange_prefix = symbol[:2].lower()
        
        ts_code = ""
        if exchange_prefix == 'sz':
            ts_code = f"{code}.SZ"
        elif exchange_prefix == 'sh':
            ts_code = f"{code}.SH"
        elif exchange_prefix == 'bj':
            ts_code = f"{code}.BJ"
        else:
            ts_code = f"{code}.SZ" # 默认假设，或根据实际情况调整

        # 2. 获取数据 (获取最近 60 天以确保有足够的均线数据)
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=60)).strftime("%Y%m%d")
        
        df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        
        if df is None or df.empty:
            message = "Tushare 未获取到数据"
        else:
            # Tushare 返回的数据通常是倒序的（最新日期在最前），需要按日期升序排列以便计算
            df = df.sort_values('trade_date')
            
            # 3. 计算指标
            # 移动平均线
            df['MA5'] = df['close'].rolling(window=5).mean()
            df['MA10'] = df['close'].rolling(window=10).mean()
            df['MA20'] = df['close'].rolling(window=20).mean()
            
            if len(df) < 5:
                message = "数据不足，无法计算 MA5"
            else:
                curr = df.iloc[-1]
                prev = df.iloc[-2]
                
                # 4. 策略逻辑：掉头向下
                # 定义：
                # A. 趋势原本向上 (前一日收盘价在 MA5 之上)
                # B. 今日跌破 MA5 (收盘价 < MA5)
                # C. 今日是阴线 (收盘 < 开盘)
                # D. (可选) 跌幅超过一定阈值或放量，这里暂取简单逻辑
                
                was_uptrend = prev['close'] > prev['MA5']
                break_ma5 = curr['close'] < curr['MA5']
                is_bearish = curr['close'] < curr['open']
                
                # 也可以增加：当前价格在高位 (例如 > MA20)
                is_high_position = curr['close'] > curr['MA20']
                
                if was_uptrend and break_ma5 and is_bearish and is_high_position:
                    triggered = True
                    signal = "SELL"
                    message = f"掉头向下预警：股价({curr['close']}) 跌破 MA5({curr['MA5']:.2f}) 且收阴"
                else:
                    # 补充信息用于调试
                    message = f"未触发：现价 {curr['close']}, MA5 {curr['MA5']:.2f}"

except Exception as e:
    triggered = False
    signal = "WAIT"
    message = f"脚本执行错误: {str(e)}"
