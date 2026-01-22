# 反弹预警脚本
# 优先使用 Tushare 接口

triggered = False
signal = "WAIT"
message = "监控中..."

try:
    if pro is None:
        message = "Tushare 未初始化，无法执行"
    else:
        # 1. 转换 Symbol
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
            ts_code = f"{code}.SZ"

        # 2. 获取数据
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=60)).strftime("%Y%m%d")
        
        df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        
        if df is None or df.empty:
            message = "Tushare 未获取到数据"
        else:
            df = df.sort_values('trade_date')
            
            # 3. 计算指标
            df['MA5'] = df['close'].rolling(window=5).mean()
            df['MA10'] = df['close'].rolling(window=10).mean()
            df['MA20'] = df['close'].rolling(window=20).mean()
            
            if len(df) < 5:
                message = "数据不足，无法计算 MA5"
            else:
                curr = df.iloc[-1]
                prev = df.iloc[-2]
                
                # 4. 策略逻辑：反弹预警
                # 定义：
                # A. 趋势原本向下 (前一日收盘价 < MA5)
                # B. 今日站上 MA5 (收盘价 > MA5)
                # C. 今日是阳线 (收盘 > 开盘)
                # D. (可选) 处于低位 (例如 < MA20 或 MA5 < MA10)
                
                was_downtrend = prev['close'] < prev['MA5']
                break_up_ma5 = curr['close'] > curr['MA5']
                is_bullish = curr['close'] > curr['open']
                
                # 简单的低位判断：当前价格在 MA20 之下，或者是 MA5 刚拐头
                is_low_position = curr['close'] < curr['MA20']
                
                if was_downtrend and break_up_ma5 and is_bullish and is_low_position:
                    triggered = True
                    signal = "BUY"
                    message = f"反弹预警：股价({curr['close']}) 站上 MA5({curr['MA5']:.2f}) 且处于相对低位"
                else:
                    message = f"未触发：现价 {curr['close']}, MA5 {curr['MA5']:.2f}"

except Exception as e:
    triggered = False
    signal = "WAIT"
    message = f"脚本执行错误: {str(e)}"
