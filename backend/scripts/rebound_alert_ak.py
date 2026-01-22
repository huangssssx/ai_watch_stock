# 反弹预警 (Akshare版)
# 结合日线历史 + 分钟级实时数据

import akshare as ak
import pandas as pd
import datetime

triggered = False
signal = "WAIT"
message = "监控中..."

try:
    # 1. 解析 Symbol
    code = symbol[2:] if symbol.startswith(("sz", "sh", "bj")) else symbol
    
    # 2. 获取数据
    start_date = (datetime.datetime.now() - datetime.timedelta(days=100)).strftime("%Y%m%d")
    end_date = datetime.datetime.now().strftime("%Y%m%d")
    
    df_daily = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
    df_min = ak.stock_zh_a_hist_min_em(symbol=code, period="1", adjust="qfq")
    
    if df_daily is None or df_daily.empty:
        message = "Akshare: 未获取到日线数据"
    elif df_min is None or df_min.empty:
        message = "Akshare: 未获取到实时数据"
    else:
        # 3. 数据融合
        latest_min_row = df_min.iloc[-1]
        today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        last_daily_date = str(df_daily.iloc[-1]['日期'])
        current_price = float(latest_min_row['收盘'])
        
        is_daily_updated = (last_daily_date == today_str)
        
        if is_daily_updated:
            df_daily.at[df_daily.index[-1], '收盘'] = current_price
        else:
            new_row = {
                '日期': today_str, 
                '收盘': current_price, 
                '开盘': float(df_min.iloc[-1]['开盘']),
                '最高': float(df_min.iloc[-1]['最高']), 
                '最低': float(df_min.iloc[-1]['最低'])
            }
            if '时间' in df_min.columns:
                df_min['day'] = pd.to_datetime(df_min['时间']).dt.strftime('%Y-%m-%d')
                today_mins = df_min[df_min['day'] == today_str]
                if not today_mins.empty:
                    new_row['开盘'] = float(today_mins.iloc[0]['开盘'])
                    new_row['最高'] = float(today_mins['最高'].max())
                    new_row['最低'] = float(today_mins['最低'].min())
                    new_row['收盘'] = float(today_mins.iloc[-1]['收盘'])
            
            df_daily = pd.concat([df_daily, pd.DataFrame([new_row])], ignore_index=True)

        # 4. 计算指标
        df_daily['MA5'] = df_daily['收盘'].rolling(window=5).mean()
        df_daily['MA20'] = df_daily['收盘'].rolling(window=20).mean()
        
        if len(df_daily) < 5:
            message = "数据不足 5 天，无法计算 MA5"
        else:
            curr = df_daily.iloc[-1]
            prev = df_daily.iloc[-2]
            
            # 5. 策略逻辑：反弹预警
            # A. 趋势原本向下 (前一日收盘价 < 前一日 MA5)
            # B. 今日站上 MA5 (最新价 > 今日 MA5)
            # C. 今日收阳 (最新价 > 今日开盘价)
            # D. 低位确认 (最新价 < MA20)
            
            was_downtrend = prev['收盘'] < prev['MA5']
            break_up_ma5 = curr['收盘'] > curr['MA5']
            is_bullish = curr['收盘'] > curr['开盘']
            is_low_pos = curr['收盘'] < curr['MA20']
            
            if was_downtrend and break_up_ma5 and is_bullish and is_low_pos:
                triggered = True
                signal = "BUY"
                message = f"反弹预警(Ak)：现价({curr['收盘']}) 站上 MA5({curr['MA5']:.2f}) 且处于低位"
            else:
                reasons = []
                if not was_downtrend: reasons.append(f"前日已站上({prev['收盘']}>{prev['MA5']:.2f})")
                if not break_up_ma5: reasons.append(f"未站上MA5")
                if not is_bullish: reasons.append(f"今日收阴")
                if not is_low_pos: reasons.append(f"处于高位(>MA20 {curr['MA20']:.2f})")
                
                message = f"未触发：{'; '.join(reasons)}"

except Exception as e:
    triggered = False
    signal = "WAIT"
    message = f"脚本错误: {str(e)}"
