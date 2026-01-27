# 掉头向下预警 (Akshare版)
# 结合日线历史 + 分钟级实时数据

import akshare as ak
import pandas as pd
import datetime
from utils.ak_fallback import get_a_minute_data

triggered = False
signal = "WAIT"
message = "监控中..."

try:
    # 1. 解析 Symbol (sz000001 -> 000001)
    code = symbol[2:] if symbol.startswith(("sz", "sh", "bj")) else symbol
    
    # 2. 获取数据
    # 2.1 获取日线历史 (用于计算均线)
    # 获取足够多的历史数据以确保 MA20 计算准确
    start_date = (datetime.datetime.now() - datetime.timedelta(days=100)).strftime("%Y%m%d")
    end_date = datetime.datetime.now().strftime("%Y%m%d")
    
    df_daily = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
    
    # 2.2 获取实时/分钟数据 (用于构建今日最新 K 线)
    # stock_zh_a_hist_min_em 返回最近的分钟数据，包含今日的最新走势
    df_min = get_a_minute_data(symbol=code, period="1", adjust="qfq")
    
    if df_daily is None or df_daily.empty:
        message = "Akshare: 未获取到日线数据"
    elif df_min is None or df_min.empty:
        message = "Akshare: 未获取到实时数据"
    else:
        # 3. 数据融合：构建"实时日线"
        # 提取分钟数据中的今日信息
        latest_min_row = df_min.iloc[-1]
        latest_time_str = str(latest_min_row['时间']) # 格式通常为 "2023-10-27 15:00:00"
        today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        
        # 简单判断：如果分钟数据里有今天的日期，则提取今日的 Open, High, Low, Close
        # 注意：df_min 通常包含多个交易日，需筛选出"今天"的数据来聚合，或者直接取最后一行作为最新价
        # 更稳健的做法：取 df_min 中属于"今天"的所有行，计算 OHLC
        # 但为了简化且高效，我们假设 df_min 的最后一行就是最新状态，
        # 并用 df_daily 的最后一行（如果是今天）进行更新，或者追加新行
        
        # 检查 df_daily 最后一行日期
        last_daily_date = str(df_daily.iloc[-1]['日期'])
        current_price = float(latest_min_row['收盘'])
        
        # 尝试从分钟数据聚合今日数据 (如果分钟数据覆盖了今天)
        # 这里的逻辑是：如果 daily 数据还没更新到今天，我们需要手动构造今天的 bar
        # 如果 daily 数据已经是今天（收盘后），则直接使用
        
        is_daily_updated = (last_daily_date == today_str)
        
        # 无论 daily 是否更新，我们都信任分钟数据的"最新价"
        # 重新构造/更新 df_daily
        
        if is_daily_updated:
            # 更新最后一行 (以防 daily 数据滞后)
            df_daily.at[df_daily.index[-1], '收盘'] = current_price
            # 注意：High/Low 也应该更新，但这里简化处理，只关注收盘价与均线的关系
        else:
            # 追加一行 "Today"
            new_row = {
                '日期': today_str, 
                '收盘': current_price, 
                '开盘': float(df_min.iloc[-1]['开盘']), # 近似，其实应该找今日第一笔
                '最高': float(df_min.iloc[-1]['最高']), 
                '最低': float(df_min.iloc[-1]['最低'])
            }
            # 如果能筛选出今日分钟线最好
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
            
            # 5. 策略逻辑：掉头向下
            # A. 趋势原本向上 (前一日收盘价 > 前一日 MA5)
            # B. 今日跌破 MA5 (最新价 < 今日 MA5)
            # C. 今日收阴 (最新价 < 今日开盘价)
            # D. 高位保护 (最新价 > MA20)
            
            was_uptrend = prev['收盘'] > prev['MA5']
            break_ma5 = curr['收盘'] < curr['MA5']
            is_bearish = curr['收盘'] < curr['开盘']
            is_high_pos = curr['收盘'] > curr['MA20']
            
            if was_uptrend and break_ma5 and is_bearish and is_high_pos:
                triggered = True
                signal = "SELL"
                message = f"掉头向下(Ak)：现价({curr['收盘']}) 跌破 MA5({curr['MA5']:.2f}) 且收阴"
            else:
                reasons = []
                if not was_uptrend: reasons.append(f"前日已破位({prev['收盘']}<{prev['MA5']:.2f})")
                if not break_ma5: reasons.append(f"未破MA5")
                if not is_bearish: reasons.append(f"今日收阳")
                if not is_high_pos: reasons.append(f"处于低位(<MA20 {curr['MA20']:.2f})")
                
                message = f"未触发：{'; '.join(reasons)}"

except Exception as e:
    triggered = False
    signal = "WAIT"
    message = f"脚本错误: {str(e)}"
