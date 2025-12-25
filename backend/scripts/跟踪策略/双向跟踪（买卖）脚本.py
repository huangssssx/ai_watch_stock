# ==============================================================================
# 双向猎杀监控 (Hybrid Hunter)
# ------------------------------------------------------------------------------
# 逻辑说明：
# 1. 卖出 (秃鹫): 优先级最高。监测跌破均价线或流星线。
# 2. 买入 (鬣狗): 监测 Alpha#2 背离 (>0.6) 叠加 站稳均线或金针探底。
# 3. 观望: 无明确信号。
# ==============================================================================

# 1. 初始化
triggered = False
signal = "WAIT"
message = "监控中：多空双向扫描..."

# 2. 参数配置
# --- 买入参数 (Hyena) ---
BUY_ALPHA_THRESHOLD = 0.6       # 买入要求：Alpha必须 > 0.6
BUY_PIN_BAR_RATIO = 1.8         # 买入形态：下影线 > 实体 1.8倍
BUY_BREAK_MA_THRESHOLD = 0.005  # 买入形态：站稳均价线 0.5%

# --- 卖出参数 (Vulture) ---
SELL_BREAK_MA_THRESHOLD = 0.005 # 卖出形态：跌破均价线 0.5%
SELL_SHOOTING_STAR_RATIO = 1.8  # 卖出形态：上影线 > 实体 1.8倍

try:
    # 3. 预处理 Symbol
    symbol_code = symbol
    if symbol.startswith(("sh", "sz", "bj")):
        symbol_code = symbol[2:]

    # 4. 时间设定
    now = datetime.datetime.now()
    today_str_start = now.strftime("%Y-%m-%d 00:00:00")
    today_str_end = now.strftime("%Y-%m-%d 23:59:59")
    yesterday_str = (now - datetime.timedelta(days=1)).strftime("%Y%m%d")
    start_dt_str = (now - datetime.timedelta(days=60)).strftime("%Y%m%d")

    # ==========================================
    # 步骤 A: 获取数据 (历史 + 实时)
    # ==========================================
    # 1) 获取历史日线 (用于计算 Alpha)
    df_hist = ak.stock_zh_a_hist(symbol=symbol_code, period="daily", start_date=start_dt_str, end_date=yesterday_str, adjust="qfq")
    
    # 2) 获取今日分时 (用于计算 VWAP 和 形态)
    df_min = ak.stock_zh_a_hist_min_em(symbol=symbol_code, start_date=today_str_start, end_date=today_str_end, period='1', adjust='')

    if df_hist is None or df_hist.empty:
        message = "未触发：历史数据不足"
    elif df_min is None or df_min.empty:
        message = "未触发：今日暂无分时数据"
    else:
        # 数据清洗
        # 历史
        df_hist = df_hist.rename(columns={"日期": "date", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low", "成交量": "volume"})
        cols = ["open", "close", "high", "low", "volume"]
        df_hist[cols] = df_hist[cols].apply(pd.to_numeric, errors='coerce')
        
        # 实时
        df_min['时间'] = pd.to_datetime(df_min['时间'])
        df_today = df_min[df_min['时间'].dt.date == now.date()]
        
        if df_today.empty:
            message = "未触发：今日尚未成交"
        else:
            # ==========================================
            # 步骤 B: 核心指标计算
            # ==========================================
            
            # --- 1. 实时聚合数据 ---
            live_open = float(df_today.iloc[0]['开盘'])
            live_close = float(df_today.iloc[-1]['收盘'])
            live_high = float(df_today['最高'].max())
            live_low = float(df_today['最低'].min())
            live_volume = float(df_today['成交量'].sum()) # 手
            live_amount = float(df_today['成交额'].sum()) # 元
            
            # --- 2. 计算 VWAP (均价) [已修复单位] ---
            if live_volume > 0:
                live_vwap = live_amount / (live_volume * 100)
            else:
                live_vwap = live_close
            
            # --- 3. 计算 Alpha #2 ---
            snapshot_data = {
                "date": now.strftime("%Y-%m-%d"), "open": live_open, "close": live_close, 
                "high": live_high, "low": live_low, "volume": live_volume
            }
            df_calc = pd.concat([df_hist, pd.DataFrame([snapshot_data])], ignore_index=True)
            
            # Alpha 计算
            if len(df_calc) >= 10:
                df_calc['log_vol'] = np.log(df_calc['volume'] + 1)
                df_calc['delta_vol'] = df_calc['log_vol'].diff(2)
                df_calc['alpha_ret'] = (df_calc['close'] - df_calc['open']) / df_calc['open']
                df_calc['corr'] = df_calc['delta_vol'].rolling(window=6).corr(df_calc['alpha_ret'])
                curr_alpha = -1 * df_calc.iloc[-1]['corr']
            else:
                curr_alpha = 0.0 # 数据不足时给0
            
            # --- 4. 形态基础计算 ---
            body_len = abs(live_close - live_open)
            if body_len == 0: body_len = 0.001
            
            # 上影线 (用于卖出)
            upper_shadow = live_high - max(live_open, live_close)
            shooting_star_ratio = upper_shadow / body_len
            
            # 下影线 (用于买入)
            lower_shadow = min(live_open, live_close) - live_low
            pin_bar_ratio = lower_shadow / body_len
            
            # 涨跌状态
            is_red = live_close > live_open
            is_green = live_close < live_open

            # ==========================================
            # 步骤 C: 决策逻辑 (双向判定)
            # ==========================================

            # ----------------------------------
            # 判定 1: 卖出信号 (Vulture / 秃鹫)
            # 优先级：最高 (风控优先)
            # ----------------------------------
            sell_reasons = []
            
            # 卖出条件A: 跌破均线 (Breakdown)
            pct_under_vwap = (live_vwap - live_close) / live_vwap
            if pct_under_vwap > SELL_BREAK_MA_THRESHOLD:
                sell_reasons.append(f"跌破均线({live_vwap:.2f})")
            
            # 卖出条件B: 流星线 (Shooting Star)
            if shooting_star_ratio > SELL_SHOOTING_STAR_RATIO:
                sell_reasons.append("流星线(冲高回落)")
            
            if sell_reasons:
                # 触发卖出 (只要满足任意卖出条件)
                triggered = True
                signal = "STRONG_SELL"
                message = f"📉【秃鹫卖出】空头控盘 | {'+'.join(sell_reasons)} | 现价:{live_close} < 均价:{live_vwap:.2f}"
            
            else:
                # ----------------------------------
                # 判定 2: 买入信号 (Hyena / 鬣狗)
                # 仅在无卖出信号时检测
                # ----------------------------------
                buy_reasons = []
                
                # 门槛: Alpha 必须达标
                if curr_alpha > BUY_ALPHA_THRESHOLD:
                    
                    # 买入条件A: 站稳均线 (Breakout)
                    pct_over_vwap = (live_close - live_vwap) / live_vwap
                    if pct_over_vwap > BUY_BREAK_MA_THRESHOLD:
                        buy_reasons.append(f"站稳均线({live_vwap:.2f})")
                    
                    # 买入条件B: 金针探底 (Pin Bar) + 必须是红盘
                    if (pin_bar_ratio > BUY_PIN_BAR_RATIO) and is_red:
                        buy_reasons.append("金针探底")
                    
                    if buy_reasons:
                        # 触发买入
                        triggered = True
                        signal = "STRONG_BUY"
                        message = f"🐺【鬣狗进攻】Alpha({curr_alpha:.2f})确认 + {'+'.join(buy_reasons)} | 现价:{live_close}"
                    else:
                        # Alpha 高但无形态 -> 观望 (蓄力)
                        triggered = False
                        signal = "WAIT"
                        message = f"👀 锁定猎物：Alpha({curr_alpha:.2f})高，等待攻击形态 (均价:{live_vwap:.2f})"
                        print(f"[跟踪] {symbol_code} Alpha:{curr_alpha:.2f} 现价:{live_close} VWAP:{live_vwap:.2f}")
                else:
                    # ----------------------------------
                    # 判定 3: 垃圾时间
                    # ----------------------------------
                    triggered = False
                    signal = "WAIT"
                    message = f"未触发：多空平衡 (Alpha:{curr_alpha:.2f} / 均价:{live_vwap:.2f})"

except Exception as e:
    triggered = False
    signal = "WAIT"
    message = f"脚本错误：{str(e)}"
    print(f"[Error] {e}")