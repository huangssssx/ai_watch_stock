# ===== 规则：鬣狗战术监控 (Alpha#2 嗅探 + 右侧攻击确认) =====
# 目标：
# 1. 嗅探：实时计算 Alpha #2，确保量价背离信号依然强烈 (>0.6)。
# 2. 攻击：只有当价格出现"分时均价线突破"或"金针探底"时，才触发最终买入信号。
#
# 注入变量：symbol, ak, pd, np, datetime, time, triggered, message

# 1) 初始化
triggered = False
message = "监控中：等待猎物露出破绽..."

# 2) 参数配置
ALPHA_THRESHOLD = 0.6      # 嗅探阈值：背离必须足够强
PIN_BAR_RATIO = 1.8        # 攻击阈值：下影线长度必须是实体的1.8倍以上 (金针探底)
BREAK_MA_THRESHOLD = 0.005 # 攻击阈值：站稳分时均价线 0.5% 以上

# 3) 代码处理
symbol_code = symbol
if symbol.startswith("sh") or symbol.startswith("sz") or symbol.startswith("bj"):
    symbol_code = symbol[2:]

try:
    # ==========================================
    # 步骤 A: 获取历史数据 (History Base)
    # ==========================================
    now = datetime.datetime.now()
    yesterday_str = (now - datetime.timedelta(days=1)).strftime("%Y%m%d")
    start_dt_str = (now - datetime.timedelta(days=60)).strftime("%Y%m%d")
    
    df_hist = ak.stock_zh_a_hist(symbol=symbol_code, period="daily", start_date=start_dt_str, end_date=yesterday_str, adjust="qfq")

    if df_hist is None or df_hist.empty:
        message = "未触发：历史数据获取失败"
    else:
        # 标准化
        df_hist = df_hist.rename(columns={"日期": "date", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low", "成交量": "volume"})
        cols = ["open", "close", "high", "low", "volume"]
        df_hist[cols] = df_hist[cols].apply(pd.to_numeric, errors='coerce')

        # ==========================================
        # 步骤 B: 获取实时分时数据并聚合
        # ==========================================
        today_str_start = now.strftime("%Y-%m-%d 00:00:00")
        today_str_end = now.strftime("%Y-%m-%d 23:59:59")
        
        df_min = ak.stock_zh_a_hist_min_em(symbol=symbol_code, start_date=today_str_start, end_date=today_str_end, period='1', adjust='')
        
        if df_min is None or df_min.empty:
            message = "未触发：今日暂无分时数据"
        else:
            # 数据清洗与聚合
            df_min['时间'] = pd.to_datetime(df_min['时间'])
            df_today = df_min[df_min['时间'].dt.date == now.date()]
            
            if df_today.empty:
                 message = "未触发：今日暂无成交"
            else:
                # --- 核心聚合 ---
                live_open = float(df_today.iloc[0]['开盘'])
                live_close = float(df_today.iloc[-1]['收盘'])
                live_high = float(df_today['最高'].max())
                live_low = float(df_today['最低'].min())
                live_volume = float(df_today['成交量'].sum())
                
                # --- 计算分时均价 (VWAP) ---
                # 攻击信号的关键：价格是否站上今日的平均成本？
                # 计算公式：总成交额 / 总成交量
                live_amount = float(df_today['成交额'].sum())
                live_vwap = live_amount / live_volume if live_volume > 0 else live_close

                # ==========================================
                # 步骤 C: 计算 Alpha #2 (嗅探)
                # ==========================================
                snapshot_data = {
                    "date": now.strftime("%Y-%m-%d"), "open": live_open, "close": live_close, 
                    "high": live_high, "low": live_low, "volume": live_volume
                }
                df_calc = pd.concat([df_hist, pd.DataFrame([snapshot_data])], ignore_index=True)
                
                if len(df_calc) < 10:
                    message = "未触发：数据不足"
                else:
                    df_calc['log_vol'] = np.log(df_calc['volume'] + 1)
                    df_calc['delta_vol'] = df_calc['log_vol'].diff(2)
                    df_calc['alpha_ret'] = (df_calc['close'] - df_calc['open']) / df_calc['open']
                    df_calc['corr'] = df_calc['delta_vol'].rolling(window=6).corr(df_calc['alpha_ret'])
                    curr_alpha = -1 * df_calc.iloc[-1]['corr']
                    
                    # ==========================================
                    # 步骤 D: 鬣狗攻击判定 (核心逻辑)
                    # ==========================================
                    
                    # 1. 嗅探条件：背离必须依然存在
                    is_alpha_strong = curr_alpha > ALPHA_THRESHOLD
                    
                    if not is_alpha_strong:
                        message = f"未触发：血腥味变淡 (Alpha2={curr_alpha:.2f} < {ALPHA_THRESHOLD})"
                    else:
                        # 2. 攻击条件 A：分时均价线突破 (VWAP Breakout)
                        # 逻辑：价格站上全天均价线，说明多头开始控盘
                        pct_over_vwap = (live_close - live_vwap) / live_vwap
                        is_break_vwap = pct_over_vwap > BREAK_MA_THRESHOLD
                        
                        # 3. 攻击条件 B：金针探底 (Pin Bar)
                        # 逻辑：长下影线，说明底部承接极强
                        body_len = abs(live_close - live_open)
                        if body_len == 0: body_len = 0.001
                        lower_shadow = min(live_open, live_close) - live_low
                        is_pin_bar = (lower_shadow / body_len) > PIN_BAR_RATIO
                        
                        # 4. 攻击条件 C：红盘确认
                        # 逻辑：不管怎样，现在必须是涨的（或微跌但强势）
                        is_red = live_close > live_open
                        
                        # --- 综合决策 ---
                        triggers = []
                        if is_break_vwap: triggers.append(f"站稳均价线({live_vwap:.2f})")
                        if is_pin_bar: triggers.append("金针探底")
                        
                        # 最终开火指令：Alpha达标 + (站稳均价线 OR (金针探底 AND 翻红))
                        if triggers and (is_break_vwap or (is_pin_bar and is_red)):
                            triggered = True
                            message = f"🐺【鬣狗撕咬】Alpha2({curr_alpha:.2f})确认 + {'+'.join(triggers)} | 现价:{live_close}"
                        else:
                            # Alpha 很高但形态不行，继续跟踪
                            message = f"👀 锁定猎物：Alpha2({curr_alpha:.2f})极高，但攻击形态未确认 (均价:{live_vwap:.2f})"
                            print(f"[跟踪] 现价:{live_close} VWAP:{live_vwap:.2f} 下影线比:{lower_shadow/body_len:.1f}")

except Exception as e:
    triggered = False
    message = f"错误：{str(e)}"
    print(f"[error] {e}")