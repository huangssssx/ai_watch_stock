# Write python code here.
import akshare as ak
import pandas as pd
import numpy as np
import datetime

# ========================================================
# 配置：严格的狙击参数
# ========================================================
ALPHA_THRESHOLD = 0.6       # 门槛：Alpha必须 > 0.6
PIN_BAR_RATIO = 1.8         # 形态：下影线必须 > 实体的1.8倍
BREAK_MA_THRESHOLD = 0.005  # 形态：现价必须站稳均价线 0.5% 以上

# ========================================================
# 1. 选股池 (Top 100 活跃股)
# ========================================================
print("🚀 正在获取全市场实时行情...")
df_spot = ak.stock_zh_a_spot_em()
# 基础过滤
df_spot = df_spot[~df_spot['名称'].str.contains("ST|退")]
df_spot = df_spot[df_spot['成交额'] > 100000000] # 过滤掉流动性差的

# 取前100名活跃股
target_stocks = df_spot.sort_values(by='换手率', ascending=False).head(100)
results = []

print(f"🎯 锁定 Top 100 活跃股，开启【严格攻击形态】扫描...")
print(f"⚔️ 触发条件：Alpha > {ALPHA_THRESHOLD} 且 (站稳均价线{BREAK_MA_THRESHOLD*100}% 或 金针探底)")

# 时间处理
now = datetime.datetime.now()
today_str = now.strftime("%Y-%m-%d")
yesterday_str = (now - datetime.timedelta(days=1)).strftime("%Y%m%d")
start_dt_str = (now - datetime.timedelta(days=60)).strftime("%Y%m%d")

# ========================================================
# 2. 循环扫描
# ========================================================
count = 0

for index, row in target_stocks.iterrows():
    count += 1
    symbol = row['代码']
    name = row['名称']
    
    # 进度提示
    if count % 20 == 0:
        print(f"正在扫描: {count}/100...")

    try:
        # --- A. 历史数据 ---
        df_hist = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_dt_str, end_date=yesterday_str, adjust="qfq")
        if df_hist is None or df_hist.empty: continue
        
        # 标准化
        df_hist = df_hist.rename(columns={"日期": "date", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low", "成交量": "volume"})
        cols = ["open", "close", "high", "low", "volume"]
        df_hist[cols] = df_hist[cols].apply(pd.to_numeric, errors='coerce')

        # --- B. 实时分时聚合 (修正VWAP单位) ---
        df_min = ak.stock_zh_a_hist_min_em(symbol=symbol, period='1', adjust='')
        
        snapshot_data = None
        live_vwap = 0.0
        live_close = float(row['最新价'])
        
        if df_min is not None and not df_min.empty:
            df_min['时间'] = pd.to_datetime(df_min['时间'])
            df_today_min = df_min[df_min['时间'].dt.date == now.date()]
            
            if not df_today_min.empty:
                live_open = float(df_today_min.iloc[0]['开盘'])
                live_close = float(df_today_min.iloc[-1]['收盘'])
                live_high = float(df_today_min['最高'].max())
                live_low = float(df_today_min['最低'].min())
                live_volume_hands = float(df_today_min['成交量'].sum()) 
                live_amount = float(df_today_min['成交额'].sum())       
                
                # VWAP计算 (关键修正：手数 -> 股数)
                if live_volume_hands > 0:
                    live_vwap = live_amount / (live_volume_hands * 100)
                else:
                    live_vwap = live_close
                
                snapshot_data = {
                    "date": today_str, "open": live_open, "close": live_close,
                    "high": live_high, "low": live_low, "volume": live_volume_hands 
                }
        
        if snapshot_data is None: continue # 如果没有今日分时数据，无法判断形态，直接跳过

        # --- C. 计算 Alpha ---
        df_snapshot = pd.DataFrame([snapshot_data])
        df_calc = pd.concat([df_hist, df_snapshot], ignore_index=True)
        if len(df_calc) < 10: continue
            
        df_calc['log_vol'] = np.log(df_calc['volume'] + 1)
        df_calc['delta_vol'] = df_calc['log_vol'].diff(2)
        df_calc['alpha_ret'] = (df_calc['close'] - df_calc['open']) / df_calc['open']
        df_calc['corr'] = df_calc['delta_vol'].rolling(window=6).corr(df_calc['alpha_ret'])
        df_calc['alpha_2'] = -1 * df_calc['corr']
        
        current_alpha = df_calc.iloc[-1]['alpha_2']
        if np.isnan(current_alpha): continue

        # ========================================================
        # D. 严格判定逻辑 (The Gatekeeper)
        # ========================================================
        
        # 1. 第一道关卡：Alpha 必须足够大
        if current_alpha <= ALPHA_THRESHOLD:
            continue  # 只有 Alpha > 0.6 才往下走，否则直接丢弃
            
        # 2. 第二道关卡：必须有攻击形态
        # 计算均价乖离
        pct_over_vwap = (live_close - live_vwap) / live_vwap if live_vwap > 0 else 0
        is_break_vwap = pct_over_vwap > BREAK_MA_THRESHOLD
        
        # 计算金针探底
        body_len = abs(live_close - snapshot_data['open'])
        if body_len == 0: body_len = 0.001
        lower_shadow = min(snapshot_data['open'], live_close) - snapshot_data['low']
        pin_ratio = lower_shadow / body_len
        is_pin_bar = (pin_ratio > PIN_BAR_RATIO) and (live_close > snapshot_data['open']) # 必须是红盘
        
        # 3. 最终开火判定
        if is_break_vwap or is_pin_bar:
            reasons = []
            if is_break_vwap: reasons.append("站稳均线")
            if is_pin_bar: reasons.append("金针探底")
            
            # 只有这里才会 append，其他情况一律忽略
            results.append({
                "代码": symbol,
                "名称": name,
                "当前价": live_close,
                "Alpha2得分": round(current_alpha, 4),
                "均价乖离%": round(pct_over_vwap * 100, 2),
                "攻击形态": '+'.join(reasons), # 这一列必须有值
                "换手率%": row['换手率']
            })
            
    except Exception as e:
        continue

# ========================================================
# 3. 输出结果
# ========================================================
df = pd.DataFrame(results)

if df.empty:
    print("\n⚠️ 扫描完成：当前时刻没有股票同时满足 [Alpha>0.6 + 攻击形态]。")
    print("建议：市场可能处于混沌期，或主力尚未发动，请稍后再试。")
else:
    # 按 Alpha 得分排序（既然都满足形态了，就看谁的背离更强）
    df = df.sort_values(by="Alpha2得分", ascending=False)
    
    print(f"\n🔥🔥🔥 扫描完成：发现 {len(df)} 只正在发起攻击的标的 🔥🔥🔥")
    print(df.to_markdown(index=False, floatfmt=".2f")) # 使用 Markdown 格式打印更清晰

# df 变量保留，供后续使用