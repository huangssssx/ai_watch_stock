# Write python code here.
import akshare as ak
import pandas as pd
import numpy as np
import datetime
import warnings

# 忽略 pandas 的一些计算警告
warnings.filterwarnings('ignore')

# ========================================================
# 核心配置：双因子 + 攻击形态
# ========================================================
# 1. 潜伏因子 (Alpha #2)
ALPHA2_THRESHOLD = 0.6          # 门槛：量价背离度 > 0.6

# 2. 确信因子 (Alpha #101)
# 公式: (close - open) / ((high - low) + 0.001)
# 作用: 过滤掉十字星或上影线太长的K线，只做实体饱满的攻击K线
ALPHA101_THRESHOLD = 0.3        # 门槛：实体长度至少占波动的 30%

# 3. 攻击形态
PIN_BAR_RATIO = 1.8             # 下影线 > 实体 1.8倍
BREAK_MA_THRESHOLD = 0.005      # 站稳均价线 0.5%

# ========================================================
# 0. 市场状态检测 (自适应核心)
# ========================================================
print("🚀 正在连接交易所实时数据...")
df_spot = ak.stock_zh_a_spot_em()
# 基础过滤：剔除ST、退市、北交所(可选，这里保留但需注意代码后缀处理)
df_spot = df_spot[~df_spot['名称'].str.contains("ST|退")]

# 判断逻辑：检查全市场总成交额
# 如果小于 100亿，认为还没开盘(或竞价刚开始)，进入【历史/竞价模式】
# 如果大于 100亿，认为已经开盘，进入【实时模式】
total_turnover = df_spot['成交额'].sum()
is_market_open = total_turnover > 10000000000 

target_stocks = None
mode_name = ""

if is_market_open:
    mode_name = "RealTime (盘中)"
    print(f"✅ [盘中模式] 市场活跃，按【换手率 & 成交额】锁定 Top 100 热点股...")
    # 逻辑：盘中只做流动性好的热点
    df_active = df_spot[df_spot['成交额'] > 100000000] # 至少1亿成交
    if len(df_active) < 50: df_active = df_spot       # 刚开盘容错
    target_stocks = df_active.sort_values(by='换手率', ascending=False).head(100)
else:
    mode_name = "History/Auction (盘前/竞价)"
    print(f"🌙 [盘前模式] 市场未开，按【流通市值】锁定 Top 100 核心资产...")
    # 逻辑：盘前没法确认热点，优先看大票/核心票的昨日表现
    target_stocks = df_spot.sort_values(by='流通市值', ascending=False).head(100)

print(f"🎯 选股池加载完毕：{len(target_stocks)} 只标的，开始双因子计算...")

# 时间参数
now = datetime.datetime.now()
today_str = now.strftime("%Y-%m-%d")
yesterday_str = (now - datetime.timedelta(days=1)).strftime("%Y%m%d")
# 多拉取一些数据以确保 Alpha 计算准确
start_dt_str = (now - datetime.timedelta(days=60)).strftime("%Y%m%d")

results = []

# ========================================================
# 1. 循环扫描
# ========================================================
count = 0
for index, row in target_stocks.iterrows():
    count += 1
    symbol = row['代码']
    name = row['名称']
    
    # 进度显示
    if count % 20 == 0:
        print(f"...进度 {count}/{len(target_stocks)}")

    try:
        # --- A. 获取日线 (Base) ---
        df_hist = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_dt_str, end_date=yesterday_str, adjust="qfq")
        if df_hist is None or df_hist.empty: continue
        
        # 清洗
        df_hist = df_hist.rename(columns={"日期": "date", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low", "成交量": "volume"})
        cols = ["open", "close", "high", "low", "volume"]
        df_hist[cols] = df_hist[cols].apply(pd.to_numeric, errors='coerce')

        # --- B. 数据源构建 (自适应拼接) ---
        df_calc = df_hist.copy()
        data_source = "History"
        snapshot_data = None
        
        live_vwap = 0.0
        live_close = 0.0
        live_open = 0.0
        
        # 尝试获取今日实时数据
        has_realtime = False
        
        # 1. 尝试分钟线 (盘中)
        if is_market_open:
            try:
                df_min = ak.stock_zh_a_hist_min_em(symbol=symbol, period='1', adjust='')
                if df_min is not None and not df_min.empty:
                    df_min['时间'] = pd.to_datetime(df_min['时间'])
                    df_today_min = df_min[df_min['时间'].dt.date == now.date()]
                    
                    if not df_today_min.empty:
                        has_realtime = True
                        data_source = "RealTime"
                        
                        live_open = float(df_today_min.iloc[0]['开盘'])
                        live_close = float(df_today_min.iloc[-1]['收盘'])
                        live_high = float(df_today_min['最高'].max())
                        live_low = float(df_today_min['最低'].min())
                        live_volume = float(df_today_min['成交量'].sum())
                        live_amount = float(df_today_min['成交额'].sum())
                        
                        # VWAP 计算 (修正单位：手 -> 股)
                        if live_volume > 0:
                            live_vwap = live_amount / (live_volume * 100)
                        else:
                            live_vwap = live_close
                            
                        snapshot_data = {
                            "date": today_str, "open": live_open, "close": live_close,
                            "high": live_high, "low": live_low, "volume": live_volume
                        }
            except:
                pass
        
        # 2. 尝试快照/竞价 (如果分钟线失败，但已有开盘价)
        if not has_realtime:
            spot_open = float(row['今开'])
            spot_close = float(row['最新价'])
            if spot_open > 0:
                data_source = "Auction/Spot"
                live_close = spot_close
                live_open = spot_open
                live_vwap = spot_close # 快照无法算 VWAP
                
                snapshot_data = {
                    "date": today_str,
                    "open": spot_open, "close": spot_close,
                    "high": float(row['最高']), "low": float(row['最低']),
                    "volume": float(row['成交量']) if float(row['成交量']) > 0 else 100
                }

        # 拼接数据
        if snapshot_data:
            df_calc = pd.concat([df_hist, pd.DataFrame([snapshot_data])], ignore_index=True)
            
        if len(df_calc) < 10: continue

        # --- C. 计算 Alpha (双因子) ---
        
        # [Alpha #2] 潜伏因子
        # 公式：量价背离相关性
        df_calc['log_vol'] = np.log(df_calc['volume'] + 1)
        df_calc['delta_vol'] = df_calc['log_vol'].diff(2)
        df_calc['alpha_ret'] = (df_calc['close'] - df_calc['open']) / df_calc['open']
        df_calc['corr'] = df_calc['delta_vol'].rolling(window=6).corr(df_calc['alpha_ret'])
        df_calc['alpha_2'] = -1 * df_calc['corr']
        
        # [Alpha #101] 确信因子
        # 公式：(Close - Open) / ((High - Low) + 0.001)
        # 含义：K线实体力度。正值越大越强，负值越小越弱。
        df_calc['range'] = (df_calc['high'] - df_calc['low']) + 0.001
        df_calc['body'] = df_calc['close'] - df_calc['open']
        df_calc['alpha_101'] = df_calc['body'] / df_calc['range']
        
        # 获取最新值
        curr_alpha2 = df_calc.iloc[-1]['alpha_2']
        curr_alpha101 = df_calc.iloc[-1]['alpha_101']
        
        if np.isnan(curr_alpha2) or np.isnan(curr_alpha101): continue

        # --- D. 综合筛选逻辑 ---
        
        status_code = 0
        status_msg = "-"
        
        # 1. 基础筛选：双因子共振
        # Alpha2 必须高 (有资金运作) 且 Alpha101 必须达标 (不是十字星，有攻击意愿)
        if curr_alpha2 > ALPHA2_THRESHOLD and curr_alpha101 > ALPHA101_THRESHOLD:
            
            # === 分场景判定 ===
            
            # [场景 1] 盘中实时：必须叠加攻击形态
            if data_source == "RealTime":
                # 形态 A: 站稳均线
                pct_over_vwap = (live_close - live_vwap) / live_vwap if live_vwap > 0 else 0
                is_break_vwap = pct_over_vwap > BREAK_MA_THRESHOLD
                
                # 形态 B: 金针探底 (且要是红盘或 Alpha101 极强)
                body_len = abs(live_close - live_open) if abs(live_close - live_open) > 0 else 0.001
                lower_shadow = min(live_open, live_close) - snapshot_data['low']
                is_pin_bar = (lower_shadow / body_len > PIN_BAR_RATIO) and (live_close >= live_open)
                
                triggers = []
                if is_break_vwap: triggers.append("站稳均线")
                if is_pin_bar: triggers.append("金针探底")
                
                if triggers:
                    status_code = 100
                    status_msg = f"🔥进攻[{'+'.join(triggers)}]"
                else:
                    # 因子虽好但形态未出，作为观察
                    status_code = 50
                    status_msg = "👀蓄力(等待突破)"

            # [场景 2] 竞价/历史：只看因子共振
            else:
                prefix = "⚡竞价" if data_source == "Auction/Spot" else "📝昨强"
                status_code = 80 if data_source == "Auction/Spot" else 60
                status_msg = f"{prefix}:双因子共振"

        # 结果收录
        # 我们只收录分数高的 (Code >= 60)，过滤掉纯蓄力的 (除非你想看)
        if status_code >= 60:
            current_price = df_calc.iloc[-1]['close']
            results.append({
                "代码": symbol,
                "名称": name,
                "模式": data_source,
                "当前价": current_price,
                "Alpha2(潜伏)": round(curr_alpha2, 2),
                "Alpha101(力度)": round(curr_alpha101, 2),
                "状态": status_msg,
                "排序分": status_code + curr_alpha2 + curr_alpha101 # 综合排序
            })

    except Exception as e:
        continue

# ========================================================
# 2. 输出报告
# ========================================================
df = pd.DataFrame(results)

if not df.empty:
    df = df.sort_values(by="排序分", ascending=False)
    # 调整列顺序
    out_cols = ["代码", "名称", "模式", "当前价", "Alpha2(潜伏)", "Alpha101(力度)", "状态"]
    df = df[out_cols]
    
    print(f"\n=== 扫描完成 [{mode_name}]：发现 {len(df)} 只共振标的 ===")
    print("说明：Alpha2>0.6代表资金背离; Alpha101>0.3代表K线实体饱满。")
    print(df.head(20).to_markdown(index=False, floatfmt=".2f"))
else:
    print(f"\n⚠️ 扫描完成，未发现符合 [双因子共振 + 形态] 的标的。")
    if is_market_open:
        print("提示：盘中标准极高，要求同时满足资金背离、K线力度和站稳均线。")
    else:
        print("提示：盘前未发现昨日强势的双因子共振股。")

        https://ark.cn-beijing.volces.com/api/v3