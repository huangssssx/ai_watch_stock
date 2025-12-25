import akshare as ak
import pandas as pd
import numpy as np
import datetime
import logging

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ========================================================
# 配置
# ========================================================
ALPHA_THRESHOLD = 0.6
PIN_BAR_RATIO = 1.8
BREAK_MA_THRESHOLD = 0.005

# ========================================================
# 1. 选股池 (Top 100)
# ========================================================
logger.info("🚀 正在获取全市场实时行情...")
try:
    df_spot = ak.stock_zh_a_spot_em()
    logger.info(f"获取到 {len(df_spot)} 只股票的实时行情")
except Exception as e:
    logger.error(f"获取实时行情失败: {e}")
    exit(1)

# 基础过滤
logger.info("开始过滤ST股票...")
df_spot = df_spot[~df_spot['名称'].str.contains("ST|退")]
logger.info(f"过滤ST后剩余: {len(df_spot)} 只")

logger.info("过滤成交额低于1亿的股票...")
df_spot = df_spot[df_spot['成交额'] > 100000000]
logger.info(f"过滤成交额后剩余: {len(df_spot)} 只")

# 按换手率排序取前100 (保证活跃度)
target_stocks = df_spot.sort_values(by='换手率', ascending=False).head(100)
results = []
logger.info(f"🎯 锁定 Top 100 活跃股，换手率范围: {target_stocks['换手率'].min():.2f}% - {target_stocks['换手率'].max():.2f}%")

# 时间处理
now = datetime.datetime.now()
today_str = now.strftime("%Y-%m-%d")
yesterday_str = (now - datetime.timedelta(days=1)).strftime("%Y%m%d")
start_dt_str = (now - datetime.timedelta(days=60)).strftime("%Y%m%d")

logger.info(f"时间设置: 今天={today_str}, 昨天={yesterday_str}, 起始日={start_dt_str}")

# ========================================================
# 2. 循环扫描
# ========================================================
count = 0
success_count = 0
alpha_pass_count = 0
signal_count = 0

stats = {
    'total': 100,
    'hist_fail': 0,
    'min_data_success': 0,
    'spot_data_success': 0,
    'history_mode': 0,
    'realtime_mode': 0,
    'auction_mode': 0,
    'alpha_fail': 0,
    'alpha_pass': 0,
    'final_signal': 0
}

for index, row in target_stocks.iterrows():
    count += 1
    symbol = row['代码']
    name = row['名称']

    if count % 10 == 0:
        logger.info(f"进度: {count}/100, 成功: {success_count}, Alpha通过: {alpha_pass_count}, 信号: {signal_count}...")

    try:
        # --- A. 历史数据 (Base) ---
        df_hist = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_dt_str, end_date=yesterday_str, adjust="qfq")

        if df_hist is None or df_hist.empty:
            logger.debug(f"{symbol} {name} - 历史数据为空")
            stats['hist_fail'] += 1
            continue

        # 标准化
        df_hist = df_hist.rename(columns={"日期": "date", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low", "成交量": "volume"})
        cols = ["open", "close", "high", "low", "volume"]
        df_hist[cols] = df_hist[cols].apply(pd.to_numeric, errors='coerce')

        logger.debug(f"{symbol} {name} - 历史数据: {len(df_hist)} 条")

        # ========================================================
        # B. 智能数据源切换 (Smart Data Source)
        # ========================================================
        snapshot_data = None
        data_mode = "History" # 默认历史模式
        live_vwap = 0.0

        # 1. 尝试获取实时分钟线 (优先)
        try:
            df_min = ak.stock_zh_a_hist_min_em(symbol=symbol, period='1', adjust='')
        except Exception as e:
            logger.debug(f"{symbol} {name} - 获取分钟线失败: {e}")
            df_min = None

        has_min_data = False
        if df_min is not None and not df_min.empty:
            df_min['时间'] = pd.to_datetime(df_min['时间'])
            df_today_min = df_min[df_min['时间'].dt.date == now.date()]

            if not df_today_min.empty:
                has_min_data = True
                data_mode = "RealTime" # 切换为实时模式
                stats['realtime_mode'] += 1

                # 聚合分钟线
                live_open = float(df_today_min.iloc[0]['开盘'])
                live_close = float(df_today_min.iloc[-1]['收盘'])
                live_high = float(df_today_min['最高'].max())
                live_low = float(df_today_min['最低'].min())
                live_volume = float(df_today_min['成交量'].sum())
                live_amount = float(df_today_min['成交额'].sum())

                # 计算 VWAP (修正单位)
                if live_volume > 0:
                    live_vwap = live_amount / (live_volume * 100)
                else:
                    live_vwap = live_close

                snapshot_data = {
                    "date": today_str, "open": live_open, "close": live_close,
                    "high": live_high, "low": live_low, "volume": live_volume
                }
                logger.debug(f"{symbol} {name} - 实时模式: 分钟线{len(df_today_min)}条, VWAP={live_vwap:.2f}")

        # 2. 如果没有分钟线 (比如 9:26 刚开盘，或者 8:00 盘前)
        # 尝试使用 Spot 数据构造"伪K线"
        if not has_min_data:
            spot_open = float(row['今开'])
            spot_close = float(row['最新价'])
            spot_vol = float(row['成交量'])

            # 只有当今天有开盘价时 (已过 9:15-9:25 竞价)，才拼接到历史数据后面
            if spot_open > 0 and spot_close > 0:
                data_mode = "Auction/Spot" # 竞价/快照模式
                stats['auction_mode'] += 1
                snapshot_data = {
                    "date": today_str,
                    "open": spot_open,
                    "close": spot_close,
                    "high": float(row['最高']),
                    "low": float(row['最低']),
                    "volume": spot_vol
                }
                # 快照模式下，无法计算准确 VWAP，暂用现价代替
                live_vwap = spot_close
                logger.debug(f"{symbol} {name} - 竞价模式: 今开={spot_open}, 现价={spot_close}")
            else:
                # 连今开都没有 (盘前 8:00)，保持 data_mode = "History"
                stats['history_mode'] += 1
                logger.debug(f"{symbol} {name} - 历史模式: 无开盘价")

        # ========================================================
        # C. 拼接与计算 Alpha
        # ========================================================
        if data_mode == "History":
            # 纯历史模式：直接用 df_hist 计算，看昨天的 Alpha
            df_calc = df_hist.copy()
        else:
            # 实时/竞价模式：拼接今日数据
            df_snapshot = pd.DataFrame([snapshot_data])
            df_calc = pd.concat([df_hist, df_snapshot], ignore_index=True)

        if len(df_calc) < 10:
            logger.debug(f"{symbol} {name} - 数据不足: {len(df_calc)} 条")
            continue

        # 计算 Alpha #2
        df_calc['log_vol'] = np.log(df_calc['volume'] + 1)
        df_calc['delta_vol'] = df_calc['log_vol'].diff(2)
        df_calc['alpha_ret'] = (df_calc['close'] - df_calc['open']) / df_calc['open']
        df_calc['corr'] = df_calc['delta_vol'].rolling(window=6).corr(df_calc['alpha_ret'])
        df_calc['alpha_2'] = -1 * df_calc['corr']

        current_alpha = df_calc.iloc[-1]['alpha_2']

        if np.isnan(current_alpha):
            logger.debug(f"{symbol} {name} - Alpha为NaN, 跳过")
            stats['alpha_fail'] += 1
            continue

        logger.debug(f"{symbol} {name} - Alpha={current_alpha:.4f}, 阈值={ALPHA_THRESHOLD}")

        # ========================================================
        # D. 分模式判定 (Adaptive Logic)
        # ========================================================

        status_msg = "-"
        status_code = 0
        current_price = snapshot_data['close'] if snapshot_data else df_hist.iloc[-1]['close']
        pct_over_vwap = 0.0

        # 门槛：任何模式下，Alpha 必须达标
        if current_alpha > ALPHA_THRESHOLD:
            alpha_pass_count += 1
            stats['alpha_pass'] += 1
            logger.info(f"✓ {symbol} {name} - Alpha达标: {current_alpha:.4f} > {ALPHA_THRESHOLD}, 模式: {data_mode}")

            # --- 场景 1: 实时盘中 (RealTime) ---
            if data_mode == "RealTime":
                # 有分钟数据，可以严谨判断 VWAP 和 PinBar
                pct_over_vwap = (current_price - live_vwap) / live_vwap if live_vwap > 0 else 0
                is_break_vwap = pct_over_vwap > BREAK_MA_THRESHOLD

                body_len = abs(current_price - snapshot_data['open'])
                if body_len == 0: body_len = 0.001
                lower_shadow = min(snapshot_data['open'], current_price) - snapshot_data['low']
                is_pin_bar = (lower_shadow / body_len > PIN_BAR_RATIO) and (current_price > snapshot_data['open'])

                if is_break_vwap or is_pin_bar:
                    status_code = 100
                    reasons = []
                    if is_break_vwap: reasons.append("站稳均线")
                    if is_pin_bar: reasons.append("金针探底")
                    status_msg = f"🔥进攻[{data_mode}]:{'+'.join(reasons)}"
                    signal_count += 1
                    stats['final_signal'] += 1
                    logger.info(f"  ★ 信号生成: {status_msg}, VWAP乖离={pct_over_vwap*100:.2f}%")
                else:
                    status_code = 50
                    status_msg = f"👀蓄力[{data_mode}]"
                    logger.info(f"  - Alpha达标但未满足其他条件: VWAP乖离={pct_over_vwap*100:.2f}%")

            # --- 场景 2: 竞价/快照 (Auction/Spot) ---
            elif data_mode == "Auction/Spot":
                # 只有开盘价，没法算 VWAP，只看 Alpha 是否强
                # 适合 9:25 - 9:30 抓高开背离
                status_code = 80
                status_msg = f"⚡竞价抢筹: Alpha高({current_alpha:.2f})"
                signal_count += 1
                stats['final_signal'] += 1
                logger.info(f"  ★ 信号生成: {status_msg}")

            # --- 场景 3: 纯历史 (History) ---
            elif data_mode == "History":
                # 盘后或盘前，选出"昨日收盘后 Alpha 依然很高"的票
                # 作为今日的"观察池"
                status_code = 60
                status_msg = f"📝昨日强背离: 纳入观察池"
                signal_count += 1
                stats['final_signal'] += 1
                logger.info(f"  ★ 信号生成: {status_msg}")

        # 结果收集 (所有 Alpha 高的都收录，状态里区分)
        if status_code > 0:
            results.append({
                "代码": symbol,
                "名称": name,
                "数据模式": data_mode,
                "当前价": current_price,
                "Alpha2得分": round(current_alpha, 4),
                "均价乖离%": round(pct_over_vwap * 100, 2) if data_mode == "RealTime" else 0,
                "形态": status_msg,
                "排序分": status_code + current_alpha,
                "换手率%": row['换手率']
            })

        success_count += 1

    except Exception as e:
        logger.error(f"✗ {symbol} {name} - 异常: {e}", exc_info=True)
        continue

# ========================================================
# 3. 输出结果
# ========================================================
print("\n" + "="*80)
print("统计信息:")
print("="*80)
for key, value in stats.items():
    print(f"  {key}: {value}")

print("\n" + "="*80)
df = pd.DataFrame(results)
if not df.empty:
    df = df.sort_values(by="排序分", ascending=False)
    out_cols = ["代码", "名称", "数据模式", "当前价", "Alpha2得分", "均价乖离%", "形态", "换手率%"]
    df = df[out_cols]

print(f"\n=== 全天候扫描完成：筛选出 {len(df)} 只标的 ===")
print("提示：'History'表示基于昨日数据; 'RealTime'表示基于今日分时; 'Auction'表示基于竞价。")

if not df.empty:
    print("\n结果列表:")
    print(df.to_string(index=False))
else:
    print("\n⚠️ 未找到符合条件的股票！")
    print("\n可能原因:")
    print(f"1. Alpha阈值({ALPHA_THRESHOLD})过高")
    print(f"2. 当前时间({now.strftime('%H:%M')})可能不在交易时段")
    print(f"3. VWAP/PinBar条件过于严格")
