# -*- coding: utf-8 -*-
"""
山谷狙击选股策略 V2.0 (实战工程版)
Valley Sniper Strategy V2.0 - Production Ready

【核心升级】
1. Map-Reduce 架构: 主线程向量化初筛 + 线程池并发回测 (耗时压缩 90%)
2. 鲁棒性增强: 指数退避重试 (Retry) + 交易日历锚点 (Trade Date Anchor)
3. 数据工程: T+0 几何不变性数据合成 (Geometric Synthesis)
4. 风控升级: 板块分层 (BJ剔除) + 动态环境滤网 (Regime Filter) + 微观结构修正 (IBS)

【使用说明】
- 本脚本由系统自动调度，也可在本地手动运行测试。
- 依赖: akshare, pandas, numpy, talib, scipy
"""

import akshare as ak
import pandas as pd
import numpy as np
import talib
import datetime
import time
import random
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from scipy.signal import argrelextrema

warnings.filterwarnings("ignore")

# --- 配置参数 ---
MAX_WORKERS = 8  # 并发线程数
RETRY_CONFIG = {'max_retries': 3, 'initial_delay': 1.0, 'backoff': 2.0, 'jitter': 0.5}

# 基础过滤参数
MIN_TURNOVER = 1.0   # 换手率下限 %
MAX_TURNOVER = 15.0  # 换手率上限 %
MAX_PCT_CHG = 9.0    # 涨跌幅绝对值上限 % (避开涨停/跌停/过热)
MIN_PRICE = 5.0      # 最低股价

# 评分阈值 (动态调整前)
THRESHOLD_HIGH_QUALITY = 7
THRESHOLD_POTENTIAL = 4

# 策略参数
MA_LONG = 60
MA_SHORT = 20
VWAP_WINDOW = 20
IBS_THRESHOLD = 0.6
VRP_WINDOW = 20

# --- 1. 工程底座 (Foundation) ---

def fetch_with_retry(max_retries=3, initial_delay=1.0, backoff=2.0, jitter=0.5):
    """装饰器：带指数退避与抖动的自动重试"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exc = None
            for i in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    if i == max_retries: break
                    sleep_t = delay * (1 + random.uniform(-jitter, jitter))
                    time.sleep(max(0.1, sleep_t))
                    delay *= backoff
            print(f"❌ [Retry] 函数 {func.__name__} 失败: {last_exc}")
            raise last_exc
        return wrapper
    return decorator

@fetch_with_retry(**RETRY_CONFIG)
def get_trade_date_anchor():
    """获取最近的一个交易日作为全局时间锚点"""
    try:
        # 尝试获取交易日历
        tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()
        recent_dates = pd.to_datetime(tool_trade_date_hist_sina_df['trade_date']).dt.date
        today = datetime.date.today()
        # 找到今天或今天之前的最近交易日
        trade_date = recent_dates[recent_dates <= today].iloc[-1]
        return trade_date.strftime("%Y-%m-%d")
    except Exception:
        # 降级方案：如果是周六日，推到周五
        today = datetime.date.today()
        if today.weekday() == 5: # Sat
            return (today - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        elif today.weekday() == 6: # Sun
            return (today - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
        return today.strftime("%Y-%m-%d")

def synthesize_realtime_data(hist_df: pd.DataFrame, spot_row: pd.Series, trade_date: str) -> pd.DataFrame:
    """
    T+0 数据合成 (核心算法)
    利用涨跌幅比率的几何不变性，将 Snapshot 拼接到 History
    """
    if hist_df.empty: return hist_df
    
    # 1. 日期防重检查
    try:
        last_hist_date = pd.to_datetime(hist_df.iloc[-1]['日期']).strftime("%Y-%m-%d")
    except:
        last_hist_date = "1970-01-01"
        
    if last_hist_date >= trade_date:
        # 历史数据已包含今日，或今日非交易日
        return hist_df

    # 2. 几何合成
    last_adj_close = float(hist_df.iloc[-1]["收盘"])
    
    spot_pre = float(spot_row.get("昨收", 0))
    if spot_pre == 0: return hist_df # 异常数据
    
    # 计算比率 (Ratios)
    r_open = float(spot_row.get("开盘", 0)) / spot_pre
    r_close = float(spot_row.get("最新价", 0)) / spot_pre
    r_high = float(spot_row.get("最高", 0)) / spot_pre
    r_low = float(spot_row.get("最低", 0)) / spot_pre
    
    # 推导今日复权价
    new_row = {
        "日期": trade_date,
        "开盘": last_adj_close * r_open,
        "收盘": last_adj_close * r_close,
        "最高": last_adj_close * r_high,
        "最低": last_adj_close * r_low,
        "成交量": float(spot_row.get("成交量", 0)),
        "成交额": float(spot_row.get("成交额", 0)),
        "换手率": float(spot_row.get("换手率", 0))
    }
    
    # 3. 拼接
    return pd.concat([hist_df, pd.DataFrame([new_row])], ignore_index=True)

# --- 2. 策略逻辑 (Logic) ---

def _kalman_filter_1d(values: pd.Series, q=1e-5, r_scale=0.20):
    """Kalman 降噪"""
    v = values.values
    if len(v) == 0: return values
    x = v[0]
    p = 1.0
    out = np.empty_like(v)
    for i in range(len(v)):
        p += q
        k = p / (p + r_scale)
        x += k * (v[i] - x)
        p *= (1 - k)
        out[i] = x
    return pd.Series(out, index=values.index)

def calculate_ibs(close, high, low):
    """Internal Bar Strength"""
    rng = high - low
    if rng == 0: return 0.5 # 边界保护：一字板/停牌视为中性
    return (close - low) / rng

def check_market_regime():
    """大盘环境滤网"""
    try:
        # 获取上证指数
        idx_df = ak.stock_zh_index_daily(symbol="sh000001")
        if idx_df.empty: return 0
        
        close = idx_df['close']
        ma20 = close.rolling(20).mean()
        ma60 = close.rolling(60).mean()
        
        curr_p = close.iloc[-1]
        curr_ma20 = ma20.iloc[-1]
        curr_ma60 = ma60.iloc[-1]
        
        penalty = 0
        if curr_p < curr_ma20:
            penalty += 1 # 黄灯
        if curr_p < curr_ma60:
            penalty += 1 # 红灯 (累积+2)
            
        return penalty
    except:
        return 0 # 获取失败默认正常

def process_single_stock(code, name, spot_row, trade_date, threshold_adj):
    """
    Reduce 阶段：单只股票深度分析
    """
    try:
        # 1. 拉取历史数据 (QFQ)
        df_hist = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
        if df_hist is None or df_hist.empty or len(df_hist) < 250: # 次新股过滤
            return None
            
        # 2. T+0 数据合成
        df = synthesize_realtime_data(df_hist, spot_row, trade_date)
        if len(df) < 250: return None
        
        # 3. 准备数据序列
        close = df["收盘"]
        high = df["最高"]
        low = df["最低"]
        vol = df["成交量"]
        
        curr_price = close.iloc[-1]
        
        score = 0
        signals = []
        
        # --- A. 成本与趋势 (Cost & Trend) ---
        ma60 = close.rolling(60).mean()
        # VWAP 20 (简易计算: Amount/Vol 近似为 Close*Vol/Vol = Close 的加权)
        # 准确 VWAP 需要 Amount, 这里用典型价格近似
        typ_price = (high + low + close) / 3
        vwap20 = (typ_price * vol).rolling(20).sum() / vol.rolling(20).sum()
        
        curr_ma60 = ma60.iloc[-1]
        curr_vwap = vwap20.iloc[-1]
        prev_vwap = vwap20.iloc[-2]
        
        # 成本支撑逻辑: 必须在 VWAP 之上 或 VWAP 拐头向上
        cost_support = (curr_price > curr_vwap) or (curr_vwap > prev_vwap)
        
        # BIAS 保护: 如果离 MA60 太远 (深跌), 必须有强力底背离才行
        bias60 = (curr_price - curr_ma60) / curr_ma60
        is_deep_fall = bias60 < -0.20
        
        if not cost_support:
            return None # 连短期成本都站不稳，直接放弃
            
        # --- B. 策略打分 (Scoring) ---
        
        # 1. 缩量 (Volume)
        vol5_med = vol.iloc[-5:].median()
        vol120_quantile = vol.iloc[-120:].rank(pct=True).iloc[-1] # 当前量在120天分位
        
        # 动态缩量分: 市值越大要求越松 (此处简化，统一逻辑)
        if vol.iloc[-1] < vol.rolling(20).mean().iloc[-1]: # 今日缩量
            if vol120_quantile < 0.15:
                score += 3
                signals.append(f"极缩量({int(vol120_quantile*100)}%)")
            elif vol120_quantile < 0.25:
                score += 1
                signals.append("缩量")
                
        # 2. VRP (恐慌溢价)
        ret = close.pct_change()
        rv = ret.rolling(5).std()
        iv_proxy = ret.rolling(VRP_WINDOW).std()
        vrp = iv_proxy - rv
        # VRP 分位
        vrp_rank = vrp.rolling(120).rank(pct=True).iloc[-1]
        if vrp_rank > 0.8:
            score += 2
            signals.append("VRP恐慌")
            
        # 3. Kalman MACD/RSI (背离)
        smooth_c = _kalman_filter_1d(close)
        
        # MACD
        dif, dea, macd_bar = talib.MACD(smooth_c.values)
        # 简单底背离检测: 价格新低(近20天) 但 MACD 未新低
        low_20 = close.rolling(20).min().iloc[-1]
        macd_low_20 = pd.Series(dif).rolling(20).min().iloc[-1]
        
        if close.iloc[-1] <= low_20 * 1.02 and dif[-1] > macd_low_20:
             # 二次确认: 金叉或即将金叉
             if macd_bar[-1] > macd_bar[-2]:
                 score += 3
                 signals.append("MACD背离")
        
        # RSI
        rsi = talib.RSI(smooth_c.values, timeperiod=14)
        if rsi[-1] < 30:
            score += 1
            signals.append("RSI超卖")
        elif rsi[-1] < 45 and rsi[-1] > rsi[-2]: # 低位回升
            score += 1
            
        # 4. 微观结构 IBS (资金承接)
        ibs = calculate_ibs(close.iloc[-1], high.iloc[-1], low.iloc[-1])
        ma5_vol = vol.rolling(5).mean().iloc[-1]
        if ibs > 0.6 and vol.iloc[-1] > ma5_vol:
            score += 2
            signals.append("资金承接")
            
        # 5. MA60 奖励 (右侧确认)
        if curr_price > curr_ma60:
            score += 1
            signals.append("站上生命线")
            
        # 6. 深跌保护逻辑校验
        if is_deep_fall:
            # 深跌时，必须有 MACD 背离 或 VRP 恐慌 才能入选
            if not ("MACD背离" in signals or "VRP恐慌" in signals):
                return None

        # --- C. 结果组装 ---
        final_threshold = THRESHOLD_POTENTIAL + threshold_adj
        
        if score >= final_threshold:
            return {
                "代码": code,
                "名称": name,
                "现价": float(spot_row["最新价"]),
                "涨跌%": float(spot_row["涨跌幅"]),
                "评分": score,
                "IBS": round(ibs, 2),
                "VRP分位": round(vrp_rank, 2),
                "缩量分位": round(vol120_quantile, 2),
                "BIAS60": round(bias60, 2),
                "信号": "+".join(signals)
            }
            
    except Exception as e:
        # print(f"Error processing {code}: {e}")
        return None
    return None

# --- 3. Map 阶段 (Map) ---

@fetch_with_retry(**RETRY_CONFIG)
def get_candidates():
    """全市场快照与向量化初筛"""
    print("📡 Map阶段: 获取全市场快照...")
    df = ak.stock_zh_a_spot_em()
    
    total = len(df)
    
    # 1. 板块过滤 (剔除 BJ/8/4/9)
    # 兼容: 代码列可能叫 '代码' 或 'code'
    code_col = '代码' if '代码' in df.columns else 'code'
    df[code_col] = df[code_col].astype(str)
    
    mask_bj = df[code_col].str.match(r'^(8|4|9|bj)')
    df = df[~mask_bj]
    
    # 2. ST 过滤
    name_col = '名称' if '名称' in df.columns else 'name'
    mask_st = df[name_col].str.contains('ST|退', na=False)
    df = df[~mask_st]
    
    # 3. 流动性与价格过滤
    # 确保数值列为 float
    num_cols = ['最新价', '涨跌幅', '换手率', '成交量', '成交额', '最高', '最低', '开盘', '昨收']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    mask_valid = (
        (df['换手率'] > MIN_TURNOVER) & 
        (df['换手率'] < MAX_TURNOVER) & 
        (df['最新价'] >= MIN_PRICE) & 
        (df['涨跌幅'].abs() < MAX_PCT_CHG)
    )
    df = df[mask_valid]
    
    print(f"🧹 清洗完成: {total} -> {len(df)} (剔除北交所/ST/僵尸股/涨停股)")
    return df

# --- 主程序 ---

def main():
    print(f"🚀 山谷狙击 V2.0 启动 | 线程数: {MAX_WORKERS}")
    
    # 1. 获取时间锚点
    trade_date = get_trade_date_anchor()
    print(f"📅 交易日锚点: {trade_date}")
    
    # 2. 检查大盘环境 (Regime)
    threshold_adj = check_market_regime()
    regime_msg = ["绿灯 (正常)", "黄灯 (回调 +1)", "红灯 (深跌 +2)"][min(threshold_adj, 2)]
    print(f"🌡️ 市场环境: {regime_msg}")
    
    # 3. Map
    candidates = get_candidates()
    if candidates.empty:
        print("⚠️ 候选池为空，结束运行。")
        return pd.DataFrame(), []

    # 4. Reduce
    results = []
    print(f"⚡ Reduce阶段: 并发回测 {len(candidates)} 只标的...")
    
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for _, row in candidates.iterrows():
            futures.append(
                executor.submit(
                    process_single_stock, 
                    row['代码'], row['名称'], row, trade_date, threshold_adj
                )
            )
        
        # 进度条
        count = 0
        total = len(futures)
        for future in as_completed(futures):
            res = future.result()
            if res:
                results.append(res)
            count += 1
            if count % 50 == 0:
                print(f"进度: {count}/{total}...")
                
    elapsed = time.time() - start_time
    print(f"\n✅ 运行耗时: {elapsed:.1f}s | 命中: {len(results)} 只")
    
    # 5. 输出
    df_res = pd.DataFrame(results)
    if not df_res.empty:
        df_res = df_res.sort_values(by="评分", ascending=False)
        
        print("\n" + "="*50)
        print(f"🌟 【严选榜】 (评分>={THRESHOLD_HIGH_QUALITY + threshold_adj})")
        print("="*50)
        high_q = df_res[df_res["评分"] >= (THRESHOLD_HIGH_QUALITY + threshold_adj)]
        print(high_q.to_string(index=False) if not high_q.empty else "暂无")
        
        print("\n" + "-"*50)
        print(f"👀 【潜力榜】 (评分>={THRESHOLD_POTENTIAL + threshold_adj})")
        print("-"*50)
        pot = df_res[(df_res["评分"] >= (THRESHOLD_POTENTIAL + threshold_adj)) & 
                     (df_res["评分"] < (THRESHOLD_HIGH_QUALITY + threshold_adj))]
        print(pot.head(50).to_string(index=False) if not pot.empty else "暂无")
        
        return df_res, results
    else:
        print("未发现符合条件的股票")
        return pd.DataFrame(), []

df, result = main()
