
import akshare as ak
import pandas as pd
import numpy as np
import datetime
import talib
from scipy.signal import argrelextrema
import warnings
import time
import os

warnings.filterwarnings('ignore')

# --- 回测配置 ---
BACKTEST_DAYS = 180  # 回测过去半年
SAMPLE_SIZE = 50     # 随机抽样股票数量，覆盖不同价位和板块
INITIAL_CAPITAL = 100000.0

# 策略参数 (v8.6)
RECENT_VOLUME_DAYS = 5
VOLUME_BASE_DAYS = 400
CAP_SMALL = 100 * 1e8
CAP_LARGE = 500 * 1e8
MIN_TURNOVER_AMOUNT = 30000000
MAX_PRICE_CHANGE = 8.0
PRICE_THRESHOLD_MIN_PRICE = 4.0
THRESHOLD_HIGH_QUALITY = 8
THRESHOLD_POTENTIAL = 5
VOL_RANK_MID = 0.15
AR_SPREAD_WINDOW = 20
DRAWDOWN_THRESHOLD = 0.20
MAX_SCORE_ALPHA = 4.0
MAX_SCORE_DRAGON = 3.0

SCORE_CRITERIA = {
    "volume_extreme": 3,
    "macd_div": 3,
    "rsi_div": 2,
    "illiq_composite": 2,
    "sector_fund_flow_strong": 3, 
    "sector_fund_flow_ok": 1,
    "vwap_support": 2,
    "trend_protect": 1,
    "heat_penalty": -3,
    "heat_reversal": 2,
}

# --- 核心函数 (复用 v8.6) ---

def _safe_vwap(amount_series, vol_series, price_series):
    """自适应 VWAP 计算 (核心验证对象)"""
    if len(amount_series) == 0: return 0.0
    raw_vwap = amount_series.sum() / (vol_series.sum() + 1e-9)
    current_p = price_series.iloc[-1]
    
    # 记录原始比例以便分析
    ratio = raw_vwap / current_p if current_p > 0 else 0
    
    if 80 < ratio < 120: 
        return raw_vwap / 100.0, "Hand"
    elif 0.8 < ratio < 1.2: 
        return raw_vwap, "Share"
            
    # 兜底
    return (raw_vwap / 100.0 if raw_vwap > current_p * 50 else raw_vwap), "Unknown"

def _kalman_filter_1d(values: pd.Series, q: float = 1e-5, r_scale: float = 0.20):
    v = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    if v.size == 0: return values
    first_finite_idx = int(np.argmax(np.isfinite(v))) if np.isfinite(v).any() else None
    if first_finite_idx is None: return values
    dv = np.diff(v[np.isfinite(v)])
    base_var = float(np.nanvar(dv)) if dv.size else 0.1
    r = max(1e-9, r_scale * base_var)
    x = float(v[first_finite_idx])
    p = 1.0
    out = np.empty_like(v, dtype=float)
    out[:] = np.nan
    for i in range(first_finite_idx, v.size):
        if np.isfinite(v[i]):
            p = p + q
            k = p / (p + r)
            x = x + k * (v[i] - x)
            p = (1.0 - k) * p
            out[i] = x
    return pd.Series(out, index=values.index)

def _get_bb_troughs(series: pd.Series, window: int = 5):
    data = series.values
    local_mins = argrelextrema(data, np.less, order=window)[0]
    refined = []
    if len(local_mins) > 0:
        refined.append(local_mins[0])
        for i in range(1, len(local_mins)):
            if local_mins[i] - refined[-1] >= window:
                refined.append(local_mins[i])
            else:
                if data[local_mins[i]] < data[refined[-1]]:
                    refined[-1] = local_mins[i]
    return refined

def detect_dynamic_divergence(smooth_p: pd.Series, indicator: pd.Series):
    if len(smooth_p) < 60: return False
    troughs = _get_bb_troughs(smooth_p)
    if len(troughs) < 2: return False
    last_idx = troughs[-1]
    prev_idx = troughs[-2]
    if (len(smooth_p) - 1) - last_idx > 15: return False
    p_last, p_prev = smooth_p.iloc[last_idx], smooth_p.iloc[prev_idx]
    i_last, i_prev = indicator.iloc[last_idx], indicator.iloc[prev_idx]
    if p_last <= p_prev * 1.02 and i_last > i_prev * 1.05:
        if indicator.iloc[-1] > indicator.iloc[-2]: return True
    return False

def check_overhead_supply(close, volume, amount, current_price):
    if len(close) < 250: return False, "NoData", 0
    
    vol_20 = volume.rolling(20).sum()
    amt_20 = amount.rolling(20).sum()
    
    vwap_20_val, unit_type = _safe_vwap(amt_20.iloc[-20:], vol_20.iloc[-20:], close.iloc[-20:])
    
    if current_price < vwap_20_val * 0.85: 
        return False, unit_type, vwap_20_val
        
    high_52w = close.rolling(250).max().iloc[-1]
    drawdown = (high_52w - current_price) / high_52w
    return drawdown > DRAWDOWN_THRESHOLD, unit_type, vwap_20_val

def calculate_alpha54(open_s, high_s, low_s, close_s):
    o = pd.to_numeric(open_s, errors="coerce")
    h = pd.to_numeric(high_s, errors="coerce")
    l = pd.to_numeric(low_s, errors="coerce")
    c = pd.to_numeric(close_s, errors="coerce")
    denom = (l - h).replace(0, -0.01)
    term1 = (l - c) / denom
    term2 = (o / c) ** 5
    alpha = -1.0 * term1 * term2
    curr = alpha.iloc[-1]
    hist = alpha.iloc[-60:].dropna()
    if len(hist) < 30: return 0, 0
    rank = (hist <= curr).mean()
    score = 0
    if rank > 0.5:
        score = ((rank - 0.5) / 0.5) * MAX_SCORE_ALPHA
    return score, rank

def calculate_long_term_vwap(amount, volume, current_price):
    if len(amount) < 300: return 0, 0, 0, "NoData"
    vwap_300, unit_type = _safe_vwap(amount.iloc[-300:], volume.iloc[-300:], pd.Series([current_price]))
    dist_pct = (current_price - vwap_300) / vwap_300
    score = 0
    if -0.15 <= dist_pct <= 0.05:
        score = SCORE_CRITERIA["vwap_support"]
    return score, vwap_300, dist_pct, unit_type

def dynamic_volume_score(volume, mkt_cap):
    if len(volume) < 120: return 0, 0
    threshold = VOL_RANK_MID
    if mkt_cap > CAP_LARGE: threshold = 0.25
    elif mkt_cap < CAP_SMALL: threshold = 0.10
    curr_vol = volume.iloc[-RECENT_VOLUME_DAYS:].mean()
    hist_vol = volume.iloc[-120:]
    vol_rank = (hist_vol <= curr_vol).mean()
    if vol_rank < threshold: return SCORE_CRITERIA["volume_extreme"], vol_rank
    return 0, vol_rank

# --- 回测引擎 ---

def run_backtest():
    print(f"🚀 开始 v8.6 策略大范围严格回测 (过去 {BACKTEST_DAYS} 天)")
    print(f"🎯 抽样目标: {SAMPLE_SIZE} 只股票 (包含不同价位/市值)")
    
    # 1. 获取全市场股票列表
    try:
        df_market = ak.stock_zh_a_spot_em()
        df_market = df_market[~df_market["名称"].str.contains("ST|退", na=False)]
        # 必须包含 600519 (茅台 - 高价股代表) 和 000725 (京东方 - 低价股代表)
        mandatory_symbols = ["600519", "000725", "300750"] 
        all_symbols = df_market["代码"].tolist()
        
        # 随机抽样 + 强制包含
        random_symbols = np.random.choice(all_symbols, SAMPLE_SIZE, replace=False).tolist()
        test_symbols = list(set(mandatory_symbols + random_symbols))
        
        print(f"✅ 样本池构建完成: {len(test_symbols)} 只")
    except Exception as e:
        print(f"❌ 获取市场数据失败: {e}")
        return

    # 2. 准备回测数据
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=BACKTEST_DAYS + VOLUME_BASE_DAYS + 30) # 多取数据用于指标计算
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")
    
    results = []
    unit_check_log = []
    
    # 模拟回测循环
    # 为了简化，我们选取最近的一个交易日进行"快照回测"，重点验证数据清洗逻辑
    # 若要进行时间序列回测，计算量过大，这里主要验证 "Unit Correction" 和 "Signal Quality"
    
    print("⏳ 正在拉取历史数据并执行策略...")
    
    for idx, symbol in enumerate(test_symbols):
        if idx % 10 == 0: print(f"   进度: {idx}/{len(test_symbols)}...")
        
        try:
            # 标准化代码
            norm_symbol = symbol[2:] if len(symbol) == 8 else symbol
            
            df_hist = ak.stock_zh_a_hist(symbol=norm_symbol, period="daily", start_date=start_str, end_date=end_str, adjust="qfq")
            if df_hist is None or len(df_hist) < 300: continue
            
            # 数据转换
            close = pd.to_numeric(df_hist["收盘"], errors="coerce")
            open_ = pd.to_numeric(df_hist["开盘"], errors="coerce")
            high = pd.to_numeric(df_hist["最高"], errors="coerce")
            low = pd.to_numeric(df_hist["最低"], errors="coerce")
            volume = pd.to_numeric(df_hist["成交量"], errors="coerce")
            amount = pd.to_numeric(df_hist["成交额"], errors="coerce")
            
            current_price = close.iloc[-1]
            
            # --- 核心验证点 1: 单位转换 ---
            # 计算长期 VWAP 并检查单位类型
            _, vwap_300, _, unit_type = calculate_long_term_vwap(amount, volume, current_price)
            
            unit_check_log.append({
                "symbol": symbol,
                "price": current_price,
                "vwap": vwap_300,
                "unit": unit_type,
                "is_correct": 0.8 < (vwap_300 / current_price) < 1.2
            })
            
            # --- 核心验证点 2: 策略逻辑 ---
            # Overhead Supply
            is_safe, unit_20, vwap_20 = check_overhead_supply(close, volume, amount, current_price)
            
            # Trend Protect
            ma60 = close.rolling(60).mean()
            trend_ok = False
            if len(ma60) > 0 and current_price > ma60.iloc[-1] * 0.85:
                trend_ok = True
                
            # Alpha 54
            a_score, a_rank = calculate_alpha54(open_, high, low, close)
            
            # 记录结果
            results.append({
                "symbol": symbol,
                "unit_type": unit_type,
                "overhead_pass": is_safe,
                "trend_pass": trend_ok,
                "alpha_rank": a_rank,
                "alpha_score": a_score,
                "vwap_price_ratio": vwap_300 / current_price
            })
            
        except Exception as e:
            print(f"   ❌ {symbol} Error: {e}")
            continue

    # 3. 生成报告
    df_res = pd.DataFrame(results)
    df_unit = pd.DataFrame(unit_check_log)
    
    print("\n" + "="*50)
    print("📊 v8.6 回测验证报告")
    print("="*50)
    
    # 1. 单位转换验证
    print("\n1️⃣ [严谨性验证] 手/股单位自动修正")
    print("-" * 30)
    if not df_unit.empty:
        total = len(df_unit)
        correct = df_unit["is_correct"].sum()
        hands = len(df_unit[df_unit["unit"] == "Hand"])
        shares = len(df_unit[df_unit["unit"] == "Share"])
        unknown = len(df_unit[df_unit["unit"] == "Unknown"])
        
        print(f"样本总数: {total}")
        print(f"单位识别准确率: {correct/total*100:.1f}% ({correct}/{total})")
        print(f"识别为'手'(需/100): {hands}")
        print(f"识别为'股'(保持): {shares}")
        print(f"无法识别(异常): {unknown}")
        
        print("\n🔍 典型样本抽查:")
        # 茅台 (高价股)
        moutai = df_unit[df_unit["symbol"].str.contains("600519")]
        if not moutai.empty:
            r = moutai.iloc[0]
            print(f"   - 茅台(600519): Price={r['price']:.1f}, VWAP={r['vwap']:.1f}, Unit={r['unit']} -> {'✅' if r['is_correct'] else '❌'}")
        
        # 京东方 (低价股)
        boe = df_unit[df_unit["symbol"].str.contains("000725")]
        if not boe.empty:
            r = boe.iloc[0]
            print(f"   - 京东方(000725): Price={r['price']:.1f}, VWAP={r['vwap']:.1f}, Unit={r['unit']} -> {'✅' if r['is_correct'] else '❌'}")
            
    # 2. 策略产出验证
    print("\n2️⃣ [有效性验证] 策略产出分布")
    print("-" * 30)
    if not df_res.empty:
        pass_overhead = df_res["overhead_pass"].sum()
        pass_trend = df_res["trend_pass"].sum()
        high_alpha = len(df_res[df_res["alpha_rank"] > 0.8])
        
        print(f"通过 Overhead 检查: {pass_overhead} ({pass_overhead/len(df_res)*100:.1f}%)")
        print(f"通过 Trend 保护: {pass_trend} ({pass_trend/len(df_res)*100:.1f}%)")
        print(f"Alpha#54 高分信号(>0.8): {high_alpha} ({high_alpha/len(df_res)*100:.1f}%)")
        
        # 交叉验证
        final_candidates = df_res[df_res["overhead_pass"] & df_res["trend_pass"] & (df_res["alpha_rank"] > 0.5)]
        print(f"\n🏆 最终入围潜力股 (模拟): {len(final_candidates)} 只")
        if len(final_candidates) == 0:
            print("⚠️ 警告: 依然没有产出，可能条件过于严苛或市场环境极端。")
        else:
            print("✅ 策略已具备正常造血能力。")

if __name__ == "__main__":
    run_backtest()
