
import akshare as ak
import pandas as pd
import numpy as np
import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
import threading

# ==========================================
# 策略配置 (参数微调)
# ==========================================
MIN_PRICE_RISE = 2.0        
MIN_VOL_RATIO = 1.5         
WASH_DAYS_WINDOW = 10       
WASH_LOW_DAYS_REQ = 5       
TURNOVER_QUANTILE = 0.20    
MAX_SHADOW_RATIO = 0.35     # 新增：允许的最大上影线比例
MAX_OPEN_GAP_PCT = 5.0
MARKET_PANIC_PCT = -1.0
MARKET_INDEX_SYMBOL = "sh000300"
MARKET_FAST_MA = 20
MARKET_SLOW_MA = 60
MARKET_SLOPE_DAYS = 5

_stats_lock = threading.Lock()
_stats = Counter()

def _stat_inc(key: str, n: int = 1):
    with _stats_lock:
        _stats[key] += int(n)

def _get_core_index_pct_changes():
    indices = None
    try:
        indices = ak.stock_zh_index_spot_em()
    except Exception:
        indices = None
    if indices is None or indices.empty:
        frames = []
        for sym in ["核心指数", "指数成份", "上证系列指数", "深证系列指数"]:
            try:
                df = ak.stock_zh_index_spot_em(symbol=sym)
                if df is not None and not df.empty:
                    frames.append(df)
            except Exception:
                continue
        if frames:
            indices = pd.concat(frames, ignore_index=True, copy=False).drop_duplicates()
    if indices is None or indices.empty:
        return {}
    if "名称" not in indices.columns:
        return {}
    pct_col = "涨跌幅" if "涨跌幅" in indices.columns else None
    if pct_col is None:
        return {}
    targets = ["上证指数", "创业板指"]
    out = {}
    filtered = indices[indices["名称"].isin(targets)]
    for _, row in filtered.iterrows():
        try:
            out[str(row["名称"])] = float(row[pct_col])
        except Exception:
            continue
    return out

def _get_market_regime_state(index_symbol: str = MARKET_INDEX_SYMBOL):
    try:
        df = ak.stock_zh_index_daily_em(symbol=index_symbol)
    except Exception:
        return True, {}
    if df is None or df.empty or "close" not in df.columns:
        return True, {}
    df = df.tail(260).copy()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"]).reset_index(drop=True)
    if len(df) < (MARKET_SLOW_MA + MARKET_SLOPE_DAYS + 5):
        return True, {}

    df["ma_fast_prev"] = df["close"].rolling(MARKET_FAST_MA).mean().shift(1)
    df["ma_slow_prev"] = df["close"].rolling(MARKET_SLOW_MA).mean().shift(1)
    df["ma_fast_slope"] = df["ma_fast_prev"] - df["ma_fast_prev"].shift(MARKET_SLOPE_DAYS)
    df["pct_change"] = df["close"].pct_change() * 100

    last = df.iloc[-1]
    ok = (
        (last["close"] > last["ma_fast_prev"])
        and (last["ma_fast_prev"] > last["ma_slow_prev"])
        and (last["ma_fast_slope"] > 0)
        and (last["pct_change"] > MARKET_PANIC_PCT)
    )
    info = {
        "symbol": index_symbol,
        "date": str(df["date"].iloc[-1]) if "date" in df.columns else None,
        "close": float(last["close"]) if pd.notna(last["close"]) else None,
        "pct_change": float(last["pct_change"]) if pd.notna(last["pct_change"]) else None,
        "ma_fast_prev": float(last["ma_fast_prev"]) if pd.notna(last["ma_fast_prev"]) else None,
        "ma_slow_prev": float(last["ma_slow_prev"]) if pd.notna(last["ma_slow_prev"]) else None,
        "ma_fast_slope": float(last["ma_fast_slope"]) if pd.notna(last["ma_fast_slope"]) else None,
    }
    return bool(ok), info

def analyze_stock_optimized(args):
    """
    分析单只股票 (优化版)
    args: (symbol, name) 元组，方便多线程传参
    """
    symbol, name = args
    _stat_inc("analyze_called")
    
    # 0. 基础风控：剔除 ST 和 退市整理
    if "ST" in name or "退" in name:
        _stat_inc("skip_st")
        return None

    try:
        # 1. 获取数据 (增加简单的重试机制逻辑)
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        # 只需要过去 ~150 天数据即可计算 MA60 和前面的洗盘
        start_date = (datetime.datetime.now() - datetime.timedelta(days=150)).strftime("%Y%m%d")
        
        # 增加重试机制
        df = None
        for _ in range(3):
            try:
                df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
                if df is not None and not df.empty:
                    break
            except:
                time.sleep(0.5)
        
        if df is None or df.empty or len(df) < 65: # 稍微多留一点buffer
            _stat_inc("skip_insufficient_daily")
            return None
            
        # 2. 单位防御 (保留 Ratio Check 逻辑)
        if df['成交量'].iloc[-1] > 0:
            last_close = df['收盘'].iloc[-1]
            last_vol = df['成交量'].iloc[-1]
            last_amt = df['成交额'].iloc[-1]
            # Raw_VWAP = Amount / Volume. 如果 Raw_VWAP / Price ≈ 100，说明 Volume 是手
            if (last_amt / last_vol) / last_close >= 80:
                df['成交量'] = df['成交量'] * 100
        
        # 3. 指标计算
        df['pct_chg'] = df['涨跌幅']
        df['turnover'] = df['换手率']
        df['ma20'] = df['收盘'].rolling(20).mean()
        df['ma60'] = df['收盘'].rolling(60).mean()
        df['vol_ma5'] = df['成交量'].rolling(5).mean()
        
        # 4. 核心逻辑判定
        curr = df.iloc[-1]
        prev_close = df["收盘"].iloc[-2]
        
        # A. 趋势共振 (新增优化)
        # 要求 MA60 向上 (今日 MA60 > 5日前 MA60) 且 收盘价 > MA60
        ma60_slope_up = curr['ma60'] > df['ma60'].iloc[-5]
        is_trend_up = (curr['收盘'] > curr['ma60']) and ma60_slope_up
        
        if not is_trend_up:
            _stat_inc("skip_trend")
            return None

        # B. 洗盘检测 (保留原有逻辑)
        turnover_threshold = df['turnover'].rolling(60).quantile(TURNOVER_QUANTILE)
        df['is_low_turnover'] = df['turnover'] < turnover_threshold
        # shift(1) 排除今日，检查之前 N 天
        wash_count = df['is_low_turnover'].shift(1).rolling(WASH_DAYS_WINDOW).sum().iloc[-1]
        
        if wash_count < WASH_LOW_DAYS_REQ:
            _stat_inc("skip_wash")
            return None
            
        # C. 爆发信号 (Trigger)
        # 修正：对比昨日均量（不含今日），因为今日的巨量会拉高 MA5，导致判定失效
        # 使用 shift(1) 获取昨日的 vol_ma5 (即前5天均量)
        # 注意：df['vol_ma5'] = df['成交量'].rolling(5).mean()
        # 所以 df['vol_ma5'].iloc[-2] 就是昨日算出来的过去5天均量 (T-1, T-2, T-3, T-4, T-5)
        # 或者直接取 shift(1).iloc[-1]
        
        vol_ma5_yesterday = df['vol_ma5'].shift(1).iloc[-1]
        if pd.isna(vol_ma5_yesterday) or vol_ma5_yesterday == 0:
            _stat_inc("skip_vol_ma_na")
            return None
            
        is_vol_spike = curr['成交量'] > (vol_ma5_yesterday * 1.5)
        is_price_rise = curr['pct_chg'] > MIN_PRICE_RISE
        is_above_ma20 = curr['收盘'] > curr['ma20']
        open_pct_change = (curr["开盘"] - prev_close) / prev_close * 100 if prev_close and prev_close > 0 else 999.0
        is_safe_open = open_pct_change < MAX_OPEN_GAP_PCT
        
        # D. 形态优化：上影线控制 (替代纯粹的 Close near High)
        # 上影线长度 / (最高 - 最低)
        high_low_range = curr['最高'] - curr['最低']
        if high_low_range == 0:
            upper_shadow_ratio = 0
        else:
            upper_shadow_ratio = (curr['最高'] - curr['收盘']) / high_low_range
            
        is_solid_close = upper_shadow_ratio < MAX_SHADOW_RATIO
        
        if is_vol_spike and is_price_rise and is_above_ma20 and is_solid_close and is_safe_open:
            _stat_inc("signal_hit")
            # 计算建议止损位 (例如：今日开盘价 或 MA20)
            stop_loss = max(curr['开盘'], curr['ma20'])
            
            return {
                "代码": symbol,
                "名称": name,
                "现价": curr['收盘'],
                "涨跌%": curr['pct_chg'],
                "量比": round(curr['成交量'] / curr['vol_ma5'], 2),
                "换手%": curr['turnover'],
                "洗盘强度": f"{int(wash_count)}/{WASH_DAYS_WINDOW}",
                "MA60趋势": "向上" if ma60_slope_up else "走平",
                "建议止损": round(stop_loss, 2),
                "今开%": round(float(open_pct_change), 2),
                "信号": "洗盘突破"
            }
        else:
            if not is_vol_spike:
                _stat_inc("fail_vol_spike")
            if not is_price_rise:
                _stat_inc("fail_price_rise")
            if not is_above_ma20:
                _stat_inc("fail_above_ma20")
            if not is_solid_close:
                _stat_inc("fail_solid_close")
            if not is_safe_open:
                _stat_inc("fail_safe_open")
            
    except Exception:
        _stat_inc("error_exception")
        return None
    return None

def run_strategy():
    print("🚀 启动多线程选股策略 (优化版)...")
    with _stats_lock:
        _stats.clear()
    
    # 1. 获取 Spot 数据
    try:
        df_market = ak.stock_zh_a_spot_em()
    except Exception as e:
        print(f"❌ 网络错误，无法获取实时行情: {e}")
        return pd.DataFrame(columns=["代码", "名称", "现价", "涨跌%", "量比", "洗盘强度", "MA60趋势", "建议止损", "信号"])

    idx_pct = _get_core_index_pct_changes()
    sh = idx_pct.get("上证指数")
    cyb = idx_pct.get("创业板指")
    if sh is not None and cyb is not None and (sh <= MARKET_PANIC_PCT) and (cyb <= MARKET_PANIC_PCT):
        print(f"⚠️ 大盘环境偏弱：上证{sh:.2f}% 创业板{cyb:.2f}%")
        return pd.DataFrame(columns=["代码", "名称", "现价", "涨跌%", "量比", "洗盘强度", "MA60趋势", "建议止损", "信号"])

    market_ok, market_info = _get_market_regime_state()
    if not market_ok:
        info = market_info or {}
        date = info.get("date") or ""
        close = info.get("close")
        pct = info.get("pct_change")
        ma_fast = info.get("ma_fast_prev")
        ma_slow = info.get("ma_slow_prev")
        sym = info.get("symbol") or MARKET_INDEX_SYMBOL
        msg_parts = [f"{sym} {date}".strip()]
        if close is not None and pct is not None:
            msg_parts.append(f"close={close:.2f} pct={pct:.2f}%")
        if ma_fast is not None and ma_slow is not None:
            msg_parts.append(f"MA{MARKET_FAST_MA}={ma_fast:.2f} MA{MARKET_SLOW_MA}={ma_slow:.2f}")
        print("⚠️ 大盘环境过滤：" + " | ".join([p for p in msg_parts if p]))
        return pd.DataFrame(columns=["代码", "名称", "现价", "涨跌%", "量比", "洗盘强度", "MA60趋势", "建议止损", "信号"])

    # 2. 初筛 (过滤掉停牌、跌停、无量个股)
    mask = (df_market['最新价'] > 0) & \
           (df_market['成交量'] > 0) & \
           (df_market['涨跌幅'] > MIN_PRICE_RISE) # 必须上涨
           
    # 如果有量比字段，先筛一下，减少请求量
    if '量比' in df_market.columns:
        mask = mask & (df_market['量比'] > 1.2) # 放宽一点给后面历史数据确认
        
    targets = df_market[mask][['代码', '名称']].values.tolist()
    print(f"🔍 初筛后待分析: {len(targets)} 只股票")

    results = []
    
    # 3. 多线程并发执行 (大幅提升速度)
    # max_workers 建议设为 10-20，太高容易被封接口
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=16) as executor:
        # 提交任务
        future_to_stock = {executor.submit(analyze_stock_optimized, t): t for t in targets}
        
        # 处理进度条
        total_tasks = len(targets)
        completed = 0
        
        for future in as_completed(future_to_stock):
            res = future.result()
            if res:
                results.append(res)
            
            completed += 1
            if completed % 20 == 0:
                print(f"进度: {completed}/{total_tasks}...", end="\r")
                
    elapsed = time.time() - start_time
    print(f"\n⏱️ 耗时: {elapsed:.2f}秒")
    with _stats_lock:
        if _stats:
            print("📊 过滤统计:", dict(_stats))

    # 4. 输出
    if results:
        df_res = pd.DataFrame(results)
        # 按量比和涨幅综合排序
        # 注意：洗盘强度是字符串 "5/10"，排序可能不准，先转数值
        df_res['洗盘天数'] = df_res['洗盘强度'].apply(lambda x: int(x.split('/')[0]))
        df_res = df_res.sort_values(by=["洗盘天数", "量比"], ascending=False)
        
        print("\n✅ 选股结果 (按洗盘质量排序)：")
        cols = ["代码", "名称", "现价", "涨跌%", "今开%", "量比", "换手%", "洗盘强度", "MA60趋势", "建议止损"]
        print(df_res[cols].to_string(index=False))
        return df_res
    else:
        print("\n⚠️ 今日无符合条件的标的")
        return pd.DataFrame(columns=["代码", "名称", "现价", "涨跌%", "量比", "换手%", "洗盘强度", "MA60趋势", "建议止损", "信号"])

# 执行策略
# 注意：在 screener_service 中执行时，需要将结果赋值给 df 变量
df = run_strategy()
