# 山谷狙击选股策略 - v8.6 安全版
# 核心修复：VWAP 单位数量级错误 (Bug Fix)
# 优化：增加限流延时，防止账号被封

import akshare as ak
import efinance as ef
import pandas as pd
import numpy as np
import datetime
import talib
from scipy.signal import argrelextrema
import warnings
import time
import traceback

warnings.filterwarnings('ignore')

# --- 参数配置 ---
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

# --- 核心函数 ---
_ERROR_COUNT = 0
_ERROR_TRACE_PRINTED = 0
_MAX_TRACEBACKS = 3

def _log_error(where: str, e: Exception):
    global _ERROR_COUNT, _ERROR_TRACE_PRINTED
    _ERROR_COUNT += 1
    print(f"❌ ERROR[{_ERROR_COUNT}] {where}: {type(e).__name__}: {e}")
    if _ERROR_TRACE_PRINTED < _MAX_TRACEBACKS:
        print(traceback.format_exc())
        _ERROR_TRACE_PRINTED += 1

def _safe_vwap(amount_series, vol_series, price_series):
    """
    自适应计算 VWAP，自动修正 '手' vs '股' 的单位问题
    """
    if len(amount_series) == 0: return 0.0
    
    # 尝试1: 直接除
    raw_vwap = amount_series.sum() / (vol_series.sum() + 1e-9)
    current_p = price_series.iloc[-1]
    
    # 检查数量级差异
    if current_p > 0:
        ratio = raw_vwap / current_p
        if 80 < ratio < 120: # 偏差约100倍，说明 Volume 是手
            return raw_vwap / 100.0
        elif 0.8 < ratio < 1.2: # 偏差不大，说明 Volume 是股
            return raw_vwap
            
    # 兜底：如果无法判断，假设是手（A股通常返回手）
    # 但为了保险，还是返回修正后的
    return raw_vwap / 100.0 if raw_vwap > current_p * 50 else raw_vwap

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
    if len(close) < 250: return False
    
    # 修复 VWAP 计算
    vol_20 = volume.rolling(20).sum()
    amt_20 = amount.rolling(20).sum()
    
    # 使用自适应函数计算当前的 20日 VWAP
    # 注意：这里简化处理，直接取最后一天的数据进行自适应判断
    vwap_20_val = _safe_vwap(amt_20.iloc[-20:], vol_20.iloc[-20:], close.iloc[-20:])
    
    # 宽松检查：允许跌破 20日线，但不能偏离太远 (0.85)
    # v8.5 Bug: 之前算出来 vwap 偏大100倍，导致这里必挂
    if current_price < vwap_20_val * 0.85: 
        return False
        
    high_52w = close.rolling(250).max().iloc[-1]
    drawdown = (high_52w - current_price) / high_52w
    return drawdown > DRAWDOWN_THRESHOLD

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
    if len(amount) < 300: return 0, 0, 0
    
    # 修复 VWAP 计算
    vwap_300 = _safe_vwap(amount.iloc[-300:], volume.iloc[-300:], pd.Series([current_price]))
    
    dist_pct = (current_price - vwap_300) / vwap_300
    score = 0
    if -0.15 <= dist_pct <= 0.05:
        score = SCORE_CRITERIA["vwap_support"]
    return score, vwap_300, dist_pct

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

# --- 主程序 ---
print("🎯 【山谷狙击选股策略 v8.6 (Bug修复版 + 安全限流 + efinance)】启动")
print("🛠️ 修复内容：VWAP 单位数量级自动修正 + 单线程安全延时")
print("📡 正在获取市场数据(via efinance)...")

try:
    df_market = ef.stock.get_realtime_quotes()
    if df_market is not None and not df_market.empty:
        df_market = df_market.rename(columns={
            '股票代码': '代码',
            '股票名称': '名称',
            '最新价': '最新价',
            '涨跌幅': '涨跌幅',
            '成交量': '成交量',
            '成交额': '成交额',
            '换手率': '换手率',
            '量比': '量比',
            '流通市值': '流通市值' # efinance has this
        })
        # Clean numeric
        for col in ['最新价', '涨跌幅', '成交量', '量比', '流通市值']:
            if col in df_market.columns:
                    df_market[col] = pd.to_numeric(df_market[col], errors='coerce')
    else:
        df_market = pd.DataFrame()

except Exception as e:
    _log_error("ef.stock.get_realtime_quotes()", e)
    df_market = pd.DataFrame()

if not df_market.empty:
    df_market = df_market[~df_market["名称"].str.contains("ST|退", na=False)]
    df_market = df_market[abs(df_market["涨跌幅"]) <= MAX_PRICE_CHANGE]
    if "成交额" in df_market.columns:
        df_market = df_market[df_market["成交额"] >= MIN_TURNOVER_AMOUNT]
    if "最新价" in df_market.columns:
        df_market["最新价"] = pd.to_numeric(df_market["最新价"], errors="coerce")
        df_market = df_market[df_market["最新价"] >= PRICE_THRESHOLD_MIN_PRICE]
    
    if "换手率" in df_market.columns:
         df_scan = df_market[(df_market["换手率"] > 0.8) & (df_market["换手率"] < 12.0)]
         df_scan = df_scan.sort_values(by="成交额", ascending=False).head(500)
    else:
         df_scan = df_market.head(500)

    print(f"🔍 深度扫描池: {len(df_scan)} 只，正在挖掘...")

    sector_fund_flow_map = {}
    try:
        fund_flow_df = ak.stock_sector_fund_flow_rank(indicator="5日", sector_type="行业资金流")
        if fund_flow_df is not None and not fund_flow_df.empty:
            for _, r in fund_flow_df.iterrows():
                sector_fund_flow_map[str(r["名称"])] = float(r["5日主力净流入-净占比"])
    except Exception as e:
         _log_error("stock_sector_fund_flow_rank()", e)
    
    sector_change_map = {} 
    hot_rank_map = {}
    try:
        hot_df = ak.stock_hot_rank_em()
        if hot_df is not None:
             for _, r in hot_df.iterrows():
                code = str(r["代码"])
                if len(code) >= 8: code = code[2:]
                code = code.zfill(6)
                hot_rank_map[code] = int(r["当前排名"])
    except Exception as e:
        _log_error("stock_hot_rank_em()", e)

    industry_cache = {}
    def _get_industry(symbol):
        if symbol in industry_cache: return industry_cache[symbol]
        try:
            info = ak.stock_individual_info_em(symbol=symbol)
            ind = info[info["item"]=="行业"]["value"].iloc[0]
            industry_cache[symbol] = ind
            return ind
        except Exception as e:
            _log_error(f"stock_individual_info_em({symbol})", e)
            return None

    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=VOLUME_BASE_DAYS + 60)
    start_date_str = start_date.strftime("%Y%m%d")
    end_date_str = end_date.strftime("%Y%m%d")
    
    results = []
    skipped_sector = 0
    rescued_dragon = 0
    
    if "涨跌幅" in df_scan.columns:
        for idx, row in df_scan.iterrows():
            symbol_raw = str(row["代码"])
            symbol = symbol_raw[2:] if len(symbol_raw) >= 8 else symbol_raw
            symbol = symbol.zfill(6)
            ind = _get_industry(symbol)
            if ind:
                if ind not in sector_change_map: sector_change_map[ind] = []
                sector_change_map[ind].append(float(row["涨跌幅"]))
            # 小延时
            time.sleep(0.05)
    
    sector_median_change = {k: np.median(v) for k, v in sector_change_map.items()}

    for idx, row in df_scan.iterrows():
        raw_symbol = str(row["代码"])
        symbol = raw_symbol[2:] if len(raw_symbol) >= 8 else raw_symbol
        symbol = symbol.zfill(6)
        name = row["名称"]
        current_price = float(row["最新价"])
        pct_chg = float(row["涨跌幅"])
        mkt_cap = float(row["流通市值"]) if "流通市值" in row else 100e8
        
        snapshot_amount = float(row["成交额"]) if "成交额" in row else 0
        hot_rank = hot_rank_map.get(symbol, 9999)
        
        industry = _get_industry(symbol)
        ind_change = sector_median_change.get(industry, -1.0)
        
        dragon_score = 0
        is_dragon = False
        if snapshot_amount > 5 * 1e8 and (pct_chg - ind_change > 0):
            is_dragon = True
            capped_amt = min(snapshot_amount, 20 * 1e8) 
            amt_score = 1.0 + (capped_amt - 5e8) / (15e8) * (MAX_SCORE_DRAGON - 1.0)
            dragon_score = max(dragon_score, amt_score)
        elif mkt_cap > 300 * 1e8:
            is_dragon = True
            capped_cap = min(mkt_cap, 2000 * 1e8)
            cap_score = 1.0 + (capped_cap - 300e8) / (1700e8) * (MAX_SCORE_DRAGON - 1.0)
            dragon_score = max(dragon_score, cap_score)
            
        sector_score = 0
        sector_msg = ""
        should_skip = False
        if industry and industry in sector_fund_flow_map:
            ff = sector_fund_flow_map[industry]
            if ff < -5.0:
                if is_dragon:
                    rescued_dragon += 1
                else:
                    should_skip = True
                    skipped_sector += 1
            if not should_skip:
                if ff >= 0.5:
                    sector_score = SCORE_CRITERIA["sector_fund_flow_strong"]
                    sector_msg = f"板块强({industry})"
                elif ff > 0:
                    sector_score = SCORE_CRITERIA["sector_fund_flow_ok"]
                    sector_msg = f"板块正({industry})"
        
        if should_skip: continue
        
        if idx % 50 == 0:
            print(f"⚡ 扫描中... 已熔断 {skipped_sector} 只杂毛，豁免 {rescued_dragon} 只龙头")

        # 关键修改：增加延时，保护账号
        time.sleep(0.1)

        try:
            # Using efinance
            hist_dict = ef.stock.get_quote_history([symbol])
            if not hist_dict or symbol not in hist_dict: continue
            
            df_hist = hist_dict[symbol]
            if df_hist is None or len(df_hist) < 300: continue
            
            # efinance columns map
            df_hist = df_hist.rename(columns={
                "收盘": "收盘",
                "开盘": "开盘",
                "最高": "最高",
                "最低": "最低",
                "成交量": "成交量",
                "成交额": "成交额",
                "换手率": "换手率",
                "涨跌幅": "涨跌幅"
            })
            
            close = pd.to_numeric(df_hist["收盘"], errors="coerce")
            open_ = pd.to_numeric(df_hist["开盘"], errors="coerce")
            high = pd.to_numeric(df_hist["最高"], errors="coerce")
            low = pd.to_numeric(df_hist["最低"], errors="coerce")
            volume = pd.to_numeric(df_hist["成交量"], errors="coerce")
            amount = pd.to_numeric(df_hist["成交额"], errors="coerce")
            
            if not check_overhead_supply(close, volume, amount, current_price): continue
            
            ma60 = close.rolling(60).mean()
            if len(ma60) > 0 and ma60.iloc[-1] > 0:
                if current_price < ma60.iloc[-1] * 0.85:
                    continue 
            
            score = sector_score
            signals = []
            if sector_msg: signals.append(sector_msg)
            
            if is_dragon and sector_score == 0:
                score += dragon_score
                signals.append(f"逆势龙头({dragon_score:.1f})")

            if current_price > ma60.iloc[-1]:
                score += SCORE_CRITERIA["trend_protect"]
            
            alpha_score, alpha_rank = calculate_alpha54(open_, high, low, close)
            if alpha_score > 0:
                score += alpha_score
                signals.append(f"Alpha54({int(alpha_rank*100)}%|{alpha_score:.1f})")
                
            vwap_score, vwap_val, vwap_dist = calculate_long_term_vwap(amount, volume, current_price)
            if vwap_score > 0:
                score += vwap_score
                signals.append("VWAP支撑")
            
            smooth_p = _kalman_filter_1d(close)
            
            macd, _, _ = talib.MACD(smooth_p.values)
            if detect_dynamic_divergence(smooth_p, pd.Series(macd)):
                score += SCORE_CRITERIA["macd_div"]
                signals.append("MACD底")
                
            rsi = talib.RSI(smooth_p.values, timeperiod=14)
            if detect_dynamic_divergence(smooth_p, pd.Series(rsi)):
                score += SCORE_CRITERIA["rsi_div"]
                signals.append("RSI底")
                
            v_score, v_rank = dynamic_volume_score(volume, mkt_cap)
            if v_score > 0:
                score += v_score
                signals.append(f"缩量({int(v_rank*100)}%)")
                
            if hot_rank <= 20:
                ma60_val = ma60.iloc[-1] if not ma60.empty else current_price
                if current_price > ma60_val * 1.2:
                    score += SCORE_CRITERIA["heat_penalty"]
                    signals.append("高位过热")
                elif current_price < ma60_val * 0.95:
                    score += SCORE_CRITERIA["heat_reversal"]
                    signals.append("恐慌聚气")

            if score >= THRESHOLD_POTENTIAL:
                results.append({
                    "代码": symbol,
                    "名称": name,
                    "现价": current_price,
                    "评分": round(score, 1),
                    "VWAP偏离": f"{vwap_dist*100:.1f}%",
                    "Alpha54": round(alpha_rank, 2),
                    "信号": "+".join(signals)
                })
                
        except Exception as e:
            _log_error(f"scan_one({symbol})", e)
            continue

    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values(by="评分", ascending=False)
        print(f"\n⚡ 扫描结束。熔断拦截 {skipped_sector} 只杂毛，逆势救回 {rescued_dragon} 只龙头。")
        print("\n" + "="*50)
        print(f"🏆 山谷狙击严选榜 (评分>={THRESHOLD_HIGH_QUALITY})")
        print("="*50)
        high_q = df[df["评分"] >= THRESHOLD_HIGH_QUALITY]
        if not high_q.empty:
            print(high_q.to_string(index=False))
        else:
            print("（暂无符合严选标准的股票）")
            
        print("\n" + "-"*50)
        print(f"📈 观察池 (评分>={THRESHOLD_POTENTIAL})")
        print("-"*50)
        pot = df[(df["评分"] >= THRESHOLD_POTENTIAL) & (df["评分"] < THRESHOLD_HIGH_QUALITY)]
        if not pot.empty:
            print(pot.head(20).to_string(index=False))
    else:
        print("⚠️ 未发现符合条件的股票。")
    if _ERROR_COUNT > 0:
        print(f"❗ 本次运行捕获异常次数: {_ERROR_COUNT}")

else:
    print("❌ 无法获取市场数据。")
