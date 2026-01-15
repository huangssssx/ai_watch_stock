# 山谷狙击选股策略 - 学术驱动优化版
# 基于 Bry-Boschan 动态窗口、市值分层流动性、STH-CB 成本模型优化
#
# 【如何使用】
# 1) Web 端：进入“选股”页面 → 选择“山谷狙击选股” → 点击 “Run Now” 运行。
#    - 系统执行方式：会直接执行数据库里该策略的脚本文本（要求脚本最终产出 `df` 或 `result`）。
# 2) 脚本更新入库：修改本文件后，使用下面命令将最新脚本文本写回数据库：
#    - `python3 backend/scripts/insert_valley_script.py --file backend/scripts/选股策略/山谷狙击选股策略_optimized.py --id 5 --force`
#    - 其中 `--id 5` 是当前库里“山谷狙击选股”的 `stock_screeners.id`（如你库里 ID 不同，请以实际为准）。
#
# 【输出约定（必须）】
# - 你需要在脚本末尾定义：
#   - `df`: pandas.DataFrame（推荐）。系统会把它转成 JSON 列表展示与落库。
#   - 或 `result`: List[Dict]（可选）。
# - 建议列名至少包含：`代码`、`名称`、`最新价`（用于前端展示与“一键加入自选”识别）。
#
# 【价格阈值过滤（织布机 / Price Threshold）】
# - A 股最小变动单位（Tick）固定为 0.01 元，股价越低 Tick 占比越大，曲线更“锯齿”：
#   - Tick Impact = 0.01 / Price
# - 本脚本提供可开关的“低价过滤”，默认剔除 `最新价 < 10.0`：
#   - `PRICE_THRESHOLD_ENABLED`：是否启用
#   - `PRICE_THRESHOLD_MIN_PRICE`：最低价阈值（激进可设 5.0）
#
# 【运行注意事项】
# - 本脚本会拉取全市场快照 + 多只股票的历史数据，运行时间与候选池大小、网络质量强相关。
# - akshare 数据接口有时会抖动/限流，出现异常时会跳过个股或返回空结果，这是正常现象。
# - 依赖：`akshare`、`pandas`、`numpy`、`talib`、`scipy`（缺依赖会导致运行失败）。

import akshare as ak
import pandas as pd
import numpy as np
import datetime
import talib
from scipy.signal import argrelextrema

# --- 参数配置 ---
RECENT_VOLUME_DAYS = 5
VOLUME_BASE_DAYS = 120

# 市值分层阈值 (单位: 元)
CAP_SMALL = 100 * 1e8
CAP_LARGE = 500 * 1e8

# 缩量阈值 (动态调整)
VOL_RANK_LARGE = 0.25
VOL_RANK_MID = 0.15
VOL_RANK_SMALL = 0.10

# 基础过滤
MIN_TURNOVER_AMOUNT = 30000000
MAX_PRICE_CHANGE = 6.0
PRICE_THRESHOLD_ENABLED = True
PRICE_THRESHOLD_MIN_PRICE = 10.0

# 评分门槛
THRESHOLD_HIGH_QUALITY = 7
THRESHOLD_POTENTIAL = 4

AR_SPREAD_WINDOW = 20
AR_SPREAD_LOOKBACK = 120
RSV_WINDOW = 20
RSV_LOOKBACK = 120
ILLIQ_COMPOSITE_THRESHOLD = 0.70
AR_SPREAD_RANK_SKIP = 0.90

# BB算法参数
BB_WINDOW = 5  # 最小相位长度

# STH-CB 参数
OVERHEAD_VOL_WINDOW = 20
DRAWDOWN_THRESHOLD = 0.20

# 评分标准
SCORE_CRITERIA = {
    "volume_extreme": 3,
    "volume_high": 1,
    "macd_div": 3,
    "rsi_div": 2,
    "illiq_composite": 2,
    "ofi_confirm": 1,
    "vrp_signal": 2,
    "rebound_confirm": 2,
    "sector_fund_flow_strong": 2,
    "sector_fund_flow_ok": 1,
    "weibo_panic": 1,
    "heat_penalty": 1,
    "weibo_hype_penalty": 1,
}

# --- 核心函数 ---

def _normalize_symbol(code: str) -> str:
    s = "" if code is None else str(code).strip()
    if len(s) >= 8 and s[:2].lower() in ("sh", "sz", "bj"):
        return s[2:]
    return s

def _kalman_filter_1d(values: pd.Series, q: float = 1e-5, r_scale: float = 0.20):
    """卡尔曼滤波降噪处理"""
    v = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    if v.size == 0:
        return values
    first_finite_idx = int(np.argmax(np.isfinite(v))) if np.isfinite(v).any() else None
    if first_finite_idx is None:
        return values
    dv = np.diff(v)
    dv = dv[np.isfinite(dv)]
    base_var = float(np.nanvar(dv)) if dv.size else 0.0
    r = max(1e-9, r_scale * base_var)
    x = float(v[first_finite_idx])
    p = 1.0
    out = np.empty_like(v, dtype=float)
    for i in range(v.size):
        p = p + q
        if np.isfinite(v[i]):
            k = p / (p + r)
            x = x + k * (v[i] - x)
            p = (1.0 - k) * p
        out[i] = x
    return pd.Series(out, index=values.index)

def _get_bb_troughs(series: pd.Series, window: int = BB_WINDOW):
    """
    基于BB规则的结构化低点识别 (Rao & Rojas 2025)
    """
    data = series.values
    # 寻找局部极小值
    local_mins_tuple = argrelextrema(data, np.less, order=window)
    local_mins = local_mins_tuple[0]
    
    refined_troughs = []
    if len(local_mins) > 0:
        refined_troughs.append(local_mins[0])
        for i in range(1, len(local_mins)):
            # 确保低点间隔
            if local_mins[i] - refined_troughs[-1] >= window:
                refined_troughs.append(local_mins[i])
            else:
                # 如果间隔太近，保留更低的那个
                if data[local_mins[i]] < data[refined_troughs[-1]]:
                    refined_troughs[-1] = local_mins[i]
                    
    return refined_troughs

def detect_dynamic_divergence(smooth_p: pd.Series, indicator: pd.Series):
    """
    动态窗口背离检测 (Nowcasting)
    """
    if len(smooth_p) < 60: return False
    
    troughs = _get_bb_troughs(smooth_p)
    if len(troughs) < 2: return False
    
    # 锁定最近的两个结构化低点
    last_idx = troughs[-1]
    prev_idx = troughs[-2]
    
    # 如果最近的低点离现在太远(超过15天)，则信号失效
    if (len(smooth_p) - 1) - last_idx > 15:
        return False
    
    p_last, p_prev = smooth_p.iloc[last_idx], smooth_p.iloc[prev_idx]
    i_last, i_prev = indicator.iloc[last_idx], indicator.iloc[prev_idx]
    
    # 牛背离逻辑：价格创新低（或二次探底），指标显著抬升
    if p_last <= p_prev * 1.02 and i_last > i_prev * 1.05:
        # 增加加速度验证: 跌速需放缓 (二阶差分 > 0)
        recent_acceleration = (smooth_p.diff().diff()).iloc[last_idx]
        if recent_acceleration > 0:
            return True
    return False

def calc_composite_illiq(close: pd.Series, amount: pd.Series, high: pd.Series, low: pd.Series):
    """
    Amihud-HL-FHT复合流动性指标 (Dong et al. 2024)
    """
    if len(close) < 20: return 0, 0
    
    # 1. Amihud (|Ret| / Amt)
    rets = close.pct_change().abs()
    amihud = rets / (amount + 1e-9) * 1e8
    
    # 2. HL Spread (Corwin-Schultz 简化版)
    hl_ratio = (high - low) / (close + 1e-9)
    
    # 计算最近20日的平均值作为当前值
    curr_amihud = amihud.iloc[-20:].mean()
    curr_hl = hl_ratio.iloc[-20:].mean()
    
    # 计算历史分位 (过去120天)
    hist_amihud = amihud.iloc[-120:]
    hist_hl = hl_ratio.iloc[-120:]
    
    amihud_rank = (hist_amihud <= curr_amihud).mean()
    hl_rank = (hist_hl <= curr_hl).mean()

    composite = (amihud_rank + hl_rank) / 2.0
    return composite

def calc_ar_spread_rank(high: pd.Series, low: pd.Series, close: pd.Series):
    h = pd.to_numeric(high, errors="coerce")
    l = pd.to_numeric(low, errors="coerce")
    c = pd.to_numeric(close, errors="coerce")
    h = np.log(h.where(h > 0))
    l = np.log(l.where(l > 0))
    c = np.log(c.where(c > 0))
    eta = (h + l) / 2.0
    term = 4.0 * (c - eta) * (c.shift(1) - eta.shift(1))
    ar = np.sqrt(np.maximum(term, 0.0))
    ar_roll = ar.rolling(window=AR_SPREAD_WINDOW, min_periods=max(3, AR_SPREAD_WINDOW // 3)).mean()
    curr = ar_roll.iloc[-1] if len(ar_roll) else np.nan
    hist = ar_roll.iloc[-AR_SPREAD_LOOKBACK:].dropna()
    rank = float((hist <= curr).mean()) if len(hist) and pd.notna(curr) else np.nan
    return rank, float(curr) if pd.notna(curr) else np.nan

def calculate_downside_rsv_rank(close: pd.Series):
    c = pd.to_numeric(close, errors="coerce").where(lambda x: x > 0)
    r = c.pct_change()
    down = np.minimum(r, 0.0) ** 2
    tot = r ** 2
    down_sum = down.rolling(window=RSV_WINDOW, min_periods=max(3, RSV_WINDOW // 3)).sum()
    tot_sum = tot.rolling(window=RSV_WINDOW, min_periods=max(3, RSV_WINDOW // 3)).sum()
    ratio = down_sum / (tot_sum + 1e-9)
    ratio = ratio.replace([np.inf, -np.inf], np.nan)
    last = ratio.iloc[-1] if len(ratio) else np.nan
    hist = ratio.iloc[-RSV_LOOKBACK:].dropna()
    rank = float((hist <= last).mean()) if len(hist) and pd.notna(last) else np.nan
    return rank, float(last) if pd.notna(last) else np.nan

def check_overhead_supply(close: pd.Series, volume: pd.Series, amount: pd.Series, current_price: float):
    """
    半山腰规避模块 (STH-CB & Drawdown)
    """
    if len(close) < 252: return False # 需要一年数据计算 drawdown
    
    # STH-CB: 20日 VWAP
    vol_20 = volume.rolling(20).sum()
    amt_20 = amount.rolling(20).sum()
    typical_px = float(pd.to_numeric(close, errors="coerce").tail(60).median())
    typical_amt_per_vol = float((pd.to_numeric(amount, errors="coerce") / (pd.to_numeric(volume, errors="coerce") + 1e-9)).tail(60).median())
    vol_unit = 100.0 if (np.isfinite(typical_px) and typical_px > 0 and np.isfinite(typical_amt_per_vol) and typical_amt_per_vol > typical_px * 20.0) else 1.0
    vwap_20 = amt_20 / (vol_20 * vol_unit + 1e-9)
    
    current_vwap = vwap_20.iloc[-1]
    prev_vwap = vwap_20.iloc[-2]
    
    # 规则1: 价格 > VWAP 或 VWAP 拐头向上
    vwap_slope = current_vwap - prev_vwap
    is_above_cost = (current_price > current_vwap) or (vwap_slope > 0)
    
    # 规则2: 距离52周高点需有足够深度 (>20%)
    high_52w = close.rolling(252).max().iloc[-1]
    drawdown = (high_52w - current_price) / high_52w
    is_deep_enough = drawdown > DRAWDOWN_THRESHOLD
    
    return is_above_cost and is_deep_enough

def dynamic_volume_score(volume: pd.Series, mkt_cap: float):
    """
    市值分层动态缩量评分
    """
    if len(volume) < 120: return 0, 0
    
    # 定义动态阈值
    if mkt_cap > CAP_LARGE:
        threshold = VOL_RANK_LARGE
    elif mkt_cap < CAP_SMALL:
        threshold = VOL_RANK_SMALL
    else:
        threshold = VOL_RANK_MID
        
    # 计算120日缩量排名
    curr_vol = volume.iloc[-RECENT_VOLUME_DAYS:].median()
    hist_vol = volume.iloc[-VOLUME_BASE_DAYS:]
    vol_rank = (hist_vol <= curr_vol).mean()
    
    if vol_rank < threshold:
        return SCORE_CRITERIA["volume_extreme"], vol_rank
    elif vol_rank < 0.40:
        return SCORE_CRITERIA["volume_high"], vol_rank
    return 0, vol_rank

def calculate_vrp_score(close: pd.Series):
    """
    波动率风险溢价 (VRP)
    """
    if len(close) < 20: return 0, 0
    
    rets = close.pct_change()
    rv = rets.rolling(5).std() # Realized Volatility
    iv_proxy = rets.rolling(20).std() # Implied Volatility Proxy (用长期波动率代替)
    
    vrp = iv_proxy - rv
    
    # 计算VRP分位
    curr_vrp = vrp.iloc[-1]
    hist_vrp = vrp.iloc[-120:]
    vrp_rank = (hist_vrp <= curr_vrp).mean()
    
    if vrp_rank > 0.8:
        return SCORE_CRITERIA["vrp_signal"], vrp_rank
    return 0, vrp_rank

def calculate_ofi_signal(open_s, close_s, vol_s):
    # 简化的 OFI 逻辑
    o = pd.to_numeric(open_s, errors="coerce")
    c = pd.to_numeric(close_s, errors="coerce")
    v = pd.to_numeric(vol_s, errors="coerce").fillna(0.0)
    diff = c - o
    sgn = np.sign(diff)
    ofi = sgn * v
    ofi_sum = ofi.rolling(10).sum()
    vol_sum = v.rolling(10).sum()
    ratio = ofi_sum / (vol_sum.abs() + 1e-9)
    last_ratio = ratio.iloc[-1]
    if last_ratio > 0.1:
        return SCORE_CRITERIA["ofi_confirm"], last_ratio
    return 0, last_ratio

# --- 主程序 ---
print("🎯 【山谷狙击选股策略 (学术优化版)】启动")
print("📡 正在获取A股实时行情...")

try:
    df_market = ak.stock_zh_a_spot_em()
except Exception as e:
    print(f"❌ 获取行情失败: {e}")
    df_market = pd.DataFrame()

if not df_market.empty:
    # 预过滤
    df_market = df_market[~df_market["名称"].str.contains("ST|退", na=False)]
    df_market = df_market[abs(df_market["涨跌幅"]) <= MAX_PRICE_CHANGE]
    if "成交额" in df_market.columns:
        df_market = df_market[df_market["成交额"] >= MIN_TURNOVER_AMOUNT]
    if PRICE_THRESHOLD_ENABLED and "最新价" in df_market.columns:
        df_market["最新价"] = pd.to_numeric(df_market["最新价"], errors="coerce")
        df_market = df_market.dropna(subset=["最新价"])
        df_market = df_market[df_market["最新价"] >= PRICE_THRESHOLD_MIN_PRICE]
    
    if len(df_market) > 300:
        df_market = df_market.sort_values(by="换手率", ascending=True).head(300)

    sector_fund_flow_map = {}
    try:
        fund_flow_df = ak.stock_sector_fund_flow_rank(indicator="5日", sector_type="行业资金流")
        if fund_flow_df is not None and not fund_flow_df.empty and "名称" in fund_flow_df.columns:
            col = "5日主力净流入-净占比"
            if col in fund_flow_df.columns:
                ff = fund_flow_df[["名称", col]].copy()
                ff["名称"] = ff["名称"].astype(str)
                ff[col] = pd.to_numeric(ff[col], errors="coerce")
                ff = ff.dropna(subset=["名称", col])
                sector_fund_flow_map = {str(r["名称"]): float(r[col]) for _, r in ff.iterrows()}
    except Exception:
        sector_fund_flow_map = {}

    hot_rank_map = {}
    try:
        hot_df = ak.stock_hot_rank_em()
        if hot_df is not None and not hot_df.empty and "代码" in hot_df.columns and "当前排名" in hot_df.columns:
            hd = hot_df[["代码", "当前排名"]].copy()
            hd["代码"] = hd["代码"].astype(str).map(_normalize_symbol)
            hd["当前排名"] = pd.to_numeric(hd["当前排名"], errors="coerce")
            hd = hd.dropna(subset=["代码", "当前排名"])
            hot_rank_map = {str(r["代码"]): int(r["当前排名"]) for _, r in hd.iterrows()}
    except Exception:
        hot_rank_map = {}

    weibo_rate_map = {}
    try:
        weibo_df = ak.stock_js_weibo_report(time_period="CNHOUR24")
        if weibo_df is not None and not weibo_df.empty and "name" in weibo_df.columns and "rate" in weibo_df.columns:
            wb = weibo_df[["name", "rate"]].copy()
            wb["name"] = wb["name"].astype(str)
            wb["rate"] = pd.to_numeric(wb["rate"], errors="coerce")
            wb = wb.dropna(subset=["name", "rate"])
            weibo_rate_map = {str(r["name"]): float(r["rate"]) for _, r in wb.iterrows()}
    except Exception:
        weibo_rate_map = {}

    industry_cache = {}

    def _get_industry(symbol: str):
        sym = _normalize_symbol(symbol)
        if sym in industry_cache:
            return industry_cache[sym]
        industry = None
        try:
            info_df = ak.stock_individual_info_em(symbol=sym)
            if info_df is not None and not info_df.empty and "item" in info_df.columns and "value" in info_df.columns:
                mask = info_df["item"].astype(str) == "行业"
                if mask.any():
                    industry = str(info_df.loc[mask, "value"].iloc[0]).strip()
                    if not industry:
                        industry = None
        except Exception:
            industry = None
        industry_cache[sym] = industry
        return industry

    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=730)
    start_date_str = start_date.strftime("%Y%m%d")
    end_date_str = end_date.strftime("%Y%m%d")
    
    results = []
    count = 0
    total = len(df_market)
    start_ts = datetime.datetime.now()
    progress_every = 10 if total <= 120 else 25
    print(f"🧮 候选池: {total} 只，进度步长: {progress_every}")
    
    for _, row in df_market.iterrows():
        count += 1
        symbol = row["代码"]
        name = row["名称"]
        current_price = float(row["最新价"])
        pct_chg = float(row["涨跌幅"])
        mkt_cap = float(row["流通市值"]) if "流通市值" in row and pd.notna(row["流通市值"]) else 100e8
        
        if count == 1 or count == total or (progress_every > 0 and count % progress_every == 0):
            elapsed = (datetime.datetime.now() - start_ts).total_seconds()
            speed = count / elapsed if elapsed > 0 else 0.0
            eta_sec = int((total - count) / speed) if speed > 0 else -1
            eta_str = f"{eta_sec}s" if eta_sec >= 0 else "?"
            print(f"⏳ 进度: {count}/{total}  用时:{elapsed:.1f}s  ETA:{eta_str}")
            
        try:
            df_hist = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date_str, end_date=end_date_str, adjust="qfq")
            if df_hist is None or df_hist.empty or len(df_hist) < 120:
                continue
                
            # 数据清洗
            df_hist["开盘"] = pd.to_numeric(df_hist["开盘"], errors="coerce")
            df_hist["收盘"] = pd.to_numeric(df_hist["收盘"], errors="coerce")
            df_hist["最高"] = pd.to_numeric(df_hist["最高"], errors="coerce")
            df_hist["最低"] = pd.to_numeric(df_hist["最低"], errors="coerce")
            df_hist["成交量"] = pd.to_numeric(df_hist["成交量"], errors="coerce")
            df_hist["成交额"] = pd.to_numeric(df_hist["成交额"], errors="coerce")
            
            close = df_hist["收盘"]
            open_ = df_hist["开盘"]
            high = df_hist["最高"]
            low = df_hist["最低"]
            volume = df_hist["成交量"]
            amount = df_hist["成交额"]
            
            # 1. 半山腰规避 (Filter)
            is_safe = check_overhead_supply(close, volume, amount, current_price)
            if not is_safe:
                continue
                
            score = 0
            signals = []
            
            # 卡尔曼平滑
            smooth_p = _kalman_filter_1d(close)
            
            # 缩量评分
            v_score, v_rank = dynamic_volume_score(volume, mkt_cap)
            if v_score > 0:
                score += v_score
                signals.append(f"缩量({int(v_rank*100)}%)")
                
            # VRP 评分
            vrp_score, vrp_rank = calculate_vrp_score(close)
            if vrp_score > 0:
                score += vrp_score
                signals.append("VRP恐慌")
            
            # MACD
            macd, signal, _ = talib.MACD(smooth_p.values)
            if detect_dynamic_divergence(smooth_p, pd.Series(macd)):
                score += SCORE_CRITERIA["macd_div"]
                signals.append("MACD底")
                
            # RSI
            rsi = talib.RSI(smooth_p.values, timeperiod=14)
            if detect_dynamic_divergence(smooth_p, pd.Series(rsi)):
                score += SCORE_CRITERIA["rsi_div"]
                signals.append("RSI底")
                
            # 复合ILLIQ
            comp_illiq = calc_composite_illiq(close, amount, high, low)
            if comp_illiq > ILLIQ_COMPOSITE_THRESHOLD: 
                 score += SCORE_CRITERIA["illiq_composite"]
                 signals.append("ILLIQ吸收")

            ar_rank, ar_spread = calc_ar_spread_rank(high, low, close)
            if pd.notna(ar_rank) and ar_rank >= AR_SPREAD_RANK_SKIP:
                continue

            down_rank, down_ratio = calculate_downside_rsv_rank(close)
            if pd.notna(down_rank) and down_rank > 0.80:
                continue
                 
            # OFI
            ofi_score, ofi_val = calculate_ofi_signal(open_, close, volume)
            if ofi_score > 0:
                score += ofi_score
                signals.append("OFI+")
                
            # 均线支撑/启动
            ma5 = close.rolling(5).mean()
            if len(ma5) > 2 and current_price > ma5.iloc[-1] and ma5.iloc[-1] > ma5.iloc[-2]:
                score += SCORE_CRITERIA["rebound_confirm"]
                signals.append("启动")

            industry = None
            sector_ff = None
            hot_rank = None
            weibo_rate = None

            if score >= THRESHOLD_POTENTIAL - 2:
                sym_norm = _normalize_symbol(symbol)

                if hot_rank_map:
                    hot_rank = hot_rank_map.get(sym_norm)
                    if hot_rank is not None and hot_rank <= 20:
                        score -= SCORE_CRITERIA["heat_penalty"]
                        signals.append("热度过高")

                if weibo_rate_map:
                    weibo_rate = weibo_rate_map.get(str(name))
                    if weibo_rate is not None and np.isfinite(weibo_rate):
                        if weibo_rate <= -2.0:
                            score += SCORE_CRITERIA["weibo_panic"]
                            signals.append("舆情偏空")
                        elif weibo_rate >= 2.0:
                            score -= SCORE_CRITERIA["weibo_hype_penalty"]
                            signals.append("舆情偏热")

                if sector_fund_flow_map:
                    industry = _get_industry(sym_norm)
                    if industry is not None:
                        sector_ff = sector_fund_flow_map.get(industry)
                        if sector_ff is not None and np.isfinite(sector_ff):
                            if sector_ff >= 0.5:
                                score += SCORE_CRITERIA["sector_fund_flow_strong"]
                                signals.append("板块净流入强")
                            elif sector_ff > 0:
                                score += SCORE_CRITERIA["sector_fund_flow_ok"]
                                signals.append("板块净流入")

            if score >= THRESHOLD_POTENTIAL:
                results.append({
                    "代码": symbol,
                    "名称": name,
                    "现价": current_price,
                    "涨跌%": pct_chg,
                    "评分": score,
                    "行业": industry,
                    "板块5日主力净占比": round(sector_ff, 2) if sector_ff is not None and np.isfinite(sector_ff) else None,
                    "热度排名": int(hot_rank) if hot_rank is not None else None,
                    "微博24h热度": round(weibo_rate, 2) if weibo_rate is not None and np.isfinite(weibo_rate) else None,
                    "缩量分位": round(v_rank, 2),
                    "VRP": round(vrp_rank, 2),
                    "ILLIQ": round(comp_illiq, 2),
                    "AR分位": round(ar_rank, 2) if pd.notna(ar_rank) else None,
                    "下行RSV": round(down_ratio, 4) if pd.notna(down_ratio) else None,
                    "下行RSV分位": round(down_rank, 2) if pd.notna(down_rank) else None,
                    "信号": "+".join(signals)
                })
                
        except Exception as e:
            continue

    if results:
        df_res = pd.DataFrame(results).sort_values(by="评分", ascending=False)
        print("\n" + "=" * 50)
        print(f"🌟 【严选榜】 (评分>={THRESHOLD_HIGH_QUALITY})")
        print("=" * 50)
        high_q = df_res[df_res["评分"] >= THRESHOLD_HIGH_QUALITY]
        if not high_q.empty:
            print(high_q.to_string(index=False))
        else:
            print("（暂无符合严选标准的股票）")
        
        print("\n" + "-" * 50)
        print(f"👀 【潜力榜】 (评分>={THRESHOLD_POTENTIAL})")
        print("-" * 50)
        pot = df_res[(df_res["评分"] >= THRESHOLD_POTENTIAL) & (df_res["评分"] < THRESHOLD_HIGH_QUALITY)]
        if not pot.empty:
            print(pot.head(300).to_string(index=False))
        else:
            print("（暂无符合潜力标准的股票）")
        df = df_res
        result = results
    else:
        print("未发现符合条件的股票")
        df = pd.DataFrame(columns=["代码", "名称", "现价", "涨跌%", "评分", "缩量分位", "VRP", "ILLIQ", "AR分位", "下行RSV", "下行RSV分位", "信号"])
        result = []

else:
    print("获取行情为空")
    df = pd.DataFrame()
    result = []
