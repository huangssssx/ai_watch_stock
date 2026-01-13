# 山谷狙击选股策略
# 目标：找到处于底部、成交量萎缩、技术指标背离的潜力股票
# 避免"买在半山腰，套在山顶"的困境

import akshare as ak
import pandas as pd
import numpy as np
import datetime

RECENT_VOLUME_DAYS = 5
VOLUME_BASE_DAYS = 120

VOL_SHRINK_LEVEL_1 = 0.3
VOL_SHRINK_LEVEL_2 = 0.6
VOL_SHRINK_LEVEL_3 = 0.8

VOL_QUANTILE_LEVEL_1 = 0.10
VOL_QUANTILE_LEVEL_2 = 0.25
VOL_QUANTILE_LEVEL_3 = 0.40

MA_SUPPORT_RANGE = 0.05
MA_REBOUND_RANGE = 0.08

MIN_TURNOVER_AMOUNT = 30000000
MAX_PRICE_CHANGE = 6.0

THRESHOLD_HIGH_QUALITY = 7
THRESHOLD_POTENTIAL = 4

REBOUND_STABLE_DAYS = 2
REBOUND_RISE_THRESHOLD = 0.0

DIVERGENCE_LOOKBACK = 30
DIVERGENCE_MIN_SEPARATION = 5
DIVERGENCE_PRICE_DROP_PCT = 0.01
DIVERGENCE_INDICATOR_RISE_PCT = 0.05
DIVERGENCE_SLOPE_GAP_MIN = 0.002

CHIP_LOOKBACK_DAYS = 20
CHIP_HEAVY_VOL_Q = 0.80

ILLIQ_WINDOW = 20
ILLIQ_PEAK_LOOKBACK = 60
ILLIQ_SCALE = 1e8
ILLIQ_BOTTOM_RATIO_MAX = 0.60

KALMAN_Q = 1e-5
KALMAN_R_SCALE = 0.20

OFI_WINDOW = 10
OFI_RATIO_MIN = 0.10

VRP_VOL_SHORT = 5
VRP_VOL_LONG = 20
VRP_STRESS_RATIO = 1.20

SCORE_CRITERIA = {
    "volume_extreme": 3,
    "volume_high": 2,
    "volume_med": 1,
    "ma60_support": 2,
    "ma120_support": 1,
    "macd_div": 3,
    "rsi_div": 2,
    "rebound_confirm": 2,
    "illiq_absorb": 2,
    "ofi_confirm": 1,
    "vrp_stress_absorb": 1,
    "chip_overhang_penalty": 1,
}

def calculate_macd(prices, fast=12, slow=26, signal=9):
    ema_fast = prices.ewm(span=fast, adjust=False).mean()
    ema_slow = prices.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def calculate_rsi(prices, period=14):
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def _find_local_lows(prices: pd.Series, window: int = 2, min_separation: int = 5):
    p = prices.reset_index(drop=True)
    is_low = (p <= p.shift(1)) & (p <= p.shift(-1))
    if window >= 2:
        for i in range(2, window + 1):
            is_low = is_low & (p <= p.shift(i)) & (p <= p.shift(-i))
    idxs = np.flatnonzero(is_low.to_numpy(dtype=bool)).tolist()
    if not idxs:
        return []
    values = p.to_numpy(dtype=float)
    selected = []
    last = None
    for i in idxs:
        if last is None or (i - last) >= min_separation:
            selected.append(i)
            last = i
        else:
            if values[i] < values[last]:
                selected[-1] = i
                last = i
    return selected

def detect_bullish_divergence(prices: pd.Series, indicator: pd.Series):
    if len(prices) < DIVERGENCE_LOOKBACK or len(indicator) < DIVERGENCE_LOOKBACK:
        return False

    p = prices.iloc[-DIVERGENCE_LOOKBACK:].reset_index(drop=True)
    ind = indicator.iloc[-DIVERGENCE_LOOKBACK:].reset_index(drop=True)

    lows = _find_local_lows(p, window=2, min_separation=DIVERGENCE_MIN_SEPARATION)
    lows = [i for i in lows if i < (len(p) - 1)]
    if len(lows) < 2:
        return False

    low1, low2 = lows[-2], lows[-1]
    p1, p2 = float(p.iloc[low1]), float(p.iloc[low2])
    i1, i2 = float(ind.iloc[low1]), float(ind.iloc[low2])

    if np.isnan(p1) or np.isnan(p2) or np.isnan(i1) or np.isnan(i2):
        return False

    d = int(low2 - low1)
    if d <= 0:
        return False

    price_pct_slope = ((p2 / p1) - 1.0) / d if p1 != 0 else (p2 - p1) / d
    ind_norm_slope = ((i2 - i1) / (abs(i1) + 1e-9)) / d
    strength = float(ind_norm_slope - price_pct_slope)

    old_price_ok = p2 <= p1 * (1 - DIVERGENCE_PRICE_DROP_PCT)
    old_ind_ok = i2 > i1 + abs(i1) * DIVERGENCE_INDICATOR_RISE_PCT
    old_ok = bool(old_price_ok and old_ind_ok)

    price_ok = old_price_ok or (price_pct_slope < 0)
    ind_ok = ind_norm_slope > 0
    slope_ok = bool(price_ok and ind_ok and (strength >= DIVERGENCE_SLOPE_GAP_MIN))
    return bool(old_ok or slope_ok)

def _has_overhead_supply(open_s: pd.Series, close_s: pd.Series, vol_s: pd.Series, current_price: float):
    if open_s is None or close_s is None or vol_s is None:
        return False
    if len(close_s) < (CHIP_LOOKBACK_DAYS + 2):
        return False
    w_open = open_s.iloc[-(CHIP_LOOKBACK_DAYS + 1) : -1].reset_index(drop=True)
    w_close = close_s.iloc[-(CHIP_LOOKBACK_DAYS + 1) : -1].reset_index(drop=True)
    w_vol = vol_s.iloc[-(CHIP_LOOKBACK_DAYS + 1) : -1].reset_index(drop=True)
    if w_open.empty or w_close.empty or w_vol.empty:
        return False
    bearish = (w_close < w_open) & w_open.notna() & w_close.notna() & w_vol.notna()
    if not bearish.any():
        return False
    base_window = vol_s.iloc[-VOLUME_BASE_DAYS:] if len(vol_s) >= VOLUME_BASE_DAYS else vol_s
    base_med = float(base_window.median()) if (base_window is not None and not base_window.empty) else float("nan")
    q_thr = float(w_vol[bearish].quantile(CHIP_HEAVY_VOL_Q)) if bearish.sum() > 0 else float("nan")
    v_thr = max(q_thr, base_med * 1.8) if not np.isnan(base_med) else q_thr
    if np.isnan(v_thr):
        return False
    heavy = bearish & (w_vol >= v_thr)
    if not heavy.any():
        return False
    body_low = w_close[heavy]
    body_high = w_open[heavy]
    mid = (body_low + body_high) / 2.0
    inside = ((current_price >= mid) & (current_price <= body_high)).any()
    return bool(inside)

def _volume_quantile(hist_volumes: pd.Series, recent_volume: float):
    if hist_volumes is None or hist_volumes.empty or np.isnan(recent_volume):
        return None
    hv = hist_volumes.dropna()
    if hv.empty:
        return None
    return float((hv <= recent_volume).mean())

def _kalman_filter_1d(values: pd.Series, q: float = KALMAN_Q, r_scale: float = KALMAN_R_SCALE):
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

def _calc_illiq(close: pd.Series, amount: pd.Series):
    c = pd.to_numeric(close, errors="coerce")
    a = pd.to_numeric(amount, errors="coerce")
    ret = c.pct_change().abs()
    illiq = (ret / (a.abs() + 1e-9)) * ILLIQ_SCALE
    illiq_rolling = illiq.rolling(ILLIQ_WINDOW, min_periods=max(3, ILLIQ_WINDOW // 3)).mean()
    peak = illiq_rolling.rolling(ILLIQ_PEAK_LOOKBACK, min_periods=max(3, ILLIQ_PEAK_LOOKBACK // 3)).max()
    ratio = illiq_rolling / (peak + 1e-9)
    return illiq_rolling, ratio

def _calc_ofi_ratio(open_s: pd.Series, close_s: pd.Series, vol_s: pd.Series):
    o = pd.to_numeric(open_s, errors="coerce")
    c = pd.to_numeric(close_s, errors="coerce")
    v = pd.to_numeric(vol_s, errors="coerce").fillna(0.0)
    diff = (c - o).to_numpy(dtype=float)
    sgn = np.where(np.isfinite(diff), np.sign(diff), 0.0)
    ofi = pd.Series(sgn, index=c.index) * v
    vol_sum = v.rolling(OFI_WINDOW, min_periods=max(3, OFI_WINDOW // 3)).sum()
    ofi_sum = ofi.rolling(OFI_WINDOW, min_periods=max(3, OFI_WINDOW // 3)).sum()
    ratio = ofi_sum / (vol_sum.abs() + 1e-9)
    return ratio

def _calc_vrp_ratio(close: pd.Series):
    c = pd.to_numeric(close, errors="coerce")
    c = c.where(c > 0)
    lr = np.log(c).diff()
    v_short = lr.rolling(VRP_VOL_SHORT, min_periods=max(3, VRP_VOL_SHORT // 2)).std()
    v_long = lr.rolling(VRP_VOL_LONG, min_periods=max(3, VRP_VOL_LONG // 2)).std()
    return v_short / (v_long + 1e-9)

print("🎯 【山谷狙击选股策略 Pro v7.0】启动")
print("📡 正在获取A股实时行情...")
print("🧠 判断依据: ILLIQ钝化 + 卡尔曼降噪 + OFI脚印 + 波动率状态 + 均线支撑 + 自适应背离 + 筹码净化")

try:
    df_market = ak.stock_zh_a_spot_em()
except Exception as e:
    print(f"❌ 获取行情失败: {e}")
    df_market = pd.DataFrame()

if df_market.empty:
    df = pd.DataFrame()
else:
    df_market = df_market[~df_market["名称"].str.contains("ST|退", na=False)]
    df_market = df_market[abs(df_market["涨跌幅"]) <= MAX_PRICE_CHANGE]

    now = datetime.datetime.now()
    is_early_session = now.hour == 9 and now.minute < 45
    vol_ratio_limit = 2.0 if is_early_session else 1.5

    if "量比" in df_market.columns and not df_market["量比"].isnull().all():
        median_vr = df_market["量比"].median()
        if median_vr > 0.1:
            df_market = df_market[df_market["量比"] < vol_ratio_limit]

    if "成交额" in df_market.columns and not df_market["成交额"].isnull().all():
        df_market = df_market[df_market["成交额"] >= MIN_TURNOVER_AMOUNT]

    if len(df_market) > 300:
        df_market = df_market.sort_values(by="换手率", ascending=True).head(300)

    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=VOLUME_BASE_DAYS + 220)
    start_date_str = start_date.strftime("%Y%m%d")
    end_date_str = end_date.strftime("%Y%m%d")

    results = []
    count = 0

    for _, row in df_market.iterrows():
        count += 1
        symbol = row["代码"]
        name = row["名称"]
        current_price = float(row["最新价"])
        pct_chg = float(row["涨跌幅"])

        if count % 50 == 0:
            print(f"⏳ 进度: {count}/{len(df_market)}...")

        try:
            df_hist = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date_str,
                end_date=end_date_str,
                adjust="qfq",
            )
            if df_hist is None or df_hist.empty:
                continue

            required_cols = {"收盘", "成交量", "开盘"}
            if not required_cols.issubset(set(df_hist.columns)):
                if len(df_hist.columns) >= 11:
                    df_hist.columns = ["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率"]
                else:
                    continue

            df_hist["开盘"] = pd.to_numeric(df_hist["开盘"], errors="coerce")
            df_hist["收盘"] = pd.to_numeric(df_hist["收盘"], errors="coerce")
            df_hist["成交量"] = pd.to_numeric(df_hist["成交量"], errors="coerce")
            if "成交额" in df_hist.columns:
                df_hist["成交额"] = pd.to_numeric(df_hist["成交额"], errors="coerce")
            df_hist = df_hist.dropna(subset=["开盘", "收盘", "成交量"])
            if len(df_hist) < (VOLUME_BASE_DAYS + 10):
                continue

            open_ = df_hist["开盘"].reset_index(drop=True)
            close = df_hist["收盘"].reset_index(drop=True)
            volume = df_hist["成交量"].reset_index(drop=True)
            amount = (
                df_hist["成交额"].reset_index(drop=True)
                if ("成交额" in df_hist.columns and not df_hist["成交额"].isnull().all())
                else (close * volume)
            )

            close_smooth = _kalman_filter_1d(close)

            recent_vol = float(volume.iloc[-RECENT_VOLUME_DAYS:].median())
            hist_volume_window = volume.iloc[-VOLUME_BASE_DAYS:]
            hist_vol = float(hist_volume_window.median()) if not hist_volume_window.empty else np.nan
            vol_ratio = (recent_vol / hist_vol) if hist_vol and hist_vol > 0 else 1.0
            vol_q = _volume_quantile(hist_volume_window, recent_vol)

            vol_score_ratio = 0
            if vol_ratio < VOL_SHRINK_LEVEL_1:
                vol_score_ratio = SCORE_CRITERIA["volume_extreme"]
            elif vol_ratio < VOL_SHRINK_LEVEL_2:
                vol_score_ratio = SCORE_CRITERIA["volume_high"]
            elif vol_ratio < VOL_SHRINK_LEVEL_3:
                vol_score_ratio = SCORE_CRITERIA["volume_med"]

            vol_score_q = 0
            if vol_q is not None:
                if vol_q <= VOL_QUANTILE_LEVEL_1:
                    vol_score_q = SCORE_CRITERIA["volume_extreme"]
                elif vol_q <= VOL_QUANTILE_LEVEL_2:
                    vol_score_q = SCORE_CRITERIA["volume_high"]
                elif vol_q <= VOL_QUANTILE_LEVEL_3:
                    vol_score_q = SCORE_CRITERIA["volume_med"]

            vol_score = max(vol_score_ratio, vol_score_q)
            illiq_rolling, illiq_ratio = _calc_illiq(close, amount)
            illiq_last = float(illiq_rolling.iloc[-1]) if pd.notna(illiq_rolling.iloc[-1]) else np.nan
            illiq_ratio_last = float(illiq_ratio.iloc[-1]) if pd.notna(illiq_ratio.iloc[-1]) else np.nan
            illiq_ok = bool(pd.notna(illiq_ratio_last) and (illiq_ratio_last <= ILLIQ_BOTTOM_RATIO_MAX))
            if vol_score == 0 and (not illiq_ok):
                continue

            ma5 = close_smooth.rolling(window=5).mean()
            ma60 = close_smooth.rolling(window=60).mean()
            ma120 = close_smooth.rolling(window=120).mean()

            score = 0
            signals = []

            if vol_score > 0:
                if vol_score == SCORE_CRITERIA["volume_extreme"]:
                    score += vol_score
                    signals.append("极缩量")
                elif vol_score == SCORE_CRITERIA["volume_high"]:
                    score += vol_score
                    signals.append("缩量")
                elif vol_score == SCORE_CRITERIA["volume_med"]:
                    score += vol_score

            if illiq_ok:
                score += SCORE_CRITERIA["illiq_absorb"]
                signals.append("ILLIQ钝化")

            ma60_last = float(ma60.iloc[-1]) if pd.notna(ma60.iloc[-1]) else None
            if ma60_last and ma60_last > 0:
                dist_ma60 = (current_price - ma60_last) / ma60_last
                if -MA_SUPPORT_RANGE < dist_ma60 < MA_REBOUND_RANGE:
                    score += SCORE_CRITERIA["ma60_support"]
                    signals.append("MA60撑")

            ma120_last = float(ma120.iloc[-1]) if pd.notna(ma120.iloc[-1]) else None
            if ma120_last and ma120_last > 0:
                dist_ma120 = (current_price - ma120_last) / ma120_last
                if -MA_SUPPORT_RANGE < dist_ma120 < MA_REBOUND_RANGE:
                    score += SCORE_CRITERIA["ma120_support"]
                    signals.append("MA120撑")

            if _has_overhead_supply(open_, close, volume, current_price):
                score -= SCORE_CRITERIA["chip_overhang_penalty"]
                signals.append("套牢")

            above_ma5 = True
            for i in range(1, REBOUND_STABLE_DAYS + 1):
                if not (pd.notna(ma5.iloc[-i]) and close_smooth.iloc[-i] > ma5.iloc[-i]):
                    above_ma5 = False
                    break
            ma5_up = pd.notna(ma5.iloc[-1]) and pd.notna(ma5.iloc[-2]) and ma5.iloc[-1] > ma5.iloc[-2]
            is_rebound = above_ma5 and ma5_up and (pct_chg > REBOUND_RISE_THRESHOLD)
            if is_rebound:
                score += SCORE_CRITERIA["rebound_confirm"]
                signals.append("⚡️启动")

            macd_line, _, _ = calculate_macd(close_smooth)
            if detect_bullish_divergence(close_smooth, macd_line):
                score += SCORE_CRITERIA["macd_div"]
                signals.append("MACD底")

            rsi = calculate_rsi(close_smooth, period=14)
            if detect_bullish_divergence(close_smooth, rsi):
                score += SCORE_CRITERIA["rsi_div"]
                signals.append("RSI底")

            current_rsi = float(rsi.iloc[-1]) if len(rsi) else np.nan
            ofi_ratio = _calc_ofi_ratio(open_, close, volume)
            ofi_ratio_last = float(ofi_ratio.iloc[-1]) if pd.notna(ofi_ratio.iloc[-1]) else np.nan
            ofi_ok = bool(pd.notna(ofi_ratio_last) and (ofi_ratio_last >= OFI_RATIO_MIN))
            if ofi_ok:
                score += SCORE_CRITERIA["ofi_confirm"]
                signals.append("OFI+")

            vrp_ratio = _calc_vrp_ratio(close)
            vrp_ratio_last = float(vrp_ratio.iloc[-1]) if pd.notna(vrp_ratio.iloc[-1]) else np.nan
            vrp_stress = bool(pd.notna(vrp_ratio_last) and (vrp_ratio_last >= VRP_STRESS_RATIO))
            if vrp_stress and illiq_ok:
                score += SCORE_CRITERIA["vrp_stress_absorb"]
                signals.append("VRP压制")

            if score >= THRESHOLD_POTENTIAL:
                results.append(
                    {
                        "代码": symbol,
                        "名称": name,
                        "现价": round(current_price, 2),
                        "涨跌%": round(pct_chg, 2),
                        "评分": int(score),
                        "缩量比": round(float(vol_ratio), 2),
                        "缩量分位": round(float(vol_q), 2) if vol_q is not None else None,
                        "ILLIQ": round(float(illiq_last), 3) if not np.isnan(illiq_last) else None,
                        "ILLIQ比": round(float(illiq_ratio_last), 2) if not np.isnan(illiq_ratio_last) else None,
                        "OFI": round(float(ofi_ratio_last), 2) if not np.isnan(ofi_ratio_last) else None,
                        "VRP": round(float(vrp_ratio_last), 2) if not np.isnan(vrp_ratio_last) else None,
                        "RSI": round(current_rsi, 1) if not np.isnan(current_rsi) else None,
                        "信号": "+".join(signals),
                    }
                )

        except KeyError:
            continue
        except IndexError:
            continue
        except Exception as e:
            print(f"❌ 股票{symbol}分析异常: {str(e)[:80]}")
            continue

    if results:
        df_res = pd.DataFrame(results).sort_values(by="评分", ascending=False)
        df_high = df_res[df_res["评分"] >= THRESHOLD_HIGH_QUALITY]
        df_pot = df_res[(df_res["评分"] >= THRESHOLD_POTENTIAL) & (df_res["评分"] < THRESHOLD_HIGH_QUALITY)]

        print("\n" + "=" * 50)
        print(f"🌟 【严选榜】 (评分>={THRESHOLD_HIGH_QUALITY})")
        print("=" * 50)
        if not df_high.empty:
            cols = ["代码", "名称", "现价", "涨跌%", "评分", "缩量比", "缩量分位", "ILLIQ比", "OFI", "VRP", "信号"]
            cols = [c for c in cols if c in df_high.columns]
            print(df_high[cols].to_string(index=False))
        else:
            print("（暂无符合严选标准的股票）")

        print("\n" + "-" * 50)
        print(f"👀 【潜力榜】 (评分>={THRESHOLD_POTENTIAL})")
        print("-" * 50)
        if not df_pot.empty:
            cols = ["代码", "名称", "现价", "涨跌%", "评分", "缩量比", "缩量分位", "ILLIQ比", "OFI", "VRP", "信号"]
            cols = [c for c in cols if c in df_pot.columns]
            print(df_pot[cols].head(20).to_string(index=False))
            if len(df_pot) > 20:
                print(f"... 以及其他 {len(df_pot) - 20} 只")
        else:
            print("（暂无符合潜力标准的股票）")

        df = df_res
    else:
        print("\n⚠️ 未发现符合条件的股票")
        df = pd.DataFrame(columns=["代码", "名称", "现价", "涨跌%", "评分", "缩量比", "缩量分位", "ILLIQ", "ILLIQ比", "OFI", "VRP", "RSI", "信号"])
