import akshare as ak
import pandas as pd
import numpy as np
import talib
from scipy.signal import argrelextrema
import datetime
import warnings
import argparse
import random

warnings.filterwarnings('ignore')

# --- 共通辅助函数 ---

def _kalman_filter_1d(values: pd.Series, q: float = 1e-5, r_scale: float = 0.20):
    v = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    if v.size == 0: return values
    first_finite_idx = int(np.argmax(np.isfinite(v))) if np.isfinite(v).any() else None
    if first_finite_idx is None: return values
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

# ==========================================
# 策略 A: 旧版 (Original)
# ==========================================
class StrategyOld:
    @staticmethod
    def run(df_hist: pd.DataFrame) -> int:
        # 参数
        RECENT_VOLUME_DAYS = 5
        VOLUME_BASE_DAYS = 120
        DIVERGENCE_LOOKBACK = 30
        
        if df_hist is None or len(df_hist) < 120:
            return 0
        if len(df_hist) > 260:
            df_hist = df_hist.iloc[-260:]
        
        close = df_hist["收盘"]
        volume = df_hist["成交量"]
        open_ = df_hist["开盘"] if "开盘" in df_hist.columns else None
        
        # 1. 缩量检测 (简单比率 + 排名)
        recent_vol = volume.iloc[-RECENT_VOLUME_DAYS:].median()
        hist_vol_window = volume.iloc[-VOLUME_BASE_DAYS:]
        hist_vol_med = hist_vol_window.median()
        
        vol_ratio = recent_vol / hist_vol_med
        vol_q = (hist_vol_window <= recent_vol).mean()
        
        score = 0
        
        if vol_ratio < 0.6 or vol_q < 0.25:
            score += 2
        
        # 2. 均线支撑 (MA60)
        ma60 = close.rolling(60).mean()
        curr_price = close.iloc[-1]
        if ma60.iloc[-1] > 0:
            dist = (curr_price - ma60.iloc[-1]) / ma60.iloc[-1]
            if -0.05 < dist < 0.08:
                score += 2
                
        # 3. MACD 背离 (简单窗口查找)
        # 简化版逻辑：寻找最近30天内的最低价，如果价格创新低但MACD没创新低
        smooth_p = _kalman_filter_1d(close)
        macd, _, _ = talib.MACD(smooth_p.values)
        
        window = df_hist.iloc[-DIVERGENCE_LOOKBACK:]
        if len(window) > 10:
            # 使用原代码逻辑的简化复刻：
            # 找两个低点
            lows = argrelextrema(smooth_p.values[-DIVERGENCE_LOOKBACK:], np.less, order=3)[0]
            if len(lows) >= 2:
                l1, l2 = lows[-2], lows[-1]
                p1 = smooth_p.values[-DIVERGENCE_LOOKBACK:][l1]
                p2 = smooth_p.values[-DIVERGENCE_LOOKBACK:][l2]
                m1 = macd[len(close)-DIVERGENCE_LOOKBACK+l1]
                m2 = macd[len(close)-DIVERGENCE_LOOKBACK+l2]
                
                if p2 <= p1 and m2 > m1:
                    score += 3

        # 4. 启动信号
        if open_ is not None:
            ma5 = close.rolling(5).mean()
            if close.iloc[-1] > ma5.iloc[-1] and ma5.iloc[-1] > ma5.iloc[-2]:
                score += 2
            
        return score

# ==========================================
# 策略 B: 新版 (Optimized)
# ==========================================
class StrategyNew:
    @staticmethod
    def run(df_hist: pd.DataFrame, mkt_cap: float = 100e8) -> int:
        # 参数
        BB_WINDOW = 5
        
        if df_hist is None or len(df_hist) < 252:
            return 0
        if len(df_hist) > 420:
            df_hist = df_hist.iloc[-420:]
        
        close = df_hist["收盘"]
        open_ = df_hist["开盘"]
        high = df_hist["最高"]
        low = df_hist["最低"]
        volume = df_hist["成交量"]
        amount = df_hist["成交额"]
        curr_price = close.iloc[-1]
        
        score = 0
        
        # 1. 半山腰规避 (STH-CB)
        # STH-CB: 20日 VWAP
        vol_20 = volume.rolling(20).sum()
        amt_20 = amount.rolling(20).sum()
        vwap_20 = amt_20 / (vol_20 + 1e-9)
        
        current_vwap = vwap_20.iloc[-1]
        prev_vwap = vwap_20.iloc[-2]
        
        # 规则: 价格 > VWAP 或 VWAP 拐头向上
        vwap_slope = current_vwap - prev_vwap
        is_above_cost = (curr_price > current_vwap) or (vwap_slope > 0)
        
        # 规则: 回撤深度
        high_52w = close.rolling(252).max().iloc[-1]
        drawdown = (high_52w - curr_price) / high_52w
        is_deep_enough = drawdown > 0.20
        
        if not (is_above_cost and is_deep_enough):
            return 0 # 直接否决
            
        # 2. 市值分层缩量
        threshold = 0.15
        if mkt_cap > 500e8: threshold = 0.25
        elif mkt_cap < 100e8: threshold = 0.10
        
        curr_vol = volume.iloc[-5:].median()
        hist_vol = volume.iloc[-120:]
        vol_rank = (hist_vol <= curr_vol).mean()
        
        if vol_rank < threshold: score += 3
        elif vol_rank < 0.40: score += 1
        
        # 3. 动态背离 (Nowcasting)
        smooth_p = _kalman_filter_1d(close)
        macd, _, _ = talib.MACD(smooth_p.values)
        
        # BB 算法找低点
        lows = argrelextrema(smooth_p.values, np.less, order=BB_WINDOW)[0]
        if len(lows) >= 2:
            last_idx = lows[-1]
            prev_idx = lows[-2]
            
            # 时效性检查
            if (len(smooth_p) - 1) - last_idx <= 15:
                p_last = smooth_p.iloc[last_idx]
                p_prev = smooth_p.iloc[prev_idx]
                m_last = macd[last_idx]
                m_prev = macd[prev_idx]
                
                if p_last <= p_prev * 1.02 and m_last > m_prev * 1.05:
                     # 加速度
                    acc = (smooth_p.diff().diff()).iloc[last_idx]
                    if acc > 0:
                        score += 3
        
        # 4. 复合 ILLIQ
        rets = close.pct_change().abs()
        amihud = rets / (amount + 1e-9) * 1e8
        hl_ratio = (high - low) / (close + 1e-9)
        
        curr_amihud = amihud.iloc[-20:].mean()
        curr_hl = hl_ratio.iloc[-20:].mean()
        
        hist_amihud = amihud.iloc[-120:]
        hist_hl = hl_ratio.iloc[-120:]
        
        amihud_rank = (hist_amihud <= curr_amihud).mean()
        hl_rank = (hist_hl <= curr_hl).mean()
        composite = (amihud_rank + hl_rank) / 2
        
        if composite > 0.7: score += 2
        
        # 5. VRP
        rets_raw = close.pct_change()
        rv = rets_raw.rolling(5).std()
        iv_proxy = rets_raw.rolling(20).std()
        vrp = iv_proxy - rv
        curr_vrp = vrp.iloc[-1]
        hist_vrp = vrp.iloc[-120:]
        vrp_rank = (hist_vrp <= curr_vrp).mean()
        
        if vrp_rank > 0.8: score += 2
        
        return score

# ==========================================
# 回测主逻辑
# ==========================================

def _normalize_symbol(code: str) -> str:
    s = "" if code is None else str(code).strip()
    if s.startswith(("sh", "sz", "bj")) and len(s) >= 8:
        return s[2:]
    return s


def _pick_universe(sample_size: int, seed: int, min_amount: float):
    spot = ak.stock_zh_a_spot_em()
    if spot is None or spot.empty:
        return {}
    spot = spot.copy()
    spot = spot[~spot["名称"].str.contains("ST|退", na=False)]
    if "成交额" in spot.columns:
        amt = pd.to_numeric(spot["成交额"], errors="coerce")
        if float(amt.notna().mean()) >= 0.30:
            spot = spot[amt.fillna(0.0) >= float(min_amount)]
    if "流通市值" in spot.columns:
        spot["流通市值"] = pd.to_numeric(spot["流通市值"], errors="coerce")
    spot = spot.dropna(subset=["代码", "名称", "流通市值"])
    if spot.empty:
        return {}
    spot["代码"] = spot["代码"].map(_normalize_symbol)
    spot = spot[spot["代码"].str.len() >= 6]
    spot = spot.sort_values("流通市值").reset_index(drop=True)

    n = int(sample_size)
    if n <= 0:
        return {}
    n = min(n, len(spot))

    random.seed(int(seed))
    thirds = np.array_split(spot, 3)
    sizes = [n // 3, n // 3, n - 2 * (n // 3)]
    chosen = []
    for part, k in zip(thirds, sizes):
        if part.empty or k <= 0:
            continue
        idxs = list(part.index)
        if k >= len(idxs):
            sel = idxs
        else:
            sel = random.sample(idxs, k)
        chosen.append(spot.loc[sel])
    uni = pd.concat(chosen, axis=0).drop_duplicates(subset=["代码"]).reset_index(drop=True)

    targets = {}
    for _, row in uni.iterrows():
        code = str(row["代码"])
        name = str(row["名称"])
        cap = float(row["流通市值"]) if pd.notna(row["流通市值"]) else 100e8
        targets[code] = {"name": name, "mkt_cap": cap}
    return targets


def _calc_forward_metrics(df: pd.DataFrame, i: int, entry_px: float):
    close = df["收盘"].to_numpy(dtype=float)
    n = len(close)
    out = {}
    for h in (5, 10, 20):
        if i + h < n:
            out[f"ret_{h}"] = float((close[i + h] - entry_px) / entry_px)
        else:
            out[f"ret_{h}"] = float("nan")
    if i + 10 < n:
        window = close[i : i + 11]
        out["mae_10"] = float((np.nanmin(window) - entry_px) / entry_px)
        out["mfe_10"] = float((np.nanmax(window) - entry_px) / entry_px)
    else:
        out["mae_10"] = float("nan")
        out["mfe_10"] = float("nan")

    rec = None
    max_days = 60
    for d in range(1, max_days + 1):
        if i + d >= n:
            break
        if close[i + d] >= entry_px:
            rec = d
            break
    out["recovery_days_60"] = rec
    return out


def run_backtest(sample_size: int, seed: int, test_days: int, min_amount: float, cooldown_days: int):
    targets = _pick_universe(sample_size=sample_size, seed=seed, min_amount=min_amount)

    print("🚀 开始回测 (对比 旧版 vs 优化版)...")
    print(f"🎯 股票样本: {len(targets)} 只 (分层抽样)  seed={seed}")
    print(f"📅 回测区间: 最近 {test_days} 个交易日  冷却期={cooldown_days}天")
    print("-" * 60)

    results_old = []
    results_new = []

    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=max(900, int(test_days) * 3))
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")

    for idx_t, (code, meta) in enumerate(targets.items(), start=1):
        name = meta["name"]
        mkt_cap = float(meta.get("mkt_cap", 100e8))
        if idx_t % 20 == 0:
            print(f"⏳ 进度: {idx_t}/{len(targets)}")
        try:
            df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_str, end_date=end_str, adjust="qfq")
            if df is None or df.empty or len(df) < 320:
                continue

            for c in ("开盘", "收盘", "最高", "最低", "成交量", "成交额"):
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
            df = df.dropna(subset=["开盘", "收盘", "最高", "最低", "成交量", "成交额"])
            if len(df) < 320:
                continue

            start_i = max(260, len(df) - int(test_days) - 25)
            end_i = len(df) - 21

            prev_old = 0
            prev_new = 0
            cd_old = 0
            cd_new = 0

            for i in range(start_i, end_i):
                hist = df.iloc[: i + 1]

                s_old = StrategyOld.run(hist)
                s_new = StrategyNew.run(hist, mkt_cap)

                if cd_old > 0:
                    cd_old -= 1
                if cd_new > 0:
                    cd_new -= 1

                trigger_old = (prev_old < 4) and (s_old >= 4) and (cd_old == 0)
                trigger_new = (prev_new < 4) and (s_new >= 4) and (cd_new == 0)

                prev_old = s_old
                prev_new = s_new

                entry_i = i + 1
                if entry_i >= len(df):
                    break
                entry_px = float(df.iloc[entry_i]["开盘"])
                if not np.isfinite(entry_px) or entry_px <= 0:
                    continue

                if trigger_old:
                    metrics = _calc_forward_metrics(df, entry_i, entry_px)
                    results_old.append(
                        {
                            "code": code,
                            "name": name,
                            "date": df.iloc[entry_i]["日期"],
                            "score": int(s_old),
                            "entry": entry_px,
                            **metrics,
                        }
                    )
                    cd_old = int(cooldown_days)

                if trigger_new:
                    metrics = _calc_forward_metrics(df, entry_i, entry_px)
                    results_new.append(
                        {
                            "code": code,
                            "name": name,
                            "date": df.iloc[entry_i]["日期"],
                            "score": int(s_new),
                            "entry": entry_px,
                            **metrics,
                        }
                    )
                    cd_new = int(cooldown_days)
        except Exception:
            continue

    # --- 统计分析 ---
    def analyze_results(res_list, name):
        if not res_list:
            print(f"\n{name}: 无信号触发")
            return
            
        df_res = pd.DataFrame(res_list)
        df_res = df_res.replace([np.inf, -np.inf], np.nan)
        count = len(df_res)
        uniq_stocks = int(df_res["code"].nunique()) if "code" in df_res.columns else 0

        def _mean(x):
            x = pd.to_numeric(x, errors="coerce")
            return float(x.mean()) if x.notna().any() else float("nan")

        def _win_rate(x):
            x = pd.to_numeric(x, errors="coerce")
            x = x.dropna()
            if x.empty:
                return float("nan")
            return float((x > 0).mean())

        win_rate_5 = _win_rate(df_res["ret_5"])
        win_rate_10 = _win_rate(df_res["ret_10"])
        win_rate_20 = _win_rate(df_res["ret_20"])
        avg_ret_5 = _mean(df_res["ret_5"])
        avg_ret_10 = _mean(df_res["ret_10"])
        avg_ret_20 = _mean(df_res["ret_20"])

        mae_10 = pd.to_numeric(df_res["mae_10"], errors="coerce")
        false_rate = float((mae_10 <= -0.05).mean()) if mae_10.notna().any() else float("nan")
        worst_mae = float(mae_10.min()) if mae_10.notna().any() else float("nan")

        rec = pd.to_numeric(df_res["recovery_days_60"], errors="coerce")
        rec_ok_rate = float(rec.notna().mean()) if len(rec) else float("nan")
        rec_avg = float(rec.mean()) if rec.notna().any() else float("nan")
        
        print(f"\n📈 {name} 绩效统计:")
        print(f"   信号总数: {count}")
        print(f"   覆盖股票: {uniq_stocks}")
        print(f"   5日胜率:  {win_rate_5:.1%}")
        print(f"   10日胜率: {win_rate_10:.1%}")
        print(f"   20日胜率: {win_rate_20:.1%}")
        print(f"   5日均收:  {avg_ret_5:.2%}")
        print(f"   10日均收: {avg_ret_10:.2%}")
        print(f"   20日均收: {avg_ret_20:.2%}")
        print(f"   假信号率(10日跌破-5%): {false_rate:.1%}")
        print(f"   10日最差回撤(MAE): {worst_mae:.2%}")
        print(f"   60日回本率: {rec_ok_rate:.1%}")
        print(f"   60日平均回本天数: {rec_avg:.1f}")

    print("\n" + "="*60)
    print("🏁 回测结果汇总")
    print("="*60)
    
    analyze_results(results_old, "🔴 旧版策略 (Original)")
    analyze_results(results_new, "🟢 优化版策略 (Optimized)")
    
    # 详细对比示例
    print("\n🔍 优化版信号样例 (前10条):")
    if results_new:
        df_new = pd.DataFrame(results_new).sort_values(["date", "code"]).head(10)
        for _, r in df_new.iterrows():
            print(
                f"   {r['date']} {r['code']} {r['name']}: 评分{int(r['score'])} "
                f"5日{float(r['ret_5']):.2%} 10日{float(r['ret_10']):.2%} 20日{float(r['ret_20']):.2%} "
                f"MAE10{float(r['mae_10']):.2%}"
            )

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--sample-size", type=int, default=120)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--test-days", type=int, default=250)
    p.add_argument("--min-amount", type=float, default=50_000_000)
    p.add_argument("--cooldown-days", type=int, default=10)
    args = p.parse_args()
    run_backtest(
        sample_size=args.sample_size,
        seed=args.seed,
        test_days=args.test_days,
        min_amount=args.min_amount,
        cooldown_days=args.cooldown_days,
    )
