
import akshare as ak
import pandas as pd
import numpy as np
import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter

# ==========================================
# 策策略配置 (与生产环境保持一致)
# ==========================================
MIN_PRICE_RISE = 2.0        
MIN_VOL_RATIO = 1.5         
WASH_DAYS_WINDOW = 10       
WASH_LOW_DAYS_REQ = 5       
TURNOVER_QUANTILE = 0.20    
MAX_SHADOW_RATIO = 0.35     
MAX_OPEN_GAP_PCT = 5.0
MARKET_PANIC_PCT = -1.0
MARKET_INDEX_SYMBOL = "sh000300"
MARKET_FAST_MA = 20
MARKET_SLOW_MA = 60
MARKET_SLOPE_DAYS = 5
UNIVERSE_MODE = "top_turnover"
UNIVERSE_SIZE = 1000
MAX_WORKERS = 8

def build_market_ok_map(start_date: str, end_date: str, index_symbol: str = MARKET_INDEX_SYMBOL):
    try:
        df = ak.stock_zh_index_daily_em(symbol=index_symbol)
    except Exception:
        return {}
    if df is None or df.empty:
        return {}
    if "date" not in df.columns or "close" not in df.columns:
        return {}

    df = df.copy()
    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date_dt", "close"]).sort_values("date_dt").reset_index(drop=True)

    start_dt = pd.to_datetime(start_date, format="%Y%m%d", errors="coerce")
    end_dt = pd.to_datetime(end_date, format="%Y%m%d", errors="coerce")
    if pd.notna(start_dt):
        df = df[df["date_dt"] >= start_dt]
    if pd.notna(end_dt):
        df = df[df["date_dt"] <= end_dt]
    if df.empty:
        return {}

    df["ma_fast_prev"] = df["close"].rolling(MARKET_FAST_MA).mean().shift(1)
    df["ma_slow_prev"] = df["close"].rolling(MARKET_SLOW_MA).mean().shift(1)
    df["ma_fast_slope"] = df["ma_fast_prev"] - df["ma_fast_prev"].shift(MARKET_SLOPE_DAYS)
    df["pct_change"] = df["close"].pct_change() * 100

    df["market_ok"] = (
        (df["close"] > df["ma_fast_prev"])
        & (df["ma_fast_prev"] > df["ma_slow_prev"])
        & (df["ma_fast_slope"] > 0)
        & (df["pct_change"] > MARKET_PANIC_PCT)
    )
    df["date_str"] = df["date_dt"].dt.strftime("%Y-%m-%d")
    return dict(zip(df["date_str"].astype(str).tolist(), df["market_ok"].astype(bool).tolist()))

def calculate_signals(df):
    """
    向量化计算信号 (比循环快得多)
    """
    df = df.copy()
    
    # 1. 指标计算
    df['pct_chg'] = df['涨跌幅']
    df['turnover'] = df['换手率']
    df['ma5'] = df['收盘'].rolling(5).mean()
    df['ma20'] = df['收盘'].rolling(20).mean()
    df['ma60'] = df['收盘'].rolling(60).mean()
    df['vol_ma5'] = df['成交量'].rolling(5).mean()
    
    # 2. 逻辑判定
    
    # A. 趋势共振 (Trend Alignment)
    # MA60 向上: 今日 MA60 > 5日前 MA60
    df['ma60_slope_up'] = df['ma60'] > df['ma60'].shift(5)
    # 价格位于长期均线之上
    df['is_trend_up'] = (df['收盘'] > df['ma60']) & df['ma60_slope_up']
    
    # B. 洗盘检测 (Wash Context)
    # 滚动计算过去60天的换手率分位数
    df['turnover_threshold'] = df['turnover'].rolling(60).quantile(TURNOVER_QUANTILE)
    df['is_low_turnover'] = df['turnover'] < df['turnover_threshold']
    
    # 检查过去 N 天 (不含今日) 的低换手天数
    # shift(1) 是为了不看今日
    df['wash_days'] = df['is_low_turnover'].shift(1).rolling(WASH_DAYS_WINDOW).sum()
    df['is_wash_context'] = df['wash_days'] >= WASH_LOW_DAYS_REQ
    
    # C. 爆发信号 (Trigger)
    # 量能激增: 今日成交量 > 昨日计算出的 MA5_Vol * 1.5
    # vol_ma5 的 shift(1) 就是昨日的 MA5 (包含昨日及之前4天)
    df['vol_ma5_yesterday'] = df['vol_ma5'].shift(1)
    df['is_vol_spike'] = df['成交量'] > (df['vol_ma5_yesterday'] * MIN_VOL_RATIO)
    
    df['is_price_rise'] = df['pct_chg'] > MIN_PRICE_RISE
    df['is_above_ma20'] = df['收盘'] > df['ma20']
    df['prev_close'] = df['收盘'].shift(1)
    df['open_pct_change'] = np.where(
        df['prev_close'] > 0,
        (df['开盘'] - df['prev_close']) / df['prev_close'] * 100,
        np.nan,
    )
    df['is_safe_open'] = df['open_pct_change'] < MAX_OPEN_GAP_PCT
    
    # D. 形态优化 (Shadow Ratio)
    df['high_low_range'] = df['最高'] - df['最低']
    df['upper_shadow'] = df['最高'] - df['收盘']
    # 避免除以零
    df['shadow_ratio'] = np.where(
        df['high_low_range'] > 0, 
        df['upper_shadow'] / df['high_low_range'], 
        0
    )
    df['is_solid_close'] = df['shadow_ratio'] < MAX_SHADOW_RATIO
    
    # E. 综合信号
    df['signal'] = (
        df['is_trend_up'] & 
        df['is_wash_context'] & 
        df['is_vol_spike'] & 
        df['is_price_rise'] & 
        df['is_above_ma20'] & 
        df['is_solid_close'] &
        df['is_safe_open']
    )

    if "market_ok" in df.columns:
        df["signal"] = df["signal"] & df["market_ok"].fillna(True)
    
    return df

def process_data(df):
    """单位修正"""
    if df.empty: return df
    last_close = df['收盘'].iloc[-1]
    last_vol = df['成交量'].iloc[-1]
    last_amt = df['成交额'].iloc[-1]
    
    if last_vol > 0 and last_close > 0:
        if (last_amt / last_vol) / last_close >= 80:
            df['成交量'] = df['成交量'] * 100
    return df

def get_universe(mode: str, size: int, seed: int = 42):
    if mode == "sample":
        return [
            "600519", "000858",
            "300750", "601138",
            "002230", "002415",
            "601919", "600030",
            "000063", "300059",
            "603259", "603986",
            "600418", "002049",
            "002931",
            "601127", "600009",
            "000725", "002594",
        ]
    try:
        spot = ak.stock_zh_a_spot_em()
    except Exception:
        return []
    if spot is None or spot.empty:
        return []
    if "代码" not in spot.columns:
        return []
    if "名称" in spot.columns:
        spot = spot[~spot["名称"].astype(str).str.contains("ST|退", na=False)]
    if mode == "top_turnover":
        if "成交额" in spot.columns:
            spot = spot.sort_values("成交额", ascending=False)
        return spot["代码"].astype(str).head(int(size)).tolist()
    if mode == "random":
        rng = np.random.default_rng(int(seed))
        codes = spot["代码"].astype(str).unique().tolist()
        if not codes:
            return []
        k = min(int(size), len(codes))
        return rng.choice(codes, size=k, replace=False).tolist()
    return []

def _backtest_one_symbol(symbol: str, start_date: str, end_date: str, market_ok_map: dict):
    try:
        df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
    except Exception as e:
        return [], "fetch_error", str(e)
    if df is None or df.empty:
        return [], "fetch_empty", None
    if len(df) < 100:
        return [], "too_short", None

    df = process_data(df)
    if market_ok_map:
        df = df.copy()
        df["market_ok"] = df["日期"].astype(str).map(market_ok_map)
    df = calculate_signals(df)
    signals = df[df["signal"]]
    if signals.empty:
        return [], "no_signal", None

    trades = []
    for idx in signals.index:
        if idx + 6 >= len(df):
            continue
        entry_date = df.iloc[idx + 1]["日期"]
        entry_price = df.iloc[idx + 1]["开盘"]
        stop_loss_price = max(df.iloc[idx]["开盘"], df.iloc[idx]["ma20"])
        future_days = df.iloc[idx + 1 : idx + 6]
        exit_price = future_days.iloc[-1]["收盘"]
        exit_reason = "Hold_5d"
        for _, day_row in future_days.iterrows():
            if day_row["最低"] < stop_loss_price:
                exit_price = min(day_row["开盘"], stop_loss_price)
                exit_reason = "StopLoss"
                break
        ret = (exit_price - entry_price) / entry_price
        trades.append(
            {
                "symbol": symbol,
                "signal_date": df.iloc[idx]["日期"],
                "entry_date": entry_date,
                "entry_price": entry_price,
                "stop_loss": stop_loss_price,
                "exit_price": exit_price,
                "exit_reason": exit_reason,
                "return": ret,
            }
        )
    if not trades:
        return [], "no_future_data", None
    return trades, "ok", None

def run_backtest(symbols, start_date, end_date, max_workers: int = MAX_WORKERS):
    print(f"Starting backtest from {start_date} to {end_date}...")
    print(f"Universe size: {len(symbols)} (mode={UNIVERSE_MODE})")

    market_ok_map = build_market_ok_map(start_date=start_date, end_date=end_date)
    status_counter = Counter()
    error_samples = []
    trades = []

    start_ts = time.time()
    with ThreadPoolExecutor(max_workers=int(max_workers)) as executor:
        futures = {
            executor.submit(_backtest_one_symbol, s, start_date, end_date, market_ok_map): s
            for s in symbols
        }
        completed = 0
        total = len(futures)
        for fut in as_completed(futures):
            symbol = futures[fut]
            try:
                t, status, err = fut.result()
            except Exception as e:
                t, status, err = [], "worker_error", str(e)
            status_counter[status] += 1
            if err and len(error_samples) < 10:
                error_samples.append((symbol, status, err))
            if t:
                trades.extend(t)
            completed += 1
            if completed % 20 == 0 or completed == total:
                elapsed = time.time() - start_ts
                print(f"Progress: {completed}/{total} elapsed={elapsed:.1f}s", end="\r")

    print("\n" + "=" * 50)
    print("Status:", dict(status_counter))
    if error_samples:
        print("Error Samples:", error_samples[:5])
    if not trades:
        print("No trades generated.")
        return

    df_trades = pd.DataFrame(trades)
    print(f"Total Trades: {len(df_trades)}")

    win_trades = df_trades[df_trades["return"] > 0]
    loss_trades = df_trades[df_trades["return"] <= 0]

    win_rate = len(win_trades) / len(df_trades)
    avg_win = win_trades["return"].mean() if not win_trades.empty else 0
    avg_loss = loss_trades["return"].mean() if not loss_trades.empty else 0

    print(f"Win Rate: {win_rate:.2%}")
    print(f"Avg Win:  {avg_win:.2%}")
    print(f"Avg Loss: {avg_loss:.2%}")
    print(f"P/L Ratio: {abs(avg_win/avg_loss):.2f}" if avg_loss != 0 else "Inf")

    print("\nSample Trades:")
    print(df_trades[["symbol", "signal_date", "exit_reason", "return"]].tail(10).to_string(index=False))

if __name__ == "__main__":
    end = datetime.datetime.now().strftime("%Y%m%d")
    start = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime("%Y%m%d")
    
    symbols = get_universe(mode=UNIVERSE_MODE, size=UNIVERSE_SIZE)
    run_backtest(symbols, start, end, max_workers=MAX_WORKERS)
