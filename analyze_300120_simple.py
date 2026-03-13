import sys
import os
sys.path.insert(0, '/Users/huangchuanjian/workspace/my_projects/ai_watch_stock')

from utils.pytdx_client import tdx, connected_endpoint
import pandas as pd
from datetime import datetime

# 连接TDX
print(f"{datetime.now().strftime('%Y%m%d_%H%M%S')} 连接TDX...")
connected_endpoint()

# 获取日线数据
def _fetch_daily_bars(market, code, count):
    try:
        bars = tdx.get_security_bars(9, market, code.zfill(6), 0, count)
    except Exception as e:
        return None, f"exception: {type(e).__name__}"
    if not bars:
        return None, "empty"
    df = tdx.to_df(bars) if bars else pd.DataFrame()
    return df, "ok"

# 获取日线
df_daily, _ = _fetch_daily_bars(0, "300120", 200)
if df_daily is not None:
    df_daily['datetime'] = pd.to_datetime(df_daily['datetime'])
    df_daily['trade_date'] = df_daily['datetime'].dt.strftime('%Y%m%d')
    print(f"\n=== 日线分析 ===")
    print(f"最新收盘价: {df_daily['close'].iloc[-1]:.2f}")
    print(f"60日均线: {df_daily['close'].rolling(60).mean().iloc[-1]:.2f}")
    print(f"120日均线: {df_daily['close'].rolling(120).mean().iloc[-1]:.2f}")
    print(f"20日均线斜率: {(df_daily['close'].rolling(20).mean().iloc[-1] - df_daily['close'].rolling(20).mean().iloc[-2]) / df_daily['close'].rolling(20).mean().iloc[-2]:.2%}")
    
    # 分析形态
    recent_low = df_daily['low'].tail(20).min()
    recent_high = df_daily['high'].tail(20).max()
    print(f"\n近期区间: {recent_low:.2f} - {recent_high:.2f}")
    print(f"当前位置: {(df_daily['close'].iloc[-1] - recent_low) / (recent_high - recent_low):.2%}")

# 获取月线
df_monthly, _ = _fetch_daily_bars(0, "300120", 24)
if df_monthly is not None:
    print(f"\n=== 月线分析 ===")
    print(f"最新月收盘价: {df_monthly['close'].iloc[-1]:.2f}")
    print(f"6月均线: {df_monthly['close'].rolling(6).mean().iloc[-1]:.2f}")
    print(f"12月均线: {df_monthly['close'].rolling(12).mean().iloc[-1]:.2f}")
    
    # 月线形态
    print(f"\n月线趋势: {'上升' if df_monthly['close'].rolling(6).mean().iloc[-1] > df_monthly['close'].rolling(6).mean().iloc[-6] else '下降'}")