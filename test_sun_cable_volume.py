#!/usr/bin/env python3
"""检查太阳电缆在10:09时的成交量条件"""

import sys
sys.path.insert(0, "/Users/huangchuanjian/workspace/my_projects/ai_watch_stock")

import pandas as pd
from datetime import datetime
from backend.utils.pytdx_client import tdx

code = "002300"
market = 0
trade_date = "20260311"

print(f"=== 检查太阳电缆 {trade_date} 10:09 的成交量条件 ===\n")

# 获取5分钟K线
bars = tdx.get_security_bars(8, market, code, 0, 500)
if not bars:
    print("获取K线失败")
    sys.exit(1)

df = pd.DataFrame(bars)
df['datetime'] = pd.to_datetime(df['datetime'])
df_today = df[df['datetime'].dt.strftime('%Y%m%d') == trade_date].copy()
df_today = df_today.sort_values('datetime')

# 获取开盘价（从行情快照）
quotes = tdx.get_security_quotes([(market, code)])
if quotes:
    open_px = quotes[0]['open']
    print(f"开盘价（行情快照）: {open_px}")
else:
    open_px = df_today['open'].iloc[0]
    print(f"开盘价（K线）: {open_px}")

# 截止到10:09
cutoff_time = datetime.strptime(f"{trade_date} 10:09", "%Y%m%d %H:%M")
df_1m = df_today[df_today['datetime'] <= cutoff_time].copy()

# 检查前30分钟数据
first30_end = datetime.strptime(f"{trade_date} 10:00", "%Y%m%d %H:%M")
first30 = df_1m[df_1m['datetime'] < first30_end].copy()

# 计算前30分钟最低价和10:00收盘价
low30 = float(first30['low'].min())
close30 = float(first30['close'].iloc[-1])
low30_pct = (low30 - open_px) / open_px * 100.0
close30_pct = (close30 - open_px) / open_px * 100.0

print(f"\n前30分钟最低价: {low30}, 跌幅: {low30_pct:.2f}%")
print(f"10:00收盘价: {close30}, 跌幅: {close30_pct:.2f}%")

# 检查站上开盘价条件
cross_above_open_pct = 0.1
cross_line = open_px * (1.0 + cross_above_open_pct / 100.0)
print(f"\n站上开盘价阈值: {cross_line:.2f}")

post = df_1m[df_1m['datetime'] >= first30_end].copy()

cross_mask = post['close'] >= cross_line
if cross_mask.any():
    cross_pos = cross_mask.idxmax()
    cross_time = post.loc[cross_pos, 'datetime']
    
    # 检查站稳条件
    after_cross = post[post['datetime'] >= cross_time].copy()
    
    hold_tolerance_pct = 0.05
    hold_line = open_px * (1.0 - hold_tolerance_pct / 100.0)
    
    hold_mask = after_cross['close'] >= hold_line
    if hold_mask.any():
        last_hold_time = after_cross.loc[hold_mask, 'datetime'].max()
        hold_duration = int((last_hold_time - cross_time).total_seconds() / 60.0)
        
        print(f"✅ 站上开盘价时间: {cross_time.strftime('%H:%M')}")
        print(f"✅ 站稳持续时间: {hold_duration}分钟")
        
        # 检查成交量条件
        if 'vol' in df_1m.columns:
            first30_vol = float(pd.to_numeric(first30["vol"], errors="coerce").mean() or 0.0)
            after_cross_vol = float(pd.to_numeric(after_cross["vol"], errors="coerce").mean() or 0.0)
            volume_ratio = after_cross_vol / first30_vol if first30_vol > 0 else 0.0
            
            print(f"\n=== 成交量条件检查 ===")
            print(f"前30分钟平均成交量: {first30_vol:.0f}")
            print(f"站上后平均成交量: {after_cross_vol:.0f}")
            print(f"成交量比: {volume_ratio:.2f}")
            
            min_volume_ratio = 2.0
            if volume_ratio >= min_volume_ratio:
                print(f"✅ 成交量比满足要求 (>= {min_volume_ratio})")
            else:
                print(f"❌ 成交量比不足 (需要 >= {min_volume_ratio})")
                print(f"   这就是太阳电缆未被检测到的原因！")
        else:
            print(f"\n❌ K线数据中没有成交量信息")
    else:
        print(f"❌ 未站稳")
else:
    print(f"❌ 未站上开盘价")
