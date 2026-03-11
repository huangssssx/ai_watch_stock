import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backend'))

from utils.pytdx_client import tdx
import pandas as pd

def analyze_market_pattern(trade_date, asof_time):
    """
    分析大盘形态
    """
    market = 1  # 上证
    code = "000001"
    
    # 获取大盘数据
    data = tdx.get_index_bars(8, market, code, 200, 200)
    if not data:
        return None
    
    df = pd.DataFrame(data)
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    df = df.dropna(subset=['datetime'])
    
    # 筛选指定日期的数据
    df_filtered = df[df['datetime'].dt.strftime('%Y%m%d') == trade_date]
    if df_filtered.empty:
        return None
    
    # 计算关键指标
    open_start = pd.to_datetime(trade_date, format="%Y%m%d") + pd.Timedelta(hours=9, minutes=30)
    first30_end = pd.to_datetime(trade_date, format="%Y%m%d") + pd.Timedelta(hours=10, minutes=0)
    
    first30 = df_filtered[df_filtered["datetime"] < first30_end]
    
    if first30.empty or len(first30) < 25:
        return {
            "pattern": "数据不足",
            "reason": "前30分钟数据不足25条",
            "open_px": 0,
            "low30": 0,
            "low30_pct": 0,
            "last_close": 0,
            "rebound_pct": 0,
            "cross_open": False,
            "conditions": ["数据不足"]
        }
    
    open_px = float(df_filtered["open"].iloc[0] or 0.0)
    low30 = float(pd.to_numeric(first30.get("low", pd.Series(dtype=float)), errors="coerce").min() or float("nan"))
    if not (low30 > 0):
        low30 = float(pd.to_numeric(first30["close"], errors="coerce").min() or float("nan"))
    
    last_close = float(df_filtered["close"].iloc[-1] or 0.0)
    
    # 计算指标
    low30_pct = (low30 - open_px) / open_px * 100.0
    rebound_pct = ((last_close - low30) / low30 * 100.0) if low30 > 0 and last_close > 0 else 0.0
    cross_open = last_close >= open_px
    
    # 判断大盘形态
    pattern = ""
    conditions = []
    
    if abs(low30_pct) < 0.3:
        pattern = "震荡/上涨"
        conditions.append(f"跌幅不足({low30_pct:.2f}% < 0.3%)")
    elif not cross_open:
        pattern = "下跌未反弹"
        conditions.append(f"未站上开盘价")
    elif rebound_pct < 0.1:
        pattern = "微弱反弹"
        conditions.append(f"反弹不足({rebound_pct:.2f}% < 0.1%)")
    else:
        pattern = "V型反转"
        conditions.append(f"跌幅{low30_pct:.2f}%，反弹{rebound_pct:.2f}%")
    
    return {
        "trade_date": trade_date,
        "asof_time": asof_time,
        "pattern": pattern,
        "open_px": open_px,
        "low30": low30,
        "low30_pct": low30_pct,
        "last_close": last_close,
        "rebound_pct": rebound_pct,
        "cross_open": cross_open,
        "conditions": conditions,
    }

# 测试更多日期的大盘形态
test_cases = [
    ("20260311", "10:25"),
    ("20260310", "10:25"),
    ("20260309", "10:25"),
    ("20260308", "10:25"),
    ("20260307", "10:25"),
    ("20260306", "10:25"),
    ("20260305", "10:25"),
]

v_reverse_count = 0
total_count = 0

for trade_date, asof_time in test_cases:
    print(f"\n{'='*60}")
    print(f"分析 {trade_date} {asof_time} 的大盘形态...")
    
    result = analyze_market_pattern(trade_date, asof_time)
    if result:
        if 'pattern' not in result:
            print(f"无法分析大盘形态: {result.get('reason', '未知原因')}")
            continue
            
        total_count += 1
        print(f"大盘形态: {result['pattern']}")
        print(f"开盘价: {result['open_px']}")
        print(f"前30分钟最低价: {result['low30']}")
        print(f"前30分钟跌幅: {result['low30_pct']:.2f}%")
        print(f"最新收盘价: {result['last_close']}")
        print(f"反弹幅度: {result['rebound_pct']:.2f}%")
        print(f"是否站上开盘价: {result['cross_open']}")
        print(f"条件说明: {', '.join(result['conditions'])}")
        
        # 判断大盘同步策略是否适用
        if result['pattern'] == "V型反转":
            print(f"✓ 大盘同步策略适用")
            v_reverse_count += 1
        else:
            print(f"✗ 大盘同步策略不适用")
    else:
        print("无法分析大盘形态")

print(f"\n{'='*60}")
print(f"统计结果:")
print(f"总测试天数: {total_count}")
print(f"V型反转天数: {v_reverse_count}")
print(f"V型反转比例: {v_reverse_count/total_count*100:.1f}%" if total_count > 0 else "N/A")