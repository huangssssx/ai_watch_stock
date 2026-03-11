import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backend'))

from utils.tushare_client import get_chip_performance

# 测试获取许继电器的筹码数据
code = "000400.SZ"
trade_date = "20260311"

print(f"获取 {code} 的筹码数据...")
df = get_chip_performance(code, trade_date)

if df is not None and not df.empty:
    print(f"筹码数据获取成功:")
    print(df)
    
    # 计算筹码集中度
    row = df.iloc[0]
    cost_5pct = float(row.get("cost_5pct", 0.0) or 0.0)
    cost_95pct = float(row.get("cost_95pct", 0.0) or 0.0)
    weight_avg = float(row.get("weight_avg", 0.0) or 0.0)
    winner_rate = float(row.get("winner_rate", 0.0) or 0.0)
    
    print(f"\n筹码分析:")
    print(f"5%分位成本: {cost_5pct}")
    print(f"95%分位成本: {cost_95pct}")
    print(f"加权平均成本: {weight_avg}")
    print(f"胜率: {winner_rate}%")
    
    if weight_avg > 0:
        chip_concentration = ((cost_95pct - cost_5pct) / weight_avg * 100.0)
        print(f"筹码集中度: {chip_concentration:.2f}%")
        
        if chip_concentration <= 15.0:
            print("✓ 筹码高度集中（≤15%）")
        else:
            print("✗ 筹码不够集中（>15%）")
            
        if winner_rate >= 40.0:
            print("✓ 胜率较高（≥40%）")
        else:
            print("✗ 胜率较低（<40%）")
else:
    print("筹码数据获取失败")