import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backend'))

from utils.tushare_client import get_chip_performance

# 测试获取筹码数据
code = "002300.SZ"
trade_date = "20240311"

print(f"获取 {code} 的筹码数据...")
df = get_chip_performance(code, trade_date)

if df is not None and not df.empty:
    print(f"筹码数据获取成功:")
    print(df)
else:
    print("筹码数据获取失败")