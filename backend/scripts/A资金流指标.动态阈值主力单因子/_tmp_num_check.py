import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../"))

from utils.pytdx_client import connect, DEFAULT_IP, DEFAULT_PORT

tdx = connect(DEFAULT_IP, DEFAULT_PORT)
tdx.__enter__()

test_dates = [
    20260410, 20260306, 20260205, 20260106,
    20251205, 20251031, 20250801, 20250602,
    20250410, 20250210, 20250103,
    20241202, 20241008, 20240801, 20240603,
    20240410, 20240219, 20240102,
    20231201, 20231008, 20230801, 20230602,
    20230410, 20230201, 20230103,
    20221201, 20221010, 20220801,
]

print("日期       逐笔笔数  状态")
print("-" * 40)
for d in test_dates:
    txs = tdx.get_history_transaction_data(market=0, code="000001", start=0, count=10, date=d)
    count = len(txs) if txs else 0
    status = "有数据" if count > 0 else "无数据"
    print(f"{d}    {count:>5}    {status}")

tdx.__exit__(None, None, None)
