import os
import sys
import pandas as pd

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from utils.pytdx_client import tdx

# 测试获取分钟数据
data = tdx.get_security_bars(8, 0, "000001", 0, 240)
df = tdx.to_df(data) if data else pd.DataFrame()
print(df.head(10))
