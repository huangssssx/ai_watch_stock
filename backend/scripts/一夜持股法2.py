import os
import sys
import argparse
import datetime as dt
from typing import Optional
import pandas as pd

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from utils.pytdx_client import tdx

# 获取全市场code
def iter_all_a_share_codes():
    for market in (0, 1):  # 0=深, 1=沪
        total = tdx.get_security_count(market)
        step = 1000  # 常见每页1000
        for start in range(0, total, step):
            rows = tdx.get_security_list(market, start) or []
            for r in rows:
                code = str(r.get("code", "")).zfill(6)
                if code:
                    yield (market, code)

for market, code in iter_all_a_share_codes():
    print(market, code)


# ((close - open) / ((high - low) + .001))
# data = tdx.get_security_bars(9, 0, "000001", 0, 1)
# df = tdx.to_df(data)
# print(df)
