import os
import sys
_backend_dir = os.path.dirname(os.path.abspath(__file__))
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)
from pymr_compat import ensure_py_mini_racer
ensure_py_mini_racer()
# Write your Python script here
# Define "df" (DataFrame) or "result" (List) for table
# Define "chart" (Dict) for visualization

import akshare as ak
import pandas as pd

df=ak.stock_board_industry_cons_em(symbol="造纸")


print("Response:", df.to_json())
