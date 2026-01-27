import requests
from bs4 import BeautifulSoup
import time
import os
import sys

# Ensure backend package can be found
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.append(project_root)

from backend.utils.tushare_client import pro as ts_pro
df = ts_pro.daily(ts_code='000001.SZ', start_date='20260101', end_date='20260127')
print(df)
