import os
import sys
import builtins
try:
    getattr(sys.stdout, 'flush', lambda: None)()
except Exception:
    try:
        sys.stdout = open(os.devnull, 'w')
    except Exception:
        pass
try:
    getattr(sys.stderr, 'flush', lambda: None)()
except Exception:
    try:
        sys.stderr = open(os.devnull, 'w')
    except Exception:
        pass
_orig_print = builtins.print
def print(*args, **kwargs):
    try:
        _orig_print(*args, **kwargs)
    except OSError:
        pass
os.environ.setdefault('TQDM_DISABLE', '1')
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
import streamlit as st

# 获取各板块资金流向信息
df_stocks=ak.stock_sector_fund_flow_rank().sort_values(by="今日涨跌幅", ascending=False)
# ak.stock_board_industry_cons_em()
print(df_stocks)
# 配置列显示格式
column_cfg = {
    "代码": st.column_config.TextColumn("代码"),
    "名称": st.column_config.TextColumn("名称"),
    "最新价": st.column_config.NumberColumn("最新价", format="%.2f"),
    "涨跌幅": st.column_config.NumberColumn("涨跌幅", format="%.2f%%"),
    "成交额": st.column_config.NumberColumn("成交额", format="￥%.0f"),
    "换手率": st.column_config.NumberColumn("换手率", format="%.2f%%"),
    "市盈率-动态": st.column_config.NumberColumn("PE(动)", format="%.1f"),
}

st.dataframe(
    df_stocks,
    use_container_width=True,
    hide_index=True,
    column_config=column_cfg
)