import sys
from pathlib import Path
import akshare as ak

backend_dir = Path(__file__).resolve().parents[1]
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from utils.tushare_client import ts, pro
import datetime

if pro is None:
    raise SystemExit("Tushare pro 未初始化")

try:
    current_date = datetime.datetime.now().strftime("%Y%m%d")

    # 行业资金流
    df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流")
    # print(df)

    # 行业资金流详情
    df = ak.stock_sector_fund_flow_summary(symbol="证券", indicator="今日")
    # print(df)

    # 概念资金流详情
    df = ak.stock_individual_fund_flow(stock="600094", market="sh")
    print(df)

except Exception as e:
    print(f"stock_board_concept_name_em 失败：{e}")


