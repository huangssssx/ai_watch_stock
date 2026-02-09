import os
import sys
import time
from typing import Optional

import pandas as pd

# 黑名单：剔除不参与计算/回测的标的（例如新股/异常标的等）
blacklist = ["603284", "688712", "688816", "688818"]
df_quote_snapshots = pd.DataFrame()
# 让脚本可以从 backend/scripts 直接运行并 import backend/utils 下的工具
backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from utils.stock_codes import get_all_a_share_codes
from utils.pytdx_client import tdx

def _infer_liutongguben_factor(price: pd.Series, liutongguben: pd.Series) -> pd.Series:
    p = pd.to_numeric(price, errors="coerce")
    s = pd.to_numeric(liutongguben, errors="coerce")
    cap_raw = p * s
    cap_scaled = cap_raw * 10000
    need_scale = (p > 0) & (s > 0) & (cap_raw < 1e8) & (cap_scaled >= 1e8)
    return pd.Series(need_scale).map(lambda x: 10000 if bool(x) else 1).astype(int)

def get_security_quotes(df_stock_codes:pd.DataFrame):
    """
     获取指定股票的实时行情
    """
    stock_codes = list(zip(df_stock_codes['market'], df_stock_codes['code'])  )
    # 按照指定数量分组，生成二维数组
    bitch_size=80
    stock_codes_batches = [stock_codes[i:i+bitch_size] for i in range(0, len(stock_codes),bitch_size)]

    # 收集所有的实时快照
    all_quote_snapshots = []
    for batch in stock_codes_batches:
        quote_snapshots = tdx.get_security_quotes(batch)
        all_quote_snapshots.extend(quote_snapshots)
    df_quote_snapshots = pd.DataFrame(all_quote_snapshots)
    return df_quote_snapshots    

def get_finance_info(df_stock_codes:pd.DataFrame):
    """
     获取指定股票的财务摘要（股本、资产负债、利润等一组字段）
    """
    date = pd.Timestamp.today()
    cache_file = f"all_finance_info_cache_{date.strftime('%Y%m%d')}.csv"

    if  os.path.exists(cache_file):
        return  pd.read_csv(cache_file)

    stock_codes = list(zip(df_stock_codes['market'], df_stock_codes['code'])  )
    # 收集所有的财务摘要
    all_finance_info = []
    for stock in stock_codes:
        res = tdx.get_finance_info(stock[0], stock[1])
        if res:
            all_finance_info.append(res)
        # 等待 1 秒，避免对服务器压力过大
        # time.sleep(1)
    df_finance_info = pd.DataFrame(all_finance_info)
    df_finance_info.to_csv(cache_file, index=False)
    return df_finance_info    

# 流通盘计算
def calcalte_circulating_stock(df_quote_snapshots: pd.DataFrame,df_finance_info: pd.DataFrame) -> pd.DataFrame:
    """
    计算流通盘（这里按“流通市值/流通盘(元)”口径：流通盘 = price * liutongguben）
    """
    df_left = df_quote_snapshots.copy()
    df_right = df_finance_info.copy()

    df_left["market"] = df_left["market"].astype(int)
    df_left["code"] = df_left["code"].astype(str).str.zfill(6)
    df_right["market"] = df_right["market"].astype(int)
    df_right["code"] = df_right["code"].astype(str).str.zfill(6)

    df = pd.merge(df_left, df_right, on=["market", "code"], how="inner")
    df["price"] = pd.to_numeric(df.get("price", 0), errors="coerce")
    df["liutongguben_raw"] = pd.to_numeric(df.get("liutongguben", 0), errors="coerce")
    df["liutongguben_factor"] = _infer_liutongguben_factor(df["price"], df["liutongguben_raw"])
    df["liutongguben"] = df["liutongguben_raw"] * df["liutongguben_factor"]

    df["流通盘"] = df["price"] * df["liutongguben"]
    df["circulating"] = df["流通盘"]
    return df

# 流通盘过滤
def filter_circulating_stock(df: pd.DataFrame) -> pd.DataFrame:
    """
    过滤出流通盘股票（流通盘大于0）
    """
    s = pd.to_numeric(df.get("circulating", pd.NA), errors="coerce")
    return df[(s > 0) & (s < 5_000_000_000)]

def calculate_daily_turnover_operator(row:pd.Series,N:int=20):
    """
    计算“多数时间缩量”口径的日换手率占比：
    - 近 N 天中，日换手率 <= 5% 的天数占比
    """
    market = row["market"]
    code_num = int(row["code"])
    code = str(code_num).zfill(6)
    bars = tdx.get_security_bars(9,market, code, 0, N)
    if bars is None or len(bars) == 0:
        return None
    df = pd.DataFrame(bars)
    df["vol"] = pd.to_numeric(df.get("vol", 0), errors="coerce")
    df["amount"] = pd.to_numeric(df.get("amount", 0), errors="coerce")
    df["close"] = pd.to_numeric(df.get("close", 0), errors="coerce")
    liutongguben = pd.to_numeric(row.get("liutongguben", 0), errors="coerce")
    if pd.isna(liutongguben) or float(liutongguben) <= 0:
        return None

    safe = (df["vol"] > 0) & (df["close"] > 0)
    raw_vwap = (df.loc[safe, "amount"] / df.loc[safe, "vol"]).astype(float)
    vwap_ratio = raw_vwap / df.loc[safe, "close"].astype(float)
    ratio_median = pd.to_numeric(vwap_ratio, errors="coerce").replace([float("inf"), float("-inf")], pd.NA).dropna().median()

    vol_shares = df["vol"]
    if pd.notna(ratio_median) and 80 <= float(ratio_median) <= 120:
        vol_shares = df["vol"] * 100

    df["turnover"] = vol_shares / liutongguben
    s = pd.to_numeric(df["turnover"], errors="coerce").dropna()
    if s.empty:
        return None
    return float((s <= 0.05).mean())

def filter_daily_turnover(df: pd.DataFrame, N: int = 20) -> pd.DataFrame:
    """ 
    - 🔒 日常换手率＜5%
    - 日成交量： get_security_bars （日线）→ vol （取近 N 天）
    - 流通股本： get_finance_info → liutongguben
    - 计算： turnover = vol / liutongguben （同样需要对 vol 单位做量级校验：股/手）
    
    “多数时间缩量”口径（更贴近你描述） ：
    - 计算 turnover_daily 后，统计 <=5% 的天数占比
    - 判断： count(turnover_daily<=0.05)/N >= 0.7 （比如 70%）
    """
    df["turnover_low_ratio"] = df.apply(lambda row: calculate_daily_turnover_operator(row, N), axis=1)
    df["turnover_low_ratio"] = pd.to_numeric(df["turnover_low_ratio"], errors="coerce")
    df["turnover_low_ok"] = df["turnover_low_ratio"].fillna(0) >= 0.7

    return df[df["turnover_low_ok"]]

def main():
    print("开始全市场 A 股股票列表拉取...")
    df_stock_codes = get_all_a_share_codes()
    # 获取全市场快照
    df_quote_snapshots = get_security_quotes(df_stock_codes)
    # 获取财务摘要（股本、资产负债、利润等一组字段）
    df_finance_info = get_finance_info(df_stock_codes)
    # 计算流通盘
    df_quote_snapshots = calcalte_circulating_stock(df_quote_snapshots, df_finance_info)
    # 过滤出流通盘股票
    df_quote_snapshots = filter_circulating_stock(df_quote_snapshots)
    df_quote_snapshots.to_csv("circulating.csv", index=False)
    # 日常换手率在 20 日内 <= 5% 的天数占比 >= 0.7
    df_quote_snapshots = filter_daily_turnover(df_quote_snapshots)
    df_quote_snapshots.to_csv("turnover.csv", index=False)
    print(len(df_quote_snapshots))

if __name__ == "__main__":
    main()
