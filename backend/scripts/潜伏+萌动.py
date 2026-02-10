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
        if not quote_snapshots:
            continue
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
    price_rt = pd.to_numeric(df.get("price", 0), errors="coerce")
    price_ref = pd.to_numeric(df.get("last_close", 0), errors="coerce")
    df["price"] = price_rt.where(price_rt > 0, price_ref)
    df["liutongguben_raw"] = pd.to_numeric(df.get("liutongguben", 0), errors="coerce")
    df["liutongguben_factor"] = _infer_liutongguben_factor(df["price"], df["liutongguben_raw"])
    df["liutongguben"] = df["liutongguben_raw"] * df["liutongguben_factor"]

    df["流通盘"] = df["price"] * df["liutongguben"]
    df["circulating"] = df["流通盘"]
    return df

# 流通盘过滤
def filter_circulating_stock(df: pd.DataFrame) -> pd.DataFrame:
    """
    过滤出流通盘股票（0 < 流通盘 < 50亿）
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
    if bars is None or len(bars) < N:
        return None
    df = pd.DataFrame(bars)
    if "datetime" in df.columns:
        df = df.sort_values("datetime")
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
    if df is None or df.empty:
        return df
    t0 = time.time()
    ratios = []
    total = int(len(df))
    for idx, row in df.iterrows():
        ratios.append(calculate_daily_turnover_operator(row, N))
        done = len(ratios)
        if done % 200 == 0 or done == total:
            dt = time.time() - t0
            print(f"[换手率] progress {done}/{total} elapsed={dt:.1f}s")
    df["turnover_low_ratio"] = ratios
    df["turnover_low_ratio"] = pd.to_numeric(df["turnover_low_ratio"], errors="coerce")
    df["turnover_low_ok"] = df["turnover_low_ratio"].fillna(0) >= 0.7
    return df[df["turnover_low_ok"]]


def filter_ma20_support_operator(row:pd.Series,N:int=40)->bool:
    """
    20日均线支撑：现价≥MA20×0.98
    """
    market = row["market"]
    code_num = int(row["code"])
    code = str(code_num).zfill(6)
    bars = tdx.get_security_bars(9,market, code, 0, N)
    if bars is None or len(bars) < 20:
        return False
    df = pd.DataFrame(bars)
    if "datetime" in df.columns:
        df = df.sort_values("datetime")
    df["close"] = pd.to_numeric(df.get("close", 0), errors="coerce")
    df["ma20"] = df["close"].rolling(window=20).mean()
    is_ma20_support = df["close"].iloc[-1] >= df["ma20"].iloc[-1] * 0.98
    return is_ma20_support

# 20日均线支撑
def filter_ma20_support(df:pd.DataFrame)->pd.DataFrame:
    """
    20日均线支撑：现价≥MA20×0.98
    """
    if df is None or df.empty:
        return df
    t0 = time.time()
    oks = []
    total = int(len(df))
    for idx, row in df.iterrows():
        oks.append(filter_ma20_support_operator(row))
        done = len(oks)
        if done % 200 == 0 or done == total:
            dt = time.time() - t0
            print(f"[MA20支撑] progress {done}/{total} elapsed={dt:.1f}s")
    df["ma20_support_ok"] = oks
    print(df)
    return df[df["ma20_support_ok"]]


def filter_ma20_oscillation_operator(row:pd.Series,N:int=40)->bool:
    market = row["market"]
    code_num = int(row["code"])
    code = str(code_num).zfill(6)
    bars = tdx.get_security_bars(9,market, code, 0, N)
    if bars is None or len(bars) == 0:
        return False
    df = pd.DataFrame(bars)
    if "datetime" in df.columns:
        df = df.sort_values("datetime")
    df["close"] = pd.to_numeric(df.get("close", 0), errors="coerce")
    close_s = pd.to_numeric(df["close"], errors="coerce").dropna()
    close_s = close_s.iloc[-20:]
    if len(close_s) < 20:
        return False
    base_close = float(close_s.iloc[0])
    last_close = float(close_s.iloc[-1])
    if base_close <= 0 or last_close <= 0:
        return False

    chg20 = last_close / base_close - 1.0

    min_close = close_s[close_s > 0].min()
    if pd.isna(min_close) or float(min_close) <= 0:
        return False
    range_amp = close_s.max() / float(min_close) - 1.0

    return bool((-0.12 <= chg20 <= 0.15) and (range_amp <= 0.30))


# 近20日整体震荡：涨跌幅 -12%~+15% 且区间振幅<=30%
def filter_ma20_oscillation(df:pd.DataFrame)->pd.DataFrame:
    """
    近20日整体震荡：涨跌幅 -12%~+15% 且区间振幅<=30%
    """
    if df is None or df.empty:
        return df
    t0 = time.time()
    oks = []
    total = int(len(df))
    for idx, row in df.iterrows():
        oks.append(filter_ma20_oscillation_operator(row))
        done = len(oks)
        if done % 200 == 0 or done == total:
            dt = time.time() - t0
            print(f"[20日震荡] progress {done}/{total} elapsed={dt:.1f}s")
    df["ma20_oscillation_ok"] = oks
    print(df)
    return df[df["ma20_oscillation_ok"]]

    # df["ma20"] = df["last_close"].rolling(window=20).mean()
    # df["ma20_oscillation"] = (df["last_close"] - df["ma20"]) / df["ma20"]
    # df["ma20_oscillation_ok"] = (df["ma20_oscillation"] >= -0.12) & (df["ma20_oscillation"] <= 0.15)
    # return df[df["ma20_oscillation_ok"]]

def main():
    t0 = time.time()

    def _log(stage: str, df: pd.DataFrame):
        n = 0 if df is None else int(len(df))
        dt = time.time() - t0
        if n == 0:
            print(f"[{dt:8.1f}s] {stage}: rows=0")
            return
        cols = []
        for c in ["market", "code", "price", "circulating", "turnover_low_ratio", "ma20_support_ok", "ma20_oscillation_ok"]:
            if c in df.columns:
                cols.append(c)
        head_preview = df[cols].head(3) if cols else df.head(3)
        print(f"[{dt:8.1f}s] {stage}: rows={n}")
        print(head_preview)

    print("开始全市场 A 股股票列表拉取...")
    df_stock_codes = get_all_a_share_codes()
    df_stock_codes["code"] = df_stock_codes["code"].astype(str).str.zfill(6)
    _log("A股代码列表", df_stock_codes)

    df_stock_codes = df_stock_codes[~df_stock_codes["code"].isin(blacklist)]
    _log("剔除黑名单后代码列表", df_stock_codes)

    df_quote_snapshots = get_security_quotes(df_stock_codes)
    _log("实时快照", df_quote_snapshots)

    df_finance_info = get_finance_info(df_stock_codes)
    _log("财务摘要", df_finance_info)

    df_quote_snapshots = calcalte_circulating_stock(df_quote_snapshots, df_finance_info)
    _log("合并并计算流通盘", df_quote_snapshots)

    df_quote_snapshots = filter_circulating_stock(df_quote_snapshots)
    _log("过滤流通盘", df_quote_snapshots)

    df_quote_snapshots = filter_daily_turnover(df_quote_snapshots)
    _log("过滤换手率占比", df_quote_snapshots)

    df_quote_snapshots = filter_ma20_support(df_quote_snapshots)
    _log("过滤MA20支撑", df_quote_snapshots)

    df_quote_snapshots = filter_ma20_oscillation(df_quote_snapshots)
    _log("过滤20日整体震荡", df_quote_snapshots)

    df_quote_snapshots.to_csv("ma20_oscillation.csv", index=False)
    print("完成，最终数量：", len(df_quote_snapshots))

if __name__ == "__main__":
    main()
