"""一夜持股法：基础数据准备脚本（pytdx 版）

整体流程：
- 获取全市场 A 股列表（深/沪）并按天缓存
- 批量拉取实时快照（quotes）
- 计算动量 Alpha、筛选候选
- 补充量能（近 N 日均量、量比）与量价相关性

注意：脚本当前使用快照字段 `price/open/high/low/vol` 做计算，
如果你希望“收盘后选股”的严格口径，建议改用日 K 的 `close/open/high/low`。
"""

import os
import sys
from typing import Optional

import pandas as pd
from datetime import datetime

# 让脚本可以从 backend/scripts 直接运行并 import backend/utils 下的工具

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from utils.pytdx_client import tdx

# 动量 Alpha（更接近“涨幅占当日振幅的比例”）
# Alpha = (close - open) / ((high - low) + 0.001)
# 这里 close 口径在脚本里实际使用的是 quotes 的 `price`（快照最新价）。
ALPHA_EFFECTIVENESS_THRESHOLD_min = 0.85
ALPHA_EFFECTIVENESS_THRESHOLD_max = 0.98

# 量价相关性计算窗口（天）
CORRELATION_DAYS = 5


# 黑名单：剔除不参与计算/回测的标的（例如新股/异常标的等）
blacklist = ["603284", "688712", "688816", "688818"]


def is_a_share_stock(market: int, code: str) -> bool:
    """判断 (market, code) 是否为 A 股股票代码。

    - market=0：深市；market=1：沪市
    - 这里通过代码前缀做“粗过滤”，用于排除基金/债券/指数等非股票品种
    """

    code = str(code or "").zfill(6)
    if market == 0:
        # 深市主板/中小板/创业板（含 301）等
        return code.startswith(("000", "001", "002", "003", "300", "301"))
    if market == 1:
        # 沪市主板/科创板
        return code.startswith(("600", "601", "603", "605", "688"))
    return False

def iter_all_a_share_codes():
    """遍历全市场 A 股 (market, code, name)。"""
    for market in (0, 1):  # 0=深, 1=沪
        total = tdx.get_security_count(market)
        step = 1000  # 常见每页1000
        for start in range(0, total, step):
            rows = tdx.get_security_list(market, start) or []
            # print(rows)
            for r in rows:
                code = str(r.get("code", "")).zfill(6)
                name = str(r.get("name", "")).strip()
                if code and is_a_share_stock(market, code):
                    yield (market, code, name)


def stock_code_cache_name(today: Optional[pd.Timestamp] = None) -> str:
    """根据日期生成缓存文件名（按天缓存）。"""
    date = today or pd.Timestamp.today()
    return f"all_a_share_codes_cache_{date.strftime('%Y%m%d')}.csv"


def load_stock_codes(cache_file: str) -> pd.DataFrame:
    """读取股票列表：优先读缓存；没有则全市场拉取并写入缓存。"""
    if not os.path.exists(cache_file):
        df_stock_codes = pd.DataFrame(iter_all_a_share_codes(), columns=["market", "code", "name"])
        df_stock_codes = df_stock_codes[~df_stock_codes["code"].isin(blacklist)]
        df_stock_codes.to_csv(cache_file, index=False)
        return df_stock_codes
    return pd.read_csv(cache_file)


def normalize_stock_codes(df_stock_codes: pd.DataFrame) -> pd.DataFrame:
    """规范化字段类型，保证 market 为 int、code 为 6 位字符串。"""
    df_stock_codes["market"] = df_stock_codes["market"].astype(int)
    df_stock_codes["code"] = df_stock_codes["code"].astype(str).str.zfill(6)
    return df_stock_codes


def fetch_quotes(stock_codes: list[tuple[int, str]], batch_size: int = 80) -> pd.DataFrame:
    """按批次拉取实时行情快照（quotes），并合并为一个 DataFrame。

    说明：
    - pytdx 的 get_security_quotes 支持一次请求多个 (market, code)
    - 批大小过大可能导致网络/服务端不稳定；这里用 batch_size 控制分片
    """

    frames: list[pd.DataFrame] = []
    for start in range(0, len(stock_codes), batch_size):
        batch = stock_codes[start : start + batch_size]
        quotes = tdx.get_security_quotes(batch)
        if quotes:
            frames.append(tdx.to_df(quotes))

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, axis=0, ignore_index=True)

def calculate_Alpha_effectiveness(sum_quotes: pd.DataFrame) -> pd.DataFrame:
    """计算动量 Alpha。

    口径：Alpha = (close - open) / ((high - low) + 0.001)

    注意：这里 close 使用的是快照字段 `price`（最新价），并非严格意义的“收盘价”。
    """

    # 调试用途：确认快照字段是否齐全（price/open/high/low 等）
    print(sum_quotes.columns)

    sum_quotes["Alpha_effectiveness"] = (
        (sum_quotes["price"] - sum_quotes["open"])
        / (sum_quotes["high"] - sum_quotes["low"] + 0.001)
    )
    return sum_quotes



# 筛选动量股票
def filter_Alpha_effectiveness_stocks(df_quotes: pd.DataFrame, alpha_effectiveness_threshold_min: float = ALPHA_EFFECTIVENESS_THRESHOLD_min, alpha_effectiveness_threshold_max: float = ALPHA_EFFECTIVENESS_THRESHOLD_max) -> pd.DataFrame:
    """筛选出量能股票（成交量 >= min_volume）。"""
    return df_quotes[(df_quotes["Alpha_effectiveness"] >= alpha_effectiveness_threshold_min) & (df_quotes["Alpha_effectiveness"] <= alpha_effectiveness_threshold_max)]


# def mean_volume_last_n_days(market,code,n_days=5,exclude_today=True):
#     start_date = 1 if exclude_today else 0
#     """计算最近n天的平均成交量"""
#     data = tdx.get_security_bars(9,market, code, start_date, n_days)
#     df = tdx.to_df(data) if data else pd.DataFrame()
#     mean_vol = df["vol"].mean() if not df.empty else None
#     return mean_vol
def calc_mean_vol(market, code, n_days=5, exclude_today=True):
    """计算单只股票最近 n 天平均成交量（基于日 K 的 vol）。"""

    # start=0 表示“最新往前”，exclude_today=True 时从前一交易日开始取
    start_date = 1 if exclude_today else 0

    data = tdx.get_security_bars(9, market, code, start_date, n_days)
    df = tdx.to_df(data) if data else pd.DataFrame()
    mean_vol = df["vol"].mean() if not df.empty else None
    return mean_vol


def mean_volume_last_n_days(df, n_days=5, exclude_today=True):
    """为候选集补充近 n 日均量与量比。

    - mean_vol_last_n_days：近 n 日平均成交量
    - volume_ratio：当日快照累计成交量 / 近 n 日平均成交量
    """

    df["mean_vol_last_n_days"] = df.apply(
        lambda row: calc_mean_vol(row["market"], row["code"], n_days, exclude_today),
        axis=1,
    )
    df["volume_ratio"] = df["vol"].astype(float) / df["mean_vol_last_n_days"].astype(float)
    df["volume_ratio"] = df["volume_ratio"].fillna(0)
    return df

def calc_volume_price_correlation_Operator(market, code, curr_price=None,curr_vol=None, n_days=20, exclude_today=True):
    """计算单只股票最近 n 天量价相关系数（Pearson）。"""

    start_date = 1 if exclude_today else 0
    data = tdx.get_security_bars(9, market, code, start_date, n_days)
    ## 使用今日
    df = tdx.to_df(data) if data else pd.DataFrame()
    ## 先对齐日期datetime(YYYY-mm-dd)方便后续corr计算
    df.sort_values(by="datetime", inplace=True)

    # 先计算价格变化
    df_tmp = pd.DataFrame(df[["datetime","close","vol"]])
    if curr_price is not None:
        new_row = pd.DataFrame([{
            "datetime": datetime.today().strftime("%Y-%m-%d %H:%M"),
            "close": float(curr_price),
            "vol": float(curr_vol),
        }])
        df_tmp = pd.concat([df_tmp, new_row], ignore_index=True)
    df_tmp["price_change"] = df_tmp["close"].astype(float).pct_change()
    df_tmp["vol_change"] = df_tmp["vol"].astype(float).pct_change()
    df_tmp.dropna(inplace=True)
    corr_value = df_tmp["price_change"].corr(df_tmp["vol_change"])
    return corr_value


def calc_volume_price_correlation(df, n_day=CORRELATION_DAYS, exclude_today=True):
    """为候选集补充量价相关性列 `price_correlation`。

    说明：该函数直接在 df 上新增列，不额外返回。
    """

    df["price_correlation"] = df.apply(
        lambda row: calc_volume_price_correlation_Operator(row["market"], row["code"], row["price"], row["vol"], n_day, exclude_today),
        axis=1,
    )


def main():
    """脚本入口：股票池 -> 实时行情 -> 指标计算 -> 输出候选。"""

    # 1) 股票池（按天缓存，避免每天重复拉取全部证券列表）
    cache_file = stock_code_cache_name()
    df_stock_codes = normalize_stock_codes(load_stock_codes(cache_file))
    stock_codes = list(df_stock_codes[["market", "code"]].itertuples(index=False, name=None))

    # 2) 实时快照（包含 price/open/high/low/vol 等字段）
    sum_quotes = fetch_quotes(stock_codes, batch_size=80)

    # 3) 动量 Alpha 并按强度排序
    sum_quotes = calculate_Alpha_effectiveness(sum_quotes).sort_values(
        by="Alpha_effectiveness", ascending=False
    )

    # 4) 用 Alpha 区间筛出候选
    df_volume_stocks = filter_Alpha_effectiveness_stocks(sum_quotes)

    # 5) 补充量能：近 N 日均量 + 量比
    df_volume_stocks = mean_volume_last_n_days(df_volume_stocks)

    # 6) 补充量价相关性（在 df 上新增一列）
    calc_volume_price_correlation(df_volume_stocks)

    print(
        df_volume_stocks[[
            "code",
            "Alpha_effectiveness",
            "mean_vol_last_n_days",
            "volume_ratio",
            "price_correlation",
        ]]
    )


if __name__ == "__main__":
    # main()
    print(calc_volume_price_correlation_Operator(1, "600000",100.01,1000000))
