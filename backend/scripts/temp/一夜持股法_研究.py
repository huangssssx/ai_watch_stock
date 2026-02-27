"""一夜持股法：实时漏斗选股脚本（pytdx 版）

脚本目标：
- 从全市场 A 股中，用“动量 + 量能 + 尾盘强度 + 委比安全阀”的思路筛出候选股
- 输出一个便于人工复核/下单前二次过滤的候选列表

数据来源（pytdx）：
- 全市场证券列表：get_security_count / get_security_list（按天缓存）
- 实时快照：get_security_quotes（用于 price/open/high/low/vol 以及买卖盘档位量）
- 历史 K 线：
  - 日线：get_security_bars(9, ...) 用于近 N 日均量
  - 30 分钟线：get_security_bars(8, ...) 用于尾部攻击系数（近 30 分钟涨幅）

逐层筛选（漏斗）：
1) 股票池：拉取全市场 A 股，排除黑名单，按天缓存
2) 快照：批量拉取实时行情快照，合并为 DataFrame
3) 动量 Alpha：Alpha = (price - open) / ((high - low) + 0.001)
   - 作用：衡量“上涨是否有效”，即涨幅在当日振幅中的占比；越接近 1 越像“收在高位/强势主导”
   - 阈值：0.85 ~ 0.98
     - 0.85：要求涨幅在振幅中占比很高，过滤“冲高回落/虚拉不封”一类弱势结构
     - 0.98：避免极端值（几乎等于 1）常见于超小振幅/数据噪声/异常快照导致的误入
4) 量能：mean_vol_last_n_days（近 N 日均量），volume_ratio = vol / mean_vol_last_n_days（量比）
   - 作用：要求上涨有成交量配合，过滤“没量硬拉/虚涨”的票；量比越高代表当日放量越明显
   - 阈值：volume_ratio >= 1.0
     - 1.0：至少不缩量；把“强势但成交量跟不上”的票先过滤掉
5) 尾部攻击：tail_attack_coefficient（近 30 分钟涨幅）
   - 作用：捕捉尾盘资金主动性，过滤“尾盘走弱/回落”的票；偏好强尾盘以提高次日延续概率
   - 阈值：tail_attack_coefficient >= 0.01
     - 0.01：要求近 30 分钟至少上涨约 1%，体现尾盘资金继续加速/托底的意愿
6) 委比安全阀：bid_ask_imbalance = (委买总量 - 委卖总量) / (委买总量 + 委卖总量)
   - 作用：从盘口抛压角度做最后风控；不要求为正，但当委比显著为负通常意味着压单/抛压过重
   - 阈值：bid_ask_imbalance > -0.3
     - -0.3：允许“强拉但委比略负”的情况保留；但当委比更差（如 -0.8）多见于卖盘压制/上方抛压过重，拉升失败概率大
   - 委比判定参考（便于人工复核盘口质量）：
     - 情况 1：正值（bid_ask_imbalance > 0）
       - 判定结果：符合风控要求（可保留）
       - 盘口状态：委买总量 > 委卖总量，买盘力量占优，盘口无抛压，是拉升的优质盘口基础。
     - 情况 2：零值（bid_ask_imbalance = 0）
       - 判定结果：符合风控要求（可保留）
       - 盘口状态：委买总量 = 委卖总量，多空力量完全均衡，无明显抛压，不影响拉升操作。
     - 情况 3：小幅负值（-0.3 < bid_ask_imbalance < 0）
       - 判定结果：符合风控要求（可保留）
       - 盘口状态：委卖总量 > 委买总量，但卖盘优势微弱（如 -0.1、-0.2 等），属于“强拉但委比略负”的允许情形，抛压可控，拉升成功概率较高。
     - 情况 4：显著负值（bid_ask_imbalance <= -0.3）
       - 判定结果：不符合风控要求（需剔除）
       - 盘口状态：委卖总量远大于委买总量，卖盘压制极强（如 -0.3、-0.5、-0.8、-1.0 等），上方抛压过重，拉升失败概率大幅升高，是核心风控规避场景。

可调参数：

注意：
- 本脚本使用快照字段 price/open/high/low/vol 做计算，属于盘中口径；若要“收盘后选股”的严格口径，
  建议改用日 K 的 close/open/high/low 重新计算 Alpha 与相关指标。
"""

import os
import sys
import time
from typing import Optional

import pandas as pd

# 让脚本可以从 backend/scripts 直接运行并 import backend/utils 下的工具

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from utils.pytdx_client import tdx

STRATEGY_PROFILES: dict[str, dict[str, float]] = {
    "1": {
        "alpha_min": 0.92,
        "alpha_max": 0.98,
        "volume_ratio_min": 1.5,
        "tail_attack_min": 0.02,
        "bid_ask_min": 0.1,
    },
    "2": {
        "alpha_min": 0.90,
        "alpha_max": 0.98,
        "volume_ratio_min": 1.2,
        "tail_attack_min": 0.015,
        "bid_ask_min": 0.0,
    },
    "3": {
        "alpha_min": 0.88,
        "alpha_max": 0.95,
        "volume_ratio_min": 1.0,
        "tail_attack_min": 0.012,
        "bid_ask_min": 0.2,
    },
}


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


def load_strategy_profile(profile_key: str = "2") -> tuple[str, dict[str, float]]:
    key = str(profile_key).strip()
    if key not in STRATEGY_PROFILES:
        key = "2"
    return key, STRATEGY_PROFILES[key]


def calc_bid_ask_imbalance(stock_def: pd.DataFrame) -> float:
    """计算委比（Bid-Ask Imbalance）。
    
    公式：(委买总量 - 委卖总量) / (委买总量 + 委卖总量)
    范围：[-1, 1]
    """
    if isinstance(stock_def, pd.Series):
        bid_vol = (
            float(stock_def.get("bid_vol1", 0))
            + float(stock_def.get("bid_vol2", 0))
            + float(stock_def.get("bid_vol3", 0))
            + float(stock_def.get("bid_vol4", 0))
            + float(stock_def.get("bid_vol5", 0))
        )
        ask_vol = (
            float(stock_def.get("ask_vol1", 0))
            + float(stock_def.get("ask_vol2", 0))
            + float(stock_def.get("ask_vol3", 0))
            + float(stock_def.get("ask_vol4", 0))
            + float(stock_def.get("ask_vol5", 0))
        )
        imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol + 0.0001)
        return round(float(imbalance), 2)

    bid_vol = (
        stock_def["bid_vol1"]
        + stock_def["bid_vol2"]
        + stock_def["bid_vol3"]
        + stock_def["bid_vol4"]
        + stock_def["bid_vol5"]
    )
    ask_vol = (
        stock_def["ask_vol1"]
        + stock_def["ask_vol2"]
        + stock_def["ask_vol3"]
        + stock_def["ask_vol4"]
        + stock_def["ask_vol5"]
    )

    imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol + 0.0001)
    return imbalance.round(2)


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

    # # 调试用途：确认快照字段是否齐全（price/open/high/low 等）
    # print(sum_quotes.columns)

    sum_quotes["Alpha_effectiveness"] = (
        (sum_quotes["price"] - sum_quotes["open"])
        / (sum_quotes["high"] - sum_quotes["low"] + 0.001)
    )
    return sum_quotes



# 筛选动量股票
def filter_Alpha_effectiveness_stocks(df_quotes: pd.DataFrame, alpha_effectiveness_threshold_min: float, alpha_effectiveness_threshold_max: float) -> pd.DataFrame:
    """筛选出量能股票（成交量 >= min_volume）。"""
    return df_quotes[(df_quotes["Alpha_effectiveness"] >= alpha_effectiveness_threshold_min) & (df_quotes["Alpha_effectiveness"] <= alpha_effectiveness_threshold_max)]


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


# 计算尾部攻击都系数
def calc_tail_attack_coefficient_operator (market, code, _df_quotes: pd.DataFrame):
    """计算当前瞬时尾部攻击都系数（Pearson）。
    - 说明：当前（下午 2:50）的瞬时 price 与 30 分钟前的 open 之差除以 30 分钟前的 open。也就是涨幅
    - 取值范围：[-1, 1]，越接近 1 表示尾部攻击都越强。
    """
    # 获取最近 30 分钟的 K 线数据
    data = tdx.get_security_bars(8, market, code, 0, 30)
    df = tdx.to_df(data) if data else pd.DataFrame()
    
    if df.empty:
        return None
    
    # 筛选出与最新一根 K 线同一天的数据（防止跨日）
    last_date = str(df.iloc[-1]["datetime"])[:10]  # 取 "YYYY-MM-DD"
    df = df[df["datetime"].astype(str).str.startswith(last_date)]

    if df.empty:
        return None

    # 计算当前瞬时尾部攻击都系数
    curr_price = df.iloc[-1]["close"]
    open_price = df.iloc[0]["open"]
    tail_attack_coefficient = (curr_price - open_price) / open_price
    # print(tail_attack_coefficient)
    return tail_attack_coefficient

def calc_tail_attack_coefficient(df):
    """计算当前瞬时尾部攻击都系数（Pearson）。"""
    df["tail_attack_coefficient"] = df.apply(
        lambda row: calc_tail_attack_coefficient_operator(row["market"], row["code"], row),
        axis=1,
    )

def main(profile_key: str = "2"):
    """脚本入口：股票池 -> 实时行情 -> 逐层筛选 -> 输出候选。"""

    print("=== 开始执行选股脚本 ===")
    profile_name, cfg = load_strategy_profile(profile_key)
    alpha_min = float(cfg["alpha_min"])
    alpha_max = float(cfg["alpha_max"])
    volume_ratio_min = float(cfg["volume_ratio_min"])
    tail_attack_min = float(cfg["tail_attack_min"])
    bid_ask_min = float(cfg["bid_ask_min"])
    print(
        f"策略配置: {profile_name} | Alpha[{alpha_min}, {alpha_max}] "
        f"| 量比>={volume_ratio_min} | 尾盘>={tail_attack_min} | 委比>{bid_ask_min}"
    )
    t_total_start = time.perf_counter()

    # 1) 股票池（按天缓存，避免每天重复拉取全部证券列表）
    t0 = time.perf_counter()
    cache_file = stock_code_cache_name()
    df_stock_codes = normalize_stock_codes(load_stock_codes(cache_file))
    stock_codes = list(df_stock_codes[["market", "code"]].itertuples(index=False, name=None))
    print(f"1. 全市场 A 股数量: {len(stock_codes)}")
    print(f"   用时: {time.perf_counter() - t0:.2f}s")

    # 2) 实时快照（包含 price/open/high/low/vol 等字段）
    print("2. 正在拉取实时快照...")
    t0 = time.perf_counter()
    sum_quotes = fetch_quotes(stock_codes, batch_size=80)
    if sum_quotes is not None and not sum_quotes.empty and "code" in sum_quotes.columns:
        sum_quotes["code"] = sum_quotes["code"].astype(str).str.zfill(6)
        name_map = df_stock_codes.set_index("code")["name"].to_dict()
        sum_quotes["name"] = sum_quotes["code"].map(name_map)
    print(f"   快照拉取完成，有效数据: {len(sum_quotes)} 条")
    print(f"   用时: {time.perf_counter() - t0:.2f}s")

    # 3) 动量 Alpha 计算
    print("3. 计算动量 Alpha...")
    t0 = time.perf_counter()
    sum_quotes = calculate_Alpha_effectiveness(sum_quotes).sort_values(
        by="Alpha_effectiveness", ascending=False
    )
    print(f"   用时: {time.perf_counter() - t0:.2f}s")

    # 4) 第一层筛选：Alpha 区间
    t0 = time.perf_counter()
    count_before = len(sum_quotes)
    df_candidates = filter_Alpha_effectiveness_stocks(
        sum_quotes, alpha_min, alpha_max
    ).copy()
    count_after = len(df_candidates)
    print(f"4. Alpha 筛选 [{alpha_min}, {alpha_max}]: {count_before} -> {count_after}")
    print(f"   用时: {time.perf_counter() - t0:.2f}s")

    if df_candidates.empty:
        print("   无满足 Alpha 条件的股票，结束。")
        print(f"\n=== 总耗时: {time.perf_counter() - t_total_start:.2f}s ===")
        return

    # 5) 补充量能并进行第二层筛选：量比
    print("5. 计算量能指标并筛选...")
    t0 = time.perf_counter()
    df_candidates = mean_volume_last_n_days(df_candidates)

    count_before = len(df_candidates)
    # 筛选量比大于阈值
    df_candidates = df_candidates[df_candidates["volume_ratio"] >= volume_ratio_min]
    count_after = len(df_candidates)
    print(f"   量比筛选 (>= {volume_ratio_min}): {count_before} -> {count_after}")
    print(f"   用时: {time.perf_counter() - t0:.2f}s")

    if df_candidates.empty:
        print("   无满足量比条件的股票，结束。")
        print(f"\n=== 总耗时: {time.perf_counter() - t_total_start:.2f}s ===")
        return

    # 6) 补充尾部攻击系数并进行第三层筛选
    print("6. 计算尾部攻击系数并筛选...")
    t0 = time.perf_counter()
    # 注意：calc_tail_attack_coefficient 是原地修改
    calc_tail_attack_coefficient(df_candidates)

    count_before = len(df_candidates)
    # 筛选尾部攻击系数大于阈值
    df_candidates = df_candidates[df_candidates["tail_attack_coefficient"] >= tail_attack_min]
    count_after = len(df_candidates)
    print(f"   尾部攻击筛选 (>= {tail_attack_min}): {count_before} -> {count_after}")
    print(f"   用时: {time.perf_counter() - t0:.2f}s")

    if df_candidates.empty:
        print("   无满足尾部攻击条件的股票，结束。")
        print(f"\n=== 总耗时: {time.perf_counter() - t_total_start:.2f}s ===")
        return

    # 7) 最后一道安全阀：委比过滤
    print("7. 计算委比并筛选...")
    t0 = time.perf_counter()
    required_cols = [
        "bid_vol1",
        "bid_vol2",
        "bid_vol3",
        "bid_vol4",
        "bid_vol5",
        "ask_vol1",
        "ask_vol2",
        "ask_vol3",
        "ask_vol4",
        "ask_vol5",
    ]
    missing_cols = [c for c in required_cols if c not in df_candidates.columns]
    if missing_cols:
        print(f"   快照缺少委比字段，跳过委比筛选: {missing_cols}")
    else:
        df_candidates["bid_ask_imbalance"] = calc_bid_ask_imbalance(df_candidates)
        count_before = len(df_candidates)
        df_candidates = df_candidates[df_candidates["bid_ask_imbalance"] > bid_ask_min]
        count_after = len(df_candidates)
        print(
            f"   委比筛选 (> {bid_ask_min}): {count_before} -> {count_after}"
        )
    print(f"   用时: {time.perf_counter() - t0:.2f}s")

    # 7) 最终结果输出
    print("\n=== 最终候选股 ===")
    if df_candidates.empty:
        print("无候选股。")
    else:
        # 按 Alpha 降序排列
        df_candidates = df_candidates.sort_values(by="Alpha_effectiveness", ascending=False)
        print(
            df_candidates[[
                "code",
                "name",
                "Alpha_effectiveness",
                "volume_ratio",
                "bid_ask_imbalance",
                "tail_attack_coefficient",
            ]]
        )
    print(f"\n=== 总耗时: {time.perf_counter() - t_total_start:.2f}s ===")


if __name__ == "__main__":
    main(profile_key="2")
