#!/usr/bin/env python3
"""运行宽松版本的隔夜套利选股脚本"""

import sys
import os
import time
import pandas as pd

backend_dir = os.path.dirname(os.path.abspath(__file__))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from utils.pytdx_client import tdx
from scripts.一夜持股法_实盘 import (
    load_stock_codes,
    normalize_stock_codes,
    stock_code_cache_name,
    fetch_quotes,
    calculate_Alpha_effectiveness,
    filter_Alpha_effectiveness_stocks,
    mean_volume_last_n_days,
    calc_bid_ask_imbalance,
)


def main_loose():
    """宽松版本选股"""
    print("=== 开始执行宽松版选股脚本 ===")
    
    # 更宽松的参数
    alpha_min = 0.70
    alpha_max = 0.98
    volume_ratio_min = 0.5
    tail_attack_min = 0.005  # 降低尾盘要求
    bid_ask_min = -0.5  # 允许委比更负
    
    print(
        f"宽松配置: Alpha[{alpha_min}, {alpha_max}] "
        f"| 量比>={volume_ratio_min} | 尾盘>={tail_attack_min} | 委比>{bid_ask_min}"
    )
    t_total_start = time.perf_counter()

    # 1) 股票池
    t0 = time.perf_counter()
    cache_file = stock_code_cache_name()
    df_stock_codes = normalize_stock_codes(load_stock_codes(cache_file))
    stock_codes = list(df_stock_codes[["market", "code"]].itertuples(index=False, name=None))
    print(f"1. 全市场 A 股数量: {len(stock_codes)}")
    print(f"   用时: {time.perf_counter() - t0:.2f}s")

    # 2) 实时快照
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

    # 4) Alpha 筛选
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

    # 5) 补充量能与尾部攻击指标
    print("5. 拉取日线&分钟线并计算量能/尾盘指标...")
    t0 = time.perf_counter()
    df_candidates = mean_volume_last_n_days(df_candidates)
    print(f"   指标拉取用时: {time.perf_counter() - t0:.2f}s")

    count_before = len(df_candidates)
    df_candidates = df_candidates[df_candidates["volume_ratio"] >= volume_ratio_min]
    count_after = len(df_candidates)
    print(f"   量比筛选 (>= {volume_ratio_min}): {count_before} -> {count_after}")

    if df_candidates.empty:
        print("   无满足量比条件的股票，结束。")
        print(f"\n=== 总耗时: {time.perf_counter() - t_total_start:.2f}s ===")
        return

    count_before = len(df_candidates)
    df_candidates = df_candidates[df_candidates["tail_attack_coefficient"] >= tail_attack_min]
    count_after = len(df_candidates)
    print(f"   尾部攻击筛选 (>= {tail_attack_min}): {count_before} -> {count_after}")

    if df_candidates.empty:
        print("   无满足尾部攻击条件的股票，结束。")
        print(f"\n=== 总耗时: {time.perf_counter() - t_total_start:.2f}s ===")
        return

    # 7) 委比过滤
    print("7. 计算委比并筛选...")
    t0 = time.perf_counter()
    required_cols = [
        "bid_vol1", "bid_vol2", "bid_vol3", "bid_vol4", "bid_vol5",
        "ask_vol1", "ask_vol2", "ask_vol3", "ask_vol4", "ask_vol5",
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

    # 输出结果
    print("\n=== 最终候选股（宽松版） ===")
    if df_candidates.empty:
        print("无候选股。")
    else:
        df_candidates = df_candidates.sort_values(by="Alpha_effectiveness", ascending=False)
        display_columns = [
            "code", "name", "Alpha_effectiveness", "volume_ratio", 
            "bid_ask_imbalance", "tail_attack_coefficient", "price"
        ]
        display_rename = {
            "code": "代码", "name": "名称", "Alpha_effectiveness": "动量Alpha",
            "volume_ratio": "量比", "bid_ask_imbalance": "委比", 
            "tail_attack_coefficient": "尾部攻击系数", "price": "当前价"
        }
        
        result_df = df_candidates[display_columns].rename(columns=display_rename)
        print(result_df)
        
        # 保存到 CSV
        output_file = "隔夜套利候选股_宽松版.csv"
        result_df.to_csv(output_file, index=False, encoding="utf-8-sig")
        print(f"\n结果已保存到: {output_file}")

    print(f"\n=== 总耗时: {time.perf_counter() - t_total_start:.2f}s ===")


if __name__ == "__main__":
    main_loose()
