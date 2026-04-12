import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../"))

import numpy as np
import pandas as pd
from utils.pytdx_client import connect, DEFAULT_IP, DEFAULT_PORT
from utils.tushare_client import pro

STOCKS = [
    ("000001", 0, "SZ", "平安银行", "大盘"),
    ("600519", 1, "SH", "贵州茅台", "超大盘"),
    ("002415", 0, "SZ", "海康威视", "大盘"),
    ("300750", 1, "SH", "宁德时代", "超大盘"),
    ("000002", 0, "SZ", "万科A", "大盘"),
    ("300999", 0, "SZ", "金龙鱼", "大盘"),
    ("601012", 1, "SH", "隆基绿能", "中盘"),
    ("002230", 0, "SZ", "科大讯飞", "中盘"),
    ("300059", 0, "SZ", "东方财富", "大盘"),
    ("688981", 1, "SH", "中芯国际", "大盘"),
    ("600111", 1, "SH", "北方稀土", "中盘"),
    ("002049", 0, "SZ", "紫光国微", "中小盘"),
    ("300782", 0, "SZ", "卓胜微", "中小盘"),
    ("688396", 1, "SH", "华润微", "中小盘"),
    ("000725", 0, "SZ", "京东方A", "大盘"),
    ("601127", 1, "SH", "赛力斯", "中盘"),
    ("301269", 0, "SZ", "华大九天", "小盘"),
    ("600176", 1, "SH", "中国巨石", "中盘"),
    ("300347", 0, "SZ", "泰格医药", "中盘"),
    ("688188", 1, "SH", "培思科技", "小盘"),
]


def _ts_code(code, mkt_suffix):
    return f"{code}.{mkt_suffix}"


def _fetch_all_ticks(tdx, market, code, date_int):
    ticks = []
    for start in range(0, 200000, 500):
        batch = tdx.get_history_transaction_data(
            market=market, code=code, start=start, count=500, date=date_int
        )
        if not batch:
            break
        for t in batch:
            if t["vol"] <= 0:
                continue
            amount = t["vol"] * 100 * t["price"]
            ticks.append({
                "time": t["time"],
                "price": t["price"],
                "vol": t["vol"],
                "buyorsell": t["buyorsell"],
                "amount": amount,
            })
    return ticks


def method_original(ticks, finance_info, price):
    liutongguben = finance_info["liutongguben"]
    free_market_cap = liutongguben * price
    threshold_base = free_market_cap * 0.00001 / 100
    amounts = [t["amount"] for t in ticks]
    p95 = np.percentile(amounts, 95) if amounts else threshold_base
    threshold = max(threshold_base, p95 * 1.5)
    return threshold, "原方案(市值×0.00001%)"


def method_percentile_90(ticks, **kwargs):
    amounts = [t["amount"] for t in ticks]
    return np.percentile(amounts, 90) if amounts else 0, "P90百分位"


def method_percentile_95(ticks, **kwargs):
    amounts = [t["amount"] for t in ticks]
    return np.percentile(amounts, 95) if amounts else 0, "P95百分位"


def method_iqr(ticks, **kwargs):
    amounts = [t["amount"] for t in ticks]
    if not amounts:
        return 0, "IQR异常值(Q3+1.5×IQR)"
    q1 = np.percentile(amounts, 25)
    q3 = np.percentile(amounts, 75)
    iqr = q3 - q1
    return q3 + 1.5 * iqr, "IQR异常值(Q3+1.5×IQR)"


def method_iqr_3x(ticks, **kwargs):
    amounts = [t["amount"] for t in ticks]
    if not amounts:
        return 0, "IQR极端值(Q3+3×IQR)"
    q1 = np.percentile(amounts, 25)
    q3 = np.percentile(amounts, 75)
    iqr = q3 - q1
    return q3 + 3.0 * iqr, "IQR极端值(Q3+3×IQR)"


def method_mean_x5(ticks, **kwargs):
    amounts = [t["amount"] for t in ticks]
    if not amounts:
        return 0, "均值×5"
    return np.mean(amounts) * 5, "均值×5"


def method_mean_x10(ticks, **kwargs):
    amounts = [t["amount"] for t in ticks]
    if not amounts:
        return 0, "均值×10"
    return np.mean(amounts) * 10, "均值×10"


def method_median_x10(ticks, **kwargs):
    amounts = [t["amount"] for t in ticks]
    if not amounts:
        return 0, "中位数×10"
    return np.median(amounts) * 10, "中位数×10"


def method_p95_x1_5(ticks, **kwargs):
    amounts = [t["amount"] for t in ticks]
    if not amounts:
        return 0, "P95×1.5"
    return np.percentile(amounts, 95) * 1.5, "P95×1.5"


def method_free_mv_adjusted(ticks, finance_info, price):
    ts_code = None
    liutongguben = finance_info["liutongguben"]
    free_market_cap = liutongguben * price
    threshold_base = free_market_cap * 0.0001 / 100
    amounts = [t["amount"] for t in ticks]
    p95 = np.percentile(amounts, 95) if amounts else threshold_base
    threshold = max(threshold_base, p95 * 1.5)
    return threshold, "市值×0.0001%(10倍系数)"


METHODS = [
    method_original,
    method_free_mv_adjusted,
    method_percentile_90,
    method_percentile_95,
    method_iqr,
    method_iqr_3x,
    method_mean_x5,
    method_mean_x10,
    method_median_x10,
    method_p95_x1_5,
]


def evaluate_threshold(ticks, threshold):
    if threshold <= 0 or not ticks:
        return {}
    mainforce = [t for t in ticks if t["amount"] >= threshold]
    mainforce_buy = [t for t in mainforce if t["buyorsell"] in [0]]
    mainforce_sell = [t for t in mainforce if t["buyorsell"] in [1]]
    total_amount = sum(t["amount"] for t in ticks)
    main_amount = sum(t["amount"] for t in mainforce)
    return {
        "笔数": len(mainforce),
        "占比%": len(mainforce) / len(ticks) * 100,
        "金额占比%": main_amount / total_amount * 100 if total_amount else 0,
        "买入金额": sum(t["amount"] for t in mainforce_buy),
        "卖出金额": sum(t["amount"] for t in mainforce_sell),
        "净流入": sum(t["amount"] for t in mainforce_buy) - sum(t["amount"] for t in mainforce_sell),
        "最小单": min(t["amount"] for t in mainforce) if mainforce else 0,
        "均值": np.mean([t["amount"] for t in mainforce]) if mainforce else 0,
    }


def run_comparison(trade_date_str):
    trade_date_int = int(trade_date_str)
    print(f"\n{'#'*100}")
    print(f"# 动态阈值方法对比实验")
    print(f"# 日期: {trade_date_str}")
    print(f"# 样本: {len(STOCKS)} 只股票")
    print(f"{'#'*100}")

    all_results = []

    with connect(DEFAULT_IP, DEFAULT_PORT) as tdx:
        for code, market, mkt_suffix, name, size_label in STOCKS:
            ts_code = _ts_code(code, mkt_suffix)
            print(f"\n{'─'*100}")
            print(f"  {name} ({ts_code}) [{size_label}] — 获取数据中...")

            finance_info = tdx.get_finance_info(market=market, code=code)
            ticks = _fetch_all_ticks(tdx, market, code, trade_date_int)
            if not ticks:
                print(f"  ⚠️ 无逐笔数据, 跳过")
                continue

            price = ticks[-1]["price"]

            df_daily = pro.daily(ts_code=ts_code, start_date=trade_date_str, end_date=trade_date_str)
            if df_daily is None or df_daily.empty:
                print(f"  ⚠️ tushare daily 无数据")
                continue

            turnover_rate = 0
            df_basic = pro.daily_basic(ts_code=ts_code, trade_date=trade_date_str)
            if df_basic is not None and not df_basic.empty:
                turnover_rate = df_basic.iloc[0].get("turnover_rate", 0)

            df_moneyflow = pro.moneyflow(ts_code=ts_code, start_date=trade_date_str, end_date=trade_date_str)
            ts_big_net = None
            if df_moneyflow is not None and not df_moneyflow.empty:
                row = df_moneyflow.iloc[0]
                ts_big_net = row.get("buy_lg_amount", 0) - row.get("sell_lg_amount", 0)
                ts_big_net = ts_big_net * 10000

            print(f"  逐笔: {len(ticks)}笔, 收盘价: {price}, 换手率: {turnover_rate:.2f}%")

            amounts = [t["amount"] for t in ticks]
            print(f"  金额分布: 均值={np.mean(amounts):,.0f}, 中位={np.median(amounts):,.0f}, P90={np.percentile(amounts,90):,.0f}, P95={np.percentile(amounts,95):,.0f}")

            print(f"\n  {'方法':<25s} {'阈值(元)':>12s} {'主力笔数':>8s} {'占比%':>7s} {'金额占比%':>9s} {'净流入(万)':>12s} {'方向':>6s}")
            print(f"  {'─'*90}")

            for method_func in METHODS:
                try:
                    threshold, method_name = method_func(
                        ticks, finance_info=finance_info, price=price
                    )
                except Exception as e:
                    print(f"  {method_func.__name__}: 错误 {e}")
                    continue

                ev = evaluate_threshold(ticks, threshold)
                if not ev:
                    print(f"  {method_name:<25s} {threshold:>12,.0f} {'无结果':>8s}")
                    continue

                direction = "流入" if ev["净流入"] > 0 else "流出"
                net_wan = ev["净流入"] / 10000

                ts_dir_match = ""
                if ts_big_net is not None:
                    ts_dir = "流入" if ts_big_net > 0 else "流出"
                    ts_dir_match = "✅" if direction == ts_dir else "❌"

                print(f"  {method_name:<25s} {threshold:>12,.0f} {ev['笔数']:>8d} {ev['占比%']:>7.1f} {ev['金额占比%']:>9.1f} {net_wan:>+12,.0f} {direction:>4s} {ts_dir_match}")

                all_results.append({
                    "code": code,
                    "name": name,
                    "size": size_label,
                    "method": method_name,
                    "threshold": threshold,
                    "main_pct": ev["占比%"],
                    "amount_pct": ev["金额占比%"],
                    "net_flow": ev["净流入"],
                    "ts_big_net": ts_big_net,
                    "direction_match": ts_dir_match,
                    "turnover": turnover_rate,
                })

    print(f"\n\n{'#'*100}")
    print(f"# 跨股票汇总对比")
    print(f"{'#'*100}")

    if not all_results:
        print("无数据")
        return

    df = pd.DataFrame(all_results)

    print(f"\n--- 各方法平均主力笔数占比 ---")
    print(f"  {'方法':<25s} {'平均占比%':>10s} {'平均金额占比%':>14s} {'方向一致率':>10s} {'样本数':>6s}")
    print(f"  {'─'*70}")
    for method_name in df["method"].unique():
        sub = df[df["method"] == method_name]
        avg_main_pct = sub["main_pct"].mean()
        avg_amt_pct = sub["amount_pct"].mean()
        match_count = sub["direction_match"].apply(lambda x: x == "✅").sum()
        total_with_ts = sub["direction_match"].apply(lambda x: x in ["✅", "❌"]).sum()
        match_rate = match_count / total_with_ts * 100 if total_with_ts > 0 else float("nan")
        print(f"  {method_name:<25s} {avg_main_pct:>10.1f} {avg_amt_pct:>14.1f} {match_rate:>9.1f}% {len(sub):>6d}")

    print(f"\n--- 各方法在不同市值档次的平均阈值(万元) ---")
    print(f"  {'方法':<25s}", end="")
    for size in ["超大盘", "大盘", "中盘", "中小盘", "小盘"]:
        print(f" {size:>10s}", end="")
    print()
    print(f"  {'─'*80}")
    for method_name in df["method"].unique():
        print(f"  {method_name:<25s}", end="")
        for size in ["超大盘", "大盘", "中盘", "中小盘", "小盘"]:
            sub = df[(df["method"] == method_name) & (df["size"] == size)]
            if len(sub) > 0:
                avg_threshold_wan = sub["threshold"].mean() / 10000
                print(f" {avg_threshold_wan:>10.1f}", end="")
            else:
                print(f" {'N/A':>10s}", end="")
        print()

    print(f"\n--- 各方法阈值与换手率的相关性 ---")
    print(f"  {'方法':<25s} {'阈值-换手率相关系数':>20s}")
    print(f"  {'─'*50}")
    for method_name in df["method"].unique():
        sub = df[df["method"] == method_name]
        if len(sub) > 2:
            corr = sub["threshold"].corr(sub["turnover"])
            print(f"  {method_name:<25s} {corr:>20.4f}")

    print(f"\n--- 理想阈值标准 ---")
    print(f"  1. 主力笔数占比: 5%-20% (太大=太敏感, 太小=漏掉真主力)")
    print(f"  2. 金额占比: 60%-85% (主力应贡献大部分成交额)")
    print(f"  3. 方向一致率: 与tushare大单方向尽可能一致")
    print(f"  4. 市值分档: 超大盘阈值应远高于小盘, 但不是线性的")
    print(f"  5. 换手率无关: 阈值不应与换手率强相关(否则高换手=低阈值,逻辑不对)")


if __name__ == "__main__":
    trade_date = sys.argv[1] if len(sys.argv) > 1 else "20260410"
    run_comparison(trade_date)
