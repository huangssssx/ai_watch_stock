"""
动态阈值主力单因子 — 有效性验证脚本

验证维度:
  1. 阈值适配性：大/中/小盘股的阈值是否合理分布
  2. IC 测试：因子信号 vs 次日收益的 Rank IC
  3. 信号准确率：主力净买入 → 次日涨 的命中率
  4. 分层回测：按信号强度分组，各组次日平均收益是否单调
  5. 对比基线：与"固定阈值100万"的信号质量对比

用法:
  python validate_factor.py                           # 默认参数
  python validate_factor.py --stocks 20 --days 5      # 自定义股票数和天数
  python validate_factor.py --codes 000001,600519     # 指定股票
"""

import argparse
import os
import sys
import time
import traceback
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../"))

from utils.pytdx_client import connect, DEFAULT_IP, DEFAULT_PORT
from utils.stock_codes import get_all_a_share_codes
from dynamic_threshold import calculate_dynamic_threshold


def _fetch_daily_bars_single(tdx, market, code, total=30):
    daily = []
    for start in range(total):
        bars = tdx.get_security_bars(9, market, code, start, 1)
        if not bars:
            break
        bar = bars[0]
        y, m, d = bar["year"], bar["month"], bar["day"]
        if not (2020 <= y <= 2030 and 1 <= m <= 12 and 1 <= d <= 31):
            continue
        date_int = int(f"{y}{m:02d}{d:02d}")
        daily.append({"date": date_int, "close": bar["close"], "open": bar["open"]})
    daily.sort(key=lambda x: x["date"])
    return daily


def _get_trading_dates(tdx, market=0, index_code="399001", count=30):
    daily = _fetch_daily_bars_single(tdx, market, index_code, count)
    return [d["date"] for d in daily]


def _get_next_day_return(tdx, market, code, date_int):
    daily = _fetch_daily_bars_single(tdx, market, code, 30)

    target_idx = None
    for i, d in enumerate(daily):
        if d["date"] == date_int:
            target_idx = i
            break

    if target_idx is None or target_idx + 1 >= len(daily):
        return None

    today_close = daily[target_idx]["close"]
    next_close = daily[target_idx + 1]["close"]
    if today_close <= 0:
        return None
    return (next_close - today_close) / today_close


def _get_intraday_return(tdx, market, code, date_int):
    daily = _fetch_daily_bars_single(tdx, market, code, 30)

    for d in daily:
        if d["date"] == date_int:
            if d["open"] <= 0:
                return None
            return (d["close"] - d["open"]) / d["open"]
    return None


def _compute_factor_signal(tdx, market, code, date_int):
    try:
        threshold_info = calculate_dynamic_threshold(market, code, date_int, tdx=tdx)
    except Exception:
        return None

    dynamic_threshold = threshold_info["动态阈值(元)"]
    total_transactions = threshold_info["逐笔成交笔数"]
    if total_transactions == 0:
        return None

    all_transactions = []
    for start in range(0, 100000, 500):
        transactions = tdx.get_history_transaction_data(
            market=market, code=code, start=start, count=500, date=date_int
        )
        if not transactions:
            break
        for t in transactions:
            t["amount"] = t["vol"] * 100 * t["price"]
            all_transactions.append(t)

    if not all_transactions:
        return None

    total_amount = sum(t["amount"] for t in all_transactions)
    mainforce = [t for t in all_transactions if t["amount"] >= dynamic_threshold]
    buy_mf = [t for t in mainforce if t["buyorsell"] in [1, 2]]
    sell_mf = [t for t in mainforce if t["buyorsell"] in [0, 8]]

    net_flow = sum(t["amount"] for t in buy_mf) - sum(t["amount"] for t in sell_mf)
    net_flow_ratio = net_flow / total_amount if total_amount > 0 else 0
    mf_amount_ratio = sum(t["amount"] for t in mainforce) / total_amount if total_amount > 0 else 0

    fixed_threshold = 1_000_000
    fixed_mf = [t for t in all_transactions if t["amount"] >= fixed_threshold]
    fixed_buy = [t for t in fixed_mf if t["buyorsell"] in [1, 2]]
    fixed_sell = [t for t in fixed_mf if t["buyorsell"] in [0, 8]]
    fixed_net = sum(t["amount"] for t in fixed_buy) - sum(t["amount"] for t in fixed_sell)
    fixed_net_ratio = fixed_net / total_amount if total_amount > 0 else 0

    buy_threshold = threshold_info["买入阈值(元)"]
    sell_threshold = threshold_info["卖出阈值(元)"]

    sep_buy_mf = [t for t in all_transactions if t["amount"] >= buy_threshold and t["buyorsell"] in [1, 2]]
    sep_sell_mf = [t for t in all_transactions if t["amount"] >= sell_threshold and t["buyorsell"] in [0, 8]]
    sep_net = sum(t["amount"] for t in sep_buy_mf) - sum(t["amount"] for t in sep_sell_mf)
    sep_net_ratio = sep_net / total_amount if total_amount > 0 else 0

    return {
        "code": code,
        "market": market,
        "date": date_int,
        "price": threshold_info["当前价格(元)"],
        "free_market_cap": threshold_info["自由流通市值(元)"],
        "dynamic_threshold": dynamic_threshold,
        "p95_threshold": threshold_info["95分位数阈值(元)"],
        "p90_threshold": threshold_info["90分位数阈值(元)"],
        "sell_threshold": float(sell_threshold),
        "total_transactions": total_transactions,
        "mf_count": len(mainforce),
        "mf_ratio": len(mainforce) / total_transactions * 100,
        "net_flow": net_flow,
        "net_flow_ratio": net_flow_ratio,
        "mf_amount_ratio": mf_amount_ratio,
        "fixed_mf_count": len(fixed_mf),
        "fixed_net_ratio": fixed_net_ratio,
        "separate_net_ratio": sep_net_ratio,
        "sep_buy_count": len(sep_buy_mf),
        "sep_sell_count": len(sep_sell_mf),
    }


def _sample_stocks(tdx, n_per_bucket=8):
    print("  获取全市场股票列表...")
    all_stocks = get_all_a_share_codes()
    if all_stocks.empty:
        print("  [错误] 无法获取股票列表")
        return []

    print(f"  全市场共 {len(all_stocks)} 只股票，按市值分层抽样...")

    sampled = []
    market_caps = []

    for _, s in all_stocks.iterrows():
        market = int(s["market"])
        code = str(s["code"]).zfill(6)
        try:
            finance = tdx.get_finance_info(market=market, code=code)
            liutong = finance.get("liutongguben", 0)
            if liutong <= 0:
                continue
            quote = tdx.get_security_quotes((market, code))
            if not quote or quote[0]["price"] <= 0:
                continue
            cap = liutong * quote[0]["price"]
            market_caps.append({
                "market": market, "code": code, "name": s.get("name", ""),
                "cap": cap,
            })
        except Exception:
            continue
        time.sleep(0.02)

    if not market_caps:
        return []

    df = pd.DataFrame(market_caps)
    df = df.sort_values("cap").reset_index(drop=True)
    n = len(df)

    large = df.iloc[int(n * 0.7):]
    mid = df.iloc[int(n * 0.3):int(n * 0.7)]
    small = df.iloc[:int(n * 0.3)]

    for label, bucket in [("大盘", large), ("中盘", mid), ("小盘", small)]:
        sample_n = min(n_per_bucket, len(bucket))
        sampled_rows = bucket.sample(n=sample_n, random_state=42)
        for _, row in sampled_rows.iterrows():
            sampled.append({
                "market": int(row["market"]),
                "code": str(row["code"]).zfill(6),
                "name": row["name"],
                "cap": row["cap"],
                "cap_bucket": label,
            })

    print(f"  抽样完成: 大盘 {sum(1 for s in sampled if s['cap_bucket']=='大盘')} 只, "
          f"中盘 {sum(1 for s in sampled if s['cap_bucket']=='中盘')} 只, "
          f"小盘 {sum(1 for s in sampled if s['cap_bucket']=='小盘')} 只")
    return sampled


def _validate_threshold_adaptation(results):
    print("\n" + "=" * 70)
    print("【验证1】阈值适配性 — 大/中/小盘股的动态阈值是否合理分化")
    print("=" * 70)

    if not results:
        print("  无数据")
        return

    df = pd.DataFrame(results)
    buckets = df.groupby("cap_bucket").agg({
        "dynamic_threshold": ["mean", "median", "std"],
        "p95_threshold": ["mean", "median"],
        "sell_threshold": ["mean", "median"],
        "mf_ratio": ["mean", "median"],
        "price": "mean",
        "free_market_cap": "mean",
    })

    for bucket in ["大盘", "中盘", "小盘"]:
        if bucket not in buckets.index:
            continue
        row = buckets.loc[bucket]
        print(f"\n  [{bucket}]")
        print(f"    平均流通市值: {row[('free_market_cap', 'mean')]:,.0f} 元")
        print(f"    平均股价: {row[('price', 'mean')]:.2f} 元")
        print(f"    买入阈值(P95×1.5) 中位数: {row[('dynamic_threshold', 'median')]:,.0f} 元, "
              f"均值: {row[('dynamic_threshold', 'mean')]:,.0f} 元")
        print(f"    卖出阈值(P90×1.2) 中位数: {row[('sell_threshold', 'median')]:,.0f} 元")
        print(f"    P95阈值 中位数: {row[('p95_threshold', 'median')]:,.0f} 元")
        print(f"    主力候选单占比 中位数: {row[('mf_ratio', 'median')]:.2f}%")

    small_med = buckets.loc["小盘", ("dynamic_threshold", "median")] if "小盘" in buckets.index else 0
    large_med = buckets.loc["大盘", ("dynamic_threshold", "median")] if "大盘" in buckets.index else 1

    if small_med > 0 and large_med > 0:
        ratio = large_med / small_med
        print(f"\n  大盘/小盘 阈值倍数比: {ratio:.1f}x")
        if ratio < 3:
            print("  ⚠️  大盘和小盘阈值差异过小（<3x），因子适配性可能不足")
        elif ratio > 500:
            print("  ⚠️  大盘和小盘阈值差异过大（>500x），需检查小盘阈值是否过低")
        else:
            print("  ✅ 阈值分化合理，大盘/小盘间有明显的量级差异")
    else:
        print(f"\n  大盘阈值中位数: {large_med:,.0f} 元, 小盘阈值中位数: {small_med:,.0f} 元")
        print("  ⚠️  小盘或大盘阈值为 0，无法计算倍数比，可能存在数据问题")

    mf_ratios = df.groupby("cap_bucket")["mf_ratio"].median()
    if all(1 <= mf_ratios.get(b, 0) <= 10 for b in ["大盘", "中盘", "小盘"]):
        print("  ✅ 各市值段的主力候选单占比均在 1%-10% 区间，过滤粒度合理")
    else:
        print(f"  ⚠️  主力候选单占比分布: " +
              ", ".join(f"{b}={mf_ratios.get(b, 0):.1f}%" for b in ["大盘", "中盘", "小盘"]))
        print("     理想范围是 1%-10%，过低说明阈值太高（漏信号），过高说明阈值太低（噪音多）")


def _validate_ic_test(results_with_returns):
    print("\n" + "=" * 70)
    print("【验证2】IC 测试 — 因子信号与次日收益的 Rank 相关性")
    print("=" * 70)

    if not results_with_returns:
        print("  无数据")
        return

    df = pd.DataFrame(results_with_returns)
    df = df.dropna(subset=["next_day_return", "net_flow_ratio"])

    if len(df) < 5:
        print(f"  有效样本过少 ({len(df)} 条)，IC 结果不可靠")
        return

    rank_ic = df["net_flow_ratio"].rank().corr(df["next_day_return"].rank())

    print(f"\n  样本数: {len(df)}")
    print(f"  Rank IC (Spearman): {rank_ic:.4f}")

    if rank_ic > 0.05:
        print("  ✅ IC > 0.05，因子对次日收益有正向预测能力")
    elif rank_ic > 0:
        print("  ⚠️  IC 在 0~0.05 之间，预测能力微弱但方向正确")
    else:
        print("  ❌ IC ≤ 0，因子对次日收益无正向预测能力")

    corr_p = df["net_flow_ratio"].corr(df["next_day_return"])
    print(f"  Pearson IC: {corr_p:.4f}")

    per_date_ic = []
    for date, group in df.groupby("date"):
        if len(group) >= 3:
            ic = group["net_flow_ratio"].rank().corr(group["next_day_return"].rank())
            per_date_ic.append(ic)

    if per_date_ic:
        mean_ic = np.mean(per_date_ic)
        ic_ir = mean_ic / np.std(per_date_ic) if np.std(per_date_ic) > 0 else 0
        ic_positive_rate = sum(1 for ic in per_date_ic if ic > 0) / len(per_date_ic)
        print(f"\n  截面 IC 均值: {mean_ic:.4f}")
        print(f"  ICIR (IC/IC_std): {ic_ir:.4f}")
        print(f"  IC 正值占比: {ic_positive_rate:.1%}")
        if ic_ir > 0.5:
            print("  ✅ ICIR > 0.5，因子稳定性较好")
        elif ic_ir > 0:
            print("  ⚠️  ICIR 在 0~0.5，因子稳定性一般")
        else:
            print("  ❌ ICIR ≤ 0，因子不稳定")


def _validate_signal_accuracy(results_with_returns):
    print("\n" + "=" * 70)
    print("【验证3】信号准确率 — 主力净买入后次日上涨的概率")
    print("=" * 70)

    if not results_with_returns:
        print("  无数据")
        return

    df = pd.DataFrame(results_with_returns)
    df = df.dropna(subset=["next_day_return", "net_flow_ratio"])

    if len(df) < 3:
        print(f"  有效样本过少 ({len(df)} 条)")
        return

    buy_signals = df[df["net_flow_ratio"] > 0]
    sell_signals = df[df["net_flow_ratio"] < 0]

    if len(buy_signals) > 0:
        buy_hit = (buy_signals["next_day_return"] > 0).sum()
        buy_total = len(buy_signals)
        buy_acc = buy_hit / buy_total
        avg_ret = buy_signals["next_day_return"].mean()
        print(f"\n  主力净买入信号 ({buy_total} 次):")
        print(f"    次日上涨次数: {buy_hit}/{buy_total} = {buy_acc:.1%}")
        print(f"    次日平均收益: {avg_ret:.4f} ({avg_ret*100:.2f}%)")

    if len(sell_signals) > 0:
        sell_hit = (sell_signals["next_day_return"] < 0).sum()
        sell_total = len(sell_signals)
        sell_acc = sell_hit / sell_total
        avg_ret = sell_signals["next_day_return"].mean()
        print(f"\n  主力净卖出信号 ({sell_total} 次):")
        print(f"    次日下跌次数: {sell_hit}/{sell_total} = {sell_acc:.1%}")
        print(f"    次日平均收益: {avg_ret:.4f} ({avg_ret*100:.2f}%)")

    baseline_acc = (df["next_day_return"] > 0).mean()
    print(f"\n  基线准确率（随机猜涨）: {baseline_acc:.1%}")

    if len(buy_signals) > 0 and buy_acc > baseline_acc + 0.05:
        print(f"  ✅ 买入信号准确率 ({buy_acc:.1%}) 显著高于基线 ({baseline_acc:.1%})")
    elif len(buy_signals) > 0:
        print(f"  ⚠️  买入信号准确率 ({buy_acc:.1%}) 未显著高于基线 ({baseline_acc:.1%})")

    strong_buy = df[df["net_flow_ratio"] > df["net_flow_ratio"].quantile(0.75)]
    if len(strong_buy) >= 3:
        strong_acc = (strong_buy["next_day_return"] > 0).mean()
        strong_avg = strong_buy["next_day_return"].mean()
        print(f"\n  强买入信号 (top 25% 净流入):")
        print(f"    次日上涨率: {strong_acc:.1%}, 平均收益: {strong_avg*100:.2f}%")


def _validate_quantile_analysis(results_with_returns):
    print("\n" + "=" * 70)
    print("【验证4】分层回测 — 按信号强度分组，各组次日收益是否单调")
    print("=" * 70)

    if not results_with_returns:
        print("  无数据")
        return

    df = pd.DataFrame(results_with_returns)
    df = df.dropna(subset=["next_day_return", "net_flow_ratio"])

    if len(df) < 10:
        print(f"  有效样本过少 ({len(df)} 条)，分层不可靠")
        return

    n_groups = min(5, max(2, len(df) // 5))
    df["signal_group"] = pd.qcut(df["net_flow_ratio"], n_groups, labels=False, duplicates="drop")

    group_stats = df.groupby("signal_group").agg({
        "net_flow_ratio": ["mean", "count"],
        "next_day_return": ["mean", "median"],
    })

    print(f"\n  分为 {n_groups} 组 (按净流入比例从小到大):")
    print(f"  {'组别':<6} {'样本数':<8} {'平均净流入比':<16} {'次日平均收益':<16} {'次日中位收益':<16}")
    print(f"  {'-'*62}")

    group_means = []
    for g in sorted(df["signal_group"].unique()):
        row = group_stats.loc[g]
        print(f"  G{g:<5} {int(row[('net_flow_ratio', 'count')]):<8} "
              f"{row[('net_flow_ratio', 'mean')]:<16.6f} "
              f"{row[('next_day_return', 'mean')]*100:>8.4f}%        "
              f"{row[('next_day_return', 'median')]*100:>8.4f}%")
        group_means.append(row[("next_day_return", "mean")])

    if len(group_means) >= 2:
        is_monotonic = all(group_means[i] <= group_means[i + 1] for i in range(len(group_means) - 1))
        spread = group_means[-1] - group_means[0]
        print(f"\n  多空收益差 (最高组 - 最低组): {spread*100:.4f}%")
        if is_monotonic:
            print("  ✅ 各组收益单调递增，因子分层效果优秀")
        elif spread > 0:
            print("  ⚠️  收益差不完全单调，但方向正确（高信号组收益更高）")
        else:
            print("  ❌ 收益差方向反转，因子分层无效")


def _validate_vs_fixed(results_with_returns):
    print("\n" + "=" * 70)
    print("【验证5】对比基线 — 动态阈值 vs 固定阈值(100万)")
    print("=" * 70)

    if not results_with_returns:
        print("  无数据")
        return

    df = pd.DataFrame(results_with_returns)
    df = df.dropna(subset=["next_day_return", "net_flow_ratio", "fixed_net_ratio"])

    if len(df) < 5:
        print(f"  有效样本过少 ({len(df)} 条)")
        return

    dyn_ic = df["net_flow_ratio"].rank().corr(df["next_day_return"].rank())
    fixed_ic = df["fixed_net_ratio"].rank().corr(df["next_day_return"].rank())

    dyn_buy = df[df["net_flow_ratio"] > 0]
    fixed_buy = df[df["fixed_net_ratio"] > 0]

    print(f"\n  {'指标':<20} {'动态阈值':<16} {'固定100万':<16} {'胜出':<8}")
    print(f"  {'-'*60}")
    print(f"  {'Rank IC':<20} {dyn_ic:<16.4f} {fixed_ic:<16.4f} "
          f"{'动态' if dyn_ic > fixed_ic else '固定' if fixed_ic > dyn_ic else '持平':<8}")

    if len(dyn_buy) > 0 and len(fixed_buy) > 0:
        dyn_acc = (dyn_buy["next_day_return"] > 0).mean()
        fixed_acc = (fixed_buy["next_day_return"] > 0).mean()
        print(f"  {'买入准确率':<20} {dyn_acc:<16.1%} {fixed_acc:<16.1%} "
              f"{'动态' if dyn_acc > fixed_acc else '固定' if fixed_acc > dyn_acc else '持平':<8}")

        dyn_avg = dyn_buy["next_day_return"].mean()
        fixed_avg = fixed_buy["next_day_return"].mean()
        print(f"  {'买入平均收益':<20} {dyn_avg*100:>8.4f}%      {fixed_avg*100:>8.4f}%      "
              f"{'动态' if dyn_avg > fixed_avg else '固定' if fixed_avg > dyn_avg else '持平':<8}")

    print(f"\n  动态阈值 信号触发次数: {len(df[df['net_flow_ratio'] != 0])}")
    print(f"  固定阈值 信号触发次数: {len(df[df['fixed_net_ratio'] != 0])}")

    if dyn_ic > fixed_ic:
        print("\n  ✅ 动态阈值的 IC 优于固定阈值，因子自适应有价值")
    else:
        print("\n  ⚠️  动态阈值的 IC 未优于固定阈值，需进一步优化参数")


def _apply_saturation_penalty(results_with_returns):
    if not results_with_returns:
        return
    df = pd.DataFrame(results_with_returns)
    df["net_flow_ratio_capped"] = df["net_flow_ratio"].apply(
        lambda x: x * np.exp(-max(0, abs(x) - 0.15) * 10)
    )
    for i in range(len(results_with_returns)):
        results_with_returns[i]["net_flow_ratio_capped"] = df.iloc[i]["net_flow_ratio_capped"]


def _validate_opt2_saturation(results_with_returns):
    print("\n" + "=" * 70)
    print("【优化2】信号饱和度惩罚 — 极端信号指数衰减，避免追高/杀跌")
    print("=" * 70)

    if not results_with_returns:
        print("  无数据")
        return

    df = pd.DataFrame(results_with_returns)
    if "net_flow_ratio_capped" not in df.columns:
        print("  [错误] 未计算饱和度惩罚")
        return
    df = df.dropna(subset=["next_day_return", "net_flow_ratio", "net_flow_ratio_capped"])

    if len(df) < 5:
        print(f"  有效样本过少 ({len(df)} 条)")
        return

    raw_ic = df["net_flow_ratio"].rank().corr(df["next_day_return"].rank())
    capped_ic = df["net_flow_ratio_capped"].rank().corr(df["next_day_return"].rank())

    print(f"\n  惩罚公式: signal_capped = signal * exp(-max(0, |signal|-0.15) * 10)")
    print(f"  含义: |净流入比| > 15% 时开始衰减，越极端衰减越厉害")
    print()
    print(f"  {'指标':<20} {'原始信号':<16} {'饱和度惩罚':<16}")
    print(f"  {'-'*52}")
    print(f"  {'Rank IC':<20} {raw_ic:<16.4f} {capped_ic:<16.4f}")

    extreme_mask = df["net_flow_ratio"].abs() > 0.15
    n_extreme = extreme_mask.sum()
    print(f"\n  极端信号数 (|signal|>15%): {n_extreme}/{len(df)}")

    if n_extreme > 0:
        extreme_raw_ret = df.loc[extreme_mask, "next_day_return"].mean()
        extreme_capped_vals = df.loc[extreme_mask, "net_flow_ratio_capped"]
        print(f"  极端信号原始平均收益: {extreme_raw_ret*100:.2f}%")

    n_groups = min(5, max(2, len(df) // 5))
    df["capped_group"] = pd.qcut(df["net_flow_ratio_capped"], n_groups, labels=False, duplicates="drop")
    group_means = df.groupby("capped_group")["next_day_return"].mean()

    print(f"\n  饱和度惩罚后分层回测:")
    print(f"  {'组别':<6} {'样本数':<8} {'次日平均收益':<16}")
    print(f"  {'-'*30}")
    for g in sorted(df["capped_group"].unique()):
        g_data = df[df["capped_group"] == g]
        print(f"  G{g:<5} {len(g_data):<8} {g_data['next_day_return'].mean()*100:>8.4f}%")

    is_monotonic = all(group_means.iloc[i] <= group_means.iloc[i+1] for i in range(len(group_means)-1))
    spread = group_means.iloc[-1] - group_means.iloc[0]

    print(f"\n  多空收益差: {spread*100:.4f}%")
    if is_monotonic:
        print("  ✅ 分层收益单调递增")

    print(f"\n  结论: ", end="")
    if capped_ic > raw_ic + 0.01 or (capped_ic > raw_ic and is_monotonic):
        print("✅ 优化2 可行，饱和度惩罚改善了因子质量")
    elif capped_ic > raw_ic:
        print("➡️  优化2 微弱改善，可考虑采用")
    else:
        print("⚠️  优化2 未带来改善，暂不建议采用")


def _validate_opt3_separate_thresholds(results_with_returns):
    print("\n" + "=" * 70)
    print("【优化3】买卖阈值分离 — 买P95×1.5, 卖P90×1.2")
    print("=" * 70)

    if not results_with_returns:
        print("  无数据")
        return

    df = pd.DataFrame(results_with_returns)
    if "separate_net_ratio" not in df.columns:
        print("  [错误] 未计算分离阈值信号")
        return
    df = df.dropna(subset=["next_day_return", "net_flow_ratio", "separate_net_ratio"])

    if len(df) < 5:
        print(f"  有效样本过少 ({len(df)} 条)")
        return

    unified_ic = df["net_flow_ratio"].rank().corr(df["next_day_return"].rank())
    separate_ic = df["separate_net_ratio"].rank().corr(df["next_day_return"].rank())

    print(f"\n  逻辑: 机构买入单笔金额通常更大(集中下注) → 用 P95×1.5 高门槛")
    print(f"        机构卖出往往拆单分散(算法交易) → 用 P90×1.2 低门槛捕捉")
    print()
    print(f"  {'指标':<20} {'统一阈值':<16} {'买卖分离':<16}")
    print(f"  {'-'*52}")
    print(f"  {'Rank IC':<20} {unified_ic:<16.4f} {separate_ic:<16.4f}")

    unified_ics, separate_ics = [], []
    for date, group in df.groupby("date"):
        if len(group) >= 3:
            unified_ics.append(group["net_flow_ratio"].rank().corr(group["next_day_return"].rank()))
            separate_ics.append(group["separate_net_ratio"].rank().corr(group["next_day_return"].rank()))

    if unified_ics:
        u_mean = np.mean(unified_ics)
        s_mean = np.mean(separate_ics)
        u_icir = u_mean / np.std(unified_ics) if np.std(unified_ics) > 0 else 0
        s_icir = s_mean / np.std(separate_ics) if np.std(separate_ics) > 0 else 0
        print(f"  {'截面IC均值':<20} {u_mean:<16.4f} {s_mean:<16.4f}")
        print(f"  {'ICIR':<20} {u_icir:<16.4f} {s_icir:<16.4f}")

    u_buy = df[df["net_flow_ratio"] > 0]
    s_buy = df[df["separate_net_ratio"] > 0]
    u_sell = df[df["net_flow_ratio"] < 0]
    s_sell = df[df["separate_net_ratio"] < 0]

    print()
    if len(u_buy) > 0 and len(s_buy) > 0:
        u_buy_acc = (u_buy["next_day_return"] > 0).mean()
        s_buy_acc = (s_buy["next_day_return"] > 0).mean()
        print(f"  {'买入准确率':<20} {u_buy_acc:<16.1%} {s_buy_acc:<16.1%}")
    if len(u_sell) > 0 and len(s_sell) > 0:
        u_sell_acc = (u_sell["next_day_return"] < 0).mean()
        s_sell_acc = (s_sell["next_day_return"] < 0).mean()
        print(f"  {'卖出准确率':<20} {u_sell_acc:<16.1%} {s_sell_acc:<16.1%}")

    n_groups = min(5, max(2, len(df) // 5))
    df["sep_group"] = pd.qcut(df["separate_net_ratio"], n_groups, labels=False, duplicates="drop")
    sep_group_means = df.groupby("sep_group")["next_day_return"].mean()

    print(f"\n  买卖分离后分层回测:")
    print(f"  {'组别':<6} {'样本数':<8} {'平均分离信号':<16} {'次日平均收益':<16}")
    print(f"  {'-'*52}")
    for g in sorted(df["sep_group"].unique()):
        g_data = df[df["sep_group"] == g]
        print(f"  G{g:<5} {len(g_data):<8} {g_data['separate_net_ratio'].mean():<16.6f} "
              f"{g_data['next_day_return'].mean()*100:>8.4f}%")

    sep_spread = sep_group_means.iloc[-1] - sep_group_means.iloc[0] if len(sep_group_means) >= 2 else 0
    print(f"\n  多空收益差: {sep_spread*100:.4f}%")

    avg_p95 = df["p95_threshold"].mean()
    avg_p90 = df["p90_threshold"].mean()
    avg_sell_t = df["sell_threshold"].mean()
    avg_dyn_t = df["dynamic_threshold"].mean()
    if avg_p90 > 0:
        print(f"\n  平均买入阈值 (P95×1.5): {avg_dyn_t:,.0f} 元")
        print(f"  平均卖出阈值 (P90×1.2): {avg_sell_t:,.0f} 元")
        print(f"  买卖阈值比: {avg_dyn_t/avg_sell_t:.2f}x")
        print(f"  平均买入主力笔数: {df['sep_buy_count'].mean():.1f}")
        print(f"  平均卖出主力笔数: {df['sep_sell_count'].mean():.1f}")

    print(f"\n  结论: ", end="")
    if separate_ic > unified_ic + 0.01:
        print("✅ 优化3 可行，买卖分离显著提升 IC，建议采用")
    elif separate_ic > unified_ic:
        print("➡️  优化3 微弱改善 IC，可考虑采用")
    else:
        print("⚠️  优化3 未带来 IC 改善，暂不建议采用")


def _apply_zscore_standardization(results_with_returns):
    if len(results_with_returns) < 3:
        return
    df = pd.DataFrame(results_with_returns)
    for date in df["date"].unique():
        mask = df["date"] == date
        vals = df.loc[mask, "net_flow_ratio"]
        if len(vals) >= 3:
            std_v = vals.std()
            df.loc[mask, "net_flow_ratio_zscore"] = (vals - vals.mean()) / std_v if std_v > 0 else 0.0
        else:
            df.loc[mask, "net_flow_ratio_zscore"] = 0.0
    for i in range(len(results_with_returns)):
        results_with_returns[i]["net_flow_ratio_zscore"] = df.iloc[i]["net_flow_ratio_zscore"]


def _validate_opt1_zscore(results_with_returns):
    print("\n" + "=" * 70)
    print("【优化1】截面 Z-score 标准化 — 消除市场整体涨跌干扰")
    print("=" * 70)

    if not results_with_returns:
        print("  无数据")
        return

    df = pd.DataFrame(results_with_returns)
    if "net_flow_ratio_zscore" not in df.columns:
        print("  [错误] 未计算 z-score，请先调用 _apply_zscore_standardization()")
        return
    df = df.dropna(subset=["next_day_return", "net_flow_ratio", "net_flow_ratio_zscore"])

    if len(df) < 5:
        print(f"  有效样本过少 ({len(df)} 条)")
        return

    raw_ic = df["net_flow_ratio"].rank().corr(df["next_day_return"].rank())
    zscore_ic = df["net_flow_ratio_zscore"].rank().corr(df["next_day_return"].rank())

    print(f"\n  {'指标':<20} {'原始信号':<16} {'Z-score标准化':<16}")
    print(f"  {'-'*52}")
    print(f"  {'Rank IC':<20} {raw_ic:<16.4f} {zscore_ic:<16.4f}")

    raw_ics, zscore_ics = [], []
    for date, group in df.groupby("date"):
        if len(group) >= 3:
            raw_ics.append(group["net_flow_ratio"].rank().corr(group["next_day_return"].rank()))
            zscore_ics.append(group["net_flow_ratio_zscore"].rank().corr(group["next_day_return"].rank()))

    if raw_ics:
        raw_mean = np.mean(raw_ics)
        zscore_mean = np.mean(zscore_ics)
        raw_icir = raw_mean / np.std(raw_ics) if np.std(raw_ics) > 0 else 0
        zscore_icir = zscore_mean / np.std(zscore_ics) if np.std(zscore_ics) > 0 else 0
        raw_pos = sum(1 for x in raw_ics if x > 0) / len(raw_ics)
        zscore_pos = sum(1 for x in zscore_ics if x > 0) / len(zscore_ics)

        print(f"  {'截面IC均值':<20} {raw_mean:<16.4f} {zscore_mean:<16.4f}")
        print(f"  {'ICIR':<20} {raw_icir:<16.4f} {zscore_icir:<16.4f}")
        print(f"  {'IC正值占比':<20} {raw_pos:<16.1%} {zscore_pos:<16.1%}")

        print()
        if zscore_ic > raw_ic + 0.01:
            print(f"  ✅ Z-score 标准化后 IC 明显提升 ({raw_ic:.4f} → {zscore_ic:.4f})")
        elif zscore_ic > raw_ic:
            print(f"  ➡️  Z-score 标准化后 IC 微升 ({raw_ic:.4f} → {zscore_ic:.4f})")
        else:
            print(f"  ⚠️  Z-score 标准化后 IC 未提升 ({raw_ic:.4f} → {zscore_ic:.4f})")

        if abs(zscore_icir) > abs(raw_icir):
            print(f"  ✅ ICIR 改善 ({raw_icir:.4f} → {zscore_icir:.4f})")
        else:
            print(f"  ⚠️  ICIR 未改善 ({raw_icir:.4f} → {zscore_icir:.4f})")

    print(f"\n  结论: ", end="")
    if zscore_ic > raw_ic and (not raw_ics or abs(zscore_icir) > abs(raw_icir)):
        print("✅ 优化1 可行，建议正式采用截面标准化")
    else:
        print("⚠️  优化1 效果不显著，暂不建议采用")


def run_validation(stocks, trading_dates, tdx):
    all_results = []
    results_with_returns = []

    total_tasks = len(stocks) * len(trading_dates)
    done = 0

    for stock in stocks:
        market = stock["market"]
        code = stock["code"]

        for date_int in trading_dates:
            done += 1
            print(f"\r  进度 {done}/{total_tasks} — {code} @ {date_int}      ", end="")
            sys.stdout.flush()

            signal = _compute_factor_signal(tdx, market, code, date_int)
            if signal is None:
                continue

            signal["cap_bucket"] = stock.get("cap_bucket", "未知")
            signal["name"] = stock.get("name", "")
            all_results.append(signal)

            next_ret = _get_next_day_return(tdx, market, code, date_int)
            if next_ret is not None:
                row = dict(signal)
                row["next_day_return"] = next_ret
                results_with_returns.append(row)

            time.sleep(0.15)

    print()
    return all_results, results_with_returns


def main():
    parser = argparse.ArgumentParser(description="动态阈值主力单因子有效性验证")
    parser.add_argument("--stocks", type=int, default=6, help="每个市值段抽样股票数 (默认6)")
    parser.add_argument("--days", type=int, default=3, help="回测天数 (默认3)")
    parser.add_argument("--codes", type=str, default=None, help="指定股票代码，逗号分隔")
    args = parser.parse_args()

    print("=" * 70)
    print("动态阈值主力单因子 — 有效性验证")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    with connect(DEFAULT_IP, DEFAULT_PORT) as tdx:
        trading_dates = _get_trading_dates(tdx, count=args.days + 5)
        if len(trading_dates) < args.days + 1:
            print("[错误] 交易日数据不足")
            return
        test_dates = trading_dates[-(args.days + 1):-1]
        print(f"\n测试日期: {test_dates}")

        if args.codes:
            stocks = []
            for code in args.codes.split(","):
                code = code.strip().zfill(6)
                market = 0 if code.startswith(("0", "3")) else 1
                try:
                    finance = tdx.get_finance_info(market=market, code=code)
                    liutong = finance.get("liutongguben", 0)
                    quote = tdx.get_security_quotes((market, code))
                    price = quote[0]["price"] if quote else 0
                    if price <= 0:
                        recent = _fetch_daily_bars_single(tdx, market, code, 5)
                        if recent:
                            price = recent[-1]["close"]
                    cap = liutong * price if liutong > 0 and price > 0 else 0
                except Exception:
                    cap = 0
                if cap >= 5e10:
                    bucket = "大盘"
                elif cap >= 1e10:
                    bucket = "中盘"
                else:
                    bucket = "小盘"
                stocks.append({
                    "market": market, "code": code, "name": code,
                    "cap": cap, "cap_bucket": bucket,
                })
        else:
            stocks = _sample_stocks(tdx, n_per_bucket=args.stocks)

        if not stocks:
            print("[错误] 无可用股票")
            return

        print(f"\n开始计算因子信号 ({len(stocks)} 只股票 x {len(test_dates)} 天)...")
        all_results, results_with_returns = run_validation(stocks, test_dates, tdx)

    print(f"\n计算完成: 共 {len(all_results)} 条信号, "
          f"{len(results_with_returns)} 条含次日收益")

    if not all_results:
        print("[错误] 未获取到任何有效信号数据")
        return

    _apply_zscore_standardization(results_with_returns)
    _apply_saturation_penalty(results_with_returns)

    _validate_threshold_adaptation(all_results)
    _validate_ic_test(results_with_returns)
    _validate_signal_accuracy(results_with_returns)
    _validate_quantile_analysis(results_with_returns)
    _validate_vs_fixed(results_with_returns)
    _validate_opt1_zscore(results_with_returns)
    _validate_opt2_saturation(results_with_returns)
    _validate_opt3_separate_thresholds(results_with_returns)

    print("\n" + "=" * 70)
    print("验证完毕")
    print("=" * 70)


if __name__ == "__main__":
    main()
