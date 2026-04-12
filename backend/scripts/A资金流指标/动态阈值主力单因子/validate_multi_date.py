import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../"))

import numpy as np
from utils.pytdx_client import connect, DEFAULT_IP, DEFAULT_PORT
from utils.tushare_client import pro

TRADE_DATES = [
    "20260410",
    "20260409",
    "20260408",
    "20260407",
    "20260403",
    "20260402",
    "20260401",
    "20260331",
    "20260330",
    "20260327",
]

MARKET_CAP_BINS = {
    "超小盘(<30亿)": (0, 30),
    "小盘(30-100亿)": (30, 100),
    "中盘(100-500亿)": (100, 500),
    "大盘(500-2000亿)": (500, 2000),
    "超大盘(>2000亿)": (2000, float("inf")),
}


def _fetch_all_ticks(tdx, market, code, date_int):
    ticks = []
    for start in range(0, 300000, 500):
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


def calc_direction(ticks, threshold):
    mainforce = [t for t in ticks if t["amount"] >= threshold]
    if not mainforce:
        return None, 0, 0, 0
    buy = sum(t["amount"] for t in mainforce if t["buyorsell"] == 0)
    sell = sum(t["amount"] for t in mainforce if t["buyorsell"] == 1)
    net = buy - sell
    direction = "流入" if net > 0 else "流出"
    return direction, net, len(mainforce), len(mainforce) / len(ticks) * 100


def get_thresholds(ticks, finance_info, price):
    amounts = [t["amount"] for t in ticks]
    q1 = np.percentile(amounts, 25)
    q3 = np.percentile(amounts, 75)
    iqr = q3 - q1

    liutongguben = finance_info["liutongguben"]
    free_market_cap = liutongguben * price
    threshold_base = free_market_cap * 0.00001 / 100
    p95 = np.percentile(amounts, 95)

    return {
        "IQR(Q3+1.5×IQR)": q3 + 1.5 * iqr,
        "P90": np.percentile(amounts, 90),
        "P95": p95,
        "原方案": max(threshold_base, p95 * 1.5),
        "固定20万": 200000,
        "固定100万": 1000000,
    }


def classify_cap(market_cap_yi):
    for label, (lo, hi) in MARKET_CAP_BINS.items():
        if lo <= market_cap_yi < hi:
            return label
    return "未知"


def process_date(tdx, trade_date_str):
    trade_date_int = int(trade_date_str)
    print(f"\n{'━'*120}")
    print(f"  处理日期: {trade_date_str}")
    print(f"{'━'*120}")

    df_lhb = pro.top_list(trade_date=trade_date_str)
    if df_lhb is None or df_lhb.empty:
        print(f"  无龙虎榜数据, 跳过")
        return []

    lhb_stocks = []
    for ts_code in df_lhb["ts_code"].unique():
        sub = df_lhb[df_lhb["ts_code"] == ts_code]
        row = sub.iloc[0]
        if ts_code.endswith(".BJ"):
            continue
        net_amount = row["net_amount"]
        direction = "流入" if net_amount > 0 else "流出"
        lhb_stocks.append({
            "ts_code": ts_code,
            "name": row["name"],
            "close": row["close"],
            "pct_change": row["pct_change"],
            "net_amount": net_amount,
            "direction": direction,
            "reason": row["reason"],
        })

    print(f"  龙虎榜: {len(lhb_stocks)} 只股票 (排除北交所)")

    results = []
    for s in lhb_stocks:
        ts_code = s["ts_code"]
        code = ts_code[:6]
        market = 0 if ts_code.endswith(".SZ") else 1

        ticks = _fetch_all_ticks(tdx, market, code, trade_date_int)
        if not ticks:
            continue

        finance_info = tdx.get_finance_info(market=market, code=code)
        price = ticks[-1]["price"]
        liutongguben = finance_info.get("liutongguben", 0)
        market_cap_yi = liutongguben * price / 1e8

        thresholds = get_thresholds(ticks, finance_info, price)

        try:
            df_moneyflow = pro.moneyflow(
                ts_code=ts_code, start_date=trade_date_str, end_date=trade_date_str
            )
        except Exception:
            df_moneyflow = None

        ts_big_dir = None
        ts_huge_dir = None
        if df_moneyflow is not None and not df_moneyflow.empty:
            mf_row = df_moneyflow.iloc[0]
            buy_lg = mf_row.get("buy_lg_amount", 0) or 0
            sell_lg = mf_row.get("sell_lg_amount", 0) or 0
            buy_elg = mf_row.get("buy_elg_amount", 0) or 0
            sell_elg = mf_row.get("sell_elg_amount", 0) or 0
            big_net = (buy_lg - sell_lg) * 10000
            huge_net = (buy_elg - sell_elg) * 10000
            ts_big_dir = "流入" if big_net > 0 else "流出"
            ts_huge_dir = "流入" if huge_net > 0 else "流出"

        ground_truth = s["direction"]

        row_result = {
            "trade_date": trade_date_str,
            "ts_code": ts_code,
            "name": s["name"],
            "pct_change": s["pct_change"] or 0,
            "close": s["close"] or 0,
            "ground_truth": ground_truth,
            "lhb_net_wan": s["net_amount"] / 10000,
            "market_cap_yi": market_cap_yi,
            "cap_label": classify_cap(market_cap_yi),
            "ticks_count": len(ticks),
            "ts_big_dir": ts_big_dir,
            "ts_huge_dir": ts_huge_dir,
        }

        for method_name, threshold in thresholds.items():
            direction, net, count, pct = calc_direction(ticks, threshold)
            if direction is None:
                continue
            match = direction == ground_truth
            row_result[f"{method_name}_方向"] = direction
            row_result[f"{method_name}_净额万"] = net / 10000
            row_result[f"{method_name}_笔数"] = count
            row_result[f"{method_name}_占比"] = pct
            row_result[f"{method_name}_match"] = match

        results.append(row_result)

    print(f"  成功处理: {len(results)}/{len(lhb_stocks)} 只")
    return results


def main():
    import io

    report = io.StringIO()

    def p(s=""):
        print(s)
        report.write(s + "\n")

    p(f"\n{'='*120}")
    p(f"  多日期 × 多盘口 龙虎榜基准验证")
    p(f"  原始策略 vs 其他方法 — 用交易所公开龙虎榜买卖净额作为 ground truth")
    p(f"  验证日期: {', '.join(TRADE_DATES)}")
    p(f"{'='*120}")

    all_results = []

    with connect(DEFAULT_IP, DEFAULT_PORT) as tdx:
        for trade_date_str in TRADE_DATES:
            date_results = process_date(tdx, trade_date_str)
            all_results.extend(date_results)
            time.sleep(0.3)

    if not all_results:
        p("\n无有效数据")
        return

    total = len(all_results)
    inflow_count = sum(1 for r in all_results if r["ground_truth"] == "流入")
    outflow_count = total - inflow_count
    p(f"\n{'='*120}")
    p(f"  汇总: 共 {total} 只×日 样本 (流入 {inflow_count}, 流出 {outflow_count})")
    p(f"{'='*120}")

    p(f"\n[1] 总体一致率")
    p(f"\n  {'方法':<25s} {'一致✅':>6s} {'不一致❌':>8s} {'总数':>6s} {'一致率':>8s}")
    p(f"  {'─'*60}")

    method_keys = ["原方案", "IQR(Q3+1.5×IQR)", "P90", "P95", "固定20万", "固定100万"]
    all_methods = [("ts大单", None), ("ts特大单", None)]
    for mk in method_keys:
        all_methods.append((mk, f"{mk}_match"))

    for method_label, match_key in all_methods:
        if method_label == "ts大单":
            match_count = sum(1 for r in all_results if r.get("ts_big_dir") == r["ground_truth"])
            mismatch_count = sum(1 for r in all_results if r.get("ts_big_dir") and r["ts_big_dir"] != r["ground_truth"])
        elif method_label == "ts特大单":
            match_count = sum(1 for r in all_results if r.get("ts_huge_dir") == r["ground_truth"])
            mismatch_count = sum(1 for r in all_results if r.get("ts_huge_dir") and r["ts_huge_dir"] != r["ground_truth"])
        else:
            match_count = sum(1 for r in all_results if match_key in r and r[match_key] is True)
            mismatch_count = sum(1 for r in all_results if match_key in r and r[match_key] is False)

        n = match_count + mismatch_count
        rate = match_count / n * 100 if n > 0 else 0
        p(f"  {method_label:<25s} {match_count:>6d} {mismatch_count:>8d} {n:>6d} {rate:>7.1f}%")

    p(f"\n[2] 按盘口分组一致率")
    header = f"\n  {'盘口':<20s} {'样本数':>6s} │ "
    for method_label, _ in all_methods:
        header += f"{method_label:>10s}  "
    p(header)
    p(f"  {'─'*120}")

    cap_order = list(MARKET_CAP_BINS.keys())
    cap_groups = {}
    for r in all_results:
        label = r["cap_label"]
        cap_groups.setdefault(label, []).append(r)

    for cap_label in cap_order:
        group = cap_groups.get(cap_label, [])
        if not group:
            continue
        n = len(group)
        line = f"  {cap_label:<20s} {n:>6d} │ "
        for method_label, match_key in all_methods:
            if method_label == "ts大单":
                mc = sum(1 for r in group if r.get("ts_big_dir") == r["ground_truth"])
                tc = sum(1 for r in group if r.get("ts_big_dir") and r["ts_big_dir"] != r["ground_truth"])
            elif method_label == "ts特大单":
                mc = sum(1 for r in group if r.get("ts_huge_dir") == r["ground_truth"])
                tc = sum(1 for r in group if r.get("ts_huge_dir") and r["ts_huge_dir"] != r["ground_truth"])
            else:
                mc = sum(1 for r in group if match_key in r and r[match_key] is True)
                tc = sum(1 for r in group if match_key in r and r[match_key] is False)
            total_n = mc + tc
            rate = mc / total_n * 100 if total_n > 0 else 0
            line += f"{rate:>9.1f}%  "
        p(line)

    p(f"\n[3] 按涨跌方向分组 (龙虎榜流入 vs 流出)")
    for gt_dir in ["流入", "流出"]:
        group = [r for r in all_results if r["ground_truth"] == gt_dir]
        if not group:
            continue
        p(f"\n  龙虎榜{gt_dir} ({len(group)} 只×日):")
        p(f"  {'方法':<25s} {'一致✅':>6s} {'不一致❌':>8s} {'一致率':>8s}")
        p(f"  {'─'*55}")
        for method_label, match_key in all_methods:
            if method_label == "ts大单":
                mc = sum(1 for r in group if r.get("ts_big_dir") == r["ground_truth"])
                tc = sum(1 for r in group if r.get("ts_big_dir") and r["ts_big_dir"] != r["ground_truth"])
            elif method_label == "ts特大单":
                mc = sum(1 for r in group if r.get("ts_huge_dir") == r["ground_truth"])
                tc = sum(1 for r in group if r.get("ts_huge_dir") and r["ts_huge_dir"] != r["ground_truth"])
            else:
                mc = sum(1 for r in group if match_key in r and r[match_key] is True)
                tc = sum(1 for r in group if match_key in r and r[match_key] is False)
            n = mc + tc
            rate = mc / n * 100 if n > 0 else 0
            p(f"  {method_label:<25s} {mc:>6d} {tc:>8d} {rate:>7.1f}%")

    p(f"\n[4] 原方案主力占比 vs 市值分布")
    p(f"\n  {'盘口':<20s} {'样本数':>6s} {'平均占比%':>10s} {'中位数%':>10s} {'平均阈值(万)':>14s}")
    p(f"  {'─'*65}")
    for cap_label in cap_order:
        group = cap_groups.get(cap_label, [])
        if not group:
            continue
        pcts = [r["原方案_占比"] for r in group if "原方案_占比" in r]
        nets = [abs(r.get("原方案_净额万", 0) or 0) for r in group if "原方案_净额万" in r]
        if pcts:
            avg_pct = np.mean(pcts)
            med_pct = np.median(pcts)
            avg_net = np.mean(nets) if nets else 0
            p(f"  {cap_label:<20s} {len(group):>6d} {avg_pct:>10.1f} {med_pct:>10.1f} {avg_net:>14,.0f}")

    p(f"\n[5] 逐只股票明细 (原方案方向 vs 龙虎榜)")
    p(f"\n  {'日期':>10s} {'代码':>12s} {'名称':>8s} {'涨跌':>6s} {'市值(亿)':>10s} {'盘口':>16s} {'龙虎榜方向':>8s} {'原方案方向':>8s} {'一致':>4s}")
    p(f"  {'─'*100}")
    for r in sorted(all_results, key=lambda x: x["market_cap_yi"]):
        gt = r["ground_truth"]
        orig_dir = r.get("原方案_方向", "N/A") or "N/A"
        match = "✅" if r.get("原方案_match") is True else "❌"
        p(f"  {r['trade_date']:>10s} {r['ts_code']:>12s} {r['name']:>8s} {r['pct_change']:>+5.1f}% {r['market_cap_yi']:>9.1f} {r['cap_label']:>16s} {gt:>6s} {orig_dir:>6s} {match:>3s}")

    p(f"\n{'='*120}")
    p(f"  样本总数: {total} 只×日 ({len(TRADE_DATES)} 个交易日)")
    p(f"  原始策略 = max(流通市值×0.00001%, P95×1.5)")
    p(f"  ground truth = 交易所龙虎榜净额方向")
    p(f"{'='*120}")

    report_path = os.path.join(os.path.dirname(__file__), "validate_multi_date_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report.getvalue())
    print(f"\n报告已保存到: {report_path}")


if __name__ == "__main__":
    main()
