import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../"))

import numpy as np
from utils.pytdx_client import connect, DEFAULT_IP, DEFAULT_PORT
from utils.tushare_client import pro


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
        "原方案(max(市值,P95×1.5))": max(threshold_base, p95 * 1.5),
        "固定20万": 200000,
        "固定100万": 1000000,
        "tushare大单(20万-100万)": 200000,
        "tushare特大单(≥100万)": 1000000,
    }


def main(trade_date_str):
    trade_date_int = int(trade_date_str)

    print(f"\n{'='*120}")
    print(f"  龙虎榜基准验证 — 用交易所公开的龙虎榜买卖净额作为 ground truth")
    print(f"  日期: {trade_date_str}")
    print(f"{'='*120}")

    print(f"\n[1] 拉取龙虎榜数据...")
    df_lhb = pro.top_list(trade_date=trade_date_str)
    if df_lhb is None or df_lhb.empty:
        print("  无龙虎榜数据")
        return

    print(f"  龙虎榜共 {len(df_lhb)} 条记录, {df_lhb['ts_code'].nunique()} 只股票")

    lhb_stocks = []
    for ts_code in df_lhb["ts_code"].unique():
        sub = df_lhb[df_lhb["ts_code"] == ts_code]
        row = sub.iloc[0]
        net_amount = row["net_amount"]
        direction = "流入" if net_amount > 0 else "流出"
        lhb_stocks.append({
            "ts_code": ts_code,
            "name": row["name"],
            "close": row["close"],
            "pct_change": row["pct_change"],
            "net_amount": net_amount,
            "l_buy": row["l_buy"],
            "l_sell": row["l_sell"],
            "direction": direction,
            "reason": row["reason"],
        })

    lhb_stocks.sort(key=lambda x: abs(x["net_amount"]), reverse=True)

    print(f"\n  {'代码':>12s} {'名称':>8s} {'涨跌%':>7s} {'龙虎榜净额(万)':>16s} {'方向':>6s} {'上榜原因'}")
    print(f"  {'─'*100}")
    for s in lhb_stocks[:10]:
        net_wan = s["net_amount"] / 10000
        print(f"  {s['ts_code']:>12s} {s['name']:>8s} {s['pct_change']:>+6.2f}% {net_wan:>+15,.0f} {s['direction']:>5s} {s['reason']}")

    print(f"\n[2] 拉取逐笔数据并计算各方法方向...")

    results = []

    with connect(DEFAULT_IP, DEFAULT_PORT) as tdx:
        for s in lhb_stocks:
            ts_code = s["ts_code"]
            code = ts_code[:6]
            market = 0 if ts_code.endswith(".SZ") else 1

            ticks = _fetch_all_ticks(tdx, market, code, trade_date_int)
            if not ticks:
                print(f"  {s['name']:>8s} ({ts_code}) — 无逐笔数据, 跳过")
                continue

            finance_info = tdx.get_finance_info(market=market, code=code)
            price = ticks[-1]["price"]

            thresholds = get_thresholds(ticks, finance_info, price)

            df_moneyflow = pro.moneyflow(
                ts_code=ts_code, start_date=trade_date_str, end_date=trade_date_str
            )
            ts_big_net = None
            ts_big_dir = None
            ts_huge_net = None
            ts_huge_dir = None
            if df_moneyflow is not None and not df_moneyflow.empty:
                row = df_moneyflow.iloc[0]
                buy_lg = row.get("buy_lg_amount", 0)
                sell_lg = row.get("sell_lg_amount", 0)
                buy_hg = row.get("buy_elg_amount", 0)
                sell_hg = row.get("sell_elg_amount", 0)
                ts_big_net = (buy_lg - sell_lg) * 10000
                ts_big_dir = "流入" if ts_big_net > 0 else "流出"
                ts_huge_net = (buy_hg - sell_hg) * 10000
                ts_huge_dir = "流入" if ts_huge_net > 0 else "流出"

            ground_truth = s["direction"]

            row_result = {
                "ts_code": ts_code,
                "name": s["name"],
                "pct_change": s["pct_change"],
                "ground_truth": ground_truth,
                "lhb_net_wan": s["net_amount"] / 10000,
                "ticks_count": len(ticks),
                "ts_big_net_wan": ts_big_net / 10000 if ts_big_net else None,
                "ts_big_dir": ts_big_dir,
                "ts_huge_net_wan": ts_huge_net / 10000 if ts_huge_net else None,
                "ts_huge_dir": ts_huge_dir,
            }

            for method_name, threshold in thresholds.items():
                direction, net, count, pct = calc_direction(ticks, threshold)
                if direction is None:
                    continue
                match = "✅" if direction == ground_truth else "❌"
                row_result[f"{method_name}_方向"] = direction
                row_result[f"{method_name}_净额万"] = net / 10000
                row_result[f"{method_name}_笔数"] = count
                row_result[f"{method_name}_占比"] = pct
                row_result[f"{method_name}_match"] = match

            results.append(row_result)

    if not results:
        print("  无有效结果")
        return

    print(f"\n  成功获取逐笔数据: {len(results)} 只股票")

    print(f"\n[3] 逐只股票对比")
    print(f"\n  {'='*140}")
    print(f"  {'代码':>12s} {'名称':>6s} {'涨跌':>6s} {'龙虎榜方向':>10s} {'龙虎榜净额万':>14s} │ {'ts大单方向':>10s} {'ts大单净额万':>14s} │ {'IQR方向':>8s} {'IQR净额万':>12s} │ {'P90方向':>8s} {'P90净额万':>12s} │ {'原方案方向':>10s} {'原方案净额万':>12s}")
    print(f"  {'─'*140}")

    for r in results:
        gt = r["ground_truth"]
        lhb_net = r["lhb_net_wan"] or 0
        pct = r.get("pct_change") or 0
        ts_dir = r.get("ts_big_dir") or "N/A"
        ts_net = r.get("ts_big_net_wan") or 0
        iqr_dir = r.get("IQR(Q3+1.5×IQR)_方向") or "N/A"
        iqr_net = r.get("IQR(Q3+1.5×IQR)_净额万") or 0
        p90_dir = r.get("P90_方向") or "N/A"
        p90_net = r.get("P90_净额万") or 0
        orig_dir = r.get("原方案(max(市值,P95×1.5))_方向") or "N/A"
        orig_net = r.get("原方案(max(市值,P95×1.5))_净额万") or 0

        ts_match = "✅" if ts_dir == gt else "❌" if ts_dir != "N/A" else "—"
        iqr_match = "✅" if iqr_dir == gt else "❌" if iqr_dir != "N/A" else "—"
        p90_match = "✅" if p90_dir == gt else "❌" if p90_dir != "N/A" else "—"
        orig_match = "✅" if orig_dir == gt else "❌" if orig_dir != "N/A" else "—"

        print(f"  {r['ts_code']:>12s} {r['name']:>6s} {pct:>+5.1f}% {gt:>8s} {lhb_net:>+13,.0f} │ {ts_dir:>6s}{ts_match} {ts_net:>+13,.0f} │ {iqr_dir:>6s}{iqr_match} {iqr_net:>+11,.0f} │ {p90_dir:>6s}{p90_match} {p90_net:>+11,.0f} │ {orig_dir:>6s}{orig_match} {orig_net:>+11,.0f}")

    print(f"\n[4] 汇总统计 — 各方法 vs 龙虎榜方向一致率")

    methods_to_check = [
        "ts大单",
        "IQR(Q3+1.5×IQR)",
        "P90",
        "P95",
        "原方案(max(市值,P95×1.5))",
        "固定20万",
        "固定100万",
    ]

    print(f"\n  {'方法':<30s} {'一致✅':>6s} {'不一致❌':>8s} {'一致率':>8s} {'备注'}")
    print(f"  {'─'*80}")

    for method_name in methods_to_check:
        match_key = f"{method_name}_match"
        dir_key = f"{method_name}_方向"
        if method_name == "ts大单":
            match_count = sum(1 for r in results if r.get("ts_big_dir") == r["ground_truth"])
            mismatch_count = sum(1 for r in results if r.get("ts_big_dir") and r["ts_big_dir"] != r["ground_truth"])
            no_data = sum(1 for r in results if not r.get("ts_big_dir"))
        else:
            if match_key not in results[0]:
                continue
            match_count = sum(1 for r in results if r.get(match_key) == "✅")
            mismatch_count = sum(1 for r in results if r.get(match_key) == "❌")
            no_data = sum(1 for r in results if match_key not in r or r.get(match_key) not in ["✅", "❌"])

        total = match_count + mismatch_count
        rate = match_count / total * 100 if total > 0 else 0
        note = ""
        if method_name == "ts大单":
            note = "tushare moneyflow 大单方向"
        elif method_name.startswith("固定"):
            note = "固定阈值基准线"
        elif method_name.startswith("tushare"):
            note = "tushare moneyflow"
        print(f"  {method_name:<30s} {match_count:>6d} {mismatch_count:>8d} {rate:>7.1f}% {note}")

    print(f"\n[5] 补充: tushare特大单方向 vs 龙虎榜")
    ts_huge_match = sum(1 for r in results if r.get("ts_huge_dir") == r["ground_truth"])
    ts_huge_mismatch = sum(1 for r in results if r.get("ts_huge_dir") and r["ts_huge_dir"] != r["ground_truth"])
    ts_huge_total = ts_huge_match + ts_huge_mismatch
    if ts_huge_total > 0:
        rate = ts_huge_match / ts_huge_total * 100
        print(f"  tushare特大单: {ts_huge_match}✅ / {ts_huge_mismatch}❌ = {rate:.1f}%")

    print(f"\n[6] 各方法主力占比分布")
    print(f"  {'方法':<30s} {'平均主力占比%':>14s} {'最小':>6s} {'最大':>6s}")
    print(f"  {'─'*60}")
    for method_name in methods_to_check:
        if method_name == "ts大单":
            continue
        pct_key = f"{method_name}_占比"
        if pct_key not in results[0]:
            continue
        pcts = [r[pct_key] for r in results if pct_key in r]
        if pcts:
            print(f"  {method_name:<30s} {np.mean(pcts):>14.1f} {min(pcts):>6.1f} {max(pcts):>6.1f}")

    print(f"\n{'='*120}")
    print(f"  结论: 龙虎榜净额方向 = 交易所公开的真实主力方向 (ground truth)")
    print(f"  各方法一致率越高, 说明该方法越能准确判断主力资金方向")
    print(f"{'='*120}")


if __name__ == "__main__":
    trade_date = sys.argv[1] if len(sys.argv) > 1 else "20260410"
    main(trade_date)
