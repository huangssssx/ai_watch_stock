import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../"))

import numpy as np
from utils.pytdx_client import connect, DEFAULT_IP, DEFAULT_PORT
from dynamic_threshold import calculate_dynamic_threshold


def analyze_stock(market: int, code: str, date: int = None) -> dict:
    with connect(DEFAULT_IP, DEFAULT_PORT) as tdx:
        threshold_info = calculate_dynamic_threshold(market, code, date, tdx=tdx)
        dynamic_threshold = threshold_info["动态阈值(元)"]

        all_transactions = []
        if date is None:
            for start in range(0, 100000, 500):
                transactions = tdx.get_transaction_data(
                    market=market, code=code, start=start, count=500
                )
                if not transactions:
                    break
                for t in transactions:
                    t["amount"] = t["vol"] * 100 * t["price"]
                    t["is_mainforce_candidate"] = t["amount"] >= dynamic_threshold
                    all_transactions.append(t)
        else:
            for start in range(0, 100000, 500):
                transactions = tdx.get_history_transaction_data(
                    market=market, code=code, start=start, count=500, date=date
                )
                if not transactions:
                    break
                for t in transactions:
                    t["amount"] = t["vol"] * 100 * t["price"]
                    t["is_mainforce_candidate"] = t["amount"] >= dynamic_threshold
                    all_transactions.append(t)

        mainforce_candidates = [t for t in all_transactions if t["is_mainforce_candidate"]]

        if mainforce_candidates:
            candidate_amounts = [t["amount"] for t in mainforce_candidates]
            candidate_vols = [t["vol"] for t in mainforce_candidates]

            buy_candidates = [t for t in mainforce_candidates if t["buyorsell"] in [1, 2]]
            sell_candidates = [t for t in mainforce_candidates if t["buyorsell"] in [0, 8]]

            net_flow = sum(t["amount"] for t in buy_candidates) - sum(
                t["amount"] for t in sell_candidates
            )
        else:
            candidate_amounts = []
            candidate_vols = []
            buy_candidates = []
            sell_candidates = []
            net_flow = 0

        return {
            "code": code,
            "market": market,
            "market_name": "深圳" if market == 0 else "上海",
            "dynamic_threshold": dynamic_threshold,
            "total_transactions": len(all_transactions),
            "mainforce_candidate_count": len(mainforce_candidates),
            "mainforce_candidate_ratio": (
                len(mainforce_candidates) / len(all_transactions) * 100
                if all_transactions
                else 0
            ),
            "total_mainforce_amount": sum(c["amount"] for c in mainforce_candidates),
            "avg_mainforce_amount": (
                np.mean(candidate_amounts) if candidate_amounts else 0
            ),
            "max_mainforce_amount": max(candidate_amounts) if candidate_amounts else 0,
            "buy_count": len(buy_candidates),
            "sell_count": len(sell_candidates),
            "buy_amount": sum(t["amount"] for t in buy_candidates),
            "sell_amount": sum(t["amount"] for t in sell_candidates),
            "net_flow": net_flow,
            "mainforce_candidates": mainforce_candidates,
        }


def print_report(result: dict):
    print(f"\n{'='*60}")
    print(f"股票: {result['market_name']} {result['code']}")
    print(f"{'='*60}")
    print(f"\n【动态阈值】")
    print(f"  动态阈值: {result['dynamic_threshold']:,.2f} 元")

    print(f"\n【当日逐笔成交统计】")
    print(f"  总笔数: {result['total_transactions']}")
    print(f"  主力候选单笔数: {result['mainforce_candidate_count']}")
    print(f"  主力候选单占比: {result['mainforce_candidate_ratio']:.2f}%")

    if result["mainforce_candidate_count"] > 0:
        print(f"\n【主力候选单详情】")
        print(f"  主力候选单总金额: {result['total_mainforce_amount']:,.2f} 元")
        print(f"  平均单笔金额: {result['avg_mainforce_amount']:,.2f} 元")
        print(f"  最大单笔金额: {result['max_mainforce_amount']:,.2f} 元")

        print(f"\n【买卖方向分析】")
        print(f"  买入笔数: {result['buy_count']}")
        print(f"  卖出笔数: {result['sell_count']}")
        print(f"  买入金额: {result['buy_amount']:,.2f} 元")
        print(f"  卖出金额: {result['sell_amount']:,.2f} 元")
        print(f"  净流入: {result['net_flow']:,.2f} 元")

        print(f"\n【主力候选单明细】(金额单位: 元)")
        print(f"{'-'*60}")
        print(f"{'时间':<10} {'价格':<10} {'成交量(手)':<12} {'金额':<15} {'方向':<6}")
        print(f"{'-'*60}")
        for t in result["mainforce_candidates"][:20]:
            direction = "买入" if t["buyorsell"] in [1, 2] else "卖出"
            print(
                f"{t['time']:<10} {t['price']:<10.2f} {t['vol']:<12} {t['amount']:<15,.2f} {direction:<6}"
            )
        if len(result["mainforce_candidates"]) > 20:
            print(f"  ... (还有 {len(result['mainforce_candidates']) - 20} 条)")
    else:
        print(f"\n【结论】: 今日无主力候选单触发")

    print(f"\n{'='*60}")
    if result["mainforce_candidate_count"] == 0:
        print("结论: 今日无明显主力活动信号")
    elif abs(result["net_flow"]) < result["total_mainforce_amount"] * 0.2:
        print("结论: 主力多空博弈，方向不明")
    elif result["net_flow"] > 0:
        print(f"结论: 主力净买入 {result['net_flow']:,.2f} 元，看多信号")
    else:
        print(f"结论: 主力净卖出 {abs(result['net_flow']):,.2f} 元，看空信号")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python use_dynamic_threshold.py <股票代码> [日期YYYYMMDD]")
        print("示例: python use_dynamic_threshold.py 000001")
        print("      python use_dynamic_threshold.py 000001 20260410")
        sys.exit(1)

    code = sys.argv[1]
    date = int(sys.argv[2]) if len(sys.argv) > 2 else None
    market = 0 if code.startswith("0") else 1

    result = analyze_stock(market, code, date)
    print_report(result)
