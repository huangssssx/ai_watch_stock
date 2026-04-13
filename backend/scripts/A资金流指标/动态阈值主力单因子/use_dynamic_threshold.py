import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../"))

import numpy as np
from utils.pytdx_client import connect, DEFAULT_IP, DEFAULT_PORT
from dynamic_threshold import calculate_dynamic_threshold


def analyze_stock(market: int, code: str, date: int = None) -> dict:
    with connect(DEFAULT_IP, DEFAULT_PORT) as tdx:
        threshold_info = calculate_dynamic_threshold(market, code, date, tdx=tdx)
        buy_threshold = threshold_info["买入阈值(元)"]
        sell_threshold = threshold_info["卖出阈值(元)"]

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
                    all_transactions.append(t)

        buy_mainforce = [t for t in all_transactions if t["amount"] >= buy_threshold and t["buyorsell"] in [1, 2]]
        sell_mainforce = [t for t in all_transactions if t["amount"] >= sell_threshold and t["buyorsell"] in [0, 8]]
        all_mainforce = buy_mainforce + sell_mainforce

        if all_mainforce:
            all_mainforce_amounts = [t["amount"] for t in all_mainforce]
            buy_amount = sum(t["amount"] for t in buy_mainforce)
            sell_amount = sum(t["amount"] for t in sell_mainforce)
            net_flow = buy_amount - sell_amount
        else:
            all_mainforce_amounts = []
            buy_amount = 0
            sell_amount = 0
            net_flow = 0

        return {
            "code": code,
            "market": market,
            "market_name": "深圳" if market == 0 else "上海",
            "buy_threshold": buy_threshold,
            "sell_threshold": sell_threshold,
            "total_transactions": len(all_transactions),
            "buy_mainforce_count": len(buy_mainforce),
            "sell_mainforce_count": len(sell_mainforce),
            "mainforce_total_count": len(all_mainforce),
            "mainforce_ratio": (
                len(all_mainforce) / len(all_transactions) * 100
                if all_transactions
                else 0
            ),
            "total_mainforce_amount": sum(t["amount"] for t in all_mainforce),
            "avg_mainforce_amount": (
                np.mean(all_mainforce_amounts) if all_mainforce_amounts else 0
            ),
            "max_mainforce_amount": max(all_mainforce_amounts) if all_mainforce_amounts else 0,
            "buy_count": len(buy_mainforce),
            "sell_count": len(sell_mainforce),
            "buy_amount": buy_amount,
            "sell_amount": sell_amount,
            "net_flow": net_flow,
            "buy_mainforce": buy_mainforce,
            "sell_mainforce": sell_mainforce,
        }


def print_report(result: dict):
    print(f"\n{'='*60}")
    print(f"股票: {result['market_name']} {result['code']}")
    print(f"{'='*60}")
    print(f"\n【动态阈值（买卖分离）】")
    print(f"  买入阈值 (P95×1.5): {result['buy_threshold']:,.2f} 元")
    print(f"  卖出阈值 (P90×1.2): {result['sell_threshold']:,.2f} 元")

    print(f"\n【当日逐笔成交统计】")
    print(f"  总笔数: {result['total_transactions']}")
    print(f"  主力买入单笔数 (≥买入阈值): {result['buy_mainforce_count']}")
    print(f"  主力卖出单笔数 (≥卖出阈值): {result['sell_mainforce_count']}")
    print(f"  主力单占比: {result['mainforce_ratio']:.2f}%")

    if result["mainforce_total_count"] > 0:
        print(f"\n【主力单详情】")
        print(f"  主力单总金额: {result['total_mainforce_amount']:,.2f} 元")
        print(f"  平均单笔金额: {result['avg_mainforce_amount']:,.2f} 元")
        print(f"  最大单笔金额: {result['max_mainforce_amount']:,.2f} 元")

        print(f"\n【买卖方向分析】")
        print(f"  买入笔数: {result['buy_count']}  (阈值: {result['buy_threshold']:,.0f}元)")
        print(f"  卖出笔数: {result['sell_count']}  (阈值: {result['sell_threshold']:,.0f}元)")
        print(f"  买入金额: {result['buy_amount']:,.2f} 元")
        print(f"  卖出金额: {result['sell_amount']:,.2f} 元")
        print(f"  净流入: {result['net_flow']:,.2f} 元")

        print(f"\n【主力买入单明细】(金额单位: 元)")
        print(f"{'-'*60}")
        print(f"{'时间':<10} {'价格':<10} {'成交量(手)':<12} {'金额':<15}")
        print(f"{'-'*60}")
        for t in result["buy_mainforce"][:10]:
            print(
                f"{t['time']:<10} {t['price']:<10.2f} {t['vol']:<12} {t['amount']:<15,.2f}"
            )
        if len(result["buy_mainforce"]) > 10:
            print(f"  ... (还有 {len(result['buy_mainforce']) - 10} 条)")

        print(f"\n【主力卖出单明细】(金额单位: 元)")
        print(f"{'-'*60}")
        print(f"{'时间':<10} {'价格':<10} {'成交量(手)':<12} {'金额':<15}")
        print(f"{'-'*60}")
        for t in result["sell_mainforce"][:10]:
            print(
                f"{t['time']:<10} {t['price']:<10.2f} {t['vol']:<12} {t['amount']:<15,.2f}"
            )
        if len(result["sell_mainforce"]) > 10:
            print(f"  ... (还有 {len(result['sell_mainforce']) - 10} 条)")
    else:
        print(f"\n【结论】: 今日无主力活动信号")

    print(f"\n{'='*60}")
    if result["mainforce_total_count"] == 0:
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
