import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../"))

import numpy as np
from utils.pytdx_client import connect, DEFAULT_IP, DEFAULT_PORT

SESSIONS = [
    ("early", "09:25", "10:30"),
    ("mid", "10:30", "13:00"),
    ("late", "13:00", "15:01"),
]


def _time_in_session(time_str: str, start: str, end: str) -> bool:
    return start <= time_str < end


def _calc_session_threshold(volumes: list) -> dict:
    if not volumes:
        return {
            "买入阈值(元)": 0,
            "卖出阈值(元)": 0,
            "逐笔成交笔数": 0,
            "均值(元)": 0,
            "最大单笔(元)": 0,
        }
    p95 = np.percentile(volumes, 95)
    p90 = np.percentile(volumes, 90)
    buy_threshold = p95 * 1.5
    sell_threshold = p90 * 1.2
    return {
        "买入阈值(元)": float(buy_threshold),
        "卖出阈值(元)": float(sell_threshold),
        "逐笔成交笔数": len(volumes),
        "均值(元)": float(np.mean(volumes)),
        "最大单笔(元)": float(max(volumes)),
    }


def calculate_dynamic_threshold(market: int, code: str, date: int = None, tdx=None) -> dict:
    own_connect = tdx is None
    if own_connect:
        tdx = connect(DEFAULT_IP, DEFAULT_PORT)
        tdx.__enter__()

    try:
        finance_info = tdx.get_finance_info(market=market, code=code)
        liutongguben = finance_info["liutongguben"]

        if date is None:
            quote = tdx.get_security_quotes((market, code))
            price = quote[0]["price"]
        else:
            date_str = str(date)
            price = None
            for page_start in range(0, 800 * 10, 800):
                bars = tdx.get_security_bars(9, market, code, page_start, 800)
                if not bars:
                    break
                for bar in bars:
                    bar_date = f"{bar['year']}{bar['month']:02d}{bar['day']:02d}"
                    if bar_date == date_str:
                        price = bar["close"]
                        break
                if price is not None:
                    break
                oldest = bars[-1]
                oldest_date = f"{oldest['year']}{oldest['month']:02d}{oldest['day']:02d}"
                if int(oldest_date) <= int(date_str):
                    break
            if price is None:
                raise ValueError(f"未找到 {code} 在 {date} 的日线数据")

        free_market_cap = liutongguben * price

        all_volumes = []
        session_volumes = {name: [] for name, _, _ in SESSIONS}

        if date is None:
            for start in range(0, 100000, 500):
                transactions = tdx.get_transaction_data(
                    market=market, code=code, start=start, count=500
                )
                if not transactions:
                    break
                for t in transactions:
                    if t["vol"] <= 0:
                        continue
                    amount = t["vol"] * 100 * t["price"]
                    all_volumes.append(amount)
                    for name, s_start, s_end in SESSIONS:
                        if _time_in_session(t["time"], s_start, s_end):
                            session_volumes[name].append(amount)
                            break
        else:
            for start in range(0, 100000, 500):
                transactions = tdx.get_history_transaction_data(
                    market=market, code=code, start=start, count=500, date=date
                )
                if not transactions:
                    break
                for t in transactions:
                    if t["vol"] <= 0:
                        continue
                    amount = t["vol"] * 100 * t["price"]
                    all_volumes.append(amount)
                    for name, s_start, s_end in SESSIONS:
                        if _time_in_session(t["time"], s_start, s_end):
                            session_volumes[name].append(amount)
                            break

        if not all_volumes:
            buy_threshold = 0
            sell_threshold = 0
            p95_val = 0
            p90_val = 0
        else:
            p95_val = float(np.percentile(all_volumes, 95))
            p90_val = float(np.percentile(all_volumes, 90))
            buy_threshold = p95_val * 1.5
            sell_threshold = p90_val * 1.2

        session_thresholds = {}
        for name, _, _ in SESSIONS:
            session_thresholds[name] = _calc_session_threshold(session_volumes[name])

        return {
            "code": code,
            "market": market,
            "date": date,
            "market_name": "深圳" if market == 0 else "上海",
            "流通股本(股)": liutongguben,
            "当前价格(元)": price,
            "自由流通市值(元)": free_market_cap,
            "95分位数阈值(元)": p95_val,
            "90分位数阈值(元)": p90_val,
            "动态阈值(元)": buy_threshold,
            "买入阈值(元)": float(buy_threshold),
            "卖出阈值(元)": float(sell_threshold),
            "逐笔成交笔数": len(all_volumes),
            "分时段阈值": session_thresholds,
        }
    finally:
        if own_connect:
            tdx.__exit__(None, None, None)


if __name__ == "__main__":
    code = sys.argv[1] if len(sys.argv) > 1 else "000001"
    date = int(sys.argv[2]) if len(sys.argv) > 2 else None
    market = 0 if code.startswith("0") else 1

    print(f"\n{'='*60}")
    result = calculate_dynamic_threshold(market, code, date)
    date_label = result["date"] or "当日"
    print(f"股票: {result['market_name']} {result['code']}  日期: {date_label}")
    print(f"流通股本: {result['流通股本(股)']:,.0f} 股")
    print(f"当前价格: {result['当前价格(元)']:.2f} 元")
    print(f"自由流通市值: {result['自由流通市值(元)']:,.2f} 元")
    print(f"P95 阈值: {result['95分位数阈值(元)']:,.2f} 元")
    print(f"P90 阈值: {result['90分位数阈值(元)']:,.2f} 元")
    print(f"{'='*60}")
    print(f">>> 买入阈值 (P95×1.5): {result['买入阈值(元)']:,.2f} 元 <<<")
    print(f">>> 卖出阈值 (P90×1.2): {result['卖出阈值(元)']:,.2f} 元 <<<")
    print(f"{'='*60}")
    print(f"逐笔成交笔数: {result['逐笔成交笔数']}")

    print(f"\n--- 分时段阈值 ---")
    for name, _, _ in SESSIONS:
        s = result["分时段阈值"][name]
        label = {"early": "早盘(09:25-10:30)", "mid": "盘中(10:30-13:00)", "late": "尾盘(13:00-15:00)"}
        print(f"  {label[name]}: {s['逐笔成交笔数']}笔, 买入={s['买入阈值(元)']:,.0f}元, 卖出={s['卖出阈值(元)']:,.0f}元")
