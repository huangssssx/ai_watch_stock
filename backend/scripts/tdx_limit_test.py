import sys
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from utils.pytdx_client import tdx, connected_endpoint


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def print_section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)


def test_get_security_bars_max_count(tdx, market: int, code: str, category: int = 9):
    print_section(f"K线测试 (category={category}, market={market}, code={code})")
    print(f"时间: {now_str()}")

    max_counts = [100, 500, 800, 1000, 1500, 2000, 3000, 5000]
    results = []

    for count in max_counts:
        t0 = time.time()
        try:
            data = tdx.get_security_bars(category, market, code, 0, count)
            elapsed = time.time() - t0
            actual_count = len(data) if data else 0
            results.append((count, actual_count, elapsed, None))
            print(f"  请求{count:5d}条 -> 返回{actual_count:5d}条, 耗时{elapsed:.3f}s")
        except Exception as e:
            elapsed = time.time() - t0
            results.append((count, 0, elapsed, str(e)))
            print(f"  请求{count:5d}条 -> 错误: {e}, 耗时{elapsed:.3f}s")

    return results


def test_kline_date_range(tdx, market: int, code: str, category: int = 9):
    print_section(f"K线时间跨度测试 (category={category}, market={market}, code={code})")

    for count in [800, 1600, 2400]:
        t0 = time.time()
        data = tdx.get_security_bars(category, market, code, 0, count)
        elapsed = time.time() - t0
        if data:
            first_date = data[-1].get('date', 'N/A') if len(data) > 0 else 'N/A'
            last_date = data[0].get('date', 'N/A') if len(data) > 0 else 'N/A'
            print(f"  请求{count:5d}条 -> 实际返回{len(data):5d}条")
            print(f"    首条日期: {first_date}, 末条日期: {last_date}")
            print(f"    耗时: {elapsed:.3f}s")
        else:
            print(f"  请求{count:5d}条 -> 无数据, 耗时{elapsed:.3f}s")


def test_kline_xdxr(tdx, market: int, code: str):
    print_section(f"K线除权测试 (market={market}, code={code})")

    categories = [
        (4, "日线"),
        (9, "日线(通达信)"),
        (5, "周线"),
        (6, "月线"),
    ]

    for cat, name in categories:
        data_normal = tdx.get_security_bars(cat, market, code, 0, 100)
        data_recent = tdx.get_security_bars(cat, market, code, 0, 10)

        if data_normal and data_recent:
            normal_last = data_normal[-1]
            recent_first = data_recent[0]

            print(f"\n  {name} (category={cat}):")
            print(f"    最近10条首条: date={recent_first.get('date')}, close={recent_first.get('close')}")
            print(f"    历史100条末条: date={normal_last.get('date')}, close={normal_last.get('close')}")

            xdxr_data = tdx.get_xdxr_info(market, code)
            print(f"    除权信息条数: {len(xdxr_data) if xdxr_data else 0}")
            if xdxr_data and len(xdxr_data) > 0:
                print(f"    最近一条除权: {xdxr_data[0]}")


def test_kline_vol_unit(tdx, market: int, code: str):
    print_section(f"K线成交量单位测试 (market={market}, code={code})")

    categories = [
        (4, "日线(category=4)"),
        (9, "日线(category=9)"),
    ]

    for cat, name in categories:
        data = tdx.get_security_bars(cat, market, code, 0, 20)
        if data and len(data) > 0:
            last = data[0]
            vol = last.get('vol', 0)
            amount = last.get('amount', 0)
            price = last.get('close', 0)

            print(f"\n  {name}:")
            print(f"    vol={vol}, amount={amount}, close={price}")
            if vol > 0 and amount > 0:
                raw_vwap = amount / vol
                ratio = raw_vwap / price if price > 0 else 0
                print(f"    amount/vol = {raw_vwap:.2f}, ratio to price = {ratio:.2f}")
                if 80 < ratio < 120:
                    print(f"    -> vol单位为'手' (1手=100股)")
                elif 0.8 < ratio < 1.2:
                    print(f"    -> vol单位为'股'")
                else:
                    print(f"    -> 无法判断单位")


def test_minute_time_data(tdx, market: int, code: str):
    print_section(f"当日分时数据测试 (market={market}, code={code})")

    t0 = time.time()
    data = tdx.get_minute_time_data(market, code)
    elapsed = time.time() - t0

    if data:
        print(f"  返回条数: {len(data)}")
        print(f"  首条: {data[0]}")
        print(f"  末条: {data[-1]}")
        print(f"  耗时: {elapsed:.3f}s")

        total_vol = sum(d.get('vol', 0) for d in data)
        print(f"  分时成交量合计: {total_vol}")

        quotes = tdx.get_security_quotes(market, code)
        if quotes:
            quote_vol = quotes[0].get('vol', 0)
            print(f"  快照总成交量: {quote_vol}")
            ratio = total_vol / quote_vol if quote_vol > 0 else 0
            print(f"  分时/快照比值: {ratio:.4f}")
            if 0.95 < ratio < 1.05:
                print(f"    -> vol单位一致")
            else:
                print(f"    -> vol单位可能不一致")
    else:
        print(f"  无数据, 耗时: {elapsed:.3f}s")


def test_history_minute_time_data(tdx, market: int, code: str):
    print_section(f"历史分时数据测试 (market={market}, code={code})")

    dates_to_test = [
        20260301,
        20260101,
        20250101,
        20230101,
        20210101,
        20190101,
        20170101,
        20150101,
    ]

    for date in dates_to_test:
        t0 = time.time()
        try:
            data = tdx.get_history_minute_time_data(market, code, date)
            elapsed = time.time() - t0
            if data:
                print(f"  date={date} -> {len(data)}条, 耗时{elapsed:.3f}s")
            else:
                print(f"  date={date} -> 无数据, 耗时{elapsed:.3f}s")
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  date={date} -> 错误: {e}, 耗时{elapsed:.3f}s")


def test_transaction_data(tdx, market: int, code: str):
    print_section(f"当日逐笔数据测试 (market={market}, code={code})")

    for start in [0, 100, 500, 1000, 2000]:
        t0 = time.time()
        try:
            data = tdx.get_transaction_data(market, code, start, 500)
            elapsed = time.time() - t0
            if data:
                print(f"  start={start:5d} -> 返回{len(data):5d}条, 耗时{elapsed:.3f}s")
                if start == 0:
                    print(f"    首条: {data[0]}")
                    print(f"    末条: {data[-1]}")
            else:
                print(f"  start={start:5d} -> 无数据, 耗时{elapsed:.3f}s")
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  start={start:5d} -> 错误: {e}, 耗时{elapsed:.3f}s")


def test_history_transaction_data(tdx, market: int, code: str):
    print_section(f"历史逐笔数据测试 (market={market}, code={code})")

    dates_to_test = [
        20260301,
        20260101,
        20250101,
        20230101,
        20210101,
        20190101,
        20170101,
        20150101,
    ]

    for date in dates_to_test:
        t0 = time.time()
        try:
            data = tdx.get_history_transaction_data(market, code, 0, 500, date)
            elapsed = time.time() - t0
            if data:
                print(f"  date={date} -> {len(data)}条, 耗时{elapsed:.3f}s")
            else:
                print(f"  date={date} -> 无数据, 耗时{elapsed:.3f}s")
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  date={date} -> 错误: {e}, 耗时{elapsed:.3f}s")


def test_security_count(tdx, market: int):
    print_section(f"股票数量测试 (market={market})")

    t0 = time.time()
    count = tdx.get_security_count(market)
    elapsed = time.time() - t0
    print(f"  股票数量: {count}, 耗时: {elapsed:.3f}s")
    return count


def test_security_list_pagination(tdx, market: int):
    print_section(f"股票列表分页测试 (market={market})")

    page_size = 1000
    offsets = list(range(0, 10000, page_size))

    for offset in offsets:
        t0 = time.time()
        try:
            data = tdx.get_security_list(market, offset)
            elapsed = time.time() - t0
            if data:
                print(f"  offset={offset:5d} -> {len(data):5d}条, 耗时{elapsed:.3f}s")
            else:
                print(f"  offset={offset:5d} -> 无数据, 耗时{elapsed:.3f}s")
                break
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  offset={offset:5d} -> 错误: {e}, 耗时{elapsed:.3f}s")
            break


def test_finance_info(tdx, market: int, code: str):
    print_section(f"财务信息测试 (market={market}, code={code})")

    t0 = time.time()
    data = tdx.get_finance_info(market, code)
    elapsed = time.time() - t0

    if data:
        print(f"  返回: 有数据 ({len(data)}字段), 耗时{elapsed:.3f}s")
        key_fields = ['liutongguben', 'zongguben', 'zongzichan', 'jingzichan',
                       'zhuyingshouru', 'jinglirun', 'meigujingzichan', 'updated_date']
        for k in key_fields:
            if k in data:
                print(f"    {k} = {data[k]}")
    else:
        print(f"  返回: 无数据, 耗时{elapsed:.3f}s")


def test_xdxr_info(tdx, market: int, code: str):
    print_section(f"除权除息信息测试 (market={market}, code={code})")

    t0 = time.time()
    data = tdx.get_xdxr_info(market, code)
    elapsed = time.time() - t0

    if data:
        print(f"  返回: {len(data)}条, 耗时{elapsed:.3f}s")
        print(f"  首条: {data[0]}")
        print(f"  末条: {data[-1]}")

        categories = {}
        for item in data:
            cat = item.get('category', 'N/A')
            cat_name = item.get('name', 'N/A')
            categories[cat] = cat_name

        print(f"  除权类型: {categories}")
    else:
        print(f"  返回: 无数据, 耗时{elapsed:.3f}s")


def test_index_bars(tdx, market: int, code: str):
    print_section(f"指数K线测试 (market={market}, code={code})")

    for cat, name in [(9, "日线"), (8, "1分钟"), (0, "5分钟")]:
        t0 = time.time()
        data = tdx.get_index_bars(cat, market, code, 0, 100)
        elapsed = time.time() - t0
        if data:
            print(f"  {name}(category={cat}) -> {len(data)}条, 耗时{elapsed:.3f}s")
            if 'up_count' in data[0] or 'down_count' in data[0]:
                print(f"    包含涨跌家数: up={data[0].get('up_count')}, down={data[0].get('down_count')}")
        else:
            print(f"  {name}(category={cat}) -> 无数据, 耗时{elapsed:.3f}s")


def test_block_info(tdx):
    print_section("板块信息测试")

    blockfiles = ['block.dat', 'block_gn.dat', 'block_zs.dat', 'block_fg.dat']

    for bf in blockfiles:
        t0 = time.time()
        try:
            meta = tdx.get_block_info_meta(bf)
            elapsed = time.time() - t0
            if meta:
                print(f"  {bf}: size={meta.get('size')}, 耗时{elapsed:.3f}s")
            else:
                print(f"  {bf}: 无元数据, 耗时{elapsed:.3f}s")
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  {bf}: 错误: {e}, 耗时{elapsed:.3f}s")


def main():
    print(f"\n{'#'*60}")
    print(f"# pytdx 极限测试")
    print(f"# 时间: {now_str()}")
    print(f"{'#'*60}")

    test_stocks = [
        (0, "000001"),
        (1, "600000"),
    ]

    with tdx:
        ep = connected_endpoint()
        print(f"\n已连接至: {ep}")

        print_section("1. 股票数量")
        for m, c in test_stocks:
            test_security_count(tdx, m)

        print_section("2. 股票列表分页")
        for m, c in test_stocks:
            test_security_list_pagination(tdx, m)

        print_section("3. K线最大条数")
        for m, c in test_stocks:
            test_get_security_bars_max_count(tdx, m, c, category=9)

        print_section("4. K线时间跨度")
        for m, c in test_stocks:
            test_kline_date_range(tdx, m, c, category=9)

        print_section("5. K线除权")
        for m, c in test_stocks:
            test_kline_xdxr(tdx, m, c)

        print_section("6. K线成交量单位")
        for m, c in test_stocks:
            test_kline_vol_unit(tdx, m, c)

        print_section("7. 指数K线")
        test_index_bars(tdx, 0, "000001")
        test_index_bars(tdx, 1, "000001")

        print_section("8. 当日分时")
        for m, c in test_stocks:
            test_minute_time_data(tdx, m, c)

        print_section("9. 历史分时")
        for m, c in test_stocks:
            test_history_minute_time_data(tdx, m, c)

        print_section("10. 当日逐笔")
        for m, c in test_stocks:
            test_transaction_data(tdx, m, c)

        print_section("11. 历史逐笔")
        for m, c in test_stocks:
            test_history_transaction_data(tdx, m, c)

        print_section("12. 财务信息")
        for m, c in test_stocks:
            test_finance_info(tdx, m, c)

        print_section("13. 除权除息")
        for m, c in test_stocks:
            test_xdxr_info(tdx, m, c)

        print_section("14. 板块信息")
        test_block_info(tdx)

    print(f"\n{'#'*60}")
    print(f"# 测试完成: {now_str()}")
    print(f"{'#'*60}\n")


if __name__ == "__main__":
    main()
