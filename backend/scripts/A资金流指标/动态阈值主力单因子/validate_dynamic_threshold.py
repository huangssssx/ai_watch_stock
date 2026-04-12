import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../"))

import numpy as np
import pandas as pd
from utils.pytdx_client import connect, DEFAULT_IP, DEFAULT_PORT
from utils.tushare_client import pro
from dynamic_threshold import calculate_dynamic_threshold

STOCKS = [
    ("000001", 0, "SZ", "平安银行"),
    ("002415", 0, "SZ", "海康威视"),
    ("600519", 1, "SH", "贵州茅台"),
]

SESSIONS = [
    ("early", "09:25", "10:30"),
    ("mid", "10:30", "13:00"),
    ("late", "13:00", "15:01"),
]


def _ts_code(code, mkt_suffix):
    return f"{code}.{mkt_suffix}"


def _fetch_all_ticks(tdx, market, code, date):
    ticks = []
    date_int = int(date) if date is not None else None
    for start in range(0, 100000, 500):
        if date_int is None:
            batch = tdx.get_transaction_data(market=market, code=code, start=start, count=500)
        else:
            batch = tdx.get_history_transaction_data(
                market=market, code=code, start=start, count=500, date=date_int
            )
        if not batch:
            break
        for t in batch:
            if t["vol"] <= 0:
                continue
            t["amount"] = t["vol"] * 100 * t["price"]
            ticks.append(t)
    return ticks


def validate_float_share(tdx, code, market, mkt_suffix, trade_date):
    print(f"\n{'='*70}")
    print(f"[维度1] 流通股本 & 市值一致性验证 — {code}")
    print(f"{'='*70}")

    ts_code = _ts_code(code, mkt_suffix)

    finance_info = tdx.get_finance_info(market=market, code=code)
    pytdx_liutong = finance_info["liutongguben"]
    pytdx_zonggu = finance_info["zongguben"]
    pytdx_updated = finance_info.get("updated_date", "N/A")

    df_basic = pro.daily_basic(ts_code=ts_code, trade_date=trade_date)
    if df_basic is None or df_basic.empty:
        print(f"  tushare daily_basic 无数据 ({ts_code} {trade_date})")
        return

    row = df_basic.iloc[0]
    ts_float = row["float_share"] * 10000
    ts_free = row["free_share"] * 10000
    ts_total = row["total_share"] * 10000
    ts_circ_mv = row["circ_mv"] * 10000
    ts_close = row["close"]

    pytdx_mv = pytdx_liutong * ts_close
    pytdx_mv_free = ts_free * ts_close

    print(f"  pytdx 流通股本(liutongguben): {pytdx_liutong:>15,.0f} 股")
    print(f"  pytdx 总股本(zongguben):      {pytdx_zonggu:>15,.0f} 股")
    print(f"  pytdx 财务更新日期:            {pytdx_updated}")
    print(f"  ---")
    print(f"  tushare 流通股本(float_share):  {ts_float:>15,.0f} 股")
    print(f"  tushare 自由流通(free_share):   {ts_free:>15,.0f} 股")
    print(f"  tushare 总股本(total_share):    {ts_total:>15,.0f} 股")
    print(f"  ---")

    diff_float = abs(pytdx_liutong - ts_float) / ts_float * 100 if ts_float else 0
    diff_free = abs(pytdx_liutong - ts_free) / ts_free * 100 if ts_free else 0
    diff_total = abs(pytdx_zonggu - ts_total) / ts_total * 100 if ts_total else 0

    print(f"  pytdx流通股本 vs tushare流通股本:  偏差 {diff_float:.2f}%")
    print(f"  pytdx流通股本 vs tushare自由流通:   偏差 {diff_free:.2f}%")
    print(f"  pytdx总股本   vs tushare总股本:    偏差 {diff_total:.2f}%")

    match_which = "流通股本" if diff_float < diff_free else "自由流通股本"
    print(f"  >>> pytdx liutongguben 更接近 tushare 的 [{match_which}]")

    print(f"\n  --- 市值对比 ---")
    print(f"  pytdx流通市值 (liutongguben×close): {pytdx_mv:>15,.0f} 元")
    print(f"  pytdx自由流通市值 (free_share×close): {pytdx_mv_free:>15,.0f} 元")
    print(f"  tushare流通市值 (circ_mv):          {ts_circ_mv:>15,.0f} 元")

    mv_diff_circ = abs(pytdx_mv - ts_circ_mv) / ts_circ_mv * 100 if ts_circ_mv else 0
    mv_diff_free = abs(pytdx_mv_free - ts_circ_mv) / ts_circ_mv * 100 if ts_circ_mv else 0
    print(f"  pytdx流通市值 vs tushare circ_mv:  偏差 {mv_diff_circ:.2f}%")
    print(f"  pytdx自由流通市值 vs tushare circ_mv: 偏差 {mv_diff_free:.2f}%")

    if diff_free < diff_float:
        print(f"\n  ⚠️  注意: pytdx liutongguben 更接近自由流通股本，脚本用'流通股本'计算市值可能有偏差")
        print(f"     差异: 流通股本 {ts_float:,.0f} vs 自由流通 {ts_free:,.0f}，差 {ts_float - ts_free:,.0f} 股")
        print(f"     对阈值影响: 用流通股本算出的阈值偏高 {(ts_float/ts_free - 1)*100:.1f}%")


def validate_daily_price(tdx, code, market, mkt_suffix, trade_date):
    print(f"\n{'='*70}")
    print(f"[维度2] 日线价格 & 成交额一致性验证 — {code}")
    print(f"{'='*70}")

    ts_code = _ts_code(code, mkt_suffix)

    price_p = None
    vol_p = None
    amount_p = None
    date_str = str(trade_date)
    for page_start in range(0, 800 * 10, 800):
        bars = tdx.get_security_bars(9, market, code, page_start, 800)
        if not bars:
            break
        for bar in bars:
            bar_date = f"{bar['year']}{bar['month']:02d}{bar['day']:02d}"
            if bar_date == date_str:
                price_p = bar["close"]
                vol_p = bar["vol"]
                amount_p = bar["amount"]
                break
        if price_p is not None:
            break
        oldest = bars[-1]
        oldest_date = f"{oldest['year']}{oldest['month']:02d}{oldest['day']:02d}"
        if int(oldest_date) <= int(date_str):
            break

    if price_p is None:
        print(f"  pytdx 未找到 {code} 在 {trade_date} 的日线数据")
        return

    df = pro.daily(ts_code=ts_code, start_date=trade_date, end_date=trade_date)
    if df is None or df.empty:
        print(f"  tushare daily 无数据")
        return

    row = df.iloc[0]
    ts_close = row["close"]
    ts_vol = row["vol"]
    ts_amount = row["amount"]

    vol_ts_in_shou = ts_vol
    vol_p_in_shou = vol_p
    amount_ts_in_yuan = ts_amount * 1000
    amount_p_in_yuan = amount_p

    price_diff = abs(price_p - ts_close) / ts_close * 100 if ts_close else 0
    vol_diff = abs(vol_p_in_shou - vol_ts_in_shou) / vol_ts_in_shou * 100 if vol_ts_in_shou else 0
    amt_diff = abs(amount_p_in_yuan - amount_ts_in_yuan) / amount_ts_in_yuan * 100 if amount_ts_in_yuan else 0

    print(f"  {'':20s} {'pytdx(category=9)':>20s} {'tushare daily':>20s} {'偏差%':>10s}")
    print(f"  {'-'*70}")
    print(f"  {'收盘价':20s} {price_p:>20.2f} {ts_close:>20.2f} {price_diff:>10.2f}%")
    print(f"  {'成交量(手)':20s} {vol_p_in_shou:>20,.0f} {vol_ts_in_shou:>20,.0f} {vol_diff:>10.2f}%")
    print(f"  {'成交额(元)':20s} {amount_p_in_yuan:>20,.0f} {amount_ts_in_yuan:>20,.0f} {amt_diff:>10.2f}%")

    if price_diff > 1.0:
        print(f"  ⚠️  价格偏差 {price_diff:.2f}%，可能是前复权导致！pytdx category=9 是前复权数据")
        df_adj = pro.adj_factor(ts_code=ts_code, start_date=trade_date, end_date=trade_date)
        if df_adj is not None and not df_adj.empty:
            adj = df_adj.iloc[0]["adj_factor"]
            real_price = price_p * adj
            real_diff = abs(real_price - ts_close) / ts_close * 100
            print(f"     复权因子={adj:.4f}, 还原后价格={real_price:.2f}, 偏差={real_diff:.2f}%")

    if vol_diff > 5.0:
        print(f"  ⚠️  成交量偏差 {vol_diff:.2f}%，需确认单位是否一致")
    if amt_diff > 5.0:
        print(f"  ⚠️  成交额偏差 {amt_diff:.2f}%，需确认单位是否一致")


def validate_tick_integrity(tdx, code, market, mkt_suffix, trade_date):
    print(f"\n{'='*70}")
    print(f"[维度3] 逐笔成交数据完整性验证 — {code}")
    print(f"{'='*70}")

    ts_code = _ts_code(code, mkt_suffix)

    ticks = _fetch_all_ticks(tdx, market, code, trade_date)
    if not ticks:
        print(f"  pytdx 无逐笔数据")
        return

    total_vol_shares = sum(t["vol"] * 100 for t in ticks)
    total_amount_yuan = sum(t["amount"] for t in ticks)

    df = pro.daily(ts_code=ts_code, start_date=trade_date, end_date=trade_date)
    if df is None or df.empty:
        print(f"  tushare daily 无数据")
        return

    row = df.iloc[0]
    ts_vol_shares = row["vol"] * 100
    ts_amount_yuan = row["amount"] * 1000

    vol_diff = abs(total_vol_shares - ts_vol_shares) / ts_vol_shares * 100 if ts_vol_shares else 0
    amt_diff = abs(total_amount_yuan - ts_amount_yuan) / ts_amount_yuan * 100 if ts_amount_yuan else 0

    print(f"  逐笔总笔数: {len(ticks)}")
    print(f"  {'':25s} {'pytdx逐笔汇总':>20s} {'tushare daily':>20s} {'偏差%':>10s}")
    print(f"  {'-'*75}")
    print(f"  {'总成交量(股)':25s} {total_vol_shares:>20,.0f} {ts_vol_shares:>20,.0f} {vol_diff:>10.2f}%")
    print(f"  {'总成交额(元)':25s} {total_amount_yuan:>20,.0f} {ts_amount_yuan:>20,.0f} {amt_diff:>10.2f}%")

    avg_tick_amount = np.mean([t["amount"] for t in ticks])
    p50 = np.percentile([t["amount"] for t in ticks], 50)
    p95 = np.percentile([t["amount"] for t in ticks], 95)
    p99 = np.percentile([t["amount"] for t in ticks], 99)
    max_tick = max(t["amount"] for t in ticks)

    print(f"\n  逐笔金额分布:")
    print(f"    均值:   {avg_tick_amount:>15,.2f} 元")
    print(f"    P50:    {p50:>15,.2f} 元")
    print(f"    P95:    {p95:>15,.2f} 元")
    print(f"    P99:    {p99:>15,.2f} 元")
    print(f"    最大值: {max_tick:>15,.2f} 元")

    if vol_diff > 5.0:
        print(f"\n  ⚠️  成交量偏差 {vol_diff:.2f}% 超过5%，逐笔数据可能不完整")
    if amt_diff > 5.0:
        print(f"  ⚠️  成交额偏差 {amt_diff:.2f}% 超过5%，逐笔数据可能不完整")


def validate_mainforce(tdx, code, market, mkt_suffix, trade_date):
    print(f"\n{'='*70}")
    print(f"[维度4] 主力单识别效果对比验证 — {code}")
    print(f"{'='*70}")

    ts_code = _ts_code(code, mkt_suffix)

    threshold_info = calculate_dynamic_threshold(market, code, trade_date, tdx=tdx)
    dynamic_threshold = threshold_info["动态阈值(元)"]
    threshold_base = threshold_info["阈值基础(市值*0.00001%)"]
    p95_threshold = threshold_info["95分位数阈值(元)"]
    liutongguben = threshold_info["流通股本(股)"]
    price = threshold_info["当前价格(元)"]

    ticks = _fetch_all_ticks(tdx, market, code, trade_date)
    if not ticks:
        print(f"  pytdx 无逐笔数据")
        return

    mainforce_ticks = [t for t in ticks if t["amount"] >= dynamic_threshold]

    buy_main = [t for t in mainforce_ticks if t["buyorsell"] in [0]]
    sell_main = [t for t in mainforce_ticks if t["buyorsell"] in [1]]

    pytdx_buy_amount = sum(t["amount"] for t in buy_main)
    pytdx_sell_amount = sum(t["amount"] for t in sell_main)
    pytdx_net_flow = pytdx_buy_amount - pytdx_sell_amount

    df_mf = pro.moneyflow(ts_code=ts_code, start_date=trade_date, end_date=trade_date)
    if df_mf is None or df_mf.empty:
        print(f"  tushare moneyflow 无数据")
        return

    mf = df_mf.iloc[0]

    ts_lg_buy = mf["buy_lg_amount"] * 10000
    ts_lg_sell = mf["sell_lg_amount"] * 10000
    ts_elg_buy = mf["buy_elg_amount"] * 10000
    ts_elg_sell = mf["sell_elg_amount"] * 10000
    ts_big_buy = ts_lg_buy + ts_elg_buy
    ts_big_sell = ts_lg_sell + ts_elg_sell
    ts_big_net = ts_big_buy - ts_big_sell
    ts_net_mf = mf["net_mf_amount"] * 10000

    fixed_lg_threshold = 200000
    fixed_elg_threshold = 1000000

    print(f"\n  --- 阈值对比 ---")
    print(f"  pytdx动态阈值:              {dynamic_threshold:>15,.2f} 元")
    print(f"  pytdx阈值基础(市值*0.00001%): {threshold_base:>15,.2f} 元")
    print(f"  pytdx P95阈值:              {p95_threshold:>15,.2f} 元")
    print(f"  tushare大单阈值(固定):      {fixed_lg_threshold:>15,} 元 (20万)")
    print(f"  tushare特大单阈值(固定):    {fixed_elg_threshold:>15,} 元 (100万)")

    if dynamic_threshold > fixed_elg_threshold:
        relation = ">> 特大单阈值(100万)"
    elif dynamic_threshold > fixed_lg_threshold:
        relation = "∈ (大单20万, 特大单100万)"
    else:
        relation = "<< 大单阈值(20万)"
    print(f"  动态阈值相对tushare分类:    {relation}")

    print(f"\n  --- 主力金额对比 (单位: 元) ---")
    print(f"  {'':25s} {'pytdx(动态阈值)':>20s} {'tushare(大单+特大单)':>22s}")
    print(f"  {'-'*70}")
    print(f"  {'主力买入金额':25s} {pytdx_buy_amount:>20,.2f} {ts_big_buy:>22,.2f}")
    print(f"  {'主力卖出金额':25s} {pytdx_sell_amount:>20,.2f} {ts_big_sell:>22,.2f}")
    print(f"  {'主力净流入':25s} {pytdx_net_flow:>20,.2f} {ts_big_net:>22,.2f}")

    buy_ratio = pytdx_buy_amount / ts_big_buy * 100 if ts_big_buy else 0
    sell_ratio = pytdx_sell_amount / ts_big_sell * 100 if ts_big_sell else 0
    print(f"  {'pytdx/tushare比值%':25s} {buy_ratio:>20.1f}% {sell_ratio:>22.1f}%")

    pytdx_direction = "净流入" if pytdx_net_flow > 0 else "净流出"
    ts_direction = "净流入" if ts_big_net > 0 else "净流出"
    direction_match = pytdx_direction == ts_direction

    print(f"\n  --- 方向一致性 ---")
    print(f"  pytdx主力方向:   {pytdx_direction} ({pytdx_net_flow:>+15,.2f} 元)")
    print(f"  tushare大单方向: {ts_direction} ({ts_big_net:>+15,.2f} 元)")
    print(f"  方向一致: {'✅ 是' if direction_match else '❌ 否'}")

    print(f"\n  --- 笔数统计 ---")
    print(f"  pytdx主力候选单笔数: {len(mainforce_ticks)} / {len(ticks)} ({len(mainforce_ticks)/len(ticks)*100:.1f}%)")
    print(f"  pytdx主力候选单金额占比: {sum(t['amount'] for t in mainforce_ticks)/sum(t['amount'] for t in ticks)*100:.1f}%")

    if mainforce_ticks:
        print(f"\n  --- 动态阈值筛选出的主力单金额分布 ---")
        amounts = [t["amount"] for t in mainforce_ticks]
        print(f"    最小值: {min(amounts):>12,.2f} 元")
        print(f"    均值:   {np.mean(amounts):>12,.2f} 元")
        print(f"    最大值: {max(amounts):>12,.2f} 元")

        in_lg = sum(1 for a in amounts if fixed_lg_threshold <= a < fixed_elg_threshold)
        in_elg = sum(1 for a in amounts if a >= fixed_elg_threshold)
        below_lg = sum(1 for a in amounts if a < fixed_lg_threshold)
        print(f"    落在大单区间(20-100万): {in_lg} 笔")
        print(f"    落在特大单区间(≥100万): {in_elg} 笔")
        print(f"    低于大单阈值(<20万):    {below_lg} 笔")

        if below_lg > 0:
            print(f"    ⚠️  有 {below_lg} 笔低于tushare大单阈值，说明动态阈值对小盘股更敏感")


def validate_multi_stock(tdx, trade_date):
    print(f"\n{'='*70}")
    print(f"[维度5] 多样本交叉验证 — 全部测试股票")
    print(f"{'='*70}")

    print(f"\n  {'代码':8s} {'名称':8s} {'动态阈值(元)':>14s} {'大单阈值(元)':>14s} {'阈值关系':20s} {'主力笔数':>8s} {'主力占比':>8s}")
    print(f"  {'-'*90}")

    for code, market, mkt_suffix, name in STOCKS:
        ts_code = _ts_code(code, mkt_suffix)

        threshold_info = calculate_dynamic_threshold(market, code, trade_date, tdx=tdx)
        dt = threshold_info["动态阈值(元)"]
        tb = threshold_info["阈值基础(市值*0.00001%)"]

        ticks = _fetch_all_ticks(tdx, market, code, trade_date)
        if not ticks:
            continue

        mainforce = [t for t in ticks if t["amount"] >= dt]
        total_amount = sum(t["amount"] for t in ticks)
        main_amount = sum(t["amount"] for t in mainforce)
        ratio = main_amount / total_amount * 100 if total_amount else 0

        if dt > 1000000:
            relation = ">>特大单"
        elif dt > 200000:
            relation = "大单~特大单"
        elif dt > 50000:
            relation = "中单~大单"
        else:
            relation = "<<大单"

        print(f"  {code:8s} {name:8s} {dt:>14,.0f} {200000:>14,} {relation:20s} {len(mainforce):>8d} {ratio:>7.1f}%")

    print(f"\n  解读: 动态阈值应随市值规模自适应调整")
    print(f"  - 大盘股(茅台): 阈值应远高于固定20万")
    print(f"  - 中盘股(海康): 阈值应接近或略高于20万")
    print(f"  - 小盘股(平安银行): 阈值可能低于20万，捕捉相对主力")


def print_validation_report(trade_date):
    print(f"\n{'#'*70}")
    print(f"# 动态阈值主力单因子 — Tushare对比验证报告")
    print(f"# 验证日期: {trade_date}")
    print(f"# 测试股票: {', '.join(f'{c}({n})' for c, _, _, n in STOCKS)}")
    print(f"{'#'*70}")

    with connect(DEFAULT_IP, DEFAULT_PORT) as tdx:
        for code, market, mkt_suffix, name in STOCKS:
            print(f"\n\n{'*'*70}")
            print(f"  >>> {name} ({code}.{mkt_suffix}) <<<")
            print(f"{'*'*70}")
            validate_float_share(tdx, code, market, mkt_suffix, trade_date)
            validate_daily_price(tdx, code, market, mkt_suffix, trade_date)
            validate_tick_integrity(tdx, code, market, mkt_suffix, trade_date)
            validate_mainforce(tdx, code, market, mkt_suffix, trade_date)

        validate_multi_stock(tdx, trade_date)

    print(f"\n\n{'#'*70}")
    print(f"# 验证完成")
    print(f"{'#'*70}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        trade_date = sys.argv[1]
    else:
        from datetime import datetime, timedelta

        today = datetime.now()
        for i in range(1, 7):
            d = today - timedelta(days=i)
            if d.weekday() < 5:
                trade_date = d.strftime("%Y%m%d")
                break
        else:
            trade_date = (today - timedelta(days=1)).strftime("%Y%m%d")

    print(f"使用交易日: {trade_date}")
    print_validation_report(trade_date)
