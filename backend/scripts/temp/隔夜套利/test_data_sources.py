#!/usr/bin/env python3
"""
隔夜套利策略 - 数据源接口测试

需要测试的接口清单：
==================

【Tushare 接口】（盘后数据，用于情绪分析）
1. trade_cal - 交易日历
2. stk_limit - 每日涨跌停价格
3. limit_list_d - 涨跌停列表（昨日涨停股）
4. daily - 日线行情（用于获取昨日数据）
5. daily_basic - 日线基本面（用于筛选）
6. moneyflow_dc - 资金流向（东方财富）
7. cyq_perf - 筹码分布（可选）

【pytdx 接口】（实时数据，用于选股）
1. get_security_quotes - 实时行情（五档、价格、成交量）
2. get_security_bars - K 线数据（1 分钟/5 分钟，用于封板时间判断）
3. get_security_list - 股票列表
4. get_security_count - 股票数量

【策略核心数据需求】
==================
情绪过滤（三步法）：
- 昨日涨停股票列表 → Tushare limit_list_d
- 今日这些股票的表现 → pytdx 实时行情
- 晋级率计算
- 跌停股票数量 → pytdx 全市场扫描

个股筛选：
- 涨停股票列表 → pytdx 全市场扫描
- 封单金额 → pytdx get_security_quotes（买一价*买一量）
- 封板时间 → pytdx get_security_bars（1 分钟 K 线）
- 板块分类 → Tushare stock_basic
- 板块内排名 → 计算

"""

import os
import sys
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict

import pandas as pd

# 项目根目录加入路径
here = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(here, "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from backend.utils. import pro
from backend.utils.pytdx_client import tdx, connect, DEFAULT_IP, DEFAULT_PORT, connected_endpoint


def test_tushare_trade_cal():
    """测试 Tushare 交易日历接口"""
    print("\n" + "="*60)
    print("【测试 1】Tushare trade_cal - 交易日历")
    print("="*60)
    
    if pro is None:
        print("❌ Tushare 未初始化")
        return False
    
    try:
        # 获取最近 30 天的交易日历
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        
        df = pro.trade_cal(
            exchange="SSE",
            start_date=start_date,
            end_date=end_date,
            fields="cal_date,is_open"
        )
        
        if df is None or df.empty:
            print("❌ 返回数据为空")
            return False
        
        # 统计交易日数量
        open_days = df[df["is_open"] == 1]
        print(f"✓ 成功获取交易日历")
        print(f"  日期范围：{start_date} - {end_date}")
        print(f"  总天数：{len(df)}")
        print(f"  交易日：{len(open_days)}")
        print(f"  最近 5 个交易日:")
        print(open_days.tail(5)[["cal_date", "is_open"]].to_string(index=False))
        return True
        
    except Exception as e:
        print(f"❌ 测试失败：{e}")
        return False


def test_tushare_stk_limit():
    """测试 Tushare 涨跌停价格接口"""
    print("\n" + "="*60)
    print("【测试 2】Tushare stk_limit - 涨跌停价格")
    print("="*60)
    
    if pro is None:
        print("❌ Tushare 未初始化")
        return False
    
    try:
        # 获取上一个交易日的涨跌停价格
        trade_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        
        df = pro.stk_limit(trade_date=trade_date)
        
        if df is None or df.empty:
            print("❌ 返回数据为空")
            return False
        
        print(f"✓ 成功获取涨跌停价格")
        print(f"  交易日期：{trade_date}")
        print(f"  股票数量：{len(df)}")
        print(f"  字段：{list(df.columns)}")
        print(f"  示例数据:")
        print(df[["ts_code", "up_limit", "down_limit", "pre_close"]].head(3).to_string(index=False))
        return True
        
    except Exception as e:
        print(f"❌ 测试失败：{e}")
        return False


def test_tushare_limit_list_d():
    """测试 Tushare 涨跌停列表接口"""
    print("\n" + "="*60)
    print("【测试 3】Tushare limit_list_d - 涨跌停列表")
    print("="*60)
    
    if pro is None:
        print("❌ Tushare 未初始化")
        return False
    
    try:
        # 获取上一个交易日的涨跌停列表
        trade_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        
        df = pro.limit_list_d(trade_date=trade_date)
        
        if df is None or df.empty:
            print("❌ 返回数据为空")
            return False
        
        print(f"✓ 成功获取涨跌停列表")
        print(f"  交易日期：{trade_date}")
        print(f"  股票数量：{len(df)}")
        print(f"  字段：{list(df.columns)}")
        
        # 统计涨停和跌停数量
        if "lb" in df.columns:
            limit_up = df[df["lb"] == "涨停"]
            limit_down = df[df["lb"] == "跌停"]
            print(f"  涨停：{len(limit_up)}只")
            print(f"  跌停：{len(limit_down)}只")
        
        print(f"  示例数据:")
        print_cols = ["ts_code", "name", "lb", "close", "pct_chg"]
        print_cols = [c for c in print_cols if c in df.columns]
        print(df[print_cols].head(5).to_string(index=False))
        return True
        
    except Exception as e:
        print(f"❌ 测试失败：{e}")
        return False


def test_tushare_daily():
    """测试 Tushare 日线行情接口"""
    print("\n" + "="*60)
    print("【测试 4】Tushare daily - 日线行情")
    print("="*60)
    
    if pro is None:
        print("❌ Tushare 未初始化")
        return False
    
    try:
        # 获取上一个交易日的日线行情
        trade_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        
        df = pro.daily(trade_date=trade_date)
        
        if df is None or df.empty:
            print("❌ 返回数据为空")
            return False
        
        print(f"✓ 成功获取日线行情")
        print(f"  交易日期：{trade_date}")
        print(f"  股票数量：{len(df)}")
        print(f"  字段：{list(df.columns)}")
        print(f"  示例数据:")
        print_cols = ["ts_code", "close", "pct_chg", "vol", "amount"]
        print_cols = [c for c in print_cols if c in df.columns]
        print(df[print_cols].head(5).to_string(index=False))
        return True
        
    except Exception as e:
        print(f"❌ 测试失败：{e}")
        return False


def test_tushare_daily_basic():
    """测试 Tushare 日线基本面接口"""
    print("\n" + "="*60)
    print("【测试 5】Tushare daily_basic - 日线基本面")
    print("="*60)
    
    if pro is None:
        print("❌ Tushare 未初始化")
        return False
    
    try:
        # 获取上一个交易日的基本面数据
        trade_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        
        df = pro.daily_basic(trade_date=trade_date)
        
        if df is None or df.empty:
            print("❌ 返回数据为空")
            return False
        
        print(f"✓ 成功获取日线基本面")
        print(f"  交易日期：{trade_date}")
        print(f"  股票数量：{len(df)}")
        print(f"  字段：{list(df.columns)}")
        print(f"  示例数据:")
        print_cols = ["ts_code", "turnover_rate", "volume_ratio", "total_mv", "circ_mv"]
        print_cols = [c for c in print_cols if c in df.columns]
        print(df[print_cols].head(3).to_string(index=False))
        return True
        
    except Exception as e:
        print(f"❌ 测试失败：{e}")
        return False


def test_tushare_stock_basic():
    """测试 Tushare 股票基本信息接口"""
    print("\n" + "="*60)
    print("【测试 6】Tushare stock_basic - 股票基本信息")
    print("="*60)
    
    if pro is None:
        print("❌ Tushare 未初始化")
        return False
    
    try:
        # 获取全部 A 股基本信息
        df = pro.stock_basic(list_status="L", fields="ts_code,name,industry,market")
        
        if df is None or df.empty:
            print("❌ 返回数据为空")
            return False
        
        print(f"✓ 成功获取股票基本信息")
        print(f"  股票数量：{len(df)}")
        print(f"  字段：{list(df.columns)}")
        print(f"  行业分布:")
        if "industry" in df.columns:
            industry_counts = df["industry"].value_counts().head(10)
            for industry, count in industry_counts.items():
                print(f"    {industry}: {count}只")
        return True
        
    except Exception as e:
        print(f"❌ 测试失败：{e}")
        return False


def test_pytdx_connect():
    """测试 pytdx 连接"""
    print("\n" + "="*60)
    print("【测试 7】pytdx 连接")
    print("="*60)
    
    try:
        api = connect(DEFAULT_IP, DEFAULT_PORT)
        
        if api is None:
            print("❌ 连接失败")
            return False
        
        endpoint = connected_endpoint()
        print(f"✓ 成功连接到 pytdx")
        print(f"  端点：{endpoint}")
        return True
        
    except Exception as e:
        print(f"❌ 测试失败：{e}")
        return False


def test_pytdx_get_security_quotes():
    """测试 pytdx 实时行情接口"""
    print("\n" + "="*60)
    print("【测试 8】pytdx get_security_quotes - 实时行情")
    print("="*60)
    
    try:
        api = connect(DEFAULT_IP, DEFAULT_PORT)
        
        # 测试几个代表性股票
        test_stocks = [
            (0, "000001"),  # 平安银行
            (1, "600000"),  # 浦发银行
            (0, "300001"),  # 特锐德
        ]
        
        all_quotes = []
        for market, code in test_stocks:
            quotes = api.get_security_quotes(market, code)
            if quotes:
                all_quotes.extend(quotes)
        
        if not all_quotes:
            print("❌ 返回数据为空")
            return False
        
        print(f"✓ 成功获取实时行情")
        print(f"  测试股票数：{len(test_stocks)}")
        print(f"  返回数据：{len(all_quotes)}条")
        print(f"  字段：{list(all_quotes[0].keys()) if all_quotes else []}")
        print(f"  示例数据:")
        for q in all_quotes[:3]:
            print(f"    {q.get('code')}: 最新价={q.get('price')}, 买一={q.get('bid1')}, 卖一={q.get('ask1')}, 买一量={q.get('bid_vol1')}, 卖一量={q.get('ask_vol1')}")
        return True
        
    except Exception as e:
        print(f"❌ 测试失败：{e}")
        return False


def test_pytdx_get_security_bars():
    """测试 pytdx K 线数据接口"""
    print("\n" + "="*60)
    print("【测试 9】pytdx get_security_bars - K 线数据")
    print("="*60)
    
    try:
        api = connect(DEFAULT_IP, DEFAULT_PORT)
        
        # 获取 1 分钟 K 线（类别 8）
        # 参数：category=8(1 分钟), market=0, code="000001", start=0, count=10
        bars = api.get_security_bars(8, 0, "000001", 0, 10)
        
        if not bars:
            print("❌ 返回数据为空")
            return False
        
        print(f"✓ 成功获取 K 线数据")
        print(f"  股票：000001")
        print(f"  类型：1 分钟 K 线")
        print(f"  返回数据：{len(bars)}条")
        print(f"  字段：{list(bars[0].keys()) if bars else []}")
        print(f"  示例数据:")
        for bar in bars[:3]:
            print(f"    时间={bar.get('datetime')}, 开盘={bar.get('open')}, 最高={bar.get('high')}, 最低={bar.get('low')}, 收盘={bar.get('close')}, 成交量={bar.get('vol')}")
        return True
        
    except Exception as e:
        print(f"❌ 测试失败：{e}")
        return False


def test_pytdx_get_security_list():
    """测试 pytdx 股票列表接口"""
    print("\n" + "="*60)
    print("【测试 10】pytdx get_security_list - 股票列表")
    print("="*60)
    
    try:
        api = connect(DEFAULT_IP, DEFAULT_PORT)
        
        # 获取深市 A 股列表 (market=0, start=0, count=10)
        stock_list = api.get_security_list(0, 0, 10)
        
        if not stock_list:
            print("❌ 返回数据为空")
            return False
        
        print(f"✓ 成功获取股票列表")
        print(f"  市场：深市")
        print(f"  返回数据：{len(stock_list)}条")
        print(f"  字段：{list(stock_list[0].keys()) if stock_list else []}")
        print(f"  示例数据:")
        for stock in stock_list[:5]:
            print(f"    代码={stock.get('code')}, 名称={stock.get('name')}")
        return True
        
    except Exception as e:
        print(f"❌ 测试失败：{e}")
        return False


def test_pytdx_get_security_count():
    """测试 pytdx 股票数量接口"""
    print("\n" + "="*60)
    print("【测试 11】pytdx get_security_count - 股票数量")
    print("="*60)
    
    try:
        api = connect(DEFAULT_IP, DEFAULT_PORT)
        
        # 获取深市股票数量
        count = api.get_security_count(0, 0)
        
        print(f"✓ 成功获取股票数量")
        print(f"  市场：深市")
        print(f"  数量：{count}")
        return True
        
    except Exception as e:
        print(f"❌ 测试失败：{e}")
        return False


def run_all_tests():
    """运行所有测试"""
    print("\n" + "="*80)
    print("隔夜套利策略 - 数据源接口测试")
    print("="*80)
    
    results = {
        "Tushare": [],
        "pytdx": []
    }
    
    # Tushare 接口测试
    tushare_tests = [
        ("trade_cal", test_tushare_trade_cal),
        ("stk_limit", test_tushare_stk_limit),
        ("limit_list_d", test_tushare_limit_list_d),
        ("daily", test_tushare_daily),
        ("daily_basic", test_tushare_daily_basic),
        ("stock_basic", test_tushare_stock_basic),
    ]
    
    for name, test_fn in tushare_tests:
        success = test_fn()
        results["Tushare"].append((name, success))
        time.sleep(0.5)  # 避免请求过快
    
    # pytdx 接口测试
    pytdx_tests = [
        ("connect", test_pytdx_connect),
        ("get_security_quotes", test_pytdx_get_security_quotes),
        ("get_security_bars", test_pytdx_get_security_bars),
        ("get_security_list", test_pytdx_get_security_list),
        ("get_security_count", test_pytdx_get_security_count),
    ]
    
    for name, test_fn in pytdx_tests:
        success = test_fn()
        results["pytdx"].append((name, success))
        time.sleep(0.3)
    
    # 汇总结果
    print("\n" + "="*80)
    print("测试结果汇总")
    print("="*80)
    
    print("\n【Tushare 接口】")
    for name, success in results["Tushare"]:
        status = "✓" if success else "❌"
        print(f"  {status} {name}")
    
    print("\n【pytdx 接口】")
    for name, success in results["pytdx"]:
        status = "✓" if success else "❌"
        print(f"  {status} {name}")
    
    # 统计
    total_tushare = len(results["Tushare"])
    success_tushare = sum(1 for _, s in results["Tushare"] if s)
    total_pytdx = len(results["pytdx"])
    success_pytdx = sum(1 for _, s in results["pytdx"] if s)
    
    print(f"\n总计：Tushare {success_tushare}/{total_tushare} 通过")
    print(f"      pytdx {success_pytdx}/{total_pytdx} 通过")
    
    return results


if __name__ == "__main__":
    run_all_tests()
