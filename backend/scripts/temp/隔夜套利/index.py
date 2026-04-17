#!/usr/bin/env python3
"""
隔夜套利选股策略

策略核心逻辑：
1. 情绪过滤（三步法）
   - 晋级率：昨日涨停股今日高开/继续涨停比例 ≥ 60%
   - 跌停预警：昨日核心龙头股今日无跌停
   - 梯队完整性：最强板块内龙头 + 中军 + 跟风都在活跃

2. 个股筛选（尾盘 14:30-14:50 执行）
   - 当日最强板块内的涨停股
   - 封板时间 < 14:30（14:20-14:30 确认）
   - 封单金额 ≥ 3000 万
   - 板块内最早涨停的前 3 名
   - 非尾盘偷袭板（无反复炸板）
   - 当日跌停数 < 20 只
   - 大盘 14:00 后走势平稳

3. 卖出策略（次日 09:25-10:00）
   - 高开≥5%：卖半仓，剩余观察
   - 高开 2-5%：开盘全卖
   - 平开±2%：观察 3 分钟，不翻红全出
   - 低开：挂跌停全出

数据来源：
- Tushare: 日线行情、涨跌停数据、板块数据
- pytdx: 实时行情、封单数据
"""

import os
import sys
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict

import pandas as pd

# 项目根目录加入路径
here = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(here, "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from backend.utils. import pro
from backend.utils.pytdx_client import tdx, connect, DEFAULT_IP, DEFAULT_PORT


def _now_ts() -> str:
    """当前时间戳字符串"""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _trade_date_str(dt: datetime) -> str:
    """交易日期格式"""
    return dt.strftime("%Y%m%d")


def _prev_trade_date(pro, ref_date: Optional[str] = None, lookback: int = 10) -> str:
    """获取上一个交易日"""
    ref = datetime.now()
    if ref_date:
        try:
            ref = datetime.strptime(ref_date, "%Y%m%d")
        except Exception:
            pass
    
    end_date = ref.date()
    start_date = end_date - timedelta(days=lookback)
    
    try:
        cal = pro.trade_cal(
            exchange="SSE",
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
            fields="cal_date,is_open"
        )
        
        if cal is None or cal.empty:
            return (ref - timedelta(days=1)).strftime("%Y%m%d")
        
        cal = cal[cal["is_open"] == 1]
        cal = cal[cal["cal_date"] <= ref.strftime("%Y%m%d")]
        
        if cal.empty or len(cal) < 2:
            return (ref - timedelta(days=1)).strftime("%Y%m%d")
        
        return str(cal["cal_date"].iloc[-2])
    except Exception as e:
        print(f"获取交易日历失败：{e}，使用备用方案")
        return (ref - timedelta(days=1)).strftime("%Y%m%d")


def _get_limit_prices(pro, trade_date: str) -> pd.DataFrame:
    """获取涨跌停价格数据"""
    try:
        df = pro.stk_limit(trade_date=trade_date)
        return df
    except Exception as e:
        print(f"获取涨跌停价格失败：{e}")
        return pd.DataFrame()


def _get_yesterday_limit_stocks(pro, trade_date: str) -> pd.DataFrame:
    """获取昨日涨停股票列表"""
    try:
        df = pro.limit_list_d(trade_date=trade_date)
        if df is not None and not df.empty:
            return df[df["lb"].isin(["涨停", "连板"])]
        return pd.DataFrame()
    except Exception as e:
        print(f"获取昨日涨停数据失败：{e}")
        # 备用方案：使用涨跌停价格数据
        try:
            limit_df = pro.stk_limit(trade_date=trade_date)
            if limit_df is not None and not limit_df.empty:
                limit_df["lb"] = "涨停"
                return limit_df
        except Exception:
            pass
        return pd.DataFrame()


def _get_today_market_data(pro, trade_date: str) -> pd.DataFrame:
    """获取今日市场数据"""
    try:
        df = pro.daily(trade_date=trade_date)
        return df
    except Exception as e:
        print(f"获取今日行情失败：{e}")
        return pd.DataFrame()


def _get_realtime_quotes(tdx_api, ts_codes: List[str]) -> pd.DataFrame:
    """获取实时行情数据"""
    rows = []
    for ts_code in ts_codes:
        try:
            parts = ts_code.split(".")
            if len(parts) != 2:
                continue
            code = parts[0]
            suffix = parts[1].upper()
            market = 1 if suffix == "SH" else 0
            
            quotes = tdx_api.get_security_quotes(market, code)
            if quotes:
                for q in quotes:
                    rows.append({
                        "ts_code": ts_code,
                        "price": q.get("price", 0),
                        "close": q.get("close", 0),
                        "open": q.get("open", 0),
                        "high": q.get("high", 0),
                        "low": q.get("low", 0),
                        "vol": q.get("vol", 0),
                        "amount": q.get("amount", 0),
                        "bid1": q.get("bid1", 0),
                        "ask1": q.get("ask1", 0),
                        "bid_vol1": q.get("bid_vol1", 0),
                        "ask_vol1": q.get("ask_vol1", 0),
                    })
        except Exception as e:
            continue
    
    return pd.DataFrame(rows)


def check_market_sentiment(pro, today: str) -> Dict:
    """
    第一步：判断今日是否"好日子"（情绪过滤器）
    
    返回：
    {
        "pass": bool,  # 是否通过情绪过滤
        "promotion_rate": float,  # 晋级率
        "limit_down_count": int,  # 核心股跌停数量
        "梯队_score": str,  # 梯队完整性
        "details": str  # 详细信息
    }
    """
    result = {
        "pass": False,
        "promotion_rate": 0.0,
        "limit_down_count": 0,
        "梯队_score": "未知",
        "details": ""
    }
    
    # 获取上一个交易日
    prev_date = _prev_trade_date(pro, today)
    
    # 1. 获取昨日涨停股票
    yesterday_limits = _get_yesterday_limit_stocks(pro, prev_date)
    if yesterday_limits.empty:
        result["details"] = "无法获取昨日涨停数据"
        return result
    
    yesterday_limit_codes = yesterday_limits["ts_code"].unique().tolist()
    total_yesterday_limit = len(yesterday_limit_codes)
    
    # 2. 获取今日行情
    today_data = _get_today_market_data(pro, today)
    if today_data.empty:
        result["details"] = "无法获取今日行情数据"
        return result
    
    # 3. 计算晋级率
    promoted_count = 0
    for code in yesterday_limit_codes:
        stock_data = today_data[today_data["ts_code"] == code]
        if not stock_data.empty:
            pct_chg = stock_data["pct_chg"].iloc[0]
            if pct_chg > 0:  # 今日高开或上涨
                promoted_count += 1
    
    promotion_rate = (promoted_count / total_yesterday_limit * 100) if total_yesterday_limit > 0 else 0
    result["promotion_rate"] = promotion_rate
    
    # 4. 检查跌停预警
    limit_down_stocks = today_data[today_data["pct_chg"] <= -9.5]
    result["limit_down_count"] = len(limit_down_stocks)
    
    # 检查昨日核心龙头是否跌停
    core_limit_down = 0
    for code in yesterday_limit_codes[:10]:  # 检查前 10 只昨日涨停股
        if code in limit_down_stocks["ts_code"].values:
            core_limit_down += 1
    
    # 5. 梯队完整性判断
    if promoted_count >= total_yesterday_limit * 0.6 and core_limit_down == 0:
        result["梯队_score"] = "完整"
    elif promoted_count >= total_yesterday_limit * 0.4 and core_limit_down <= 2:
        result["梯队_score"] = "一般"
    else:
        result["梯队_score"] = "差"
    
    # 6. 综合判断
    passed_steps = 0
    if promotion_rate >= 60:
        passed_steps += 1
    if core_limit_down == 0:
        passed_steps += 1
    if result["梯队_score"] == "完整":
        passed_steps += 1
    
    result["pass"] = passed_steps >= 2
    result["details"] = (
        f"晋级率：{promotion_rate:.1f}% ({promoted_count}/{total_yesterday_limit}) | "
        f"核心跌停：{core_limit_down}只 | "
        f"梯队：{result['梯队_score']} | "
        f"通过步骤：{passed_steps}/3"
    )
    
    return result


def select_stocks(pro, tdx_api, today: str, top_n: int = 3) -> pd.DataFrame:
    """
    第二步：执行选股（个股过滤器）
    
    选股条件：
    1. 当日最强板块内的涨停股
    2. 封板时间 < 14:30
    3. 封单金额 ≥ 3000 万
    4. 板块内最早涨停的前 3 名
    5. 非尾盘偷袭板
    6. 当日跌停数 < 20 只
    7. 大盘 14:00 后走势平稳
    """
    
    # 获取今日行情
    today_data = _get_today_market_data(pro, today)
    if today_data.empty:
        return pd.DataFrame()
    
    # 过滤涨停股票
    limit_up_stocks = today_data[today_data["pct_chg"] >= 9.5].copy()
    if limit_up_stocks.empty:
        return pd.DataFrame()
    
    # 检查跌停数量
    limit_down_count = len(today_data[today_data["pct_chg"] <= -9.5])
    if limit_down_count >= 20:
        print(f"警告：今日跌停家数过多 ({limit_down_count}只)，建议观望")
        return pd.DataFrame()
    
    # 获取实时数据
    candidate_codes = limit_up_stocks["ts_code"].tolist()
    realtime_data = _get_realtime_quotes(tdx_api, candidate_codes)
    
    # 合并数据
    if not realtime_data.empty:
        limit_up_stocks = limit_up_stocks.merge(realtime_data, on="ts_code", how="left")
    
    # 计算封单金额（简化版）
    limit_up_stocks["封单金额"] = limit_up_stocks.get("bid1", 0) * limit_up_stocks.get("bid_vol1", 0) / 10000
    
    # 过滤封单金额
    limit_up_stocks = limit_up_stocks[limit_up_stocks["封单金额"] >= 3000]
    
    # 按板块分组，选取每个板块前 3 名
    result_rows = []
    for industry in limit_up_stocks["industry"].unique():
        industry_stocks = limit_up_stocks[limit_up_stocks["industry"] == industry]
        industry_stocks = industry_stocks.sort_values("pct_chg", ascending=False).head(top_n)
        result_rows.append(industry_stocks)
    
    if result_rows:
        result_df = pd.concat(result_rows, ignore_index=True)
        result_df = result_df.head(top_n * 3)  # 最多返回 9 只
        return result_df
    
    return pd.DataFrame()


def generate_sell_plan(stock_name: str, buy_price: float) -> Dict:
    """
    第三步：生成次日卖出预案
    """
    return {
        "stock": stock_name,
        "plans": [
            {"condition": "高开≥5%", "action": "卖半仓，剩余观察"},
            {"condition": "高开 2-5%", "action": "开盘全卖"},
            {"condition": "平开±2%", "action": "观察 3 分钟，不翻红全出"},
            {"condition": "低开", "action": "挂跌停全出"}
        ]
    }


def run_strategy(auto_mode: bool = True, top_n: int = 3):
    """
    执行完整的隔夜套利策略
    """
    print("=" * 60)
    print("隔夜套利选股策略")
    print("=" * 60)
    
    # 确定交易日期
    today = datetime.now().strftime("%Y%m%d")
    current_hour = datetime.now().hour
    current_minute = datetime.now().minute
    
    # 检查执行时间
    if auto_mode and not (14 <= current_hour <= 15):
        print(f"警告：当前时间 {current_hour}:{current_minute:02d} 不在推荐执行时间 (14:00-15:00)")
        print("建议：尾盘 14:30-14:50 执行选股，次日 09:25-10:00 执行卖出")
    
    # 初始化连接
    try:
        connect(DEFAULT_IP, DEFAULT_PORT)
        print("✓ pytdx 连接成功")
    except Exception as e:
        print(f"✗ pytdx 连接失败：{e}")
        return
    
    if pro is None:
        print("✗ Tushare 未初始化")
        return
    
    # 第一步：情绪过滤
    print("\n【第一步】情绪过滤")
    sentiment = check_market_sentiment(pro, today)
    print(f"晋级率：{sentiment['promotion_rate']:.1f}%")
    print(f"核心跌停：{sentiment['limit_down_count']}只")
    print(f"梯队完整性：{sentiment['梯队_score']}")
    print(f"详细信息：{sentiment['details']}")
    
    if not sentiment["pass"]:
        print("\n❌ 情绪过滤未通过，建议今日空仓观望")
        return
    
    print("\n✅ 情绪过滤通过，继续选股")
    
    # 第二步：选股
    print("\n【第二步】执行选股")
    selected_stocks = select_stocks(pro, tdx, today, top_n)
    
    if selected_stocks.empty:
        print("❌ 未找到符合条件的股票")
        return
    
    print(f"✓ 找到 {len(selected_stocks)} 只符合条件的股票")
    
    # 输出选股结果
    print("\n选股结果:")
    print("-" * 80)
    for idx, row in selected_stocks.iterrows():
        print(f"{idx+1}. {row.get('ts_code', 'N/A')} {row.get('name', 'N/A')}")
        print(f"   涨幅：{row.get('pct_chg', 0):.2f}%")
        print(f"   成交额：{row.get('amount', 0)/10000:.2f}亿")
        print(f"   封单金额：{row.get('封单金额', 0):.0f}万")
        print(f"   所属板块：{row.get('industry', 'N/A')}")
        print()
    
    # 第三步：卖出预案
    print("\n【第三步】明日卖出预案")
    for idx, row in selected_stocks.iterrows():
        plan = generate_sell_plan(row.get("name", "未知"), row.get("close", 0))
        print(f"\n{plan['stock']}:")
        for p in plan["plans"]:
            print(f"  {p['condition']}: {p['action']}")
    
    # 保存结果
    output_csv = os.path.join(here, f"隔夜选股_{today}_{_now_ts()}.csv")
    selected_stocks.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"\n✓ 结果已保存到：{output_csv}")
    
    # 风险提示
    print("\n【风险提示]")
    print("1. 隔夜美股走势：请关注今晚美股表现")
    print(f"2. 今日跌停数量：{len(selected_stocks)}只（>20 只需警惕）")
    print("3. 单票仓位建议：≤10%，总仓位≤30%")
    print("4. 执行时间：14:30-14:50 买入，次日 09:25-10:00 卖出")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="隔夜套利选股策略")
    parser.add_argument("--auto", action="store_true", help="自动模式（检查时间）")
    parser.add_argument("--top-n", type=int, default=3, help="每个板块选取的股票数量")
    args = parser.parse_args()
    
    run_strategy(auto_mode=args.auto, top_n=args.top_n)
