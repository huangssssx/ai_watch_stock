#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
突破后站稳3日选股脚本
核心策略：先等放量突破关键位，再连续3日收盘价站稳，才确认有效、再入场
"""

import os
import sys
import time
import pandas as pd
import traceback
from datetime import datetime, timedelta

# 添加项目根目录到路径，以便导入模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# 导入tushare client
from backend.utils.tushare_client import pro
# 导入pytdx client
from backend.utils.pytdx_client import tdx

DEFAULT_VERIFY_TS_CODES = ["000001.SZ", "600519.SH", "300750.SZ"]
PRINT_API_PARAMS = True
PRINT_STAGE_LOGS = True


def normalize_ts_code(code):
    code = str(code).strip().upper()
    if code.endswith(".SZ") or code.endswith(".SH"):
        return code
    if len(code) == 6 and code.isdigit():
        return f"{code}.SH" if code.startswith("6") else f"{code}.SZ"
    return code


def get_target_ts_codes():
    args = [a.strip() for a in sys.argv[1:] if a.strip()]
    if not args:
        return [normalize_ts_code(x) for x in DEFAULT_VERIFY_TS_CODES]
    if "--all" in args:
        return []
    return [normalize_ts_code(x) for x in args if x != "--verify"]


def stage_print(ts_code, stage, message):
    if PRINT_STAGE_LOGS:
        print(f"[{ts_code}][S{stage}] {message}")


def get_trading_date(n_days_ago=0):
    """
    获取最近的交易日
    :param n_days_ago: 多少天前的交易日
    :return: 交易日期字符串，格式为YYYYMMDD
    """
    today = datetime.now()
    for i in range(30):
        target_date = today - timedelta(days=i + n_days_ago)
        date_str = target_date.strftime('%Y%m%d')
        try:
            if PRINT_API_PARAMS and n_days_ago == 0 and i == 0:
                print(f"[API] pro.trade_cal exchange=SSE start_date={date_str} end_date={date_str}")
            cal_df = pro.trade_cal(exchange='SSE', start_date=date_str, end_date=date_str)
            if not cal_df.empty and str(cal_df.iloc[0].get('is_open', '0')) == '1':
                return date_str
        except Exception as e:
            pass
    raise Exception("无法获取有效的交易日")


def get_trading_dates(n_days=30):
    """
    获取最近的n个交易日
    :param n_days: 需要的交易日数量
    :return: 交易日期字符串列表，格式为YYYYMMDD
    """
    today = datetime.now()
    end_date = today.strftime("%Y%m%d")
    start_date = (today - timedelta(days=240)).strftime("%Y%m%d")
    try:
        if PRINT_API_PARAMS:
            print(f"[API] pro.trade_cal exchange=SSE start_date={start_date} end_date={end_date}")
        cal_df = pro.trade_cal(exchange="SSE", start_date=start_date, end_date=end_date)
        if cal_df is not None and not cal_df.empty:
            open_df = cal_df[cal_df["is_open"].astype(str) == "1"].sort_values("cal_date", ascending=False)
            dates = open_df["cal_date"].head(n_days).tolist()
            if len(dates) >= n_days:
                return [str(x) for x in dates]
    except Exception as e:
        msg = str(e)
        print(f"[WARN] trade_cal 失败：{msg}")

    dates = pd.bdate_range(end=today, periods=n_days).strftime("%Y%m%d").tolist()
    dates = list(reversed(dates))
    print("[WARN] 使用工作日回退生成交易日列表（可能包含节假日，但用于拉取日线区间足够）")
    return dates


def get_all_stock_codes():
    """
    使用 pytdx 获取全市场股票代码列表（比 tushare 快很多）
    :return: 包含ts_code和name的DataFrame
    """
    try:
        # 连接 pytdx
        with tdx:
            # 获取沪市股票列表
            sh_list = []
            for start in range(0, 5000, 100):
                result = tdx.get_security_list(1, start)
                if result:
                    sh_list.extend(result)
                else:
                    break
            
            # 获取深市股票列表
            sz_list = []
            for start in range(0, 5000, 100):
                result = tdx.get_security_list(0, start)
                if result:
                    sz_list.extend(result)
                else:
                    break
            
            # 合并列表
            all_stocks = sh_list + sz_list
            
            # 转换为 DataFrame
            df = pd.DataFrame(all_stocks)
            df['ts_code'] = df['code'].apply(lambda x: f"{x}.SH" if x.startswith('6') else f"{x}.SZ")
            df['name'] = df['name'].str.decode('gbk') if df['name'].dtype == 'object' else df['name']
            
            print(f"获取到 {len(df)} 只股票")
            return df[['ts_code', 'name']]
            
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        # 回退到 tushare
        try:
            df = pro.stock_basic(
                exchange='',
                list_status='L',
                fields='ts_code,name'
            )
            print(f"获取到 {len(df)} 只股票")
            return df
        except Exception as e2:
            print(f"tushare 也失败: {e2}")
            return pd.DataFrame()


def get_daily_data(ts_code, start_date, end_date):
    """
    获取股票日线数据（使用 tushare，因为 pytdx 对 ETF/指数数据有问题）
    :param ts_code: 股票代码
    :param start_date: 开始日期
    :param end_date: 结束日期
    :return: 日线数据DataFrame
    """
    try:
        if PRINT_API_PARAMS:
            print(f"[API] pro.daily ts_code={ts_code} start_date={start_date} end_date={end_date}")
        df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if not df.empty:
            df = df.sort_values('trade_date', ascending=False)
        return df
    except Exception as e:
        print(f"获取{ts_code}日线数据失败: {e}")
        return pd.DataFrame()


def get_daily_basic_data(ts_code, trade_date):
    """
    获取股票每日指标数据
    :param ts_code: 股票代码
    :param trade_date: 交易日期
    :return: 每日指标DataFrame
    """
    try:
        if PRINT_API_PARAMS:
            print(f"[API] pro.daily_basic ts_code={ts_code} trade_date={trade_date}")
        df = pro.daily_basic(ts_code=ts_code, trade_date=trade_date)
        return df
    except Exception as e:
        return pd.DataFrame()


def get_money_flow_data(ts_codes, trade_date):
    """获取股票资金流向数据"""
    try:
        trade_date_str = str(trade_date)
        batch_size = 200
        result = []
        
        for i in range(0, len(ts_codes), batch_size):
            batch_codes = ts_codes[i:i+batch_size]
            if PRINT_API_PARAMS:
                print(f"[API] pro.moneyflow_dc ts_code={','.join(batch_codes)} trade_date={trade_date_str}")
            df = pro.moneyflow_dc(ts_code=','.join(batch_codes), trade_date=trade_date_str)
            result.append(df)
            time.sleep(0.5)
        
        if result:
            return pd.concat(result, ignore_index=True)
        return pd.DataFrame()
    except Exception as e:
        print(f"获取资金流向数据失败: {e}")
        return pd.DataFrame()


def detect_key_levels(df, lookback_days=60):
    """
    检测多个关键阻力位（箱体上沿/前期高点/均线等）
    只要突破任意一个关键位并站稳，就算符合条件
    
    :param df: 日线数据DataFrame
    :param lookback_days: 回看天数
    :return: 关键位列表
    """
    try:
        if df is None or df.empty:
            return []

        df = df.sort_values('trade_date', ascending=True)
        if len(df) < lookback_days:
            lookback_days = len(df)
        
        if lookback_days < 20:
            return []
        
        recent_data = df.tail(lookback_days)
        
        key_levels = []
        
        # 1. 箱体上沿
        recent_high = recent_data['high'].max()
        recent_low = recent_data['low'].min()
        box_height = recent_high - recent_low
        
        # 只有当箱体高度合理时才添加箱体上沿（5%-50%）
        box_height_ratio = box_height / recent_low
        if 0.05 <= box_height_ratio <= 0.5:
            key_levels.append(('箱体上沿', recent_high))
        
        # 2. 前期高点（60天/90天）
        if lookback_days >= 60:
            key_levels.append(('前期高点60天', recent_high))
        if lookback_days >= 90:
            recent_data_90 = df.tail(90)
            high_90 = recent_data_90['high'].max()
            key_levels.append(('前期高点90天', high_90))
        
        # 3. 重要均线（60日、120日）
        if len(df) >= 60:
            ma60 = df['close'].tail(60).mean()
            key_levels.append(('60日均线', ma60))
        
        if len(df) >= 120:
            ma120 = df['close'].tail(120).mean()
            key_levels.append(('120日均线', ma120))
        
        # 4. 颈线位（简化：取箱体中位）
        if box_height > 0:
            neck_line = recent_low + box_height * 0.5
            key_levels.append(('颈线位', neck_line))
        
        # 去重并排序
        seen = set()
        deduped = []
        for name, level in key_levels:
            level_val = float(level)
            if level_val in seen:
                continue
            seen.add(level_val)
            deduped.append((name, level_val))
        deduped.sort(key=lambda x: x[1], reverse=True)
        return deduped
        
    except Exception as e:
        return []


def check_breakout_and_stable(df, current_date, breakout_lookback_days=10, key_level_lookback_days=90):
    """
    检测是否发生突破并站稳3日（检测多个关键位）
    :param df: 日线数据DataFrame
    :param current_date: 当前日期
    :return: (是否突破, 突破日期, 站稳天数, 突破时成交量倍数, 突破的关键位名称, 突破关键位价格, 站稳窗口明细DataFrame, debug_info)
    """
    try:
        debug_info = {"candidates": [], "selected": None}
        if df is None or df.empty or len(df) < 25:
            return False, None, 0, 0, None, None, pd.DataFrame(), debug_info

        df_sorted = df.sort_values('trade_date', ascending=True).reset_index(drop=True)
        if current_date in df_sorted['trade_date'].astype(str).tolist():
            latest_pos = int(df_sorted.index[df_sorted['trade_date'].astype(str) == str(current_date)][-1])
        else:
            latest_pos = len(df_sorted) - 1
        debug_info["latest_trade_date"] = str(df_sorted.iloc[latest_pos]["trade_date"])
        debug_info["latest_pos"] = int(latest_pos)

        end_breakout_pos = latest_pos - 2
        start_breakout_pos = max(0, latest_pos - (breakout_lookback_days + 2))
        debug_info["breakout_pos_range"] = [int(start_breakout_pos), int(end_breakout_pos)]
        if end_breakout_pos < 20:
            return False, None, 0, 0, None, None, pd.DataFrame(), debug_info

        for breakout_pos in range(end_breakout_pos, start_breakout_pos - 1, -1):
            pre_start = max(0, breakout_pos - key_level_lookback_days)
            pre_df = df_sorted.iloc[pre_start:breakout_pos].copy()
            if len(pre_df) < 25:
                continue

            if breakout_pos >= 20:
                avg_volume_20 = df_sorted['vol'].iloc[breakout_pos - 20:breakout_pos].mean()
            else:
                avg_volume_20 = df_sorted['vol'].iloc[:breakout_pos].mean()

            if avg_volume_20 <= 0:
                continue

            key_levels = detect_key_levels(pre_df, lookback_days=min(90, len(pre_df)))
            if not key_levels:
                continue

            breakout_row = df_sorted.iloc[breakout_pos]
            stable_window = df_sorted.iloc[breakout_pos:breakout_pos + 3].copy()
            if len(stable_window) < 3:
                continue

            for level_name, key_level in key_levels:
                price_ok = breakout_row['close'] >= key_level * 1.015
                if not price_ok:
                    continue
                vol_ok = breakout_row['vol'] >= avg_volume_20 * 1.5
                if not vol_ok:
                    continue

                close_series = stable_window['close']
                vol_series = stable_window['vol']

                soft_ok = (close_series >= key_level * 0.99).all()
                weak_days = int((close_series < key_level).sum())
                volume_ok = (vol_series >= avg_volume_20 * 0.5).all()
                stable_ok = bool(soft_ok and weak_days <= 1 and volume_ok)

                candidate = {
                    "breakout_date": str(breakout_row["trade_date"]),
                    "level_name": str(level_name),
                    "key_level": float(key_level),
                    "breakout_close": float(breakout_row["close"]),
                    "breakout_vol": float(breakout_row["vol"]),
                    "avg_volume_20": float(avg_volume_20),
                    "volume_ratio": float(breakout_row["vol"]) / float(avg_volume_20),
                    "window_dates": [str(x) for x in stable_window["trade_date"].tolist()],
                    "window_closes": [float(x) for x in close_series.tolist()],
                    "window_vols": [float(x) for x in vol_series.tolist()],
                    "soft_ok": bool(soft_ok),
                    "weak_days": int(weak_days),
                    "volume_ok": bool(volume_ok),
                    "stable_ok": bool(stable_ok),
                }
                debug_info["candidates"].append(candidate)

                if not stable_ok:
                    continue

                volume_ratio = float(breakout_row['vol']) / float(avg_volume_20)
                stable_window = stable_window.assign(
                    key_level=float(key_level),
                    soft_ok=(close_series >= key_level * 0.99).values,
                    hard_ok=(close_series >= key_level).values,
                    volume_ok=(vol_series >= avg_volume_20 * 0.5).values,
                )
                debug_info["selected"] = candidate
                return True, candidate["breakout_date"], 3, volume_ratio, level_name, float(key_level), stable_window, debug_info

        return False, None, 0, 0, None, None, pd.DataFrame(), debug_info
        
    except Exception as e:
        return False, None, 0, 0, None, None, pd.DataFrame(), {"candidates": [], "selected": None, "error": str(e)}


def is_valid_box_shape(df, key_level, current_date):
    """
    检测是否具有清晰的箱体形态
    :param df: 日线数据DataFrame
    :param key_level: 关键位价格
    :param current_date: 当前日期
    :return: 是否符合箱体形态
    """
    try:
        # 放宽数据长度要求到20天
        if len(df) < 20:
            return False
        
        # 获取最近20天的数据
        recent_data = df.head(20)
        
        # 获取最新收盘价
        latest_close = recent_data.iloc[0]['close']
        
        # 如果股价已经站上关键位，说明已经突破，不再需要严格的箱体形态
        if latest_close > key_level * 0.98:
            return True
        
        # 如果还没突破，检查是否有箱体形态
        box_high = recent_data['high'].max()
        box_low = recent_data['low'].min()
        box_height = box_high - box_low
        
        # 箱体高度要求：5%-50%（覆盖双底、头肩底、上升三角形、长期横盘）
        box_height_ratio = box_height / box_low
        
        if not (0.05 <= box_height_ratio <= 0.5):
            return False
        
        # 检测价格是否在箱体内多次测试
        touches = 0
        for i in range(len(recent_data)):
            row = recent_data.iloc[i]
            # 接近箱体上沿或下沿
            if abs(row['high'] - box_high) < box_height * 0.15 or abs(row['low'] - box_low) < box_height * 0.15:
                touches += 1
        
        # 至少有1次触碰（放宽要求）
        if touches < 1:
            return False
        
        # 最近价格应接近箱体上沿（准备突破状态）
        if latest_close < box_high * 0.90:
            return False
        
        return True
        
    except Exception as e:
        return False


def analyze_stock(ts_code, name, trading_dates):
    """
    分析单只股票是否符合突破后站稳3日策略
    :param ts_code: 股票代码
    :param name: 股票名称
    :param trading_dates: 交易日期列表（最新的在前）
    :return: 分析结果dict或None
    """
    try:
        t0 = time.time()
        stage_print(ts_code, 1, "开始")
        latest_date = trading_dates[0]
        start_date = trading_dates[-1]
        stage_print(ts_code, 1, f"交易区间 start_date={start_date} end_date={latest_date}")
        
        stage_print(ts_code, 2, "拉取日线数据")
        print(f"[获取数据]", end=' ', flush=True)
        df = get_daily_data(ts_code, start_date, latest_date)
        if df.empty:
            print(f"[空数据]", flush=True)
            stage_print(ts_code, 3, "FAIL：日线返回为空")
            return None
        print(f"[{len(df)}条]", flush=True)

        required_cols = ["trade_date", "open", "high", "low", "close", "vol"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            stage_print(ts_code, 3, f"FAIL：日线缺列 {missing_cols}")
            return None
        stage_print(ts_code, 3, f"PASS：日线列齐全，rows={len(df)}")

        df_sorted = df.sort_values("trade_date", ascending=True)
        earliest_date = str(df_sorted.iloc[0]["trade_date"])
        latest_trade_date = str(df_sorted.iloc[-1]["trade_date"])
        stage_print(ts_code, 4, f"日期范围 {earliest_date} ~ {latest_trade_date}")
        stage_print(ts_code, 4, f"最新3行:\n{df_sorted.tail(3)[['trade_date','close','vol']].to_string(index=False)}")
        stage_print(ts_code, 4, f"最旧3行:\n{df_sorted.head(3)[['trade_date','close','vol']].to_string(index=False)}")

        close_num = pd.to_numeric(df["close"], errors="coerce")
        vol_num = pd.to_numeric(df["vol"], errors="coerce")
        stage_print(ts_code, 4, f"数据质量 close_nan={int(close_num.isna().sum())} vol_nan={int(vol_num.isna().sum())}")
        stage_print(ts_code, 4, f"异常值 close<=0={int((close_num<=0).sum())} vol<=0={int((vol_num<=0).sum())}")
        
        stage_print(ts_code, 5, "检测突破与站稳")
        is_stable, breakout_date, stable_days, volume_ratio, 突破关键位名称, 突破关键位, 站稳窗口, debug_info = check_breakout_and_stable(
            df, latest_date
        )
        
        stage_print(ts_code, 5, f"候选数={len(debug_info.get('candidates', []))} selected={bool(debug_info.get('selected'))}")
        if debug_info.get("selected"):
            sel = debug_info["selected"]
            stage_print(ts_code, 5, f"命中：breakout_date={sel['breakout_date']} level={sel['level_name']} key_level={sel['key_level']:.2f} vol_ratio={sel['volume_ratio']:.2f}")
            stage_print(ts_code, 5, f"3日窗口 dates={sel['window_dates']} closes={sel['window_closes']} vols={sel['window_vols']}")
            stage_print(ts_code, 5, f"校验 soft_ok={sel['soft_ok']} weak_days={sel['weak_days']} volume_ok={sel['volume_ok']}")
        
        if not is_stable:
            stage_print(ts_code, 6, "FAIL：未满足突破+站稳3日+不缩量")
            return None
        
        latest_data = df_sorted.iloc[-1]
        latest_close = float(latest_data["close"])
        
        if 突破关键位:
            breakout_price = 突破关键位 * 1.015
            breakout_percent = ((latest_close - 突破关键位) / 突破关键位) * 100
        else:
            breakout_price = latest_close
            breakout_percent = 0
        
        # 计算近期涨幅（突破后）
        df_sorted = df_sorted.reset_index(drop=True)
        if breakout_date and breakout_date in df_sorted['trade_date'].astype(str).tolist():
            breakout_pos = int(df_sorted.index[df_sorted['trade_date'].astype(str) == str(breakout_date)][-1])
            breakout_close = float(df_sorted.iloc[breakout_pos]['close'])
            current_close = float(df_sorted.iloc[-1]['close'])
            gain_since_breakout = ((current_close - breakout_close) / breakout_close) * 100 if breakout_close > 0 else 0
        else:
            gain_since_breakout = 0

        stage_print(ts_code, 6, f"PASS：入选 key_level={float(突破关键位) if 突破关键位 else 0:.2f} breakout_date={breakout_date} breakout%={breakout_percent:.2f} vol_ratio={volume_ratio:.2f}")
        stage_print(ts_code, 6, f"耗时 {time.time()-t0:.2f}s")
        
        return {
            'symbol': ts_code,
            'name': name,
            'key_level': round(float(突破关键位) if 突破关键位 else 0, 2),
            'latest_close': round(latest_close, 2),
            'breakout_price': round(breakout_price, 2),
            'breakout_percent': round(breakout_percent, 2),
            'stable_days': stable_days,
            'volume_ratio': round(volume_ratio, 2),
            'gain_since_breakout': round(gain_since_breakout, 2),
            'breakout_date': breakout_date,
            'reason': f'突破关键位{float(突破关键位) if 突破关键位 else 0:.2f}并站稳{stable_days}日，成交量放大{volume_ratio:.2f}倍'
        }
        
    except Exception as e:
        print(f"分析{ts_code}时出错: {e}")
        return None


def main():
    """
    主函数
    """
    print("=" * 60)
    print("开始执行'突破后站稳3日'选股策略...")
    print("=" * 60)
    
    # 获取最近的30个交易日
    trading_dates = get_trading_dates(30)
    latest_date = trading_dates[0]
    print(f"最近的30个交易日: {trading_dates[:5]}... (共{len(trading_dates)}天)")
    
    target_ts_codes = get_target_ts_codes()
    if target_ts_codes:
        print(f"仅分析指定股票: {target_ts_codes}")
        stock_list = pd.DataFrame({"ts_code": target_ts_codes})
        try:
            stock_basic = pro.stock_basic(exchange="", list_status="L", fields="ts_code,name")
            if stock_basic is not None and not stock_basic.empty:
                stock_list = stock_list.merge(stock_basic, on="ts_code", how="left")
            else:
                stock_list["name"] = ""
        except Exception:
            stock_list["name"] = ""
    else:
        print("获取全市场股票代码...")
        stock_list = get_all_stock_codes()
        if stock_list.empty:
            print("没有获取到股票列表，退出")
            return
    
    # 筛选条件
    print("\n筛选条件：")
    print("1. 箱体高度5%-50%（横盘整理1-3个月）")
    print("2. 收盘价站上关键位幅度>=1.5%")
    print("3. 突破日成交量>=前期均量1.5倍")
    print("4. 连续3日站稳关键位（允许1天回踩≤1%）")
    print("5. 结果附加字段：资金净流入")
    print()
    
    # 存储符合条件的股票
    result_stocks = []
    
    # 遍历股票列表
    total = len(stock_list)
    test_mode = False if target_ts_codes else True
    max_stocks = total if not test_mode else min(50, total)
    
    print(f"开始分析 {max_stocks} 只股票...")
    print()
    
    for i, row in stock_list.iterrows():
        if i >= max_stocks:
            break
        
        ts_code = row['ts_code']
        name = row.get('name', '')
        
        # 只打印进度每10只一次
        if (i + 1) % 10 == 0 or i == 0:
            print(f"分析 {i+1}/{max_stocks}: {ts_code} {name} ...", end=' ', flush=True)
        
        result = analyze_stock(ts_code, name, trading_dates)
        
        if result:
            result_stocks.append(result)
            if (i + 1) % 10 == 0 or i == 0:
                print(f"✓ 符合条件！", flush=True)
        else:
            if (i + 1) % 10 == 0 or i == 0:
                print("✗", flush=True)
        
        # 避免请求过于频繁
        time.sleep(0.3)
    
    # 输出结果
    print()
    print("=" * 60)
    print(f"分析完成！共发现 {len(result_stocks)} 只符合条件的股票")
    print("=" * 60)
    
    if result_stocks:
        result_df = pd.DataFrame(result_stocks)
        
        # 按突破后涨幅排序
        result_df = result_df.sort_values('gain_since_breakout', ascending=False)
        result_df = result_df.reset_index(drop=True)
        
        # 批量获取资金流向和每日指标（只对符合条件的股票）
        print("\n批量获取资金流向和市值数据...")
        ts_codes = result_df['symbol'].tolist()
        latest_date_str = str(latest_date)
        
        # 批量获取资金流向
        moneyflow_df = get_money_flow_data(ts_codes, latest_date_str)
        moneyflow_dict = {}
        if not moneyflow_df.empty:
            for _, row in moneyflow_df.iterrows():
                moneyflow_dict[row.get('ts_code', '')] = row.get('net_amount', 0)
        
        # 批量获取每日指标
        daily_basic_dict = {}
        for ts_code in ts_codes:
            basic_df = get_daily_basic_data(ts_code, latest_date_str)
            if not basic_df.empty:
                daily_basic_dict[ts_code] = basic_df.iloc[0].get('circ_mv', 0)
        
        # 更新结果DataFrame
        result_df['net_amount'] = result_df['symbol'].apply(lambda x: moneyflow_dict.get(x, 0))
        result_df['circ_mv'] = result_df['symbol'].apply(lambda x: daily_basic_dict.get(x, 0))
        
        # 保存结果
        output_file = f"突破后站稳3日_{latest_date}.csv"
        result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"结果已保存到: {output_file}")
        
        # 打印前10只股票
        print("\n前10只符合条件的股票：")
        print("-" * 100)
        for idx, row in result_df.head(10).iterrows():
            print(f"{idx+1}. {row['symbol']} {row['name']}")
            print(f"   关键位: {row['key_level']:.2f}, 当前价: {row['latest_close']:.2f}, 突破幅度: {row['breakout_percent']:.2f}%")
            print(f"   站稳天数: {row['stable_days']}日, 成交量倍数: {row['volume_ratio']:.2f}倍")
            print(f"   资金流入: {row['net_amount']:.2f}万元, 突破后涨幅: {row['gain_since_breakout']:.2f}%")
            print(f"   市值: {row['circ_mv']:.2f}亿元")
            print(f"   入选原因: {row['reason']}")
            print()
        
        # 策略总结
        print("\n策略执行总结：")
        print(f"  - 分析股票数: {max_stocks}")
        print(f"  - 符合条件数: {len(result_stocks)}")
        print(f"  - 通过率: {len(result_stocks)/max_stocks*100:.2f}%")
        
        # 按资金流入排序的前5只
        print("\n资金流入最多的前5只股票：")
        top_by_flow = result_df.nlargest(5, 'net_amount')
        for idx, row in top_by_flow.iterrows():
            print(f"  {row['symbol']} {row['name']}: {row['net_amount']:.2f}万元")
        
    else:
        print("未发现符合条件的股票")


if __name__ == "__main__":
    main()
