#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回踩低吸循环监控脚本
核心策略：监控突破后站稳3日的股票，寻找回踩关键位且缩量的低吸机会

适用：当天/次日入场
入场条件：
1. 股价回踩关键位（High60/MA60/MA120）
2. 回踩时缩量（量能＜昨日50% 最好）
3. 不跌破关键位的 0.98（硬止损位）

运行示例：
1）一次性跑一轮（便于验证）：
   python3 "backend/scripts/突破后站稳3日策略/2_回踩低吸循环监控.py" --once --bars 160 --categories 回踩触发,靠近关键位

2）持续循环监控（每分钟一轮）：
   python3 "backend/scripts/突破后站稳3日策略/2_回踩低吸循环监控.py" --bars 160 --categories 回踩触发,靠近关键位 --interval 60
"""

import argparse
import os
import sys
import time
from typing import Optional, Tuple, List, Dict, Any

import pandas as pd

_script_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = None
_probe_dir = _script_dir
for _ in range(8):
    if os.path.exists(os.path.join(_probe_dir, "utils", "pytdx_client.py")):
        backend_dir = _probe_dir
        break
    parent = os.path.dirname(_probe_dir)
    if parent == _probe_dir:
        break
    _probe_dir = parent
if backend_dir is None:
    backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from utils.pytdx_client import tdx, connected_endpoint

try:
    from utils.tushare_client import pro
except Exception:
    pro = None


PYTDX_VOL_UNIT = "手"
PYTDX_VOL_MULTIPLIER = 100


def _ts_code(market: int, code: str) -> str:
    code = str(code).zfill(6)
    if int(market) == 0:
        return f"{code}.SZ"
    return f"{code}.SH"


def _is_a_share_stock(market: int, code: str) -> bool:
    code = str(code or "").zfill(6)
    if int(market) == 0:
        return code.startswith(("000", "001", "002", "003", "300", "301"))
    if int(market) == 1:
        return code.startswith(("600", "601", "603", "605", "688"))
    return False


def _daily_bars(market: int, code: str, count: int) -> pd.DataFrame:
    data = tdx.get_security_bars(9, int(market), str(code).zfill(6), 0, int(count))
    df = tdx.to_df(data) if data else pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    if "datetime" not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"]).sort_values("datetime", ascending=True)
    for c in ("open", "close", "high", "low", "vol", "amount"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["open", "close", "high", "low", "vol"])
    df["vol"] = df["vol"] * PYTDX_VOL_MULTIPLIER
    df = df.reset_index(drop=True)
    return df


def calc_ma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window, min_periods=window).mean()


def calc_rolling_high(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window, min_periods=window).max()


def prepare_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or len(df) < 130:
        return df
    df = df.copy()
    df["ma20"] = calc_ma(df["close"], 20)
    df["ma60"] = calc_ma(df["close"], 60)
    df["ma120"] = calc_ma(df["close"], 120)
    df["ma20_vol"] = calc_ma(df["vol"], 20)
    df["high60"] = calc_rolling_high(df["high"], 60)
    df["ma60_prev"] = df["ma60"].shift(1)
    df["ma120_prev"] = df["ma120"].shift(1)
    df["ma20_vol_prev"] = df["ma20_vol"].shift(1)
    df["high60_prev"] = df["high60"].shift(1)
    return df


def get_key_levels(row: pd.Series) -> dict:
    levels = {}
    if pd.notna(row.get("high60_prev")):
        levels["High60"] = float(row["high60_prev"])
    elif pd.notna(row.get("high60")):
        levels["High60"] = float(row["high60"])
    if pd.notna(row.get("ma60_prev")):
        levels["MA60"] = float(row["ma60_prev"])
    elif pd.notna(row.get("ma60")):
        levels["MA60"] = float(row["ma60"])
    if pd.notna(row.get("ma120_prev")):
        levels["MA120"] = float(row["ma120_prev"])
    elif pd.notna(row.get("ma120")):
        levels["MA120"] = float(row["ma120"])
    return levels


def screen_one(
    symbol: str,
    name: str,
    market: int,
    breakout_date: str,
    key_level_type: str,
    key_level: float,
    breakout_price: float,
    bars_count: int,
) -> Optional[dict]:
    symbol = str(symbol).strip().zfill(6)
    df = _daily_bars(market, symbol, bars_count)
    if df.empty or len(df) < 130:
        return None

    df = prepare_indicators(df)
    if df.empty:
        return None

    last = df.iloc[-1]
    last_close = float(last["close"])
    last_vol = float(last["vol"])
    last_dt = pd.Timestamp(last["datetime"]).strftime("%Y-%m-%d")

    prev = df.iloc[-2]
    prev_vol = float(prev["vol"])

    if key_level <= 0:
        return None

    anchor_level = float(key_level)
    hard_stop = anchor_level * 0.98

    if last_close < hard_stop:
        return None

    key_levels = get_key_levels(last)

    if key_level_type == "High60" and "High60" not in key_levels:
        return None
    elif key_level_type == "MA60" and "MA60" not in key_levels:
        return None
    elif key_level_type == "MA120" and "MA120" not in key_levels:
        return None

    current_key_level = float(key_levels.get(key_level_type, anchor_level))

    if current_key_level <= 0:
        return None

    pullback_ratio = (anchor_level - last_close) / anchor_level
    if pullback_ratio < -0.01 or pullback_ratio > 0.05:
        return None

    if last_vol > prev_vol * 0.5:
        return None

    pullback_days = 1
    for i in range(1, min(6, len(df))):
        check_row = df.iloc[-(i + 1)]
        check_close = float(check_row["close"])
        check_low = float(check_row["low"])

        if check_low >= anchor_level * 0.98 and check_close <= anchor_level * 1.01:
            pullback_days += 1
        else:
            break

    pullback_vol_ratio = last_vol / prev_vol if prev_vol > 0 else 0

    last_ma20_vol = float(last["ma20_vol"])
    vol_ratio_to_ma20 = last_vol / last_ma20_vol if last_ma20_vol > 0 else 0

    return {
        "symbol": symbol,
        "name": name,
        "market": market,
        "trade_date": last_dt,
        "current_price": round(last_close, 4),
        "key_level_type": key_level_type,
        "key_level": round(anchor_level, 4),
        "current_key_level": round(current_key_level, 4),
        "pullback_ratio": round(pullback_ratio * 100, 2),
        "pullback_days": pullback_days,
        "vol_ratio_prev": round(pullback_vol_ratio, 3),
        "vol_ratio_ma20": round(vol_ratio_to_ma20, 3),
        "breakout_date": breakout_date,
        "breakout_price": round(breakout_price, 4),
        "hard_stop": round(hard_stop, 4),
        "reason": f"回踩{key_level_type}缩量{pullback_days}日,量能{pullback_vol_ratio*100:.1f}%昨日",
    }


def _load_input_df(input_file: str) -> pd.DataFrame:
    return pd.read_csv(
        input_file,
        dtype={
            "symbol": str,
            "name": str,
            "market": "Int64",
            "breakout_date": str,
            "key_level_type": str,
            "key_level": float,
            "breakout_price": float,
            "category": str,
        },
    )


def _parse_categories_arg(categories: str) -> List[str]:
    raw = (categories or "").strip()
    if not raw:
        return []
    if raw.lower() in ("all", "*"):
        return ["*"]
    parts = [p.strip() for p in raw.replace("，", ",").split(",")]
    return [p for p in parts if p]


def _filter_input_df_by_categories(input_df: pd.DataFrame, categories: List[str]) -> pd.DataFrame:
    if input_df is None or input_df.empty:
        return input_df
    if not categories or categories == ["*"]:
        return input_df
    if "category" not in input_df.columns:
        return input_df
    s = input_df["category"].astype(str).fillna("")
    keep = s.isin(set(categories))
    return input_df[keep].reset_index(drop=True)


def scan_once(input_df: pd.DataFrame, bars: int, per_stock_sleep: float) -> Tuple[bool, List[Dict[str, Any]], int, int]:
    rows: List[Dict[str, Any]] = []
    stat_total = 0
    stat_hit = 0

    last_error = None
    for _ in range(2):
        try:
            with tdx:
                print(f"pytdx connected_endpoint={connected_endpoint()}")
                for i, row in input_df.iterrows():
                    symbol = str(row.get("symbol", "")).strip()
                    name = str(row.get("name", "")).strip()
                    market = int(row.get("market", 0))
                    breakout_date = str(row.get("breakout_date", "")).strip()
                    key_level_type = str(row.get("key_level_type", "")).strip()
                    key_level = float(row.get("key_level", 0))
                    breakout_price = float(row.get("breakout_price", 0))

                    r = None
                    try:
                        r = screen_one(
                            symbol=symbol,
                            name=name,
                            market=market,
                            breakout_date=breakout_date,
                            key_level_type=key_level_type,
                            key_level=key_level,
                            breakout_price=breakout_price,
                            bars_count=bars,
                        )
                    except Exception as e:
                        print(f"异常: {symbol} {name} {e}")
                        r = None

                    stat_total += 1
                    if r is not None:
                        stat_hit += 1
                        rows.append(r)
                        print(
                            f"命中: {r['symbol']} {r['name']} | "
                            f"回踩{r['key_level_type']} {r['pullback_ratio']}% | "
                            f"缩量{r['vol_ratio_prev']*100:.1f}% | "
                            f"回踩{r['pullback_days']}日"
                        )

                    if per_stock_sleep and per_stock_sleep > 0:
                        time.sleep(float(per_stock_sleep))

                    if i % 50 == 0:
                        print(f"进度: {i+1}/{len(input_df)}")
            last_error = None
            break
        except Exception as e:
            last_error = e
            time.sleep(0.6)

    if last_error is not None:
        print(f"pytdx 连接失败或执行异常: {last_error}")
        return False, [], stat_total, 0

    return True, rows, stat_total, stat_hit


def main():
    parser = argparse.ArgumentParser(description="回踩低吸循环监控策略")
    parser.add_argument(
        "--input",
        type=str,
        default=os.getenv("INPUT", ""),
        help="输入CSV路径（默认：同目录 breakout_hold_3days.csv）",
    )
    parser.add_argument(
        "--categories",
        type=str,
        default=os.getenv("CATEGORIES", "回踩触发,靠近关键位"),
        help="按breakout_hold_3days.csv里的category过滤输入（逗号分隔；all表示不过滤）",
    )
    parser.add_argument(
        "--bars",
        type=int,
        default=int(os.getenv("BARS", "130")),
        help="每只股票拉取的日线条数",
    )
    parser.add_argument(
        "--out",
        type=str,
        default=os.getenv("OUT", ""),
        help="输出CSV路径（留空则输出到脚本目录，文件名带时间戳）",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=float(os.getenv("SLEEP", "0.0")),
        help="每处理一只股票后的休眠秒数",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.getenv("INTERVAL", "60")),
        help="每轮扫描间隔秒数（默认60秒）",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="只跑一轮后退出（默认循环监控）",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=int(os.getenv("ROUNDS", "0")),
        help="最多运行轮数（0表示无限循环）",
    )

    args = parser.parse_args()

    input_file = args.input.strip()
    if not input_file:
        input_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "breakout_hold_3days.csv",
        )

    if not os.path.exists(input_file):
        print(f"输入文件不存在: {input_file}")
        return

    input_df = _load_input_df(input_file)
    categories = _parse_categories_arg(args.categories)
    input_df = _filter_input_df_by_categories(input_df, categories)

    print("=" * 60)
    print("回踩低吸循环监控策略")
    print("=" * 60)
    print(f"输入文件: {input_file}")
    print(f"监控股票数量: {len(input_df)}")
    if categories == ["*"] or not categories:
        print("category过滤: 不过滤")
    else:
        print(f"category过滤: {','.join(categories)}")
    print(f"参数: bars={args.bars}, interval={args.interval}s, once={args.once}, rounds={args.rounds}")

    rounds_done = 0
    try:
        while True:
            rounds_done += 1
            round_ts = time.strftime("%Y-%m-%d %H:%M:%S")
            print("-" * 60)
            print(f"第{rounds_done}轮开始: {round_ts}")

            t0 = time.perf_counter()
            ok, rows, stat_total, stat_hit = scan_once(input_df, args.bars, args.sleep)
            elapsed = time.perf_counter() - t0

            print(f"本轮耗时: {elapsed:.2f}s | 统计: 监控{stat_total}只, 命中{stat_hit}只")

            if ok and rows:
                output_df = pd.DataFrame(rows)
                output_df = output_df.sort_values(["pullback_days", "vol_ratio_prev"], ascending=[False, True])
                output_df = output_df.reset_index(drop=True)

                ts = time.strftime("%Y%m%d_%H%M%S")
                out = args.out.strip()
                if not out:
                    out = os.path.join(
                        os.path.dirname(os.path.abspath(__file__)),
                        f"回踩低吸_{ts}.csv",
                    )

                output_df.to_csv(out, index=False, encoding="utf-8-sig")
                print(f"输出: {out}")
                print(output_df[[
                    "symbol", "name", "trade_date", "current_price", "key_level_type",
                    "key_level", "pullback_ratio", "pullback_days", "vol_ratio_prev", "reason"
                ]].head(50).to_string())
            else:
                print("无结果")

            if args.once:
                break
            if args.rounds and rounds_done >= args.rounds:
                break

            sleep_s = max(0.0, float(args.interval) - elapsed)
            time.sleep(sleep_s)
    except KeyboardInterrupt:
        print("收到中断信号，退出")


if __name__ == "__main__":
    main()
