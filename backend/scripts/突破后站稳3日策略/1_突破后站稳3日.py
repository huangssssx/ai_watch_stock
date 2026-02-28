#!/usr/bin/env python3
"""
突破后站稳3日选股策略

用途：
- 从 A 股全市场股票池中，筛选出“在 T-3 日发生突破，随后连续 3 日站稳关键位”的强形态候选
- 结果更适合作为后续人工/模型分析的候选池，而不是直接的次日涨跌预测器

策略逻辑：
1. 关键价位：High60（60日最高）、MA60、MA120
2. 突破条件（T-3日）：
   - High60突破：Close >= Key Level * 1.005
   - MA突破：Close >= Key Level * 1.015
   - 成交量放大：Volume >= 1.5 * MA20_Volume
   - 防脉冲：Volume <= 4.0 * MA20_Volume（可选）
3. 站稳条件（T-2, T-1, T日）：
   - 三日收盘 >= Key Level * 0.99
   - 至少2日收盘 >= Key Level
   - 每日成交量 >= 0.5 * MA20_Volume
"""
import argparse
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Optional

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

from utils.pytdx_client import tdx

try:
    from utils.tushare_client import pro
except Exception:
    pro = None


PYTDX_VOL_UNIT = "手"
PYTDX_VOL_MULTIPLIER = 100


@dataclass(frozen=True)
class StockDef:
    """
    股票基本定义（来自 pytdx 的证券列表）。

    - market: 0 深市，1 沪市（与 pytdx 的市场定义一致）
    - code: 6 位证券代码
    - name: 证券名称
    """

    market: int
    code: str
    name: str


@dataclass
class StrategyConfig:
    """
    策略参数集合。

    这里的 buffer/ratio 都是“硬阈值”，用于把候选池收敛到更强、形态更稳定的一批票。
    """

    breakout_buffer_high60: float = 1.005
    breakout_buffer_ma: float = 1.015
    vol_ratio_min: float = 1.5
    vol_ratio_max: float = 4.0
    stand_buffer: float = 0.99
    stand_days_min: int = 2
    stand_vol_ratio_min: float = 0.5
    min_listing_days: int = 140


def _ts_code(market: int, code: str) -> str:
    """
    将 pytdx 的 (market, code) 转成 tushare 的 ts_code。

    - market==0 => .SZ
    - market==1 => .SH
    """

    code = str(code).zfill(6)
    if int(market) == 0:
        return f"{code}.SZ"
    return f"{code}.SH"


def _is_a_share_stock(market: int, code: str) -> bool:
    """
    判断是否属于 A 股主流股票代码段。

    目的：过滤掉基金、债券、指数、B 股等非目标品种，减少无意义请求。
    """

    code = str(code or "").zfill(6)
    if int(market) == 0:
        return code.startswith(("000", "001", "002", "003", "300", "301"))
    if int(market) == 1:
        return code.startswith(("600", "601", "603", "605", "688"))
    return False


def _iter_all_a_share_defs() -> Iterable[StockDef]:
    """
    遍历 pytdx 全市场证券列表，并按代码段过滤出 A 股股票池。

    这里取的是“交易所证券列表”，不等价于“可交易且不 ST 的在市股票”；
    如果 tushare pro 可用，会在 main() 中进一步按 pro.stock_basic 做二次过滤。
    """

    for market in (0, 1):
        total = tdx.get_security_count(market)
        step = 1000
        for start in range(0, int(total), step):
            rows = tdx.get_security_list(market, start) or []
            for r in rows:
                code = str(r.get("code", "")).zfill(6)
                name = str(r.get("name", "")).strip()
                if code and _is_a_share_stock(market, code):
                    yield StockDef(market=int(market), code=code, name=name)


def _load_active_codes_from_tushare(config: StrategyConfig) -> Optional[dict[str, dict]]:
    """
    使用 tushare pro 获取“在市股票池”，并做基础过滤：

    - list_status="L"：在市
    - 名称含 ST：剔除（包括 *ST 等）
    - 上市天数过短：剔除（默认 min_listing_days=140，为避免边界误差做了 *1.5 的缓冲）

    返回：
    - {ts_code: {"name": str, "list_date": str}}
    - pro 不可用或异常时返回 None（会退化为仅依赖 pytdx 股票池）
    """

    if pro is None:
        return None
    try:
        df = pro.stock_basic(
            exchange="",
            list_status="L",
            fields="ts_code,name,list_status,list_date",
        )
        if df is None or df.empty:
            return None
        df = df.dropna(subset=["ts_code"]).copy()
        df["ts_code"] = df["ts_code"].astype(str).str.strip()
        df["name"] = df.get("name", "").astype(str).str.strip()
        
        df = df[~df["name"].str.contains("ST", na=False)]
        
        if "list_date" in df.columns:
            today = datetime.now()
            df["list_date"] = df["list_date"].astype(str).str.strip()
            df = df[df["list_date"].notna() & (df["list_date"] != "")]
            df["list_dt"] = pd.to_datetime(df["list_date"], format="%Y%m%d", errors="coerce")
            min_date = today - timedelta(days=int(config.min_listing_days) * 1.5)
            df = df[(df["list_dt"].isna()) | (df["list_dt"] <= min_date)]
        
        result = {}
        for _, row in df.iterrows():
            ts_code = row["ts_code"]
            result[ts_code] = {
                "name": row.get("name", ""),
                "list_date": row.get("list_date", ""),
            }
        return result
    except Exception as e:
        print(f"tushare获取股票列表异常: {e}")
        return None


def _daily_bars(market: int, code: str, count: int) -> pd.DataFrame:
    """
    获取并清洗日线数据（pytdx）。

    - category=9：日线
    - pytdx 的 vol 通常单位为“手”，此处统一转换为“股”（×100）方便与 amount 配合及后续比较
    - 返回按 datetime 升序排列、数值列已转为 numeric、必要字段缺失会返回空 DataFrame
    """

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
    """
    简单移动平均（SMA）。

    min_periods=window：确保在窗口未满时返回 NaN，避免把不完整均值误当作有效信号。
    """

    return series.rolling(window, min_periods=window).mean()


def calc_rolling_high(series: pd.Series, window: int) -> pd.Series:
    """
    滚动窗口最高值（用于 High60）。

    min_periods=window：确保 60 日窗口未满时不产生 High60，避免新股/数据不足导致的假信号。
    """

    return series.rolling(window, min_periods=window).max()


def prepare_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算策略所需指标。

    关键点：
    - 指标采用“当日值”主要用于展示与后续输出
    - 用于做突破判断的“关键位/量能基准”优先使用 *_prev（前一日值），避免把当日数据揉进阈值造成轻微的前视/自洽问题
      - High60：用 high60_prev 避免“当日创新高同时抬高 High60 阈值”导致 High60 突破难以触发
      - MA60/MA120：用 ma60_prev/ma120_prev，语义上更贴近“对前一日已形成的均线水平的突破”
      - MA20 成交量：用 ma20_vol_prev，避免用突破当日的放量去参与计算量比基准
    """

    if df is None or df.empty or len(df) < 120:
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
    """
    生成当日用于判断突破的关键位集合。

    优先使用 *_prev（前一日关键位），减少“阈值被当日数据影响”的问题。
    返回示例：
    - {"High60": 12.34, "MA60": 10.23, "MA120": 9.87}
    """

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


def detect_breakout(
    row: pd.Series,
    prev_row: pd.Series,
    config: StrategyConfig,
) -> Optional[dict]:
    """
    判断某一天（设计上为 T-3）是否满足“突破”条件。

    输入：
    - row: 当天数据（T-3）
    - prev_row: 前一日数据（用于做“穿越”判断，避免本来就站在阈值之上也被算作突破）

    输出：
    - 满足则返回突破信息（关键位类型、关键位数值、突破价、量比）
    - 否则返回 None
    """

    close = float(row.get("close", 0))
    prev_close = float(prev_row.get("close", 0))
    vol = float(row.get("vol", 0))
    ma20_vol = float(row.get("ma20_vol_prev", 0))
    
    if ma20_vol <= 0:
        return None
    
    vol_ratio = vol / ma20_vol
    if vol_ratio < config.vol_ratio_min:
        return None
    if vol_ratio > config.vol_ratio_max:
        return None
    
    key_levels = get_key_levels(row)
    if not key_levels:
        return None
    
    for level_type, level_value in key_levels.items():
        if level_value <= 0:
            continue
        
        if level_type == "High60":
            threshold = level_value * config.breakout_buffer_high60
        else:
            threshold = level_value * config.breakout_buffer_ma
        
        # 必须是“昨天没过阈值，今天过了阈值”的穿越式突破
        # 否则很多处于长期趋势上方的票，只要某天放量就会被误判为突破
        if prev_close >= threshold:
            continue
        if close >= threshold:
            return {
                "key_level_type": level_type,
                "key_level": level_value,
                "breakout_price": close,
                "breakout_vol_ratio": vol_ratio,
            }
    
    return None


def check_stand_firm(
    df: pd.DataFrame,
    breakout_idx: int,
    key_level: float,
    config: StrategyConfig,
) -> Optional[dict]:
    """
    检查突破后的“站稳 3 日”条件（T-2, T-1, T）。

    条件结构：
    - 价格：三天最低收盘不低于 key_level * stand_buffer（允许小幅回踩但不允许明显跌破）
    - 价格：三天里至少 stand_days_min 天收盘 >= key_level（多数时间站在关键位之上）
    - 成交量：三天每天成交量 >= ma20_vol_prev * stand_vol_ratio_min（站稳期不能无量虚站）
    """

    if breakout_idx + 3 > len(df):
        return None
    
    stand_days = df.iloc[breakout_idx + 1 : breakout_idx + 4]
    if len(stand_days) < 3:
        return None
    
    closes = stand_days["close"].values
    vols = stand_days["vol"].values
    ma20_vols = stand_days["ma20_vol_prev"].values
    
    min_close_ratio = min(closes) / key_level
    if min_close_ratio < config.stand_buffer:
        return None
    
    days_above = sum(1 for c in closes if c >= key_level)
    if days_above < config.stand_days_min:
        return None
    
    for i in range(3):
        if ma20_vols[i] <= 0:
            return None
        if vols[i] < ma20_vols[i] * config.stand_vol_ratio_min:
            return None
    
    return {
        "stand_days_above": int(days_above),
        "min_close_ratio": round(float(min_close_ratio), 4),
        "day1_close": float(closes[0]),
        "day2_close": float(closes[1]),
        "day3_close": float(closes[2]),
    }


def screen_one(
    stock: StockDef,
    bars_count: int,
    config: StrategyConfig,
) -> Optional[dict]:
    """
    对单只股票做策略筛选。

    设计约束（用于避免“历史任意一次满足就命中”导致命中过多）：
    - 只检查固定的 breakout_idx = len(df)-4（即 T-3）是否发生突破
    - 再检查其后 3 天（T-2/T-1/T）是否站稳
    - 若命中，则返回结构化结果；否则返回 None
    """

    df = _daily_bars(stock.market, stock.code, bars_count)
    if df.empty or len(df) < 130:
        return None
    
    df = prepare_indicators(df)
    if df.empty:
        return None
    
    valid_df = df.dropna(subset=["ma20_vol", "high60", "ma60", "ma120"])
    if len(valid_df) < 10:
        return None
    
    # 固定使用倒数第 4 根 K 作为突破日（T-3）
    # - df.iloc[-1] 视为 T 日（最新交易日）
    # - df.iloc[-4] 视为 T-3（突破发生日）
    breakout_idx = len(df) - 4
    if breakout_idx <= 0:
        return None
    row = df.iloc[breakout_idx]
    prev_row = df.iloc[breakout_idx - 1]
    
    breakout = detect_breakout(row, prev_row, config)
    if breakout is None:
        return None
    
    stand_result = check_stand_firm(df, breakout_idx, breakout["key_level"], config)
    if stand_result is None:
        return None
    
    last = df.iloc[-1]
    breakout_date = pd.Timestamp(row["datetime"]).strftime("%Y-%m-%d")
    
    return {
        "symbol": stock.code,
        "name": stock.name,
        "market": stock.market,
        "breakout_date": breakout_date,
        "key_level_type": breakout["key_level_type"],
        "key_level": round(breakout["key_level"], 4),
        "breakout_price": round(breakout["breakout_price"], 4),
        "current_price": round(float(last["close"]), 4),
        "breakout_vol_ratio": round(breakout["breakout_vol_ratio"], 3),
        "stand_days_above": stand_result["stand_days_above"],
        "min_close_ratio": stand_result["min_close_ratio"],
        "trade_date": pd.Timestamp(last["datetime"]).strftime("%Y-%m-%d"),
    }
    
    return None


def main():
    """
    脚本入口。

    运行方式示例：
    - python3 "backend/scripts/突破后站稳3日策略/1_突破后站稳3日.py" --max-stocks 500 --bars 150

    输出：
    - CSV：包含命中股票列表及关键信息，便于后续人工/模型分析
    """

    parser = argparse.ArgumentParser(description="突破后站稳3日选股策略")
    parser.add_argument(
        "--max-stocks",
        type=int,
        default=int(os.getenv("MAX_STOCKS", "0")),
        help="最多处理多少只股票（从股票池前 N 只依次处理）；<=0 表示全市场（默认）",
    )
    parser.add_argument(
        "--bars",
        type=int,
        default=int(os.getenv("BARS", "150")),
        help="每只股票拉取的日线条数（至少需要覆盖 MA120/High60 与固定的 T-3/T-2/T-1/T 窗口）",
    )
    parser.add_argument(
        "--out",
        type=str,
        default=os.getenv("OUT", ""),
        help="输出 CSV 路径（留空则输出到脚本目录，文件名带时间戳）",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=float(os.getenv("SLEEP", "0.0")),
        help="每处理一只股票后的休眠秒数，用于放慢请求节奏（默认 0）",
    )
    
    parser.add_argument(
        "--breakout-buffer-high60",
        type=float,
        default=1.005,
        help="High60 突破阈值系数：Close >= High60_prev * 系数（更接近 1 越容易触发）",
    )
    parser.add_argument(
        "--breakout-buffer-ma",
        type=float,
        default=1.015,
        help="MA60/MA120 突破阈值系数：Close >= MA_prev * 系数（用于过滤“刚站上均线”的强势票）",
    )
    parser.add_argument(
        "--vol-ratio-min",
        type=float,
        default=1.5,
        help="突破日最小量比：Vol / MA20_Vol_prev >= 该值（过滤无量假突破）",
    )
    parser.add_argument(
        "--vol-ratio-max",
        type=float,
        default=4.0,
        help="突破日最大量比：Vol / MA20_Vol_prev <= 该值（过滤过度爆量脉冲）",
    )
    parser.add_argument(
        "--stand-buffer",
        type=float,
        default=0.99,
        help="站稳容忍回撤：站稳 3 日最低收盘 >= key_level * 该值（越接近 1 越严格）",
    )
    parser.add_argument(
        "--stand-days-min",
        type=int,
        default=2,
        help="站稳 3 日中，至少有几天收盘 >= key_level（默认 2，要求多数时间站上）",
    )
    parser.add_argument(
        "--min-listing-days",
        type=int,
        default=140,
        help="股票池过滤：上市至少多少天（仅 tushare pro 股票池生效；脚本内部会额外做 *1.5 的缓冲）",
    )
    
    args = parser.parse_args()
    
    config = StrategyConfig(
        breakout_buffer_high60=args.breakout_buffer_high60,
        breakout_buffer_ma=args.breakout_buffer_ma,
        vol_ratio_min=args.vol_ratio_min,
        vol_ratio_max=args.vol_ratio_max,
        stand_buffer=args.stand_buffer,
        stand_days_min=args.stand_days_min,
        min_listing_days=args.min_listing_days,
    )
    
    t0 = time.perf_counter()
    print("=" * 60)
    print("突破后站稳3日选股策略")
    print("=" * 60)
    print(f"参数: max_stocks={args.max_stocks}, bars={args.bars}")
    print(f"策略参数: breakout_buffer_high60={config.breakout_buffer_high60}, "
          f"breakout_buffer_ma={config.breakout_buffer_ma}")
    print(f"成交量: vol_ratio_min={config.vol_ratio_min}, vol_ratio_max={config.vol_ratio_max}")
    print(f"站稳: stand_buffer={config.stand_buffer}, stand_days_min={config.stand_days_min}")
    
    # 股票池优先使用 tushare pro 的“在市且非 ST 且上市较久”的列表
    # - pro 不可用时退化为仅用 pytdx 证券列表按代码段过滤
    active_map = _load_active_codes_from_tushare(config)
    if active_map is None:
        print("tushare pro 不可用或无数据：仅用 pytdx 股票池")
    else:
        print(f"tushare pro 股票池: {len(active_map)}")
    
    stocks: list[StockDef] = []
    with tdx:
        # 组装股票池：在 pytdx 证券列表上做过滤，并可选对齐 tushare 的在市股票池
        for s in _iter_all_a_share_defs():
            if active_map is not None:
                ts_code = _ts_code(s.market, s.code)
                if ts_code not in active_map:
                    continue
                name = active_map.get(ts_code, {}).get("name") or s.name
                stocks.append(StockDef(market=s.market, code=s.code, name=name))
            else:
                stocks.append(s)
            if args.max_stocks > 0 and len(stocks) >= int(args.max_stocks):
                break
        
        print(f"股票池数量: {len(stocks)}")
        print("-" * 60)
        
        rows: list[dict] = []
        stat_total = 0
        stat_breakout = 0
        
        for i, s in enumerate(stocks, start=1):
            r = None
            try:
                r = screen_one(s, bars_count=args.bars, config=config)
            except Exception as e:
                print(f"异常: {s.code} {s.name} {e}")
                r = None
            
            if r is not None:
                stat_total += 1
                rows.append(r)
                print(f"命中: {r['symbol']} {r['name']} | 突破日:{r['breakout_date']} | "
                      f"关键位:{r['key_level_type']}={r['key_level']} | "
                      f"站稳天数:{r['stand_days_above']}/3")
            
            if args.sleep and args.sleep > 0:
                time.sleep(float(args.sleep))
            
            if i % 200 == 0:
                print(f"进度: {i}/{len(stocks)}")
    
    print("-" * 60)
    print(f"统计: 共筛选 {stat_total} 只股票")
    
    df = pd.DataFrame(rows)
    if df.empty:
        print("无结果")
        return
    
    # 默认按量比与站稳强度排序，方便先看“更强”的候选
    df = df.sort_values(["breakout_vol_ratio", "stand_days_above"], ascending=[False, False])
    df = df.reset_index(drop=True)
    
    ts = time.strftime("%Y%m%d_%H%M%S")
    out = args.out.strip()
    if not out:
        out = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f"breakout_hold_3days_{ts}.csv",
        )
    df.to_csv(out, index=False, encoding="utf-8-sig")
    
    print(f"完成: {len(df)} 条")
    print(f"输出: {out}")
    print("\nTop 20 结果:")
    print(df.head(20)[["symbol", "name", "breakout_date", "key_level_type", 
                       "key_level", "breakout_price", "current_price", 
                       "stand_days_above"]].to_string(index=False))
    print(f"\n总耗时: {time.perf_counter() - t0:.2f}s")


if __name__ == "__main__":
    main()
