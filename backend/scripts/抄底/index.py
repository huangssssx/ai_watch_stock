import argparse
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd


def _backend_dir() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    probe = here
    for _ in range(12):
        if os.path.exists(os.path.join(probe, "utils", "pytdx_client.py")):
            return probe
        parent = os.path.dirname(probe)
        if parent == probe:
            break
        probe = parent
    return os.path.abspath(os.path.join(here, "..", ".."))


_BACKEND_DIR = _backend_dir()
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from utils.pytdx_client import connected_endpoint, tdx
from utils.stock_codes import get_all_a_share_codes

try:
    from utils.tushare_client import pro
except Exception:
    pro = None


def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _parse_markets(s: str) -> List[int]:
    s = str(s or "").strip().lower()
    if s in {"sz", "0"}:
        return [0]
    if s in {"sh", "1"}:
        return [1]
    return [0, 1]


def _chunks(items: List, n: int):
    n = max(1, int(n))
    for i in range(0, len(items), n):
        yield items[i:i + n]


def _to_ts_code(market: int, code: str) -> str:
    code = str(code or "").zfill(6)
    return f"{code}.SH" if int(market) == 1 else f"{code}.SZ"


def _normalize_daily_df(df0: pd.DataFrame, market: int, code: str) -> Tuple[Optional[pd.DataFrame], str]:
    sid = f"{int(market)}-{str(code).zfill(6)}"
    if df0 is None or df0.empty:
        return None, f"{sid}:empty"

    df = df0.copy()
    rename_map = {}
    if "datetime" not in df.columns:
        for c in ("date", "day", "trade_date"):
            if c in df.columns:
                rename_map[c] = "datetime"
                break
    if rename_map:
        df.rename(columns=rename_map, inplace=True)

    required = ["datetime", "open", "close", "high", "low", "vol"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        return None, f"{sid}:missing:{','.join(missing)}"

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    if df["datetime"].isna().all():
        return None, f"{sid}:bad_datetime"

    for c in ["open", "close", "high", "low", "vol", "amount"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["datetime", "open", "close", "high", "low"]).copy()
    if df.empty:
        return None, f"{sid}:empty_after_clean"

    df = df.sort_values("datetime", ascending=True).reset_index(drop=True)
    df = df[(df["high"] >= df["low"]) & (df["low"] > 0)].copy().reset_index(drop=True)
    if df.empty:
        return None, f"{sid}:empty_after_filter"

    df["trade_date"] = df["datetime"].dt.strftime("%Y%m%d")
    return df, "ok"


def _fetch_daily_bars(market: int, code: str, count: int) -> Tuple[Optional[pd.DataFrame], str]:
    sid = f"{int(market)}-{str(code).zfill(6)}"
    try:
        bars = tdx.get_security_bars(9, int(market), str(code).zfill(6), 0, int(count))
    except Exception as e:
        return None, f"{sid}:bars_exception:{type(e).__name__}"
    if not bars:
        return None, f"{sid}:bars_empty"
    df0 = tdx.to_df(bars) if bars else pd.DataFrame()
    return _normalize_daily_df(df0, market=int(market), code=str(code).zfill(6))


def _get_latest_trading_date() -> str:
    today = datetime.now().strftime("%Y%m%d")
    try:
        df, st = _fetch_daily_bars(market=0, code="399001", count=10)
        if st == "ok" and df is not None and not df.empty:
            return str(df["trade_date"].iloc[-1])
    except Exception:
        pass
    return today


def _safe_num(v, default=0.0) -> float:
    try:
        x = float(v)
        if pd.isna(x):
            return float(default)
        return x
    except Exception:
        return float(default)


def _fetch_daily_basic_map(trade_date: str, enable: bool) -> Dict[str, Dict]:
    if not enable or pro is None:
        return {}
    fields = "ts_code,trade_date,pe_ttm,pb,turnover_rate,total_mv,circ_mv"
    df = None
    try:
        df = pro.daily_basic(trade_date=str(trade_date), fields=fields)
    except Exception:
        try:
            df = pro.query("daily_basic", trade_date=str(trade_date), fields=fields)
        except Exception:
            df = None
    if df is None or df.empty or "ts_code" not in df.columns:
        return {}
    for c in ["pe_ttm", "pb", "turnover_rate", "total_mv", "circ_mv"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return {str(r["ts_code"]): r.to_dict() for _, r in df.iterrows()}


def _fetch_chip_map(trade_date: str, enable: bool) -> Dict[str, Dict]:
    if not enable or pro is None:
        return {}
    fields = "ts_code,trade_date,cost_5pct,cost_95pct,weight_avg,winner_rate"
    df = None
    try:
        df = pro.cyq_perf(trade_date=str(trade_date), fields=fields)
    except Exception:
        try:
            df = pro.query("cyq_perf", trade_date=str(trade_date), fields=fields)
        except Exception:
            df = None
    if df is None or df.empty or "ts_code" not in df.columns:
        return {}
    for c in ["cost_5pct", "cost_95pct", "weight_avg", "winner_rate"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return {str(r["ts_code"]): r.to_dict() for _, r in df.iterrows()}


def _eval_one(
    market: int,
    code: str,
    name: str,
    target_date: str,
    lookback_days: int,
    min_drawdown: float,
    max_price_pos_year: float,
    max_box_range_60: float,
    max_vol_shrink_ratio: float,
    min_ma20_slope: float,
    max_ma20_below: float,
    bottom_break_allow: float,
    support_break_pct: float,
    support_rebound_pct: float,
    min_rebound_pct: float,
    use_daily_basic: bool,
    daily_basic_map: Dict[str, Dict],
    max_pb: float,
    max_pe_ttm: float,
    use_chip: bool,
    chip_map: Dict[str, Dict],
    max_chip_concentration: float,
    min_winner_rate: float,
    max_chip_price_pos: float,
) -> Tuple[Optional[Dict], str]:
    if "ST" in str(name).upper():
        return None, "skip_st"

    df, st = _fetch_daily_bars(market=int(market), code=str(code).zfill(6), count=int(lookback_days))
    if st != "ok" or df is None or df.empty:
        return None, f"bad_daily:{st}"

    df = df[df["trade_date"] <= str(target_date)].reset_index(drop=True)
    if df.empty:
        return None, "no_bars_before_target"
    if str(df["trade_date"].iloc[-1]) != str(target_date):
        return None, "target_not_latest_bar"
    if len(df) < 130:
        return None, "too_short"

    close = pd.to_numeric(df["close"], errors="coerce")
    open_ = pd.to_numeric(df["open"], errors="coerce")
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    vol = pd.to_numeric(df["vol"], errors="coerce").fillna(0.0)

    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()

    if pd.isna(ma20.iloc[-1]) or pd.isna(ma20.iloc[-2]) or pd.isna(ma60.iloc[-1]):
        return None, "ma_nan"

    close_last = float(close.iloc[-1])
    open_last = float(open_.iloc[-1])
    ma20_last = float(ma20.iloc[-1])
    ma20_prev = float(ma20.iloc[-2])
    ma60_last = float(ma60.iloc[-1])

    high120 = float(high.tail(120).max())
    low120 = float(low.tail(120).min())
    high60 = float(high.tail(60).max())
    low60 = float(low.tail(60).min())

    if high120 <= 0 or low120 <= 0 or high120 <= low120:
        return None, "bad_range"

    drawdown = (high120 - close_last) / high120
    price_pos_year = (close_last - low120) / (high120 - low120 + 1e-12)
    box_range_60 = (high60 - low60) / (low60 + 1e-12)

    vol20 = float(vol.tail(20).mean())
    vol20_prev = float(vol.tail(40).head(20).mean()) if len(vol) >= 40 else 0.0
    if vol20_prev <= 0:
        return None, "bad_vol_prev"
    vol_shrink_ratio = vol20 / vol20_prev
    vol_last = float(vol.iloc[-1])
    vol_q20_60 = float(vol.tail(60).quantile(0.2)) if len(vol) >= 60 else float(vol.quantile(0.2))

    ma20_slope = (ma20_last - ma20_prev) / (abs(ma20_prev) + 1e-12)
    close_pct_1d = (close_last - float(close.iloc[-2])) / (abs(float(close.iloc[-2])) + 1e-12)

    low20_min = float(low.tail(20).min())
    low3_min = float(low.tail(3).min())
    stabilize_ok = low3_min >= low20_min * (1.0 - float(bottom_break_allow))
    trend_ok = (ma20_slope >= float(min_ma20_slope)) and (close_last >= ma20_last * (1.0 - float(max_ma20_below)))

    cond_drawdown = drawdown >= float(min_drawdown)
    cond_low_pos = price_pos_year <= float(max_price_pos_year)
    cond_box = box_range_60 <= float(max_box_range_60)
    cond_vol = vol_shrink_ratio <= float(max_vol_shrink_ratio)

    patterns_core: List[str] = []
    patterns_extra: List[str] = []

    core_box = cond_drawdown and cond_low_pos and cond_box
    core_support = cond_vol and (close_last >= ma60_last * (1.0 - float(support_break_pct))) and (close_last <= ma60_last * (1.0 + float(support_rebound_pct)))
    core_ground = (vol_last <= vol_q20_60) and (close_last <= low60 * 1.08)

    if core_box:
        patterns_core.append("低位横盘筑底")
    if core_support:
        patterns_core.append("缩量回踩支撑")
    if core_ground:
        patterns_core.append("地量地价")

    if close_last > ma20_last and close_pct_1d >= float(min_rebound_pct):
        patterns_extra.append("右侧转强")
    if stabilize_ok:
        patterns_extra.append("短期止跌")

    if not patterns_core:
        return None, "no_core_pattern"
    if not cond_low_pos:
        return None, "price_not_low_enough"
    if (not core_support) and (not core_ground) and (not cond_vol):
        return None, "vol_not_shrink"
    if not trend_ok:
        return None, "trend_not_stable"
    if not stabilize_ok:
        return None, "not_stabilized"

    score = 0.0
    score += min(1.8, drawdown / max(1e-9, float(min_drawdown))) * 28.0
    score += max(0.0, 1.0 - price_pos_year / max(1e-9, float(max_price_pos_year))) * 26.0
    score += max(0.0, (float(max_vol_shrink_ratio) - vol_shrink_ratio) / max(1e-9, float(max_vol_shrink_ratio))) * 16.0
    score += 8.0 if cond_box else 0.0
    score += 6.0 if close_last > ma20_last else 0.0
    score += 6.0 if ma20_slope > 0 else 0.0
    score += len(patterns_core) * 4.0
    score += len(patterns_extra) * 2.0

    ts_code = _to_ts_code(market=int(market), code=str(code))
    db = daily_basic_map.get(ts_code, {})
    pb = _safe_num(db.get("pb"), default=float("nan"))
    pe_ttm = _safe_num(db.get("pe_ttm"), default=float("nan"))
    turnover_rate = _safe_num(db.get("turnover_rate"), default=float("nan"))
    total_mv = _safe_num(db.get("total_mv"), default=float("nan"))
    circ_mv = _safe_num(db.get("circ_mv"), default=float("nan"))

    if use_daily_basic:
        if not db:
            return None, "no_daily_basic"
        if not pd.isna(pb) and pb > float(max_pb):
            return None, "pb_too_high"
        if not pd.isna(pe_ttm) and pe_ttm > float(max_pe_ttm):
            return None, "pe_too_high"

    if not pd.isna(pb) and pb > 0:
        score += max(0.0, (float(max_pb) - pb) / max(1e-9, float(max_pb))) * 10.0
    if not pd.isna(pe_ttm) and pe_ttm > 0:
        score += max(0.0, (float(max_pe_ttm) - min(pe_ttm, float(max_pe_ttm))) / max(1e-9, float(max_pe_ttm))) * 6.0

    chip = chip_map.get(ts_code, {})
    chip_concentration = float("nan")
    winner_rate = float("nan")
    chip_price_pos = float("nan")
    cost_5pct = float("nan")
    cost_95pct = float("nan")
    weight_avg = float("nan")

    if chip:
        cost_5pct = _safe_num(chip.get("cost_5pct"), default=float("nan"))
        cost_95pct = _safe_num(chip.get("cost_95pct"), default=float("nan"))
        weight_avg = _safe_num(chip.get("weight_avg"), default=float("nan"))
        winner_rate = _safe_num(chip.get("winner_rate"), default=float("nan"))
        if (not pd.isna(cost_5pct)) and (not pd.isna(cost_95pct)) and (not pd.isna(weight_avg)) and weight_avg > 0 and cost_95pct > cost_5pct:
            chip_concentration = (cost_95pct - cost_5pct) / weight_avg * 100.0
            chip_price_pos = (close_last - cost_5pct) / (cost_95pct - cost_5pct) * 100.0

    if use_chip:
        if not chip:
            return None, "no_chip"
        if pd.isna(chip_concentration) or chip_concentration > float(max_chip_concentration):
            return None, "chip_not_concentrated"
        if pd.isna(winner_rate) or winner_rate < float(min_winner_rate):
            return None, "winner_too_low"
        if pd.isna(chip_price_pos) or chip_price_pos > float(max_chip_price_pos):
            return None, "chip_price_too_high"

    if not pd.isna(chip_concentration):
        score += max(0.0, (float(max_chip_concentration) - chip_concentration) / max(1e-9, float(max_chip_concentration))) * 12.0
    if not pd.isna(winner_rate):
        score += min(1.2, winner_rate / max(1e-9, float(min_winner_rate))) * 8.0
    if not pd.isna(chip_price_pos):
        score += max(0.0, (float(max_chip_price_pos) - chip_price_pos) / max(1e-9, float(max_chip_price_pos))) * 8.0

    reason = "、".join(patterns_core + patterns_extra)

    out = {
        "symbol": str(code).zfill(6),
        "name": str(name or ""),
        "ts_code": ts_code,
        "trade_date": str(target_date),
        "score": round(float(score), 3),
        "reason": reason,
        "patterns_core": "、".join(patterns_core),
        "patterns_extra": "、".join(patterns_extra),
        "close": round(close_last, 3),
        "open": round(open_last, 3),
        "drawdown_120d": round(drawdown, 4),
        "price_pos_year": round(price_pos_year, 4),
        "box_range_60d": round(box_range_60, 4),
        "vol_shrink_ratio_20_20": round(vol_shrink_ratio, 4),
        "ma20_slope_1d": round(ma20_slope, 5),
        "close_pct_1d": round(close_pct_1d, 4),
        "pb": None if pd.isna(pb) else round(pb, 4),
        "pe_ttm": None if pd.isna(pe_ttm) else round(pe_ttm, 4),
        "turnover_rate": None if pd.isna(turnover_rate) else round(turnover_rate, 4),
        "total_mv_wanyuan": None if pd.isna(total_mv) else round(total_mv, 2),
        "circ_mv_wanyuan": None if pd.isna(circ_mv) else round(circ_mv, 2),
        "chip_concentration_pct": None if pd.isna(chip_concentration) else round(chip_concentration, 3),
        "winner_rate_pct": None if pd.isna(winner_rate) else round(winner_rate, 3),
        "chip_price_pos_pct": None if pd.isna(chip_price_pos) else round(chip_price_pos, 3),
        "cost_5pct": None if pd.isna(cost_5pct) else round(cost_5pct, 3),
        "cost_95pct": None if pd.isna(cost_95pct) else round(cost_95pct, 3),
        "weight_avg_cost": None if pd.isna(weight_avg) else round(weight_avg, 3),
    }
    return out, "ok"


def main():
    parser = argparse.ArgumentParser(description="低价筹码扫描：底部形态 + 缩量 + 估值 + 筹码集中度")
    parser.add_argument("--markets", type=str, default="all", help="all/sz/sh")
    parser.add_argument("--lookback-days", type=int, default=260, help="拉取日线条数")
    parser.add_argument("--chunk-size", type=int, default=60, help="处理分块")
    parser.add_argument("--max-stocks", type=int, default=0, help="限制股票数，0为不限制")
    parser.add_argument("--target-date", type=str, default=None, help="目标交易日 YYYYMMDD，默认最近交易日")
    parser.add_argument("--output", type=str, default=None, help="输出CSV路径")
    parser.add_argument("--top-n", type=int, default=200, help="展示TopN")

    parser.add_argument("--min-drawdown", type=float, default=0.45, help="120日高点到现价最小回撤")
    parser.add_argument("--max-price-pos-year", type=float, default=0.35, help="现价在120日区间中的最大位置")
    parser.add_argument("--max-box-range-60", type=float, default=0.30, help="60日振幅上限")
    parser.add_argument("--max-vol-shrink-ratio", type=float, default=0.80, help="近20日均量/前20日均量上限")
    parser.add_argument("--min-ma20-slope", type=float, default=-0.002, help="MA20单日斜率下限")
    parser.add_argument("--max-ma20-below", type=float, default=0.02, help="收盘低于MA20的允许比例")
    parser.add_argument("--bottom-break-allow", type=float, default=0.02, help="近3日对近20日低点破位容忍")
    parser.add_argument("--support-break-pct", type=float, default=0.03, help="回踩MA60容忍下破")
    parser.add_argument("--support-rebound-pct", type=float, default=0.05, help="回踩MA60向上偏离上限")
    parser.add_argument("--min-rebound-pct", type=float, default=0.003, help="右侧转强的单日涨幅下限")

    parser.add_argument("--enable-daily-basic", action="store_true", help="启用daily_basic过滤")
    parser.add_argument("--max-pb", type=float, default=3.5, help="PB上限")
    parser.add_argument("--max-pe-ttm", type=float, default=60.0, help="PE_TTM上限")

    parser.add_argument("--enable-chip", action="store_true", help="启用筹码过滤(cyq_perf)")
    parser.add_argument("--max-chip-concentration", type=float, default=18.0, help="筹码集中度上限(%%)")
    parser.add_argument("--min-winner-rate", type=float, default=35.0, help="胜率下限(%%)")
    parser.add_argument("--max-chip-price-pos", type=float, default=70.0, help="现价在筹码区位置上限(%%)")

    parser.add_argument("--include-st", action="store_true", help="包含ST")
    args = parser.parse_args()

    if not connected_endpoint():
        try:
            _ = tdx.get_security_bars(9, 0, "000001", 0, 2)
        except Exception as e:
            raise SystemExit(f"pytdx 连接失败：{e}")

    target_date = str(args.target_date).strip() if args.target_date else _get_latest_trading_date()
    markets = _parse_markets(args.markets)

    print(f"{_now_ts()} 目标交易日: {target_date}", flush=True)
    print(f"{_now_ts()} 市场: {markets}", flush=True)

    df_codes = get_all_a_share_codes()
    if df_codes is None or getattr(df_codes, "empty", True):
        raise SystemExit("股票列表为空")
    df_codes = df_codes[df_codes["market"].isin(markets)].copy().reset_index(drop=True)
    if not bool(args.include_st) and "name" in df_codes.columns:
        df_codes = df_codes[~df_codes["name"].astype(str).str.upper().str.contains("ST", na=False)].copy()
    if int(args.max_stocks) > 0:
        df_codes = df_codes.head(int(args.max_stocks)).copy()

    use_daily_basic = bool(args.enable_daily_basic)
    use_chip = bool(args.enable_chip)

    if (use_daily_basic or use_chip) and pro is None:
        print(f"{_now_ts()} 警告：tushare 不可用，自动关闭估值/筹码过滤", flush=True)
        use_daily_basic = False
        use_chip = False

    daily_basic_map = _fetch_daily_basic_map(target_date, enable=use_daily_basic)
    chip_map = _fetch_chip_map(target_date, enable=use_chip)

    print(f"{_now_ts()} 股票池: {len(df_codes)}", flush=True)
    if use_daily_basic:
        print(f"{_now_ts()} daily_basic条数: {len(daily_basic_map)}", flush=True)
    if use_chip:
        print(f"{_now_ts()} cyq_perf条数: {len(chip_map)}", flush=True)

    stocks = list(df_codes[["market", "code", "name"]].itertuples(index=False, name=None))
    t0 = time.perf_counter()
    results: List[Dict] = []
    bad: Dict[str, int] = {}

    for chunk in _chunks(stocks, int(args.chunk_size)):
        for market, code, name in chunk:
            out, st = _eval_one(
                market=int(market),
                code=str(code).zfill(6),
                name=str(name or ""),
                target_date=str(target_date),
                lookback_days=int(args.lookback_days),
                min_drawdown=float(args.min_drawdown),
                max_price_pos_year=float(args.max_price_pos_year),
                max_box_range_60=float(args.max_box_range_60),
                max_vol_shrink_ratio=float(args.max_vol_shrink_ratio),
                min_ma20_slope=float(args.min_ma20_slope),
                max_ma20_below=float(args.max_ma20_below),
                bottom_break_allow=float(args.bottom_break_allow),
                support_break_pct=float(args.support_break_pct),
                support_rebound_pct=float(args.support_rebound_pct),
                min_rebound_pct=float(args.min_rebound_pct),
                use_daily_basic=bool(use_daily_basic),
                daily_basic_map=daily_basic_map,
                max_pb=float(args.max_pb),
                max_pe_ttm=float(args.max_pe_ttm),
                use_chip=bool(use_chip),
                chip_map=chip_map,
                max_chip_concentration=float(args.max_chip_concentration),
                min_winner_rate=float(args.min_winner_rate),
                max_chip_price_pos=float(args.max_chip_price_pos),
            )
            if out is not None and st == "ok":
                results.append(out)
            else:
                bad[str(st)] = int(bad.get(str(st), 0)) + 1

        done = len(results) + sum(bad.values())
        if done % 300 == 0 or done == len(stocks):
            elapsed = time.perf_counter() - t0
            print(f"进度 {done}/{len(stocks)} | 命中 {len(results)} | 耗时 {elapsed:.1f}s", end="\r")

    print("")
    df_out = pd.DataFrame(results)
    if not df_out.empty:
        sort_cols = ["score", "chip_concentration_pct", "price_pos_year", "vol_shrink_ratio_20_20"]
        exist_cols = [c for c in sort_cols if c in df_out.columns]
        asc = [False, True, True, True][:len(exist_cols)]
        df_out = df_out.sort_values(by=exist_cols, ascending=asc)
        df_show = df_out.head(max(1, int(args.top_n))).copy()
        print(df_show.to_string(index=False, max_rows=80))
    else:
        print("未命中。失败原因分布 Top10：")
        top = sorted(bad.items(), key=lambda x: x[1], reverse=True)[:10]
        for k, v in top:
            print(f"  {k}: {v}")

    output_path = args.output
    if not output_path:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(script_dir, f"低价筹码扫描_{target_date}_{_now_ts()}.csv")
    df_out.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"结果已保存到：{output_path}")
    print(f"总耗时：{time.perf_counter() - t0:.2f}s | 结果数：{len(df_out)}")


if __name__ == "__main__":
    main()
