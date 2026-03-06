"""
全市场扫描：开盘前 30 分钟先下跌，随后反弹并稳稳站上开盘价（强主力扫货信号）
10:20–10:40 之间跑一遍
对结果标的进行排查：先看所属板块情况，再看当前量比
典型用法（盘中任意时刻运行，asof_time 默认取当前时间）：
python3 "backend/scripts/开盘半小时下跌后反弹站上开盘价.py"

更严格/更快：
python3 "backend/scripts/开盘半小时下跌后反弹站上开盘价.py" --prefilter-pct-from-open 0.8 --max-stocks 2000
"""

import argparse
import os
import sys
import time
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

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

from utils.pytdx_client import tdx, connected_endpoint
from utils.stock_codes import get_all_a_share_codes


def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _trade_date_default() -> str:
    return datetime.now().strftime("%Y%m%d")


def _asof_time_default_hhmm() -> str:
    return datetime.now().strftime("%H:%M")


def _parse_markets(s: str) -> List[int]:
    s = str(s or "").strip().lower()
    if s in {"sz", "0"}:
        return [0]
    if s in {"sh", "1"}:
        return [1]
    return [0, 1]


def _chunks(items: List, n: int) -> Iterable[List]:
    n = max(1, int(n))
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _quotes_snapshot_df(
    tdx_,
    df_codes: pd.DataFrame,
    chunk_size: int,
    sleep_s: float,
) -> pd.DataFrame:
    req_pairs = [(int(r["market"]), str(r["code"]).zfill(6)) for _, r in df_codes.iterrows()]
    rows: List[Dict] = []
    for idxs in _chunks(list(range(len(req_pairs))), int(chunk_size)):
        req = [req_pairs[i] for i in idxs]
        try:
            ret = tdx_.get_security_quotes(req)
        except Exception:
            ret = []
        if not isinstance(ret, list):
            ret = []
        for i, q in zip(idxs, ret):
            if not isinstance(q, dict):
                continue
            market = int(df_codes.iloc[i]["market"])
            code = str(df_codes.iloc[i]["code"]).zfill(6)
            name = str(df_codes.iloc[i].get("name") or "").strip()
            open_px = float(q.get("open") or 0.0)
            price = float(q.get("price") or 0.0)
            last_close = float(q.get("last_close") or 0.0)
            pct_from_open = ((price - open_px) / open_px * 100.0) if open_px > 0 and price > 0 else float("nan")
            rows.append(
                {
                    "market": market,
                    "code": code,
                    "name": name,
                    "open": open_px,
                    "price": price,
                    "last_close": last_close,
                    "pct_from_open": pct_from_open,
                }
            )
        if float(sleep_s) > 0:
            time.sleep(float(sleep_s))
    df = pd.DataFrame(rows)
    if df is None or df.empty:
        return pd.DataFrame()
    df["market"] = df["market"].astype(int)
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["name"] = df["name"].astype(str)
    return df


def _fetch_intraday_1m_bars(
    tdx_,
    market: int,
    code: str,
    trade_date: str,
    asof_time: str,
    max_total: int,
    step: int,
) -> Optional[pd.DataFrame]:
    trade_date = str(trade_date).strip()
    asof_time = str(asof_time).strip()
    code = str(code).zfill(6)
    try:
        open_start = datetime.strptime(f"{trade_date} 09:30", "%Y%m%d %H:%M")
        asof_dt = datetime.strptime(f"{trade_date} {asof_time}", "%Y%m%d %H:%M")
    except Exception:
        return None

    max_total = max(60, int(max_total))
    step = max(50, int(step))
    start = 0
    fetched = 0
    frames: List[pd.DataFrame] = []
    while fetched < max_total:
        count = min(step, max_total - fetched)
        try:
            bars = tdx_.get_security_bars(8, int(market), code, int(start), int(count))
        except Exception:
            bars = []
        part = tdx_.to_df(bars) if bars else pd.DataFrame()
        if part is None or part.empty or "datetime" not in part.columns:
            break
        part = part.copy()
        part["datetime"] = pd.to_datetime(part["datetime"], errors="coerce")
        part = part.dropna(subset=["datetime"])
        for c in ("open", "close", "high", "low", "vol", "amount"):
            if c in part.columns:
                part[c] = pd.to_numeric(part[c], errors="coerce")
        part = part.dropna(subset=["open", "close"], how="any")
        if part.empty:
            break
        frames.append(part)
        fetched += len(part)

        min_dt = part["datetime"].min()
        if pd.notna(min_dt) and min_dt.to_pydatetime() <= open_start:
            break
        if len(part) < count:
            break
        start += count

    if not frames:
        return None

    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=["datetime"]).sort_values("datetime", ascending=True).reset_index(drop=True)
    if df.empty:
        return None
    df = df[df["datetime"].dt.strftime("%Y%m%d") == trade_date].copy()
    if df.empty:
        return None
    df = df[(df["datetime"] >= open_start) & (df["datetime"] <= asof_dt)].copy()
    if df.empty:
        return None
    return df.reset_index(drop=True)


def _detect_signal(
    df_1m: pd.DataFrame,
    trade_date: str,
    first30_drop_pct: float,
    first30_close_below_open_pct: float,
    min_rebound_pct: float,
    cross_above_open_pct: float,
    max_cross_minutes: int,
    hold_tolerance_pct: float,
    min_hold_minutes: int,
    enable_after_cross_support: bool,
    min_after_cross_up_dn_vol_ratio: float,
    max_after_cross_down_vol_share: float,
) -> Optional[Dict]:
    if df_1m is None or df_1m.empty:
        return None
    try:

        open_start = datetime.strptime(f"{trade_date} 09:30", "%Y%m%d %H:%M")
        first30_end = datetime.strptime(f"{trade_date} 10:00", "%Y%m%d %H:%M")
    except Exception:
        return None

    if "datetime" not in df_1m.columns or "open" not in df_1m.columns or "close" not in df_1m.columns:
        return None

    df_1m = df_1m.copy()
    df_1m = df_1m.dropna(subset=["datetime", "open", "close"])
    if df_1m.empty:
        return None

    if df_1m["datetime"].max().to_pydatetime() < first30_end:
        return None

    open_px = float(df_1m["open"].iloc[0] or 0.0)
    if open_px <= 0:
        return None

    first30 = df_1m[df_1m["datetime"] < first30_end].copy()
    if first30.empty or len(first30) < 25:
        return None

    low30 = float(pd.to_numeric(first30.get("low", pd.Series(dtype=float)), errors="coerce").min() or float("nan"))
    if not (low30 > 0):
        low30 = float(pd.to_numeric(first30["close"], errors="coerce").min() or float("nan"))
    close30 = float(first30["close"].iloc[-1] or 0.0)
    if low30 <= 0 or close30 <= 0:
        return None

    low30_pct = (low30 - open_px) / open_px * 100.0
    close30_pct = (close30 - open_px) / open_px * 100.0
    if low30_pct > -abs(float(first30_drop_pct)):
        return None
    if close30_pct > -abs(float(first30_close_below_open_pct)):
        return None

    post = df_1m[df_1m["datetime"] >= first30_end].copy()
    if post.empty:
        return None

    cross_line = open_px * (1.0 + float(cross_above_open_pct) / 100.0)
    cross_mask = pd.to_numeric(post["close"], errors="coerce") >= cross_line
    if not bool(cross_mask.any()):
        return None

    cross_pos = int(cross_mask.idxmax())
    cross_time = post.loc[cross_pos, "datetime"].to_pydatetime()
    cross_minutes = int((cross_time - open_start).total_seconds() / 60.0)
    if cross_minutes > int(max_cross_minutes):
        return None

    after_cross = post[post["datetime"] >= cross_time].copy()
    if after_cross.empty:
        return None

    min_close_after_cross = float(pd.to_numeric(after_cross["close"], errors="coerce").min() or float("nan"))
    if not (min_close_after_cross > 0):
        return None
    hold_line = open_px * (1.0 - float(hold_tolerance_pct) / 100.0)
    if min_close_after_cross < hold_line:
        return None

    if bool(enable_after_cross_support):
        if "vol" not in df_1m.columns:
            return None
        ac = after_cross.copy()
        ac["_o"] = pd.to_numeric(ac["open"], errors="coerce")
        ac["_c"] = pd.to_numeric(ac["close"], errors="coerce")
        ac["_v"] = pd.to_numeric(ac["vol"], errors="coerce")
        ac = ac.dropna(subset=["_o", "_c", "_v"])
        ac = ac[ac["_v"] > 0].copy()
        if ac.empty:
            return None
        up = ac[ac["_c"] >= ac["_o"]]
        dn = ac[ac["_c"] < ac["_o"]]
        up_vol = float(pd.to_numeric(up["_v"], errors="coerce").sum() or 0.0)
        dn_vol = float(pd.to_numeric(dn["_v"], errors="coerce").sum() or 0.0)
        denom = up_vol + dn_vol
        if denom <= 0:
            return None
        up_dn_ratio = up_vol / (dn_vol + 1e-12)
        dn_share = dn_vol / (denom + 1e-12)
        if float(min_after_cross_up_dn_vol_ratio) > 0 and up_dn_ratio < float(min_after_cross_up_dn_vol_ratio):
            return None
        if 0 < float(max_after_cross_down_vol_share) < 1 and dn_share > float(max_after_cross_down_vol_share):
            return None

    last_close = float(pd.to_numeric(df_1m["close"].iloc[-1], errors="coerce") or 0.0)
    if last_close <= 0:
        return None
    pct_from_open = (last_close - open_px) / open_px * 100.0
    if pct_from_open < float(min_rebound_pct):
        return None

    min_hold_minutes = max(5, int(min_hold_minutes))
    tail = df_1m.iloc[-min(min_hold_minutes, len(df_1m)) :].copy()
    if tail.empty:
        return None
    if float(pd.to_numeric(tail["close"], errors="coerce").min() or float("nan")) < hold_line:
        return None

    return {
        "open": open_px,
        "low_0930_1000": low30,
        "low_0930_1000_pct": low30_pct,
        "close_0959": close30,
        "close_0959_pct": close30_pct,
        "cross_time": cross_time.strftime("%H:%M"),
        "cross_minutes": cross_minutes,
        "last_close": last_close,
        "pct_from_open": pct_from_open,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date", default=_trade_date_default(), help="交易日 YYYYMMDD（默认今天）")
    parser.add_argument("--asof-time", default=_asof_time_default_hhmm(), help="截至时间 HH:MM（默认当前）")
    parser.add_argument("--markets", default="all", help="all/sz/sh")
    parser.add_argument("--exclude-st", action="store_true", help="剔除名称包含 ST 的标的")
    parser.add_argument("--max-stocks", type=int, default=0, help="最多扫描多少只（0=不限制）")

    parser.add_argument("--prefilter-pct-from-open", type=float, default=0.2, help="快照预筛：当前价相对开盘涨幅下限(%%)")
    parser.add_argument("--first30-drop-pct", type=float, default=0.4, help="前30分钟最低价相对开盘的跌幅下限(%%)")
    parser.add_argument("--first30-close-below-open-pct", type=float, default=0.05, help="前30分钟结束时(≈09:59)收盘低于开盘的跌幅下限(%%)")
    parser.add_argument("--min-rebound-pct", type=float, default=0.9, help="截至时刻相对开盘涨幅下限(%%)")
    parser.add_argument("--cross-above-open-pct", type=float, default=0.1, help="站上开盘价的阈值(%%)")
    parser.add_argument("--max-cross-minutes", type=int, default=45, help="必须在开盘后多少分钟内站上开盘价")
    parser.add_argument("--hold-tolerance-pct", type=float, default=0.03, help="站上后允许回踩开盘价的容忍度(%%)")
    parser.add_argument("--min-hold-minutes", type=int, default=10, help="最近 N 分钟需要持续站稳开盘价")
    parser.add_argument("--enable-after-cross-support", action="store_true", help="启用站上后承接过滤")
    parser.add_argument("--min-after-cross-up-dn-vol-ratio", type=float, default=1.5, help="站上后上涨分钟量/下跌分钟量下限")
    parser.add_argument("--max-after-cross-down-vol-share", type=float, default=0.4, help="站上后下跌分钟量占比上限(0-1)")

    parser.add_argument("--quote-chunk-size", type=int, default=80, help="快照请求分块大小")
    parser.add_argument("--quote-sleep-s", type=float, default=0.02, help="快照分块间隔(秒)")
    parser.add_argument("--bars-max-total", type=int, default=320, help="分钟K最多拉取条数（不足会导致信号缺失）")
    parser.add_argument("--bars-step", type=int, default=200, help="分钟K单次拉取条数")
    parser.add_argument("--per-stock-sleep-s", type=float, default=0.0, help="每只股票分钟K拉取后休眠(秒)")

    parser.add_argument("--topk", type=int, default=200, help="输出TopK（按截至时刻涨幅排序）")
    parser.add_argument("--output-csv", default="", help="输出 CSV 路径（默认脚本同目录）")
    args = parser.parse_args()

    trade_date = str(args.trade_date).strip()
    asof_time = str(args.asof_time).strip()
    markets = set(_parse_markets(args.markets))

    out_path = str(args.output_csv or "").strip()
    if not out_path:
        out_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f"开盘半小时下跌后反弹站上开盘价_{trade_date}_{_now_ts()}.csv",
        )

    with tdx:
        ep = connected_endpoint()
        if ep is not None:
            print(f"{_now_ts()} pytdx 已连接: {ep[0]}:{ep[1]}", flush=True)

        df_codes = get_all_a_share_codes()
        if df_codes is None or df_codes.empty:
            print(f"{_now_ts()} 股票列表为空", flush=True)
            return 2

        df_codes = df_codes[df_codes["market"].isin(list(markets))].copy()
        if bool(args.exclude_st) and "name" in df_codes.columns:
            df_codes = df_codes[~df_codes["name"].astype(str).str.upper().str.contains("ST", na=False)].copy()
        if int(args.max_stocks) > 0:
            df_codes = df_codes.head(int(args.max_stocks)).copy()

        print(f"{_now_ts()} 股票池: {len(df_codes)}", flush=True)

        df_quotes = _quotes_snapshot_df(
            tdx,
            df_codes=df_codes,
            chunk_size=int(args.quote_chunk_size),
            sleep_s=float(args.quote_sleep_s),
        )
        if df_quotes is None or df_quotes.empty:
            print(f"{_now_ts()} 快照为空，无法预筛", flush=True)
            return 2

        df_quotes = df_quotes.dropna(subset=["open", "price", "pct_from_open"])
        df_quotes = df_quotes[(df_quotes["open"] > 0) & (df_quotes["price"] > 0)]
        df_quotes = df_quotes[df_quotes["pct_from_open"] >= float(args.prefilter_pct_from_open)].copy()
        df_quotes = df_quotes.sort_values("pct_from_open", ascending=False).reset_index(drop=True)

        print(f"{_now_ts()} 预筛后候选: {len(df_quotes)} (pct_from_open>={float(args.prefilter_pct_from_open):.2f}%)", flush=True)

        rows: List[Dict] = []
        for i, r in df_quotes.iterrows():
            market = int(r["market"])
            code = str(r["code"]).zfill(6)
            name = str(r.get("name") or "").strip()
            df_1m = _fetch_intraday_1m_bars(
                tdx,
                market=market,
                code=code,
                trade_date=trade_date,
                asof_time=asof_time,
                max_total=int(args.bars_max_total),
                step=int(args.bars_step),
            )
            sig = _detect_signal(
                df_1m=df_1m,
                trade_date=trade_date,
                first30_drop_pct=float(args.first30_drop_pct),
                first30_close_below_open_pct=float(args.first30_close_below_open_pct),
                min_rebound_pct=float(args.min_rebound_pct),
                cross_above_open_pct=float(args.cross_above_open_pct),
                max_cross_minutes=int(args.max_cross_minutes),
                hold_tolerance_pct=float(args.hold_tolerance_pct),
                min_hold_minutes=int(args.min_hold_minutes),
                enable_after_cross_support=bool(args.enable_after_cross_support),
                min_after_cross_up_dn_vol_ratio=float(args.min_after_cross_up_dn_vol_ratio),
                max_after_cross_down_vol_share=float(args.max_after_cross_down_vol_share),
            )
            if sig is not None:
                rows.append(
                    {
                        "ts_code": f"{code}.SZ" if market == 0 else f"{code}.SH",
                        "name": name,
                        "trade_date": trade_date,
                        "asof_time": asof_time,
                        "open": sig["open"],
                        "low_0930_1000": sig["low_0930_1000"],
                        "low_0930_1000_pct": sig["low_0930_1000_pct"],
                        "close_0959": sig["close_0959"],
                        "close_0959_pct": sig["close_0959_pct"],
                        "cross_time": sig["cross_time"],
                        "cross_minutes": sig["cross_minutes"],
                        "last_close": sig["last_close"],
                        "pct_from_open": sig["pct_from_open"],
                        "reason": f"前30分钟回撤{sig['low_0930_1000_pct']:.2f}%，随后站上开盘价并稳住",
                    }
                )

            if float(args.per_stock_sleep_s) > 0:
                time.sleep(float(args.per_stock_sleep_s))

            if (i + 1) % 200 == 0:
                print(f"{_now_ts()} 进度: {i+1}/{len(df_quotes)} 命中: {len(rows)}", flush=True)

        df_out = pd.DataFrame(rows)
        if df_out is None or df_out.empty:
            print(f"{_now_ts()} 未找到符合条件的标的（trade_date={trade_date}, asof_time={asof_time}）", flush=True)
            return 0

        df_out = df_out.sort_values("pct_from_open", ascending=False).reset_index(drop=True)
        topk = max(1, int(args.topk))
        df_out = df_out.head(topk).copy()
        df_out.to_csv(out_path, index=False)
        print(f"{_now_ts()} 输出: {out_path} (rows={len(df_out)})", flush=True)
        print(df_out.head(min(30, len(df_out))).to_string(index=False), flush=True)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
