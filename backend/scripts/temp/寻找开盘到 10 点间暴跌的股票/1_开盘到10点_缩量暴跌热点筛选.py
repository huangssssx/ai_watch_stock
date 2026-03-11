"""
全市场筛选：开盘到 10:00 跌幅 >= 指定阈值的股票

默认用 pytdx 行情快照（open 与当前 price）做筛选，适合在 10:00 附近运行。
如需严格取 10:00 的价格，可加 --verify-minute-bars 用 1 分钟 K 二次验证（更慢）。
"""

import argparse
import os
import sys
import time
from dataclasses import dataclass
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


def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _trade_date_default() -> str:
    return datetime.now().strftime("%Y%m%d")


def _ts_code(market: int, code: str) -> str:
    code = str(code).zfill(6)
    return f"{code}.SZ" if int(market) == 0 else f"{code}.SH"


def _is_a_share_stock(market: int, code: str) -> bool:
    code = str(code or "").zfill(6)
    if int(market) == 0:
        return code.startswith(("000", "001", "002", "003", "300", "301"))
    if int(market) == 1:
        return code.startswith(("600", "601", "603", "605", "688"))
    return False


def _chunks(items: List, n: int) -> Iterable[List]:
    n = max(1, int(n))
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _parse_markets(s: str) -> List[int]:
    s = str(s or "").strip().lower()
    if s in {"sz", "0"}:
        return [0]
    if s in {"sh", "1"}:
        return [1]
    return [0, 1]


@dataclass(frozen=True)
class StockDef:
    market: int
    code: str
    name: str

    @property
    def ts_code(self) -> str:
        return _ts_code(self.market, self.code)


def _iter_all_a_share_defs(tdx, exclude_st: bool) -> Iterable[StockDef]:
    for market in (0, 1):
        total = int(tdx.get_security_count(int(market)) or 0)
        step = 1000
        for start in range(0, int(total), step):
            rows = tdx.get_security_list(int(market), int(start)) or []
            for r in rows:
                code = str(r.get("code", "")).zfill(6)
                name = str(r.get("name", "")).strip()
                if exclude_st and ("ST" in name.upper()):
                    continue
                if code and _is_a_share_stock(int(market), code):
                    yield StockDef(market=int(market), code=code, name=name)


def _quotes_snapshot_df(
    tdx,
    stocks: List[StockDef],
    chunk_size: int,
    sleep_s: float,
) -> pd.DataFrame:
    items: List[Tuple[StockDef, Tuple[int, str]]] = [(s, (int(s.market), str(s.code))) for s in stocks]
    rows: List[Dict] = []
    for part in _chunks(items, int(chunk_size)):
        req = [p for _, p in part]
        keep = [s for s, _ in part]
        try:
            ret = tdx.get_security_quotes(req)
        except Exception:
            ret = []
        if not isinstance(ret, list):
            ret = []
        for s, q in zip(keep, ret):
            if not isinstance(q, dict):
                continue
            open_px = float(q.get("open") or 0.0)
            price = float(q.get("price") or 0.0)
            last_close = float(q.get("last_close") or 0.0)
            vol_hand = float(q.get("vol") or 0.0)
            amount_yuan = float(q.get("amount") or 0.0)
            pct_from_open = ((price - open_px) / open_px * 100.0) if open_px > 0 and price > 0 else float("nan")
            pct_from_last_close = (
                ((price - last_close) / last_close * 100.0) if last_close > 0 and price > 0 else float("nan")
            )
            rows.append(
                {
                    "ts_code": s.ts_code,
                    "name": s.name,
                    "market": int(s.market),
                    "code": str(s.code),
                    "open": open_px,
                    "price": price,
                    "last_close": last_close,
                    "pct_from_open": pct_from_open,
                    "pct_from_last_close": pct_from_last_close,
                    "vol_hand": vol_hand,
                    "amount_yuan": amount_yuan,
                }
            )
        if float(sleep_s) > 0:
            time.sleep(float(sleep_s))
    df = pd.DataFrame(rows)
    if df is None or df.empty:
        return pd.DataFrame()
    df["ts_code"] = df["ts_code"].astype(str)
    if "name" in df.columns:
        df["name"] = df["name"].astype(str)
    return df


def _minute_price_at(
    tdx,
    market: int,
    code: str,
    trade_date: str,
    asof_time: str,
    bars_n: int,
    liangbi_days: int,
) -> Optional[Dict]:
    trade_date = str(trade_date).strip()
    asof_time = str(asof_time).strip()
    try:
        asof_dt = datetime.strptime(f"{trade_date} {asof_time}", "%Y%m%d %H:%M")
    except Exception:
        return None

    open_start = datetime.strptime(f"{trade_date} 09:30", "%Y%m%d %H:%M")
    liangbi_days = max(0, int(liangbi_days))
    max_total = max(120, int(bars_n), (liangbi_days + 1) * 300)
    step = 200
    start = 0
    frames: List[pd.DataFrame] = []
    fetched = 0
    while fetched < max_total:
        count = min(step, max_total - fetched)
        try:
            bars = tdx.get_security_bars(8, int(market), str(code).zfill(6), int(start), int(count))
        except Exception:
            bars = []
        part = tdx.to_df(bars) if bars else pd.DataFrame()
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
        if pd.notna(part["datetime"].min()) and part["datetime"].min().to_pydatetime() <= open_start:
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

    open_px = float(df["open"].iloc[0] or 0.0)
    price_asof = float(df["close"].iloc[-1] or 0.0)
    last_dt = df["datetime"].iloc[-1].to_pydatetime()
    if open_px <= 0 or price_asof <= 0:
        return None

    pct_from_open = (price_asof - open_px) / open_px * 100.0
    cum_vol_asof = float(df["vol"].sum() or 0.0) if "vol" in df.columns else float("nan")

    liangbi_asof = float("nan")
    avg_cum_vol_hist = float("nan")
    hist_days_used = 0
    if liangbi_days > 0 and "vol" in df.columns:
        all_df = pd.concat(frames, ignore_index=True)
        all_df = (
            all_df.drop_duplicates(subset=["datetime"])
            .sort_values("datetime", ascending=True)
            .reset_index(drop=True)
            .copy()
        )
        all_df["date"] = all_df["datetime"].dt.strftime("%Y%m%d")
        unique_dates = sorted([d for d in all_df["date"].dropna().unique().tolist() if str(d).isdigit()])
        hist_dates = [d for d in unique_dates if str(d) < trade_date][-liangbi_days:]
        hist_cum_vols: List[float] = []
        for d in hist_dates:
            try:
                open_dt_d = datetime.strptime(f"{d} 09:30", "%Y%m%d %H:%M")
                asof_dt_d = datetime.strptime(f"{d} {asof_time}", "%Y%m%d %H:%M")
            except Exception:
                continue
            part = all_df[(all_df["date"] == d) & (all_df["datetime"] >= open_dt_d) & (all_df["datetime"] <= asof_dt_d)].copy()
            if part is None or part.empty or "vol" not in part.columns:
                continue
            v = pd.to_numeric(part["vol"], errors="coerce").dropna()
            if v.empty:
                continue
            hist_cum_vols.append(float(v.sum() or 0.0))
        hist_cum_vols = [v for v in hist_cum_vols if v > 0]
        if hist_cum_vols:
            hist_days_used = len(hist_cum_vols)
            avg_cum_vol_hist = float(sum(hist_cum_vols) / float(hist_days_used))
            if avg_cum_vol_hist > 0 and pd.notna(cum_vol_asof) and float(cum_vol_asof) > 0:
                liangbi_asof = float(cum_vol_asof) / float(avg_cum_vol_hist)

    return {
        "open_0930": open_px,
        "price_asof": price_asof,
        "pct_from_open_asof": pct_from_open,
        "asof_bar_dt": last_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "cum_vol_asof": cum_vol_asof,
        "liangbi_asof": liangbi_asof,
        "liangbi_hist_days": hist_days_used,
        "avg_cum_vol_hist": avg_cum_vol_hist,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="全市场：开盘到 10:00 暴跌筛选（pytdx）")
    parser.add_argument("--drop-pct", type=float, default=3.0)
    parser.add_argument("--exclude-st", action="store_true")
    parser.add_argument("--markets", type=str, default="both")
    parser.add_argument("--max-stocks", type=int, default=0)
    parser.add_argument("--chunk-size", type=int, default=80)
    parser.add_argument("--sleep-s", type=float, default=0.08)
    parser.add_argument("--top", type=int, default=80)
    parser.add_argument("--out", type=str, default="")
    parser.add_argument("--trade-date", type=str, default="")
    parser.add_argument("--asof-time", type=str, default="10:00")
    parser.add_argument("--verify-minute-bars", action="store_true")
    parser.add_argument("--minute-bars-n", type=int, default=90)
    parser.add_argument("--liangbi-max", type=float, default=1.5)
    parser.add_argument("--liangbi-days", type=int, default=5)
    args = parser.parse_args()

    backend_dir = _backend_dir()
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)

    try:
        from utils.pytdx_client import tdx, connected_endpoint
    except Exception as e:
        raise SystemExit(f"导入 pytdx_client 失败: {type(e).__name__}:{e}")

    drop_pct = abs(float(args.drop_pct))
    trade_date = str(args.trade_date).strip() or _trade_date_default()
    asof_time = str(args.asof_time).strip() or "10:00"
    verify = bool(args.verify_minute_bars)
    out_path = str(args.out).strip()
    markets = set(_parse_markets(str(args.markets)))
    max_stocks = max(0, int(args.max_stocks))
    liangbi_max = float(args.liangbi_max)
    liangbi_days = max(0, int(args.liangbi_days))
    need_minute_bars = verify or (liangbi_max > 0)

    with tdx:
        print(f"{_now_ts()} pytdx connected_endpoint={connected_endpoint()}", flush=True)
        stocks = [s for s in _iter_all_a_share_defs(tdx, exclude_st=bool(args.exclude_st)) if int(s.market) in markets]
        if max_stocks > 0:
            stocks = stocks[:max_stocks]
        print(f"{_now_ts()} 股票池: {len(stocks)}", flush=True)

        snap = _quotes_snapshot_df(
            tdx,
            stocks=stocks,
            chunk_size=int(args.chunk_size),
            sleep_s=float(args.sleep_s),
        )

    if snap is None or snap.empty:
        raise SystemExit("行情快照为空，无法筛选")

    snap = snap.dropna(subset=["pct_from_open"]).copy()
    filtered = snap[snap["pct_from_open"] <= -drop_pct].copy()
    filtered.sort_values(["pct_from_open", "amount_yuan"], ascending=[True, False], inplace=True)
    filtered.reset_index(drop=True, inplace=True)
    print(f"{_now_ts()} 快照筛出: {len(filtered)} (drop_pct>={drop_pct:.2f}%)", flush=True)

    if need_minute_bars and not filtered.empty:
        rows = []
        with tdx:
            action = []
            if verify:
                action.append("跌幅验证")
            if liangbi_max > 0:
                action.append(f"量比<{liangbi_max:g}")
            action_s = " + ".join(action) if action else "分钟K处理"
            print(
                f"{_now_ts()} 分钟K({action_s}): trade_date={trade_date} asof_time={asof_time} liangbi_days={liangbi_days}",
                flush=True,
            )
            for _, r in filtered.iterrows():
                bars_n = max(int(args.minute_bars_n), (liangbi_days + 1) * 300) if (liangbi_max > 0) else int(args.minute_bars_n)
                v = _minute_price_at(
                    tdx,
                    market=int(r["market"]),
                    code=str(r["code"]),
                    trade_date=trade_date,
                    asof_time=asof_time,
                    bars_n=bars_n,
                    liangbi_days=liangbi_days,
                )
                if v is None:
                    continue
                keep = True
                if verify and float(v.get("pct_from_open_asof") or 0.0) > -drop_pct:
                    keep = False
                if liangbi_max > 0:
                    lb = v.get("liangbi_asof")
                    if lb is None or not pd.notna(lb) or float(lb) >= float(liangbi_max):
                        keep = False
                if not keep:
                    continue
                if not verify:
                    for k in ("open_0930", "price_asof", "pct_from_open_asof", "asof_bar_dt"):
                        v.pop(k, None)
                rows.append({**r.to_dict(), **v})

        minute_df = pd.DataFrame(rows)
        if minute_df is None or minute_df.empty:
            filtered = pd.DataFrame()
        else:
            if verify and "pct_from_open_asof" in minute_df.columns:
                minute_df.sort_values(["pct_from_open_asof", "amount_yuan"], ascending=[True, False], inplace=True)
            else:
                minute_df.sort_values(["pct_from_open", "amount_yuan"], ascending=[True, False], inplace=True)
            minute_df.reset_index(drop=True, inplace=True)
            filtered = minute_df
        print(f"{_now_ts()} 分钟K处理后: {len(filtered)}", flush=True)

    if out_path:
        out_csv = out_path
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        out_csv = os.path.join(script_dir, f"开盘到10点暴跌筛选_{trade_date}_{datetime.now().strftime('%H%M%S')}.csv")
    if filtered is None or filtered.empty:
        print(f"{_now_ts()} 无结果，仍会落盘空 CSV: {out_csv}", flush=True)
        pd.DataFrame().to_csv(out_csv, index=False)
        return

    filtered.to_csv(out_csv, index=False)
    print(f"{_now_ts()} 输出: {out_csv}", flush=True)

    top_n = max(0, int(args.top))
    show = filtered.head(top_n) if top_n > 0 else filtered
    cols = [
        "ts_code",
        "name",
        "pct_from_open_asof" if "pct_from_open_asof" in show.columns else "pct_from_open",
        "liangbi_asof" if "liangbi_asof" in show.columns else None,
        "open_0930" if "open_0930" in show.columns else "open",
        "price_asof" if "price_asof" in show.columns else "price",
        "amount_yuan",
    ]
    cols = [c for c in cols if c and c in show.columns]
    if cols:
        with pd.option_context("display.max_rows", 200, "display.max_columns", 50, "display.width", 200):
            print(show[cols].to_string(index=False), flush=True)


if __name__ == "__main__":
    main()
