#!/usr/bin/env python3

import argparse
import os
import sys
import time
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd


def _project_root() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(here, "..", "..", ".."))


def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _to_market_code(ts_code: str) -> Optional[Tuple[int, str]]:
    ts_code = str(ts_code or "").strip()
    if not ts_code:
        return None
    if "." in ts_code:
        code, suf = ts_code.split(".", 1)
        code = code.strip()
        suf = suf.strip().upper()
        if suf == "SZ":
            return 0, code
        if suf == "SH":
            return 1, code
        return None
    if len(ts_code) == 6 and ts_code.startswith("6"):
        return 1, ts_code
    if len(ts_code) == 6:
        return 0, ts_code
    return None


def _chunks(items: list, n: int) -> list[list]:
    n = max(1, int(n))
    return [items[i : i + n] for i in range(0, len(items), n)]


def _calc_metrics(q: dict) -> dict:
    price = float(q.get("price") or 0.0)
    last_close = float(q.get("last_close") or 0.0)
    bid1 = float(q.get("bid1") or 0.0)
    ask1 = float(q.get("ask1") or 0.0)

    bid_value = 0.0
    ask_value = 0.0
    for i in range(1, 6):
        bp = float(q.get(f"bid{i}") or 0.0)
        bv = float(q.get(f"bid_vol{i}") or 0.0)
        ap = float(q.get(f"ask{i}") or 0.0)
        av = float(q.get(f"ask_vol{i}") or 0.0)
        bid_value += bp * bv
        ask_value += ap * av

    denom = ask_value if ask_value > 0 else 1e-9
    imbalance = bid_value / denom
    spread_bp = ((ask1 - bid1) / price * 10000.0) if price > 0 else float("nan")
    speed_raw = float(q.get("reversed_bytes9") or 0.0)
    speed_pct = speed_raw / 100.0
    gap_pct = ((price - last_close) / last_close * 100.0) if last_close > 0 else float("nan")
    return {
        "price": price,
        "last_close": last_close,
        "bid1": bid1,
        "ask1": ask1,
        "bid_value": bid_value,
        "ask_value": ask_value,
        "imbalance": imbalance,
        "spread_bp": spread_bp,
        "speed_pct": speed_pct,
        "gap_pct": gap_pct,
    }


def fetch_quotes(tdx, ts_codes: list[str], chunk_size: int, sleep_s: float) -> pd.DataFrame:
    pairs = []
    keep_codes = []
    for c in ts_codes:
        mc = _to_market_code(c)
        if mc is None:
            continue
        pairs.append(mc)
        keep_codes.append(c)

    rows = []
    for part in _chunks(list(zip(keep_codes, pairs)), chunk_size):
        req = [p for _, p in part]
        try:
            ret = tdx.get_security_quotes(req)
        except Exception:
            ret = []
        if not isinstance(ret, list):
            ret = []
        for (ts_code, _), q in zip(part, ret):
            if not isinstance(q, dict):
                continue
            m = _calc_metrics(q)
            m["ts_code"] = ts_code
            m["servertime"] = str(q.get("servertime") or "")
            rows.append(m)
        if sleep_s and sleep_s > 0:
            time.sleep(float(sleep_s))
    df = pd.DataFrame(rows)
    if not df.empty:
        df["ts_code"] = df["ts_code"].astype(str)
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="隔夜套利：次日盘口确认（pytdx 五档/涨速）")
    parser.add_argument("--input-csv", type=str, default="")
    parser.add_argument("--ts-codes", type=str, default="")
    parser.add_argument("--topk", type=int, default=60)
    parser.add_argument("--chunk-size", type=int, default=80)
    parser.add_argument("--sleep-s", type=float, default=0.2)
    parser.add_argument("--min-imbalance", type=float, default=1.2)
    parser.add_argument("--max-spread-bp", type=float, default=50.0)
    parser.add_argument("--min-speed-pct", type=float, default=-0.05)
    parser.add_argument("--min-gap-pct", type=float, default=-9.9)
    parser.add_argument("--out", type=str, default="")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    root = _project_root()
    if root not in sys.path:
        sys.path.insert(0, root)

    if bool(args.dry_run):
        print("dry_run=1，不请求数据")
        return

    try:
        from backend.utils.pytdx_client import tdx
    except Exception as e:
        raise SystemExit(f"导入 pytdx_client 失败: {type(e).__name__}:{e}")

    ts_codes = []
    input_csv = str(args.input_csv).strip()
    if input_csv:
        df_in = pd.read_csv(input_csv)
        if "ts_code" not in df_in.columns:
            raise SystemExit("input-csv 缺少 ts_code 列")
        ts_codes = df_in["ts_code"].astype(str).dropna().tolist()
    else:
        raw = str(args.ts_codes).strip()
        if raw:
            ts_codes = [x.strip() for x in raw.split(",") if x.strip()]

    if not ts_codes:
        raise SystemExit("未提供股票列表（用 --input-csv 或 --ts-codes）")

    ts_codes = list(dict.fromkeys(ts_codes))[: max(1, int(args.topk))]

    with tdx:
        quotes = fetch_quotes(
            tdx=tdx,
            ts_codes=ts_codes,
            chunk_size=int(args.chunk_size),
            sleep_s=float(args.sleep_s),
        )

    if quotes.empty:
        raise SystemExit("未取到盘口数据（quotes 为空）")

    merged = quotes.copy()
    if input_csv:
        df_in = pd.read_csv(input_csv)
        df_in["ts_code"] = df_in["ts_code"].astype(str)
        merged = df_in.merge(merged, on="ts_code", how="left")

    merged["imbalance"] = pd.to_numeric(merged.get("imbalance"), errors="coerce")
    merged["spread_bp"] = pd.to_numeric(merged.get("spread_bp"), errors="coerce")
    merged["speed_pct"] = pd.to_numeric(merged.get("speed_pct"), errors="coerce")
    merged["gap_pct"] = pd.to_numeric(merged.get("gap_pct"), errors="coerce")

    flt = merged.copy()
    flt = flt[flt["imbalance"].fillna(0.0) >= float(args.min_imbalance)]
    flt = flt[flt["spread_bp"].fillna(1e9) <= float(args.max_spread_bp)]
    flt = flt[flt["speed_pct"].fillna(-1e9) >= float(args.min_speed_pct)]
    flt = flt[flt["gap_pct"].fillna(-1e9) >= float(args.min_gap_pct)]

    sort_cols = []
    for c in ["imbalance", "speed_pct", "gap_pct"]:
        if c in flt.columns:
            sort_cols.append(c)
    if sort_cols:
        flt = flt.sort_values(sort_cols, ascending=[False] * len(sort_cols), na_position="last")

    out = str(args.out).strip()
    if not out:
        out = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"盘口确认_{_now_ts()}.csv")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    flt.to_csv(out, index=False, encoding="utf-8-sig")

    print(f"input={input_csv or 'manual'} total={len(merged)} passed={len(flt)} out={out}")
    show_cols = [c for c in ["ts_code", "name", "score", "price", "gap_pct", "imbalance", "spread_bp", "speed_pct", "servertime"] if c in flt.columns]
    with pd.option_context("display.max_rows", 80, "display.max_columns", 50, "display.width", 240):
        print(flt[show_cols].head(min(60, len(flt))).to_string(index=False))


if __name__ == "__main__":
    main()
