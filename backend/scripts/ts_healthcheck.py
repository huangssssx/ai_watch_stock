import argparse
import os
import random
import sys
import time
from datetime import datetime, timedelta

import pandas as pd


def _project_root() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(here, "..", ".."))


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _fmt_bool(v: bool) -> str:
    return "OK" if bool(v) else "FAIL"


def _pick_ts_code(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return "000001.SZ"
    if "," in s:
        items = [x.strip() for x in s.split(",") if x.strip()]
        if not items:
            return "000001.SZ"
        return random.choice(items)
    return s


def _date_range_days(days: int) -> tuple[str, str]:
    end = datetime.now().date()
    start = end - timedelta(days=int(days))
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def _call_once(pro, endpoint: str, ts_code: str, lookback_days: int) -> tuple[bool, float, str, int]:
    endpoint = str(endpoint).strip()
    ts_code = str(ts_code).strip()
    start_date, end_date = _date_range_days(int(lookback_days))

    t0 = time.time()
    err = ""
    rows = 0
    ok = False
    try:
        if endpoint == "trade_cal":
            df = pro.trade_cal(exchange="SSE", start_date=end_date, end_date=end_date, fields="exchange,cal_date,is_open")
        elif endpoint == "daily_basic":
            df = pro.daily_basic(ts_code=ts_code, start_date=start_date, end_date=end_date, fields="ts_code,trade_date,turnover_rate,volume_ratio")
        elif endpoint == "moneyflow":
            df = pro.moneyflow(ts_code=ts_code, start_date=start_date, end_date=end_date, fields="ts_code,trade_date,net_mf_amount")
        elif endpoint == "stk_limit":
            df = pro.stk_limit(ts_code=ts_code, start_date=start_date, end_date=end_date, fields="ts_code,trade_date,up_limit,down_limit")
        else:
            raise ValueError(f"unknown endpoint: {endpoint}")
        if df is not None and not getattr(df, "empty", True):
            rows = int(len(df))
        ok = df is not None
    except Exception as e:
        err = f"{type(e).__name__}:{e}"
        ok = False
    elapsed = time.time() - t0
    return ok, elapsed, err, rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Tushare 服务器可用性探测：每隔 N 秒请求一次指定接口")
    parser.add_argument("--interval-s", type=float, default=5.0)
    parser.add_argument("--endpoint", type=str, default="rotate", choices=["rotate", "trade_cal", "daily_basic", "moneyflow", "stk_limit"])
    parser.add_argument("--ts-code", type=str, default="000001.SZ")
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    root = _project_root()
    if root not in sys.path:
        sys.path.insert(0, root)

    try:
        from backend.utils.tushare_client import pro
    except Exception as e:
        raise SystemExit(f"导入 tushare_client 失败: {type(e).__name__}:{e}")

    if pro is None:
        raise SystemExit("pro=None，无法探测（请检查 tushare token/config）")

    endpoints = ["trade_cal", "daily_basic", "moneyflow", "stk_limit"]
    interval_s = max(0.2, float(args.interval_s))
    i = 0
    while True:
        i += 1
        ts_code = _pick_ts_code(str(args.ts_code))
        if str(args.endpoint) == "rotate":
            endpoint = endpoints[(i - 1) % len(endpoints)]
        else:
            endpoint = str(args.endpoint)

        ok, elapsed, err, rows = _call_once(pro, endpoint=endpoint, ts_code=ts_code, lookback_days=int(args.lookback_days))
        msg = f"{_now()} {_fmt_bool(ok)} endpoint={endpoint} ts_code={ts_code} elapsed_s={elapsed:.2f} rows={rows}"
        if err:
            msg += f" err={err}"
        print(msg, flush=True)

        if bool(args.once):
            return
        time.sleep(interval_s)


if __name__ == "__main__":
    main()
