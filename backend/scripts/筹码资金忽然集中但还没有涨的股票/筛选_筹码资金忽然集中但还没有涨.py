"""
全市场筛选：最近一个交易日“筹码/资金忽然集中但还没有涨”的股票列表

核心思路（最近一个交易日）：
1) 资金集中：net_mf_amount（万元）占当日成交额（万元）比例高，且较前N日均值显著放大
2) 还没有涨：当日涨跌幅 pct_chg 不大（默认 <= 2%）
3) 辅助确认：当日成交量相对前N日均量放大（默认 >= 1.2）

输出：
- 生成 CSV 到当前目录
- 同时暴露 df 变量，便于作为系统“选股脚本”结果消费
"""

import argparse
import os
import sys
import time
import traceback
from datetime import datetime, timedelta

import pandas as pd


_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from backend.utils.tushare_client import pro


def _ensure_pro():
    if pro is None:
        raise RuntimeError("Tushare pro 未初始化成功，请先检查 backend/utils/tushare_client.py 配置与网络连通性")


def _get_recent_trade_dates(n: int, end_date=None):
    _ensure_pro()
    end = str(end_date or datetime.now().strftime("%Y%m%d"))
    start = (datetime.strptime(end, "%Y%m%d") - timedelta(days=120)).strftime("%Y%m%d")
    cal = pro.trade_cal(exchange="SSE", start_date=start, end_date=end, fields="cal_date,is_open")
    if cal is None or cal.empty:
        raise RuntimeError("trade_cal 返回为空，无法确定交易日")
    cal = cal.copy()
    cal["cal_date"] = cal["cal_date"].astype(str)
    cal = cal[cal["is_open"] == 1].sort_values("cal_date")
    dates = cal["cal_date"].tolist()
    if len(dates) < n:
        raise RuntimeError(f"交易日数量不足：需要 {n}，实际 {len(dates)}，请扩大回看窗口或检查 end_date={end}")
    return dates[-n:]


def _to_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def _fetch_df(fetch_fn, attempts: int = 3, sleep_s: float = 1.0):
    last_exc = None
    last_df = None
    for i in range(int(attempts)):
        try:
            last_df = fetch_fn()
            if last_df is not None and not last_df.empty:
                return last_df
        except Exception as e:
            last_exc = e
        if i < int(attempts) - 1:
            time.sleep(float(sleep_s))
    if last_exc:
        raise last_exc
    return last_df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date", type=str, default="")
    parser.add_argument("--lookback-days", type=int, default=5)
    parser.add_argument("--max-stocks", type=int, default=300)
    parser.add_argument("--min-net-mf-ratio", type=float, default=0.10)
    parser.add_argument("--min-net-mf-amount-wan", type=float, default=2000.0)
    parser.add_argument("--spike-multiplier", type=float, default=2.0)
    parser.add_argument("--min-vol-ratio", type=float, default=1.2)
    parser.add_argument("--min-amount-yi", type=float, default=3.0)
    parser.add_argument("--max-pct-chg", type=float, default=2.0)
    parser.add_argument("--min-pct-chg", type=float, default=-3.0)
    parser.add_argument("--include-st", action="store_true", default=False)
    parser.add_argument("--keep-proxy", action="store_true", default=False)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--sleep-s", type=float, default=0.3)
    args = parser.parse_args()

    global df
    df = pd.DataFrame()

    try:
        if not args.keep_proxy:
            for k in [
                "HTTP_PROXY",
                "HTTPS_PROXY",
                "ALL_PROXY",
                "http_proxy",
                "https_proxy",
                "all_proxy",
            ]:
                os.environ.pop(k, None)
            os.environ["NO_PROXY"] = "*"
            os.environ["no_proxy"] = "*"

        _ensure_pro()
        try:
            pro._DataApi__timeout = int(args.timeout)
        except Exception:
            pass

        lookback = max(1, int(args.lookback_days))
        n_days = lookback + 1
        trade_dates = _get_recent_trade_dates(max(n_days, 12), end_date=args.trade_date or None)
        requested_trade_date = str(args.trade_date).strip() or None

        pivot_idx = None
        for i in range(len(trade_dates) - 1, lookback - 1, -1):
            d = trade_dates[i]
            if requested_trade_date and d > requested_trade_date:
                continue
            try:
                daily_probe = _fetch_df(
                    lambda dd=d: pro.daily(
                        trade_date=dd,
                        fields="ts_code,trade_date,open,close,pre_close,pct_chg,vol,amount",
                    ),
                    attempts=2,
                    sleep_s=float(args.sleep_s),
                )
            except Exception:
                continue
            if daily_probe is None or daily_probe.empty:
                continue
            try:
                mf_probe = _fetch_df(
                    lambda dd=d: pro.moneyflow(
                        trade_date=dd,
                        fields="ts_code,trade_date,net_mf_amount",
                    ),
                    attempts=2,
                    sleep_s=float(args.sleep_s),
                )
            except Exception:
                continue
            if mf_probe is None or mf_probe.empty:
                continue
            pivot_idx = i
            break

        if pivot_idx is None:
            print("无数据：最近交易日资金流数据不可用（可能是当日数据未入库或接口返回缺失）")
            return

        trade_dates = trade_dates[pivot_idx - lookback : pivot_idx + 1]
        trade_date = trade_dates[-1]

        daily_parts = []
        moneyflow_parts = []
        for d in trade_dates:
            daily_parts.append(
                _fetch_df(
                    lambda dd=d: pro.daily(
                        trade_date=dd,
                        fields="ts_code,trade_date,open,close,pre_close,pct_chg,vol,amount",
                    ),
                    attempts=3,
                    sleep_s=float(args.sleep_s),
                )
            )
            moneyflow_parts.append(
                _fetch_df(
                    lambda dd=d: pro.moneyflow(
                        trade_date=dd,
                        fields="ts_code,trade_date,net_mf_amount",
                    ),
                    attempts=3,
                    sleep_s=float(args.sleep_s),
                )
            )
            time.sleep(float(args.sleep_s))

        daily = pd.concat([x for x in daily_parts if x is not None and not x.empty], ignore_index=True)
        moneyflow = pd.concat([x for x in moneyflow_parts if x is not None and not x.empty], ignore_index=True)
        if daily.empty:
            print("无数据：daily 为空")
            return
        if moneyflow.empty:
            print("无数据：moneyflow 为空")
            return

        stock_basic = _fetch_df(lambda: pro.stock_basic(exchange="", list_status="L", fields="ts_code,symbol,name"))
        if stock_basic is None or stock_basic.empty:
            print("无数据：stock_basic 为空")
            return

        daily = daily.copy()
        daily["trade_date"] = daily["trade_date"].astype(str)
        daily = daily[daily["ts_code"].astype(str).str.endswith((".SZ", ".SH", ".BJ"))]
        daily = _to_numeric(daily, ["open", "close", "pre_close", "pct_chg", "vol", "amount"])
        daily = daily.dropna(subset=["ts_code", "trade_date", "close", "pct_chg", "vol", "amount"])

        moneyflow = moneyflow.copy()
        moneyflow["trade_date"] = moneyflow["trade_date"].astype(str)
        moneyflow = moneyflow[moneyflow["ts_code"].astype(str).str.endswith((".SZ", ".SH", ".BJ"))]
        moneyflow = _to_numeric(moneyflow, ["net_mf_amount"])
        moneyflow = moneyflow.dropna(subset=["ts_code", "trade_date", "net_mf_amount"])

        stock_basic = stock_basic.copy()
        stock_basic["symbol"] = stock_basic["symbol"].astype(str).str.strip()
        stock_basic["name"] = stock_basic["name"].astype(str).str.strip()
        if not args.include_st:
            stock_basic = stock_basic[~stock_basic["name"].str.contains("ST", na=False)]

        df_last_daily = daily[daily["trade_date"] == trade_date].copy()
        df_prev_daily = daily[daily["trade_date"] != trade_date].copy()
        if df_last_daily.empty or df_prev_daily.empty:
            print("无数据：最近交易日或回看窗口数据不足")
            return

        vol_prev_mean = df_prev_daily.groupby("ts_code", as_index=True)["vol"].mean().rename("vol_prev_mean")
        df_last_daily = df_last_daily.merge(vol_prev_mean, on="ts_code", how="left")
        df_last_daily["vol_ratio"] = df_last_daily["vol"] / df_last_daily["vol_prev_mean"].replace({0.0: pd.NA})

        mf_join = moneyflow.merge(
            daily[["ts_code", "trade_date", "amount"]],
            on=["ts_code", "trade_date"],
            how="left",
        )
        mf_join = mf_join.dropna(subset=["amount"])
        mf_join["amount_wan"] = mf_join["amount"] / 10.0
        mf_join = mf_join[mf_join["amount_wan"] > 0]
        mf_join["net_mf_ratio"] = mf_join["net_mf_amount"] / mf_join["amount_wan"]

        ratio_prev_mean = (
            mf_join[mf_join["trade_date"] != trade_date]
            .groupby("ts_code", as_index=True)["net_mf_ratio"]
            .mean()
            .rename("net_mf_ratio_prev_mean")
        )

        df_last_mf = moneyflow[moneyflow["trade_date"] == trade_date].copy()
        df_last = df_last_daily.merge(df_last_mf[["ts_code", "net_mf_amount"]], on="ts_code", how="left")
        df_last = df_last.merge(ratio_prev_mean, on="ts_code", how="left")

        df_last["amount_wan"] = df_last["amount"] / 10.0
        df_last["net_mf_ratio"] = df_last["net_mf_amount"] / df_last["amount_wan"].replace({0.0: pd.NA})
        eps = 1e-12
        df_last["net_mf_ratio_prev_mean"] = df_last["net_mf_ratio_prev_mean"].fillna(0.0)
        df_last["net_mf_ratio_spike"] = df_last["net_mf_ratio"] / (df_last["net_mf_ratio_prev_mean"].abs().clip(lower=eps))

        df_last["amount_yi"] = df_last["amount"] / 100000.0

        df_last = df_last.merge(stock_basic[["ts_code", "symbol", "name"]], on="ts_code", how="left")
        df_last = df_last.dropna(subset=["symbol"])

        df_last["vol_ratio"] = pd.to_numeric(df_last["vol_ratio"], errors="coerce")
        df_last["net_mf_amount"] = pd.to_numeric(df_last["net_mf_amount"], errors="coerce")
        df_last["net_mf_ratio"] = pd.to_numeric(df_last["net_mf_ratio"], errors="coerce")
        df_last["net_mf_ratio_spike"] = pd.to_numeric(df_last["net_mf_ratio_spike"], errors="coerce")

        work = df_last.copy()
        work = work.dropna(
            subset=[
                "name",
                "pct_chg",
                "amount_yi",
                "net_mf_amount",
                "net_mf_ratio",
                "net_mf_ratio_spike",
                "vol_ratio",
            ]
        )

        work = work[
            (work["net_mf_amount"] > 0)
            & (work["amount_yi"] >= float(args.min_amount_yi))
            & (work["pct_chg"] <= float(args.max_pct_chg))
            & (work["pct_chg"] >= float(args.min_pct_chg))
            & (work["net_mf_ratio"] >= float(args.min_net_mf_ratio))
            & (work["net_mf_amount"] >= float(args.min_net_mf_amount_wan))
            & (work["net_mf_ratio_spike"] >= float(args.spike_multiplier))
            & (work["vol_ratio"] >= float(args.min_vol_ratio))
        ]

        work = work.sort_values(["net_mf_ratio_spike", "net_mf_amount"], ascending=[False, False])
        work = work.reset_index(drop=True)

        cols = [
            "symbol",
            "name",
            "ts_code",
            "trade_date",
            "close",
            "pct_chg",
            "amount_yi",
            "net_mf_amount",
            "net_mf_ratio",
            "net_mf_ratio_prev_mean",
            "net_mf_ratio_spike",
            "vol_ratio",
        ]
        df = work[cols].head(int(args.max_stocks)).copy()

        out_name = "筹码资金忽然集中但还没有涨_latest.csv"
        out_path = os.path.join(os.path.dirname(__file__), out_name)
        df.to_csv(out_path, index=False, encoding="utf-8-sig")

        print(f"trade_date={trade_date} lookback_days={lookback} 结果数量={len(df)} 输出={out_path}")
        if not df.empty:
            print(df.head(20).to_string(index=False))
    except Exception as e:
        print("脚本异常:", str(e))
        print(traceback.format_exc())
        df = pd.DataFrame()


if __name__ == "__main__":
    main()
