import argparse
import os
import sys
import time
import traceback
from datetime import datetime, timedelta
from math import log
from typing import Optional
 
import numpy as np
import pandas as pd
 
 
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
 
from backend.utils.tushare_client import pro
 
 
def _ensure_pro() -> None:
    if pro is None:
        raise RuntimeError("pro=None，无法运行（请检查 backend/utils/tushare_client.py 配置与网络连通性）")
 
 
def _fetch_df(fetch_fn, attempts: int = 3, sleep_s: float = 0.6) -> pd.DataFrame:
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
    return last_df if isinstance(last_df, pd.DataFrame) else pd.DataFrame()
 
 
def _get_recent_trade_dates(n: int, end_date: Optional[str] = None) -> list[str]:
    _ensure_pro()
    end = str(end_date or datetime.now().strftime("%Y%m%d"))
    start = (datetime.strptime(end, "%Y%m%d") - timedelta(days=max(90, int(n) * 4))).strftime("%Y%m%d")
    cal = _fetch_df(lambda: pro.trade_cal(exchange="SSE", start_date=start, end_date=end, fields="cal_date,is_open"))
    if cal is None or cal.empty:
        raise RuntimeError("trade_cal 返回为空，无法确定交易日")
    cal = cal.copy()
    cal["cal_date"] = cal["cal_date"].astype(str)
    cal = cal[cal["is_open"].astype(int) == 1].sort_values("cal_date")
    dates = cal["cal_date"].tolist()
    if len(dates) < int(n):
        raise RuntimeError(f"交易日数量不足：需要 {n}，实际 {len(dates)}，请扩大回看窗口或检查 end_date={end}")
    return dates[-int(n) :]
 
 
def _pick_pivot_trade_date(trade_dates: list[str], requested_trade_date: Optional[str], sleep_s: float) -> str:
    if not trade_dates:
        raise RuntimeError("trade_dates 为空")
    req = str(requested_trade_date).strip() if requested_trade_date else ""
    for d in reversed(trade_dates):
        if req and d > req:
            continue
        try:
            daily_probe = _fetch_df(
                lambda dd=d: pro.daily(
                    trade_date=dd,
                    fields="ts_code,trade_date,open,high,low,close,pre_close,pct_chg,vol,amount",
                ),
                attempts=2,
                sleep_s=max(0.2, float(sleep_s)),
            )
        except Exception:
            continue
        if daily_probe is None or daily_probe.empty:
            continue
        try:
            basic_probe = _fetch_df(
                lambda dd=d: pro.daily_basic(
                    trade_date=dd,
                    fields="ts_code,trade_date,turnover_rate,volume_ratio,pe,pe_ttm,pb,circ_mv,total_mv",
                ),
                attempts=2,
                sleep_s=max(0.2, float(sleep_s)),
            )
        except Exception:
            continue
        if basic_probe is None or basic_probe.empty:
            continue
        return str(d)
    raise RuntimeError("找不到可用的最近交易日（daily/daily_basic 数据可能未就绪）")
 
 
def _to_num(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out
 
 
def _score_row(row: pd.Series, near_low_pct: float, min_amount_yi: float) -> tuple[float, str]:
    dist_low_pct = float(row.get("dist_low_pct", np.nan))
    dd_pct = float(row.get("drawdown_pct", np.nan))
    amount_yi = float(row.get("amount_yi", np.nan))
    pe_ttm = float(row.get("pe_ttm", np.nan))
    pb = float(row.get("pb", np.nan))
 
    parts = []
    score = 0.0
 
    if np.isfinite(dist_low_pct):
        near_low_max = float(near_low_pct) * 100.0
        if near_low_max > 0:
            s = (near_low_max - max(0.0, dist_low_pct)) / near_low_max
            score += float(np.clip(s, 0.0, 1.0)) * 42.0
        parts.append(f"{dist_low_pct:+.2f}%距区间低点")
 
    if np.isfinite(dd_pct):
        dd_abs = max(0.0, -float(dd_pct))
        score += float(np.clip(dd_abs / 55.0, 0.0, 1.0)) * 28.0
        parts.append(f"{dd_pct:.1f}%距区间高点")
 
    if np.isfinite(amount_yi):
        if amount_yi >= float(min_amount_yi):
            x = log(max(1e-6, amount_yi / float(min_amount_yi)))
            score += 12.0 + float(np.clip(x / 1.4, 0.0, 1.0)) * 8.0
        parts.append(f"{amount_yi:.2f}亿成交额")
 
    val_s = 0.0
    if np.isfinite(pe_ttm) and pe_ttm > 0:
        if pe_ttm <= 30:
            val_s += 6.0
        elif pe_ttm <= 80:
            val_s += 3.0
    if np.isfinite(pb) and pb > 0:
        if pb <= 2.0:
            val_s += 4.0
        elif pb <= 5.0:
            val_s += 2.0
    if val_s > 0:
        score += val_s
        parts.append(f"估值PEttm={pe_ttm:.1f} PB={pb:.2f}")
 
    score = float(np.clip(score, 0.0, 100.0))
    reason = "；".join(parts[:6])[:200]
    return score, reason
 
 
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date", type=str, default="")
    parser.add_argument("--lookback-days", type=int, default=90)
    parser.add_argument("--near-low-pct", type=float, default=0.03)
    parser.add_argument("--min-drawdown-pct", type=float, default=25.0)
    parser.add_argument("--min-list-days", type=int, default=365)
    parser.add_argument("--min-amount-yi", type=float, default=2.0)
    parser.add_argument("--min-total-mv-yi", type=float, default=30.0)
    parser.add_argument("--max-total-mv-yi", type=float, default=3000.0)
    parser.add_argument("--min-turnover", type=float, default=0.3)
    parser.add_argument("--max-turnover", type=float, default=20.0)
    parser.add_argument("--min-pe-ttm", type=float, default=5.0)
    parser.add_argument("--max-pe-ttm", type=float, default=80.0)
    parser.add_argument("--min-pb", type=float, default=0.7)
    parser.add_argument("--max-pb", type=float, default=8.0)
    parser.add_argument("--max-results", type=int, default=200)
    parser.add_argument("--include-st", action="store_true", default=False)
    parser.add_argument("--keep-proxy", action="store_true", default=False)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--sleep-s", type=float, default=0.25)
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
 
        lookback = max(40, int(args.lookback_days))
        need_days = lookback + 1
        trade_dates = _get_recent_trade_dates(max(need_days, 25), end_date=args.trade_date or None)
        pivot_trade_date = _pick_pivot_trade_date(trade_dates, requested_trade_date=args.trade_date, sleep_s=float(args.sleep_s))
 
        pivot_idx = trade_dates.index(pivot_trade_date)
        start_idx = max(0, pivot_idx - lookback)
        window_dates = trade_dates[start_idx : pivot_idx + 1]
        if len(window_dates) < 40:
            raise RuntimeError(f"交易日窗口不足：{len(window_dates)}，请扩大 --lookback-days")
 
        daily_parts = []
        for i, d in enumerate(window_dates, start=1):
            daily_parts.append(
                _fetch_df(
                    lambda dd=d: pro.daily(
                        trade_date=dd,
                        fields="ts_code,trade_date,open,high,low,close,pre_close,pct_chg,vol,amount",
                    ),
                    attempts=3,
                    sleep_s=max(0.15, float(args.sleep_s)),
                )
            )
            if i < len(window_dates):
                time.sleep(float(args.sleep_s))
 
        daily = pd.concat([x for x in daily_parts if x is not None and not x.empty], ignore_index=True)
        if daily is None or daily.empty:
            raise RuntimeError("daily 拉取为空")
 
        basic = _fetch_df(
            lambda: pro.daily_basic(
                trade_date=pivot_trade_date,
                fields="ts_code,trade_date,turnover_rate,volume_ratio,pe,pe_ttm,pb,circ_mv,total_mv",
            ),
            attempts=3,
            sleep_s=max(0.25, float(args.sleep_s)),
        )
        stock_basic = _fetch_df(
            lambda: pro.stock_basic(
                exchange="",
                list_status="L",
                fields="ts_code,symbol,name,list_date,market",
            ),
            attempts=3,
            sleep_s=max(0.25, float(args.sleep_s)),
        )
 
        daily = daily.copy()
        daily["ts_code"] = daily["ts_code"].astype(str).str.strip()
        daily["trade_date"] = daily["trade_date"].astype(str).str.strip()
        daily = daily[daily["ts_code"].str.endswith((".SZ", ".SH", ".BJ"))].copy()
        daily = _to_num(daily, ["open", "high", "low", "close", "pre_close", "pct_chg", "vol", "amount"])
        daily = daily.dropna(subset=["ts_code", "trade_date", "close", "high", "low", "pct_chg", "amount"])
        daily = daily.sort_values(["ts_code", "trade_date"], ascending=True).reset_index(drop=True)
 
        g = daily.groupby("ts_code", group_keys=False)
        daily["low_n"] = g["low"].transform("min")
        daily["high_n"] = g["high"].transform("max")
        daily["close_5"] = g["close"].shift(5)
        daily["ret5"] = daily["close"] / daily["close_5"].replace(0.0, np.nan) - 1.0
        daily["down_days_5"] = g["pct_chg"].transform(lambda s: (s < 0).rolling(5, min_periods=5).sum())
 
        latest = g.tail(1).copy()
        latest = latest[latest["trade_date"] == pivot_trade_date].copy()
        if latest.empty:
            latest = g.tail(1).copy()
 
        latest["dist_low_pct"] = latest["close"] / latest["low_n"].replace(0.0, np.nan) - 1.0
        latest["near_low_ratio"] = latest["close"] / latest["low_n"].replace(0.0, np.nan)
        latest["drawdown_pct"] = (latest["close"] / latest["high_n"].replace(0.0, np.nan) - 1.0) * 100.0
        latest["ret5_pct"] = latest["ret5"] * 100.0
        latest["amount_yi"] = latest["amount"] / 100000.0
 
        if basic is not None and not basic.empty:
            basic = basic.copy()
            basic["ts_code"] = basic["ts_code"].astype(str).str.strip()
            basic["trade_date"] = basic["trade_date"].astype(str).str.strip()
            basic = _to_num(basic, ["turnover_rate", "volume_ratio", "pe", "pe_ttm", "pb", "circ_mv", "total_mv"])
            latest = latest.merge(basic, on=["ts_code", "trade_date"], how="left")
 
        if stock_basic is None or stock_basic.empty:
            raise RuntimeError("stock_basic 拉取为空")
        stock_basic = stock_basic.copy()
        stock_basic["ts_code"] = stock_basic["ts_code"].astype(str).str.strip()
        stock_basic["symbol"] = stock_basic["symbol"].astype(str).str.strip()
        stock_basic["name"] = stock_basic["name"].astype(str).str.strip()
        stock_basic["list_date"] = stock_basic["list_date"].astype(str).str.strip()
        if not args.include_st:
            stock_basic = stock_basic[~stock_basic["name"].str.contains("ST", na=False)].copy()
 
        latest = latest.merge(stock_basic[["ts_code", "symbol", "name", "list_date", "market"]], on="ts_code", how="inner")
 
        latest["total_mv_yi"] = pd.to_numeric(latest.get("total_mv", np.nan), errors="coerce") / 10000.0
        latest["turnover_rate"] = pd.to_numeric(latest.get("turnover_rate", np.nan), errors="coerce")
        latest["volume_ratio"] = pd.to_numeric(latest.get("volume_ratio", np.nan), errors="coerce")
        latest["pe_ttm"] = pd.to_numeric(latest.get("pe_ttm", np.nan), errors="coerce")
        latest["pb"] = pd.to_numeric(latest.get("pb", np.nan), errors="coerce")
 
        list_dt = pd.to_datetime(latest["list_date"], format="%Y%m%d", errors="coerce")
        ref_dt = datetime.strptime(str(pivot_trade_date), "%Y%m%d")
        latest["list_days"] = (ref_dt - list_dt).dt.days
 
        work = latest.copy()
        work = work.dropna(subset=["symbol", "name", "close", "low_n", "high_n", "dist_low_pct", "drawdown_pct", "amount_yi"])
        work = work[(work["list_days"].isna()) | (work["list_days"] >= int(args.min_list_days))].copy()
 
        near_low_pct = float(args.near_low_pct)
        min_drawdown_pct = float(args.min_drawdown_pct)
        work = work[
            (work["dist_low_pct"] >= 0)
            & (work["dist_low_pct"] <= near_low_pct)
            & ((-work["drawdown_pct"]) >= min_drawdown_pct)
            & (work["amount_yi"] >= float(args.min_amount_yi))
        ].copy()
 
        work = work[
            (work["turnover_rate"].isna())
            | ((work["turnover_rate"] >= float(args.min_turnover)) & (work["turnover_rate"] <= float(args.max_turnover)))
        ].copy()
 
        work = work[
            (work["total_mv_yi"].isna())
            | ((work["total_mv_yi"] >= float(args.min_total_mv_yi)) & (work["total_mv_yi"] <= float(args.max_total_mv_yi)))
        ].copy()
 
        work = work[
            (work["pe_ttm"].notna())
            & (work["pe_ttm"] >= float(args.min_pe_ttm))
            & (work["pe_ttm"] <= float(args.max_pe_ttm))
            & (work["pb"].notna())
            & (work["pb"] >= float(args.min_pb))
            & (work["pb"] <= float(args.max_pb))
        ].copy()
 
        scores = []
        reasons = []
        for _, row in work.iterrows():
            s, r = _score_row(row, near_low_pct=near_low_pct, min_amount_yi=float(args.min_amount_yi))
            scores.append(s)
            reasons.append(r)
        work["score"] = scores
        work["reason"] = reasons
 
        work = work.sort_values(["score", "amount_yi"], ascending=[False, False]).reset_index(drop=True)
 
        keep_cols = [
            "symbol",
            "name",
            "ts_code",
            "trade_date",
            "close",
            "pct_chg",
            "amount_yi",
            "turnover_rate",
            "volume_ratio",
            "total_mv_yi",
            "pe_ttm",
            "pb",
            "list_days",
            "low_n",
            "high_n",
            "dist_low_pct",
            "drawdown_pct",
            "ret5_pct",
            "down_days_5",
            "score",
            "reason",
        ]
        for c in keep_cols:
            if c not in work.columns:
                work[c] = np.nan
        df = work[keep_cols].head(int(args.max_results)).copy()
 
        out_name = f"跌到底的股票_{pivot_trade_date}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        out_path = os.path.join(os.path.dirname(__file__), out_name)
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
 
        print(
            f"trade_date={pivot_trade_date} lookback_days={lookback} near_low_pct={near_low_pct} min_drawdown_pct={min_drawdown_pct} "
            f"结果数量={len(df)} 输出={out_path}",
            flush=True,
        )
        if not df.empty:
            show_cols = ["symbol", "name", "close", "pct_chg", "amount_yi", "total_mv_yi", "pe_ttm", "pb", "score", "reason"]
            show_cols = [c for c in show_cols if c in df.columns]
            print(df[show_cols].head(30).to_string(index=False), flush=True)
    except Exception as e:
        print("脚本异常:", str(e))
        print(traceback.format_exc())
        df = pd.DataFrame()
 
 
if __name__ == "__main__":
    main()
