import os
import sys
import traceback
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

here = os.path.abspath(os.path.dirname(__file__))
project_root = os.path.abspath(os.path.join(here, "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from backend.utils.tushare_client import pro

print("开始运行：地量企稳反转信号检测")

result_rows = []
df = pd.DataFrame()


def _date_str(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def _get_trade_dates(base_date: str, n: int) -> list:
    base_date = str(base_date or "").strip()
    if not base_date:
        base_date = _date_str(datetime.now())
    end_dt = datetime.strptime(base_date, "%Y%m%d")
    start_dt = end_dt - timedelta(days=int(n * 2.5))
    cal = pro.trade_cal(
        exchange="SSE",
        start_date=_date_str(start_dt),
        end_date=base_date,
        fields="cal_date,is_open",
    )
    if cal is None or cal.empty:
        return []
    cal = cal[cal["is_open"] == 1]
    open_dates = sorted(cal["cal_date"].astype(str).unique())
    if not open_dates:
        return []
    resolved = base_date
    now = datetime.now()
    today = _date_str(now)
    if base_date == today and resolved == today and now.time().strftime("%H%M") < "1630":
        candidates_before = [d for d in open_dates if d <= base_date]
        if len(candidates_before) >= 2:
            resolved = candidates_before[-2]
    valid = [d for d in open_dates if d <= resolved]
    return sorted(valid[-n:])


def _load_index_daily(end_date: str, n_days: int = 30) -> pd.DataFrame:
    end_dt = datetime.strptime(end_date, "%Y%m%d")
    start_dt = end_dt - timedelta(days=int(n_days * 2))
    idx = pro.index_daily(
        ts_code="000001.SH",
        start_date=_date_str(start_dt),
        end_date=end_date,
        fields="trade_date,close",
    )
    if idx is None or idx.empty:
        return pd.DataFrame()
    idx["close"] = pd.to_numeric(idx["close"], errors="coerce")
    idx = idx.dropna(subset=["close"]).sort_values("trade_date").reset_index(drop=True)
    return idx


def _check_index_ma20_bullish(idx_df: pd.DataFrame) -> bool:
    if idx_df is None or len(idx_df) < 20:
        return True
    ma20 = idx_df["close"].rolling(20).mean()
    last_ma20 = ma20.iloc[-1]
    prev_ma20 = ma20.iloc[-2] if len(ma20) >= 2 else ma20.iloc[-1]
    if pd.isna(last_ma20) or pd.isna(prev_ma20):
        return True
    return last_ma20 >= prev_ma20


def _load_weekly_data(ts_code: str, end_date: str, n_weeks: int = 20) -> pd.DataFrame:
    end_dt = datetime.strptime(end_date, "%Y%m%d")
    start_dt = end_dt - timedelta(weeks=n_weeks * 2)
    import tushare as ts

    w = ts.pro_bar(
        ts_code=ts_code,
        asset="E",
        freq="W",
        start_date=_date_str(start_dt),
        end_date=end_date,
    )
    if w is None or w.empty:
        return pd.DataFrame()
    for c in ["open", "close", "high", "low", "vol"]:
        if c in w.columns:
            w[c] = pd.to_numeric(w[c], errors="coerce")
    w = w.sort_values("trade_date").reset_index(drop=True)
    return w


def _check_weekly_stabilization(w_df: pd.DataFrame) -> dict:
    result = {"weekly_ok": True, "weekly_detail": ""}
    if w_df is None or len(w_df) < 6:
        return result
    last_3 = w_df.tail(3)
    low_min = w_df["low"].min()
    no_new_low = (last_3["low"] >= low_min * 0.98).all()
    vol_ma6 = w_df["vol"].tail(6).mean()
    recent_vol_low = (last_3["vol"].mean() < vol_ma6 * 0.7) if vol_ma6 > 0 else False
    if not no_new_low:
        result["weekly_ok"] = False
        result["weekly_detail"] = "周线仍在创新低"
    elif recent_vol_low:
        result["weekly_detail"] = "周线缩量企稳"
    return result


try:
    if pro is None:
        print("Tushare 未初始化，无法获取数据")
    else:
        today = _date_str(datetime.now())
        trade_date = os.getenv("TRADE_DATE", "").strip() or today

        lookback_days = int(os.getenv("LOOKBACK_DAYS", "120"))
        decline_window = int(os.getenv("DECLINE_WINDOW", "30"))
        min_decline_pct = float(os.getenv("MIN_DECLINE_PCT", "15.0"))
        decline_window2 = int(os.getenv("DECLINE_WINDOW2", "60"))
        min_decline_pct2 = float(os.getenv("MIN_DECLINE_PCT2", "30.0"))
        vol_percentile_window = int(os.getenv("VOL_PERCENTILE_WINDOW", "60"))
        vol_percentile_threshold = float(os.getenv("VOL_PERCENTILE_THRESHOLD", "5.0"))
        consec_low_vol_days = int(os.getenv("CONSEC_LOW_VOL_DAYS", "3"))
        turnover_rate_max = float(os.getenv("TURNOVER_RATE_MAX", "1.0"))
        turnover_rate_max_small = float(os.getenv("TURNOVER_RATE_MAX_SMALL", "3.0"))
        small_cap_mv_threshold = float(os.getenv("SMALL_CAP_MV_THRESHOLD", "500000"))
        consec_no_new_low = int(os.getenv("CONSEC_NO_NEW_LOW", "3"))
        ma5_hold = int(os.getenv("MA5_HOLD", "1"))
        confirm_vol_ratio = float(os.getenv("CONFIRM_VOL_RATIO", "1.5"))
        confirm_yang_line = int(os.getenv("CONFIRM_YANG_LINE", "1"))
        use_weekly_filter = int(os.getenv("USE_WEEKLY_FILTER", "0"))
        use_index_filter = int(os.getenv("USE_INDEX_FILTER", "0"))
        fallback_relax = int(os.getenv("FALLBACK_RELAX", "1"))
        top_n = int(os.getenv("TOP_N", "200"))

        need_days = max(lookback_days, decline_window2 + 20, vol_percentile_window + 20)
        trade_dates = _get_trade_dates(trade_date, need_days)
        if not trade_dates or len(trade_dates) < need_days:
            print(f"交易日不足: trade_date={trade_date} 获取={len(trade_dates)} 需要={need_days}")
        else:
            start_date = trade_dates[0]
            end_date = trade_dates[-1]
            print(f"交易日范围: {start_date}-{end_date} 共{len(trade_dates)}天")

            idx_df = _load_index_daily(end_date, 30)
            index_bullish = _check_index_ma20_bullish(idx_df)
            print(f"大盘MA20方向: {'向上/持平' if index_bullish else '向下'}")

            daily_frames = []
            basic_frames = []
            for idx, d in enumerate(trade_dates, start=1):
                try:
                    day = pro.daily(
                        trade_date=d,
                        fields="ts_code,trade_date,open,close,high,low,pct_chg,vol,amount,pre_close",
                    )
                except Exception:
                    day = None
                if day is not None and not day.empty:
                    daily_frames.append(day)
                try:
                    bday = pro.daily_basic(
                        trade_date=d,
                        fields="ts_code,trade_date,turnover_rate,total_mv,circ_mv",
                    )
                except Exception:
                    bday = None
                if bday is not None and not bday.empty:
                    basic_frames.append(bday)
                if idx == 1 or idx % 20 == 0 or idx == len(trade_dates):
                    print(f"已加载 {idx}/{len(trade_dates)}: {d}")

            daily = pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame()
            basic = pd.concat(basic_frames, ignore_index=True) if basic_frames else pd.DataFrame()

            if daily.empty:
                print("无行情数据")
            else:
                for c in ["open", "close", "high", "low", "pct_chg", "vol", "amount"]:
                    if c in daily.columns:
                        daily[c] = pd.to_numeric(daily[c], errors="coerce")
                daily = daily.dropna(subset=["open", "close", "pct_chg", "vol", "amount"]).reset_index(drop=True)
                daily["trade_dt"] = pd.to_datetime(daily["trade_date"], format="%Y%m%d", errors="coerce")
                daily = daily.dropna(subset=["trade_dt"]).copy()

                if not basic.empty:
                    for c in ["turnover_rate", "total_mv", "circ_mv"]:
                        if c in basic.columns:
                            basic[c] = pd.to_numeric(basic[c], errors="coerce")
                    basic = basic.dropna(subset=["turnover_rate"]).reset_index(drop=True)
                    basic["trade_dt"] = pd.to_datetime(basic["trade_date"], format="%Y%m%d", errors="coerce")
                    daily = daily.merge(
                        basic[["ts_code", "trade_dt", "turnover_rate", "total_mv", "circ_mv"]],
                        on=["ts_code", "trade_dt"],
                        how="left",
                    )
                else:
                    daily["turnover_rate"] = np.nan
                    daily["total_mv"] = np.nan
                    daily["circ_mv"] = np.nan

                grouped = daily.groupby("ts_code", dropna=False)
                total_groups = int(grouped.ngroups)
                print(f"开始扫描：股票数={total_groups}")

                def _scan(
                    *,
                    scan_min_decline_pct: float,
                    scan_min_decline_pct2: float,
                    scan_vol_pct_threshold: float,
                    scan_consec_low_vol: int,
                    scan_turnover_max: float,
                    scan_consec_no_new_low: int,
                    scan_confirm_vol_ratio: float,
                    scan_use_weekly: bool,
                    scan_use_index: bool,
                ) -> list:
                    rows = []
                    for idx_g, (ts_code, g) in enumerate(grouped, start=1):
                        if idx_g == 1 or idx_g % 500 == 0 or idx_g == total_groups:
                            print(f"扫描进度 {idx_g}/{total_groups}")
                        g = g.sort_values("trade_dt").reset_index(drop=True)
                        if len(g) < max(vol_percentile_window + 10, decline_window + 10):
                            continue
                        if g["trade_date"].nunique() < vol_percentile_window:
                            continue

                        g["ma5"] = g["close"].rolling(5).mean()
                        g["ma20"] = g["close"].rolling(20).mean()
                        g["vol_ma20"] = g["vol"].rolling(20).mean()

                        recent_n = g.tail(vol_percentile_window).copy()
                        if len(recent_n) < vol_percentile_window:
                            continue
                        recent_n["vol_pct"] = recent_n["vol"].rank(pct=True) * 100

                        last_date = g.iloc[-1]["trade_date"]
                        tail_n = recent_n.tail(scan_consec_low_vol).copy()
                        if len(tail_n) < scan_consec_low_vol:
                            continue

                        low_vol_all = (tail_n["vol_pct"] <= scan_vol_pct_threshold).all()
                        if not low_vol_all:
                            continue

                        tail_start = g.iloc[-scan_consec_low_vol]
                        tail_end = g.iloc[-1]
                        if pd.notna(tail_end["turnover_rate"]):
                            tr_max = scan_turnover_max
                            if pd.notna(tail_end["circ_mv"]) and tail_end["circ_mv"] < small_cap_mv_threshold:
                                tr_max = turnover_rate_max_small
                            tail_tr = tail_n["turnover_rate"].dropna()
                            if len(tail_tr) > 0 and tail_tr.mean() > tr_max:
                                continue

                        decline_ok = False
                        decline_pct = 0.0
                        decline_detail = ""
                        if len(g) >= decline_window:
                            w1_start = g.iloc[-decline_window]["close"]
                            w1_end = g.iloc[-1]["close"]
                            if w1_start > 0:
                                dp = (w1_end - w1_start) / w1_start * 100
                                if dp <= -scan_min_decline_pct:
                                    decline_ok = True
                                    decline_pct = dp
                                    decline_detail = f"{decline_window}日跌{dp:.1f}%"
                        if not decline_ok and len(g) >= decline_window2:
                            w2_start = g.iloc[-decline_window2]["close"]
                            w2_end = g.iloc[-1]["close"]
                            if w2_start > 0:
                                dp2 = (w2_end - w2_start) / w2_start * 100
                                if dp2 <= -scan_min_decline_pct2:
                                    decline_ok = True
                                    decline_pct = dp2
                                    decline_detail = f"{decline_window2}日跌{dp2:.1f}%"
                        if not decline_ok:
                            continue

                        recent_tail = g.tail(scan_consec_no_new_low)
                        lookback_for_low = g.iloc[-(scan_consec_no_new_low + 20) : -scan_consec_no_new_low]
                        if len(lookback_for_low) < 5:
                            lookback_for_low = g.iloc[: -scan_consec_no_new_low]
                        if len(lookback_for_low) == 0:
                            continue
                        low_min = lookback_for_low["low"].min()
                        no_new_low = (recent_tail["low"] >= low_min * 0.99).all()
                        if not no_new_low:
                            continue

                        last_close = float(g.iloc[-1]["close"])
                        last_ma5 = g.iloc[-1]["ma5"]
                        holds_ma5 = True
                        if ma5_hold and pd.notna(last_ma5) and last_ma5 > 0:
                            holds_ma5 = last_close >= last_ma5 * 0.995
                        if not holds_ma5:
                            continue

                        last_vol = float(g.iloc[-1]["vol"])
                        vol_ma20 = g.iloc[-1]["vol_ma20"]
                        vol_confirm = True
                        vol_ratio_val = np.nan
                        if scan_confirm_vol_ratio > 0 and pd.notna(vol_ma20) and vol_ma20 > 0:
                            vol_ratio_val = last_vol / vol_ma20
                            vol_confirm = vol_ratio_val >= scan_confirm_vol_ratio
                        if not vol_confirm:
                            continue

                        yang_confirm = True
                        if confirm_yang_line:
                            last_pct = float(g.iloc[-1]["pct_chg"])
                            yang_confirm = last_pct > 0
                        if not yang_confirm:
                            continue

                        weekly_ok = True
                        weekly_detail = ""
                        if scan_use_weekly:
                            w_data = _load_weekly_data(ts_code, last_date)
                            w_result = _check_weekly_stabilization(w_data)
                            weekly_ok = w_result["weekly_ok"]
                            weekly_detail = w_result["weekly_detail"]

                        if scan_use_index and not index_bullish:
                            continue

                        if not weekly_ok:
                            continue

                        last_row = g.iloc[-1]
                        avg_amount_yi = float(tail_n["amount"].mean()) / 1e5
                        reason_parts = [
                            decline_detail,
                            f"连续{scan_consec_low_vol}日地量(<{scan_vol_pct_threshold}%分位)",
                            f"放量{vol_ratio_val:.1f}倍" if pd.notna(vol_ratio_val) else "放量",
                            "阳线确认" if yang_confirm else "",
                            weekly_detail,
                        ]
                        reason = " ".join([p for p in reason_parts if p])

                        rows.append(
                            {
                                "ts_code": ts_code,
                                "symbol": str(ts_code).split(".")[0],
                                "trade_date": last_date,
                                "decline_pct": round(decline_pct, 2),
                                "decline_detail": decline_detail,
                                "vol_pct_avg": round(float(tail_n["vol_pct"].mean()), 2),
                                "vol_ratio": round(vol_ratio_val, 2) if pd.notna(vol_ratio_val) else "",
                                "last_close": round(last_close, 2),
                                "last_pct_chg": round(float(last_row["pct_chg"]), 2),
                                "turnover_rate": round(float(tail_tr.mean()), 2) if len(tail_tr) > 0 else "",
                                "circ_mv_yi": round(float(last_row["circ_mv"]) / 1e4, 2) if pd.notna(last_row.get("circ_mv")) else "",
                                "avg_amount_yi": round(avg_amount_yi, 2),
                                "weekly_detail": weekly_detail,
                                "index_bullish": int(index_bullish),
                                "reason": reason,
                            }
                        )
                    return rows

                result_rows = _scan(
                    scan_min_decline_pct=min_decline_pct,
                    scan_min_decline_pct2=min_decline_pct2,
                    scan_vol_pct_threshold=vol_percentile_threshold,
                    scan_consec_low_vol=consec_low_vol_days,
                    scan_turnover_max=turnover_rate_max,
                    scan_consec_no_new_low=consec_no_new_low,
                    scan_confirm_vol_ratio=confirm_vol_ratio,
                    scan_use_weekly=bool(use_weekly_filter),
                    scan_use_index=bool(use_index_filter),
                )

                if not result_rows and fallback_relax:
                    print("首轮无结果，放宽参数重扫（不要求放量+不要求阳线）")
                    result_rows = _scan(
                        scan_min_decline_pct=max(8.0, min_decline_pct * 0.6),
                        scan_min_decline_pct2=max(15.0, min_decline_pct2 * 0.6),
                        scan_vol_pct_threshold=vol_percentile_threshold + 5.0,
                        scan_consec_low_vol=max(2, consec_low_vol_days - 1),
                        scan_turnover_max=turnover_rate_max * 2,
                        scan_consec_no_new_low=max(2, consec_no_new_low - 1),
                        scan_confirm_vol_ratio=1.2,
                        scan_use_weekly=False,
                        scan_use_index=False,
                    )

                if len(result_rows) < 20 and fallback_relax:
                    print(f"结果偏少({len(result_rows)}条)，二次放宽重扫")
                    result_rows2 = _scan(
                        scan_min_decline_pct=max(5.0, min_decline_pct * 0.4),
                        scan_min_decline_pct2=max(10.0, min_decline_pct2 * 0.4),
                        scan_vol_pct_threshold=vol_percentile_threshold + 10.0,
                        scan_consec_low_vol=2,
                        scan_turnover_max=turnover_rate_max * 3,
                        scan_consec_no_new_low=2,
                        scan_confirm_vol_ratio=0.0,
                        scan_use_weekly=False,
                        scan_use_index=False,
                    )
                    existing_codes = {r["ts_code"] for r in result_rows}
                    for r in result_rows2:
                        if r["ts_code"] not in existing_codes:
                            result_rows.append(r)

                df = pd.DataFrame(result_rows)
                if df.empty:
                    print("无满足条件的股票")
                else:
                    stock_basic = pro.stock_basic(list_status="L", fields="ts_code,name,industry,market")
                    if stock_basic is not None and not stock_basic.empty:
                        df = df.merge(stock_basic, on="ts_code", how="left")
                    df["name"] = df["name"].fillna("").astype(str)
                    df = df[~df["name"].str.contains("ST", case=False, na=False)]
                    df = df[~df["name"].str.contains("退", case=False, na=False)]

                    keep_cols = [
                        "symbol",
                        "name",
                        "industry",
                        "trade_date",
                        "decline_pct",
                        "decline_detail",
                        "vol_pct_avg",
                        "vol_ratio",
                        "last_close",
                        "last_pct_chg",
                        "turnover_rate",
                        "circ_mv_yi",
                        "avg_amount_yi",
                        "weekly_detail",
                        "index_bullish",
                        "reason",
                        "ts_code",
                    ]
                    keep_cols = [c for c in keep_cols if c in df.columns]
                    df = df[keep_cols].sort_values(
                        ["vol_ratio", "decline_pct"],
                        ascending=[False, True],
                    ).head(top_n)

                    csv_path = os.path.join(here, f"地量企稳反转_{end_date}.csv")
                    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
                    print(f"完成：trade_date={end_date} 结果={len(df)} 已写入 {csv_path}")

except Exception as e:
    print("脚本异常:", str(e))
    print(traceback.format_exc())
    df = pd.DataFrame()
