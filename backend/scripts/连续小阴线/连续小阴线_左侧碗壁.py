import os
import sys
import traceback
from datetime import datetime, timedelta

import pandas as pd

here = os.path.abspath(os.path.dirname(__file__))
project_root = os.path.abspath(os.path.join(here, "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from backend.utils. import pro

print("开始运行：连续小阴线-左侧碗壁")

result_rows = []
df = pd.DataFrame()


def _date_str(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def _get_trade_dates(base_date: str, days: int) -> list:
    base_date = str(base_date or "").strip()
    if not base_date:
        base_date = _date_str(datetime.now())
    end_dt = datetime.strptime(base_date, "%Y%m%d")
    start_dt = end_dt - timedelta(days=120)
    cal = pro.trade_cal(
        exchange="SSE",
        start_date=_date_str(start_dt),
        end_date=base_date,
        fields="cal_date,is_open",
    )
    if cal is None or cal.empty:
        return []
    cal = cal[cal["is_open"] == 1]
    if cal.empty:
        return []
    open_dates = sorted(cal["cal_date"].astype(str).unique())
    if not open_dates:
        return []
    if base_date < open_dates[-1]:
        open_dates = [d for d in open_dates if d <= base_date]
    if not open_dates:
        return []
    resolved = open_dates[-1]
    now = datetime.now()
    today = _date_str(now)
    if base_date == today and resolved == today and now.time().strftime("%H%M") < "1630":
        if len(open_dates) >= 2:
            resolved = open_dates[-2]
    candidates = [d for d in open_dates if d <= resolved]
    picked = []
    min_valid_rows = 1000
    for d in reversed(candidates[-30:]):
        try:
            chk = pro.daily(trade_date=d, fields="ts_code,trade_date,open,close,pct_chg")
        except Exception:
            chk = None
        if chk is None or chk.empty:
            continue
        for c in ["open", "close", "pct_chg"]:
            if c in chk.columns:
                chk[c] = pd.to_numeric(chk[c], errors="coerce")
        valid = chk.dropna(subset=["open", "close", "pct_chg"])
        if len(valid) < min_valid_rows:
            continue
        picked.append(d)
        if len(picked) >= int(days):
            break
    return sorted(picked)


try:
    if pro is None:
        print("Tushare 未初始化，无法获取数据")
    else:
        today = _date_str(datetime.now())
        trade_date = os.getenv("TRADE_DATE", "").strip() or today
        window_days = int(os.getenv("WINDOW_DAYS", "20"))
        consec_days = int(os.getenv("CONSEC_DAYS", "5"))
        min_abs_pct = float(os.getenv("MIN_ABS_PCT", "0.3"))
        max_abs_pct = float(os.getenv("MAX_ABS_PCT", "2.5"))
        min_drop_pct = float(os.getenv("MIN_DROP_PCT", "6.0"))
        bottom_within_pct = float(os.getenv("BOTTOM_WITHIN_PCT", "2.0"))
        bottom_stable_abs_pct = float(os.getenv("BOTTOM_STABLE_ABS_PCT", "1.0"))
        vol_ratio_max = float(os.getenv("VOL_RATIO_MAX", "0.85"))
        desc_ratio_min = float(os.getenv("DESC_RATIO_MIN", "0.55"))
        low_pos_min = float(os.getenv("LOW_POS_MIN", "0.6"))
        tail_min_ratio = float(os.getenv("TAIL_MIN_RATIO", "0.8"))
        fallback_relax = int(os.getenv("FALLBACK_RELAX", "1"))
        top_n = int(os.getenv("TOP_N", "200"))

        trade_dates = _get_trade_dates(trade_date, window_days)
        if not trade_dates or len(trade_dates) < window_days:
            print(f"交易日不足: trade_date={trade_date} dates={trade_dates}")
        else:
            start_date = trade_dates[0]
            end_date = trade_dates[-1]
            daily_frames = []
            print(f"加载行情数据：{start_date}-{end_date} 共{len(trade_dates)}个交易日")
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
                print(f"已加载 {idx}/{len(trade_dates)}: {d} 行数={0 if day is None else len(day)}")
            daily = pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame()
            if daily is None or daily.empty:
                print(f"无行情数据 start_date={start_date} end_date={end_date}")
            else:
                daily = daily[daily["trade_date"].isin(trade_dates)].copy()
                for c in ["open", "close", "high", "low", "pct_chg", "vol", "amount"]:
                    if c in daily.columns:
                        daily[c] = pd.to_numeric(daily[c], errors="coerce")
                daily = daily.dropna(subset=["open", "close", "pct_chg", "vol", "amount"]).reset_index(drop=True)

                daily["trade_dt"] = pd.to_datetime(daily["trade_date"], format="%Y%m%d", errors="coerce")
                daily = daily.dropna(subset=["trade_dt"]).copy()

                grouped = daily.groupby("ts_code", dropna=False)
                total_groups = int(grouped.ngroups)
                print(f"开始筛选：股票数={total_groups}")

                def _scan(
                    *,
                    scan_min_abs_pct: float,
                    scan_max_abs_pct: float,
                    scan_min_drop_pct: float,
                    scan_desc_ratio_min: float,
                    scan_low_pos_min: float,
                    scan_tail_min_ratio: float,
                ) -> list:
                    rows = []
                    for idx, (ts_code, g) in enumerate(grouped, start=1):
                        if idx == 1 or idx % 300 == 0 or idx == total_groups:
                            print(f"进度 {idx}/{total_groups}")
                        if g["trade_date"].nunique() != len(trade_dates):
                            continue
                        g = g.sort_values("trade_dt").reset_index(drop=True)
                        if len(g) < max(window_days, consec_days):
                            continue
                        tail = g.tail(consec_days)
                        tail_small_yin = (
                            (tail["close"] < tail["open"])
                            & (tail["pct_chg"] <= -float(scan_min_abs_pct))
                            & (tail["pct_chg"] >= -float(scan_max_abs_pct))
                        )
                        tail_ratio = float(tail_small_yin.mean()) if len(tail_small_yin) > 0 else 0.0
                        if tail_ratio < float(scan_tail_min_ratio):
                            continue
                        start_close = float(g.iloc[0]["close"])
                        end_close = float(g.iloc[-1]["close"])
                        if not (start_close > 0 and end_close > 0):
                            continue
                        drop_pct = (end_close - start_close) / start_close * 100.0
                        if drop_pct > -float(scan_min_drop_pct):
                            continue
                        close_diff = g["close"].diff().dropna()
                        desc_ratio = float((close_diff < 0).mean()) if len(close_diff) > 0 else 0.0
                        if desc_ratio < float(scan_desc_ratio_min):
                            continue
                        low_idx = int(g["low"].astype(float).idxmin())
                        low_pos_ratio = (low_idx - g.index.min()) / float(max(1, len(g) - 1))
                        if low_pos_ratio < float(scan_low_pos_min):
                            continue
                        last_row = g.iloc[-1]
                        last_pct = float(last_row["pct_chg"])
                        last_close = float(last_row["close"])
                        min_low = float(g["low"].min())
                        last_abs_pct = abs(last_pct)
                        near_bottom_price = last_close <= min_low * (1.0 + float(bottom_within_pct) / 100.0)
                        vol_avg = float(g["vol"].mean()) if float(g["vol"].mean()) > 0 else 0.0
                        vol_ratio = float(last_row["vol"]) / vol_avg if vol_avg > 0 else float("nan")
                        recent_abs = g["pct_chg"].abs().tail(3).mean()
                        prev_abs = g["pct_chg"].abs().tail(6).head(3).mean()
                        decelerating = bool(recent_abs <= prev_abs) if pd.notna(recent_abs) and pd.notna(prev_abs) else False
                        near_bottom = (
                            bool(near_bottom_price)
                            and last_abs_pct <= float(bottom_stable_abs_pct)
                            and (pd.isna(vol_ratio) or vol_ratio <= float(vol_ratio_max))
                            and decelerating
                        )
                        sum_pct = float(tail["pct_chg"].sum())
                        avg_amount_yi = float(g["amount"].tail(consec_days).mean()) / 1e5
                        rows.append(
                            {
                                "ts_code": ts_code,
                                "symbol": str(ts_code).split(".")[0],
                                "trade_date": end_date,
                                "window_days": len(trade_dates),
                                "consec_days": int(consec_days),
                                "sum_pct": round(sum_pct, 2),
                                "drop_pct": round(drop_pct, 2),
                                "desc_ratio": round(desc_ratio, 2),
                                "low_pos_ratio": round(low_pos_ratio, 2),
                                "last_pct_chg": round(last_pct, 2),
                                "last_close": round(last_close, 2),
                                "min_low": round(min_low, 2),
                                "near_bottom": int(bool(near_bottom)),
                                "vol_ratio": round(vol_ratio, 2) if pd.notna(vol_ratio) else "",
                                "avg_amount_yi": round(avg_amount_yi, 2),
                            }
                        )
                    return rows

                result_rows = _scan(
                    scan_min_abs_pct=min_abs_pct,
                    scan_max_abs_pct=max_abs_pct,
                    scan_min_drop_pct=min_drop_pct,
                    scan_desc_ratio_min=desc_ratio_min,
                    scan_low_pos_min=low_pos_min,
                    scan_tail_min_ratio=tail_min_ratio,
                )
                if not result_rows and int(fallback_relax) == 1:
                    print("首轮无结果，启用宽松参数重扫")
                    result_rows = _scan(
                        scan_min_abs_pct=max(0.05, min_abs_pct * 0.5),
                        scan_max_abs_pct=max_abs_pct + 1.5,
                        scan_min_drop_pct=max(2.5, min_drop_pct * 0.6),
                        scan_desc_ratio_min=max(0.45, desc_ratio_min - 0.15),
                        scan_low_pos_min=max(0.45, low_pos_min - 0.1),
                        scan_tail_min_ratio=max(0.6, tail_min_ratio - 0.2),
                    )
                if len(result_rows) < 30 and int(fallback_relax) == 1:
                    print("结果偏少，启用二次宽松参数重扫")
                    result_rows = _scan(
                        scan_min_abs_pct=max(0.03, min_abs_pct * 0.4),
                        scan_max_abs_pct=max_abs_pct + 2.0,
                        scan_min_drop_pct=max(1.5, min_drop_pct * 0.5),
                        scan_desc_ratio_min=max(0.4, desc_ratio_min - 0.2),
                        scan_low_pos_min=max(0.4, low_pos_min - 0.15),
                        scan_tail_min_ratio=max(0.5, tail_min_ratio - 0.3),
                    )

                df = pd.DataFrame(result_rows)
                if df.empty:
                    print("无满足条件的股票")
                else:
                    basic = pro.stock_basic(list_status="L", fields="ts_code,name,industry,market")
                    if basic is not None and not basic.empty:
                        df = df.merge(basic, on="ts_code", how="left")
                    df["name"] = df["name"].fillna("").astype(str)
                    df = df[~df["name"].str.contains("ST", case=False, na=False)]
                    df = df[~df["name"].str.contains("退", case=False, na=False)]
                    df["reason"] = df.apply(
                        lambda r: f"{start_date}-{end_date} 连续{int(r.get('consec_days', 0))}日小阴线 左侧碗壁 drop{r.get('drop_pct', 0)}% 触底={int(r.get('near_bottom', 0))}",
                        axis=1,
                    )
                    keep_cols = [
                        "symbol",
                        "name",
                        "industry",
                        "sum_pct",
                        "drop_pct",
                        "desc_ratio",
                        "low_pos_ratio",
                        "last_pct_chg",
                        "last_close",
                        "min_low",
                        "near_bottom",
                        "vol_ratio",
                        "avg_amount_yi",
                        "consec_days",
                        "window_days",
                        "trade_date",
                        "reason",
                        "ts_code",
                    ]
                    keep_cols = [c for c in keep_cols if c in df.columns]
                    df = df[keep_cols].sort_values(
                        ["near_bottom", "drop_pct", "sum_pct"],
                        ascending=[False, True, True],
                    ).head(top_n)
                    csv_path = os.path.join(here, f"连续小阴线_左侧碗壁_{end_date}.csv")
                    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
                    print(f"完成：trade_date={end_date} 结果={len(df)} 已写入 {csv_path}")

except Exception as e:
    print("脚本异常:", str(e))
    print(traceback.format_exc())
    df = pd.DataFrame()
