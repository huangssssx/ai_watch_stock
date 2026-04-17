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

print("开始运行：连续小阳线（近3日）")

result_rows = []
df = pd.DataFrame()


def _date_str(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def _get_trade_dates(base_date: str, days: int) -> list:
    base_date = str(base_date or "").strip()
    if not base_date:
        base_date = _date_str(datetime.now())
    end_dt = datetime.strptime(base_date, "%Y%m%d")
    start_dt = end_dt - timedelta(days=90)
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
    for d in reversed(candidates[-20:]):
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
        days = int(os.getenv("DAYS", "3"))
        min_pct_chg = float(os.getenv("MIN_PCT_CHG", "0.2"))
        max_pct_chg = float(os.getenv("MAX_PCT_CHG", "3.0"))
        top_n = int(os.getenv("TOP_N", "200"))

        trade_dates = _get_trade_dates(trade_date, days)
        if not trade_dates or len(trade_dates) < days:
            print(f"交易日不足: trade_date={trade_date} dates={trade_dates}")
        else:
            start_date = trade_dates[0]
            end_date = trade_dates[-1]
            daily_frames = []
            for d in trade_dates:
                try:
                    day = pro.daily(
                        trade_date=d,
                        fields="ts_code,trade_date,open,close,high,low,pct_chg,vol,amount,pre_close",
                    )
                except Exception:
                    day = None
                if day is not None and not day.empty:
                    daily_frames.append(day)
            daily = pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame()
            if daily is None or daily.empty:
                print(f"无行情数据 start_date={start_date} end_date={end_date}")
            else:
                daily = daily[daily["trade_date"].isin(trade_dates)].copy()
                for c in ["open", "close", "high", "low", "pct_chg", "vol", "amount"]:
                    if c in daily.columns:
                        daily[c] = pd.to_numeric(daily[c], errors="coerce")
                daily = daily.dropna(subset=["open", "close", "pct_chg", "vol", "amount"]).reset_index(drop=True)

                daily["is_small_yang"] = (
                    (daily["close"] > daily["open"])
                    & (daily["pct_chg"] >= float(min_pct_chg))
                    & (daily["pct_chg"] <= float(max_pct_chg))
                )

                daily["trade_dt"] = pd.to_datetime(daily["trade_date"], format="%Y%m%d", errors="coerce")
                daily = daily.dropna(subset=["trade_dt"]).copy()

                grouped = daily.groupby("ts_code", dropna=False)
                for ts_code, g in grouped:
                    if g["trade_date"].nunique() != len(trade_dates):
                        continue
                    if not bool(g["is_small_yang"].all()):
                        continue
                    g = g.sort_values("trade_dt").reset_index(drop=True)
                    sum_pct = float(g["pct_chg"].sum())
                    avg_amount_yi = float(g["amount"].mean()) / 1e5
                    last_row = g.iloc[-1]
                    result_rows.append(
                        {
                            "ts_code": ts_code,
                            "symbol": str(ts_code).split(".")[0],
                            "trade_date": end_date,
                            "days": len(trade_dates),
                            "sum_pct": round(sum_pct, 2),
                            "last_pct_chg": round(float(last_row["pct_chg"]), 2),
                            "avg_amount_yi": round(avg_amount_yi, 2),
                        }
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
                        lambda r: f"{start_date}-{end_date} 连续{int(r.get('days', 0))}日小阳线 累计{r.get('sum_pct', 0)}%",
                        axis=1,
                    )
                    keep_cols = [
                        "symbol",
                        "name",
                        "industry",
                        "sum_pct",
                        "last_pct_chg",
                        "avg_amount_yi",
                        "days",
                        "trade_date",
                        "reason",
                        "ts_code",
                    ]
                    keep_cols = [c for c in keep_cols if c in df.columns]
                    df = df[keep_cols].sort_values(["sum_pct", "last_pct_chg"], ascending=False).head(top_n)
                    csv_path = os.path.join(here, f"连续小阳线_近3日_{end_date}.csv")
                    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
                    print(f"完成：trade_date={end_date} 结果={len(df)} 已写入 {csv_path}")

except Exception as e:
    print("脚本异常:", str(e))
    print(traceback.format_exc())
    df = pd.DataFrame()
