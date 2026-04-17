import os
import sys
import math
import traceback
from datetime import datetime, timedelta

import pandas as pd

here = os.path.abspath(os.path.dirname(__file__))
project_root = os.path.abspath(os.path.join(here, "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from backend.utils. import pro

print("开始运行：寻找今日龙头股")

df = pd.DataFrame()
result_rows = []

try:
    if pro is None:
        print("Tushare 未初始化，无法获取数据")
    else:
        today = datetime.now().strftime("%Y%m%d")
        trade_date = os.getenv("TRADE_DATE", "").strip() or today
        top_n = int(os.getenv("TOP_N", "120"))
        per_industry = int(os.getenv("PER_INDUSTRY", "2"))
        min_pct_chg = float(os.getenv("MIN_PCT_CHG", "3.0"))
        min_amount_yi = float(os.getenv("MIN_AMOUNT_YI", "3.0"))

        def _fetch_daily(d: str):
            return pro.daily(
                trade_date=d,
                fields="ts_code,trade_date,open,high,low,close,pct_chg,vol,amount,pre_close",
            )

        daily = _fetch_daily(trade_date)

        if daily is None or daily.empty:
            cal_start = (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")
            cal = pro.trade_cal(exchange="SSE", start_date=cal_start, end_date=today, fields="cal_date,is_open")
            candidate_dates = []
            if cal is not None and not cal.empty:
                cal = cal[cal["is_open"] == 1]
                if not cal.empty:
                    candidate_dates = [str(d) for d in sorted(cal["cal_date"].unique(), reverse=True)]
            if not candidate_dates:
                candidate_dates = [
                    (datetime.now() - timedelta(days=i)).strftime("%Y%m%d") for i in range(1, 31)
                ]
            for d in candidate_dates:
                daily = _fetch_daily(d)
                if daily is not None and not daily.empty:
                    trade_date = d
                    break

        if daily is None or daily.empty:
            print(f"无行情数据 trade_date={trade_date}")
        else:
            daily_basic = pro.daily_basic(
                trade_date=trade_date,
                fields="ts_code,trade_date,turnover_rate,volume_ratio,total_mv,circ_mv",
            )
            basic = pro.stock_basic(
                list_status="L",
                fields="ts_code,name,industry,market",
            )

            df = daily.copy()
            if daily_basic is not None and not daily_basic.empty:
                df = df.merge(daily_basic, on=["ts_code", "trade_date"], how="left")
            if basic is not None and not basic.empty:
                df = df.merge(basic, on="ts_code", how="left")

            for c in ["pct_chg", "turnover_rate", "volume_ratio", "amount"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")

            df["name"] = df["name"].fillna("").astype(str)
            df = df[~df["name"].str.contains("ST", case=False, na=False)]
            df = df[~df["name"].str.contains("退", case=False, na=False)]

            df["amount_yi"] = df["amount"].fillna(0.0) / 1e5
            df["pct_chg"] = df["pct_chg"].fillna(0.0)
            df["turnover_rate"] = df["turnover_rate"].fillna(0.0)
            df["volume_ratio"] = df["volume_ratio"].fillna(0.0)

            df = df[df["pct_chg"] >= min_pct_chg]
            df = df[df["amount_yi"] >= min_amount_yi]

            df["score"] = (
                df["pct_chg"] * 2.2
                + df["turnover_rate"] * 0.6
                + df["volume_ratio"] * 1.2
                + df["amount_yi"].apply(lambda x: math.log1p(x)) * 4.0
            )

            df["symbol"] = df["ts_code"].astype(str).str.split(".").str[0]

            def _fmt(val, fmt, default="--"):
                try:
                    if pd.isna(val):
                        return default
                    return format(val, fmt)
                except Exception:
                    return default

            df["reason"] = df.apply(
                lambda r: "涨幅"
                + _fmt(r.get("pct_chg"), ".1f")
                + "% 成交额"
                + _fmt(r.get("amount_yi"), ".1f")
                + "亿 换手"
                + _fmt(r.get("turnover_rate"), ".1f")
                + "% 量比"
                + _fmt(r.get("volume_ratio"), ".2f"),
                axis=1,
            )

            df = df.sort_values("score", ascending=False)
            leaders = (
                df.groupby("industry", dropna=False).head(per_industry)
                if per_industry > 0
                else df.head(top_n)
            )
            final = pd.concat([leaders, df.head(top_n)], ignore_index=True)
            final = final.drop_duplicates(subset=["ts_code"])
            final = final.sort_values("score", ascending=False).head(top_n)

            keep_cols = [
                "symbol",
                "name",
                "industry",
                "score",
                "pct_chg",
                "turnover_rate",
                "volume_ratio",
                "amount_yi",
                "trade_date",
                "reason",
                "ts_code",
            ]
            keep_cols = [c for c in keep_cols if c in final.columns]
            df = final[keep_cols].reset_index(drop=True)
            csv_path = os.path.join(here, "今日龙头股.csv")
            df.to_csv(csv_path, index=False, encoding="utf-8-sig")
            print(f"完成：trade_date={trade_date} 结果={len(df)} 已写入 {csv_path}")

except Exception as e:
    print("脚本异常:", str(e))
    print(traceback.format_exc())
    df = pd.DataFrame()
