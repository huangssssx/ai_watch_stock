import os
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

here = os.path.abspath(os.path.dirname(__file__))
project_root = os.path.abspath(os.path.join(here, "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from backend.utils.tushare_client import pro

print("=== 诊断：逐条件检查通过率 ===")

def _date_str(dt):
    return dt.strftime("%Y%m%d")

today = _date_str(datetime.now())
end_dt = datetime.strptime(today, "%Y%m%d")
start_dt = end_dt - timedelta(days=int(120 * 2.5))
cal = pro.trade_cal(exchange="SSE", start_date=_date_str(start_dt), end_date=today, fields="cal_date,is_open")
cal = cal[cal["is_open"] == 1]
open_dates = sorted(cal["cal_date"].astype(str).unique())
now = datetime.now()
if today == _date_str(now) and now.time().strftime("%H%M") < "1630":
    candidates_before = [d for d in open_dates if d <= today]
    if len(candidates_before) >= 2:
        resolved = candidates_before[-2]
    else:
        resolved = open_dates[-1]
else:
    resolved = today
valid = [d for d in open_dates if d <= resolved]
trade_dates = sorted(valid[-120:])
print(f"交易日范围: {trade_dates[0]}-{trade_dates[-1]} 共{len(trade_dates)}天")

daily_frames = []
basic_frames = []
for idx, d in enumerate(trade_dates, start=1):
    try:
        day = pro.daily(trade_date=d, fields="ts_code,trade_date,open,close,high,low,pct_chg,vol,amount,pre_close")
    except Exception:
        day = None
    if day is not None and not day.empty:
        daily_frames.append(day)
    try:
        bday = pro.daily_basic(trade_date=d, fields="ts_code,trade_date,turnover_rate,total_mv,circ_mv")
    except Exception:
        bday = None
    if bday is not None and not bday.empty:
        basic_frames.append(bday)
    if idx == 1 or idx % 20 == 0 or idx == len(trade_dates):
        print(f"已加载 {idx}/{len(trade_dates)}: {d}")

daily = pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame()
basic = pd.concat(basic_frames, ignore_index=True) if basic_frames else pd.DataFrame()

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

counts = {
    "total": 0,
    "has_enough_data": 0,
    "decline_ok": 0,
    "low_vol_3days": 0,
    "decline_and_lowvol": 0,
    "no_new_low": 0,
    "holds_ma5": 0,
    "vol_confirm_1_5": 0,
    "yang_line": 0,
    "all_pass": 0,
}

sample_reasons = []

grouped = daily.groupby("ts_code", dropna=False)
total = int(grouped.ngroups)
print(f"\n扫描 {total} 只股票...")

for gi, (ts_code, g) in enumerate(grouped, start=1):
    if gi % 1000 == 0 or gi == total:
        print(f"  {gi}/{total}")
    g = g.sort_values("trade_dt").reset_index(drop=True)
    counts["total"] += 1

    if len(g) < 70:
        continue
    if g["trade_date"].nunique() < 60:
        continue
    counts["has_enough_data"] += 1

    g["ma5"] = g["close"].rolling(5).mean()
    g["vol_ma20"] = g["vol"].rolling(20).mean()

    recent_n = g.tail(60).copy()
    recent_n["vol_pct"] = recent_n["vol"].rank(pct=True) * 100

    tail_3 = recent_n.tail(3).copy()
    low_vol_all = (tail_3["vol_pct"] <= 5.0).all()

    decline_ok = False
    decline_pct = 0.0
    decline_detail = ""
    if len(g) >= 30:
        w1_start = g.iloc[-30]["close"]
        w1_end = g.iloc[-1]["close"]
        if w1_start > 0:
            dp = (w1_end - w1_start) / w1_start * 100
            if dp <= -15:
                decline_ok = True
                decline_pct = dp
                decline_detail = f"30日跌{dp:.1f}%"
    if not decline_ok and len(g) >= 60:
        w2_start = g.iloc[-60]["close"]
        w2_end = g.iloc[-1]["close"]
        if w2_start > 0:
            dp2 = (w2_end - w2_start) / w2_start * 100
            if dp2 <= -30:
                decline_ok = True
                decline_pct = dp2
                decline_detail = f"60日跌{dp2:.1f}%"

    if decline_ok:
        counts["decline_ok"] += 1
    if low_vol_all:
        counts["low_vol_3days"] += 1
    if decline_ok and low_vol_all:
        counts["decline_and_lowvol"] += 1

    if not (decline_ok and low_vol_all):
        continue

    recent_tail = g.tail(3)
    lookback_for_low = g.iloc[-23:-3]
    if len(lookback_for_low) < 5:
        lookback_for_low = g.iloc[:-3]
    if len(lookback_for_low) == 0:
        continue
    low_min = lookback_for_low["low"].min()
    no_new_low = (recent_tail["low"] >= low_min * 0.99).all()
    if no_new_low:
        counts["no_new_low"] += 1
    else:
        continue

    last_close = float(g.iloc[-1]["close"])
    last_ma5 = g.iloc[-1]["ma5"]
    holds_ma5 = pd.isna(last_ma5) or last_ma5 <= 0 or last_close >= last_ma5 * 0.995
    if holds_ma5:
        counts["holds_ma5"] += 1
    else:
        continue

    last_vol = float(g.iloc[-1]["vol"])
    vol_ma20 = g.iloc[-1]["vol_ma20"]
    vol_ratio_val = np.nan
    vol_confirm = True
    if pd.notna(vol_ma20) and vol_ma20 > 0:
        vol_ratio_val = last_vol / vol_ma20
        vol_confirm = vol_ratio_val >= 1.5
    if vol_confirm:
        counts["vol_confirm_1_5"] += 1
    else:
        sample_reasons.append(f"{ts_code}: vol_ratio={vol_ratio_val:.2f} < 1.5")
        continue

    last_pct = float(g.iloc[-1]["pct_chg"])
    yang_confirm = last_pct > 0
    if yang_confirm:
        counts["yang_line"] += 1
    else:
        sample_reasons.append(f"{ts_code}: pct_chg={last_pct:.2f} <= 0")
        continue

    counts["all_pass"] += 1
    sample_reasons.append(f"✅ {ts_code}: {decline_detail}, vol_pct_avg={tail_3['vol_pct'].mean():.1f}, vol_ratio={vol_ratio_val:.2f}, pct={last_pct:.2f}")

print("\n=== 通过率统计 ===")
for k, v in counts.items():
    pct = v / max(counts["total"], 1) * 100
    print(f"  {k}: {v} ({pct:.2f}%)")

print("\n=== 样本原因（最后20条） ===")
for r in sample_reasons[-20:]:
    print(f"  {r}")
