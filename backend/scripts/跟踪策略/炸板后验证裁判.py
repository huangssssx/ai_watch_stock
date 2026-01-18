try:
    ak
except NameError:
    import akshare as ak

try:
    pd
except NameError:
    import pandas as pd

try:
    datetime
except NameError:
    import datetime

try:
    symbol
except NameError:
    symbol = "sz301668"

triggered = False
message = "未执行"

symbol_raw = symbol
symbol_code = str(symbol_raw).lower().replace("sh", "").replace("sz", "").replace("bj", "")

LOOKBACK_DAYS = 30
EVAL_DAYS_PRIMARY = 3
EVAL_DAYS_SECONDARY = 5
MANUAL_SUPPORT_PRICE = None
GAP_DOWN_OPEN_LT_PREV_LOW_PCT = 0.995

now_cn = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
start_date = (now_cn - datetime.timedelta(days=LOOKBACK_DAYS * 3)).strftime("%Y%m%d")
end_date = (now_cn - datetime.timedelta(days=1)).strftime("%Y%m%d")

df_hist = ak.stock_zh_a_hist(symbol=symbol_code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
if df_hist is None or df_hist.empty:
    message = f"{symbol_raw}: 无历史数据"
else:
    cols = {c: c for c in df_hist.columns}
    if "日期" in cols:
        df_hist = df_hist.rename(
            columns={
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "成交额": "amount",
                "换手率": "turnover",
            }
        )
    df_hist["date"] = pd.to_datetime(df_hist["date"])
    df_hist = df_hist.sort_values("date").reset_index(drop=True)

    if len(df_hist) < 3:
        message = f"{symbol_raw}: 数据不足"
    else:
        recent = df_hist.tail(LOOKBACK_DAYS).reset_index(drop=True)

        zhaban_idx = None
        for i in range(1, len(recent)):
            prev_close = float(recent.loc[i - 1, "close"])
            day_high = float(recent.loc[i, "high"])
            day_close = float(recent.loc[i, "close"])
            if prev_close <= 0 or day_high <= 0:
                continue
            day_high_pct = (day_high - prev_close) / prev_close
            drawdown = (day_high - day_close) / day_high
            if (day_high_pct > 0.05) and (drawdown > 0.04):
                zhaban_idx = i

        if zhaban_idx is None:
            message = f"{symbol_raw}: 近{LOOKBACK_DAYS}日未发现炸板形态"
        else:
            ref = recent.loc[zhaban_idx]
            ref_date = pd.to_datetime(ref["date"]).date().isoformat()
            ref_close = float(ref["close"])
            ref_high = float(ref["high"])
            ref_low = float(ref["low"])

            prev = recent.loc[zhaban_idx - 1]
            prev_close = float(prev["close"])
            prev_low = float(prev["low"])

            support_price = float(MANUAL_SUPPORT_PRICE) if MANUAL_SUPPORT_PRICE is not None else ref_low

            full_recent = recent.copy()
            full_recent["vol_ma5"] = full_recent["volume"].rolling(5, min_periods=1).mean()

            start_i = zhaban_idx + 1
            end_i_3 = min(len(full_recent), zhaban_idx + 1 + EVAL_DAYS_PRIMARY)
            end_i_5 = min(len(full_recent), zhaban_idx + 1 + EVAL_DAYS_SECONDARY)
            w3 = full_recent.iloc[start_i:end_i_3].reset_index(drop=True)
            w5 = full_recent.iloc[start_i:end_i_5].reset_index(drop=True)

            script_signals = []
            manual_signals = []

            gap_down_confirmed = False
            support_confirmed_break = False
            volume_structure_bad = False

            if len(w3) > 0:
                for j in range(len(w3)):
                    day = w3.loc[j]
                    o = float(day["open"])
                    c = float(day["close"])
                    if (o < prev_low * GAP_DOWN_OPEN_LT_PREV_LOW_PCT) and (c < prev_low):
                        gap_down_confirmed = True

            if len(w3) >= 2:
                for j in range(len(w3) - 1):
                    c0 = float(w3.loc[j, "close"])
                    c1 = float(w3.loc[j + 1, "close"])
                    if (c0 < support_price) and (c1 < support_price):
                        support_confirmed_break = True

            if len(w3) > 0:
                downs = []
                ups = []
                for j in range(len(w3)):
                    day = w3.loc[j]
                    c = float(day["close"])
                    v = float(day["volume"])
                    v_ma5 = float(day["vol_ma5"]) if float(day["vol_ma5"]) > 0 else v
                    if j == 0:
                        prev_c = ref_close
                    else:
                        prev_c = float(w3.loc[j - 1, "close"])
                    if c < prev_c:
                        downs.append(v / v_ma5 if v_ma5 > 0 else 0.0)
                    elif c > prev_c:
                        ups.append(v / v_ma5 if v_ma5 > 0 else 0.0)
                if (len(downs) > 0 and max(downs) >= 1.3) and (len(ups) > 0 and min(ups) <= 0.9):
                    volume_structure_bad = True

            if gap_down_confirmed:
                script_signals.append("Gap_Down")
            if support_confirmed_break:
                script_signals.append(f"Support_Break_2D({support_price:.2f})")
            if volume_structure_bad:
                script_signals.append("Volume_Structure_Bad")

            rebound_quick = False
            reclaim_support = False
            break_ref_high = False

            if len(w3) > 0:
                max_close_2d = None
                for j in range(min(2, len(w3))):
                    c = float(w3.loc[j, "close"])
                    max_close_2d = c if max_close_2d is None else max(max_close_2d, c)
                if max_close_2d is not None and max_close_2d >= ref_close:
                    rebound_quick = True

            if len(w3) > 0:
                min_low_3d = float(w3["low"].min())
                if min_low_3d >= support_price:
                    reclaim_support = True

            if len(w5) > 0:
                max_close_5d = float(w5["close"].max())
                if max_close_5d >= ref_high:
                    break_ref_high = True

            if rebound_quick:
                manual_signals.append("Quick_Rebound(<=2D)")
            if reclaim_support:
                manual_signals.append(f"Hold_Support(>= {support_price:.2f})")
            if break_ref_high:
                manual_signals.append(f"Close_Above_RefHigh({ref_high:.2f})")

            verdict = "UNDECIDED"
            if len(script_signals) > 0:
                verdict = "SCRIPT_RIGHT"
            elif break_ref_high:
                verdict = "MANUAL_RIGHT"

            print(f"symbol={symbol_raw} code={symbol_code}")
            print(f"ref_date={ref_date} prev_close={prev_close:.2f} prev_low={prev_low:.2f} ref_high={ref_high:.2f} ref_close={ref_close:.2f} ref_low={ref_low:.2f}")
            print(f"support_price={support_price:.2f} eval_days_primary={EVAL_DAYS_PRIMARY} eval_days_secondary={EVAL_DAYS_SECONDARY}")
            print(f"script_signals={script_signals}")
            print(f"manual_signals={manual_signals}")
            print(f"verdict={verdict}")

            if verdict == "SCRIPT_RIGHT":
                triggered = True
                message = f"🧪验证: {verdict} | 炸板={ref_date} | {' + '.join(script_signals)}"
            elif verdict == "MANUAL_RIGHT":
                triggered = False
                message = f"🧪验证: {verdict} | 炸板={ref_date} | {' + '.join(manual_signals)}"
            else:
                triggered = False
                message = f"🧪验证: {verdict} | 炸板={ref_date} | 观察: GapDown/破位({support_price:.2f})/站回前高({ref_high:.2f})"
