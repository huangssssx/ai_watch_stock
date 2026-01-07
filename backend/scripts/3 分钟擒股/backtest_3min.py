import datetime
import math
import pandas as pd
import numpy as np
import akshare as ak

MODES = ["strict", "adaptive", "adaptive_loose"]
TOP_N = 120
MIN_AMOUNT = 50_000_000
EXCLUDE_ST = True
MAX_DATES_PER_SYMBOL = 4

VOL_RATIO_LOW = 1.3
VOL_RATIO_HIGH = 2.0
MA_DIST_MAX = 0.08
HIGH_POS_RATIO = 1.8
CALLBACK_MAX = 0.03
RECOVER_RATIO = 0.8
END_RISE_LOW = 0.005
END_RISE_HIGH = 0.01
END_VOL_RATIO = 1.2


def _to_num(df: pd.DataFrame, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def calculate_ma_slope(ma_series: pd.Series) -> float:
    s = ma_series.dropna()
    if len(s) < 5:
        return 0.0
    x = np.arange(len(s))
    return float(np.polyfit(x, s.values, 1)[0])


def is_full_day(minute_df: pd.DataFrame) -> bool:
    if minute_df is None or minute_df.empty:
        return False
    if "time" not in minute_df.columns:
        return False
    tmax = str(minute_df["time"].max())
    return (tmax >= "14:55") and (len(minute_df) >= 235)


def limit_up_threshold(symbol: str) -> float:
    if symbol.startswith(("300", "301", "688")):
        return 0.195
    if symbol.startswith(("8", "4", "92")):
        return 0.295
    return 0.095


def mode_params(mode: str, full_day: bool):
    if mode == "strict":
        return {
            "recover_threshold": RECOVER_RATIO,
            "end_rise_low": END_RISE_LOW,
            "end_rise_high": END_RISE_HIGH,
            "end_vol_ratio": END_VOL_RATIO,
            "morning_share_low": 0.25,
            "morning_share_high": 0.45,
            "end_vs_morning": 0.4,
            "strength_and": True,
        }
    if mode == "adaptive":
        return {
            "recover_threshold": 0.5 if full_day else 0.3,
            "end_rise_low": 0.002,
            "end_rise_high": 0.02,
            "end_vol_ratio": 1.1,
            "morning_share_low": 0.2,
            "morning_share_high": 0.5,
            "end_vs_morning": 0.3,
            "strength_and": False,
        }
    return {
        "recover_threshold": 0.3,
        "end_rise_low": 0.002,
        "end_rise_high": 0.02,
        "end_vol_ratio": 1.1,
        "morning_share_low": 0.15,
        "morning_share_high": 0.6,
        "end_vs_morning": 0.25,
        "strength_and": False,
    }


def eval_one_day(
    symbol: str,
    name: str,
    daily: pd.DataFrame,
    minute_all: pd.DataFrame,
    day: datetime.date,
    mode: str,
):
    if daily is None or daily.empty:
        return None

    daily = daily.copy()
    daily["date_dt"] = pd.to_datetime(daily["date"], errors="coerce").dt.date
    daily = daily[daily["date_dt"].notna()].sort_values("date_dt").reset_index(drop=True)

    idx_list = daily.index[daily["date_dt"] == day].tolist()
    if not idx_list:
        return None
    i = int(idx_list[0])
    if i < 21 or i + 1 >= len(daily):
        return None

    today = daily.iloc[i]
    prev_5d = daily.iloc[i - 5 : i]

    prev_5d_vol_avg = float(prev_5d["volume"].mean()) if "volume" in prev_5d.columns else 0.0
    if not prev_5d_vol_avg or math.isnan(prev_5d_vol_avg):
        return None
    vol_ratio = float(today["volume"]) / prev_5d_vol_avg if float(today["volume"]) > 0 else 0.0
    cond_vol_total = (vol_ratio >= VOL_RATIO_LOW) and (vol_ratio <= VOL_RATIO_HIGH)

    daily["ma5"] = daily["close"].rolling(window=5).mean()
    daily["ma10"] = daily["close"].rolling(window=10).mean()
    ma5 = float(daily.loc[i, "ma5"]) if not pd.isna(daily.loc[i, "ma5"]) else float("nan")
    ma10 = float(daily.loc[i, "ma10"]) if not pd.isna(daily.loc[i, "ma10"]) else float("nan")
    ma5_slope = calculate_ma_slope(daily.loc[max(0, i - 4) : i, "ma5"])
    cond_price_on_ma5 = float(today["low"]) >= ma5 if not math.isnan(ma5) else False
    ma_dist = (ma5 - ma10) / ma10 if (not math.isnan(ma5) and not math.isnan(ma10) and ma10 > 0) else 999.0
    cond_ma = (ma5_slope > 0) and (ma5 > ma10) and cond_price_on_ma5 and (ma_dist <= MA_DIST_MAX)

    low_20d = float(daily.loc[i - 19 : i, "close"].min())
    pos_ratio = float(today["close"]) / low_20d if low_20d > 0 else 999.0
    cond_not_high = pos_ratio <= HIGH_POS_RATIO

    minute_all = minute_all.copy()
    minute_all["time_dt"] = pd.to_datetime(minute_all["时间"], errors="coerce")
    minute_all = minute_all[minute_all["time_dt"].notna()].copy()
    minute_all["date_dt"] = minute_all["time_dt"].dt.date
    minute_df = minute_all[minute_all["date_dt"] == day].copy()
    if minute_df.empty:
        return None
    minute_df.rename(columns={"收盘": "min_close", "成交量": "min_volume"}, inplace=True)
    _to_num(minute_df, ["min_close", "min_volume"])
    minute_df["time"] = minute_df["time_dt"].dt.strftime("%H:%M")

    full_day = is_full_day(minute_df)
    if mode == "strict" and not full_day:
        return None
    mp = mode_params(mode, full_day)

    morning_mask = minute_df["time"].between("09:30", "10:30")
    end_mask = minute_df["time"].between("14:30", "15:00")
    pre_end_mask = minute_df["time"].between("14:00", "14:30")

    morning_vol = float(minute_df[morning_mask]["min_volume"].sum())
    end_slice = minute_df[end_mask]
    pre_end_slice = minute_df[pre_end_mask]
    if end_slice.empty:
        end_slice = minute_df.tail(30) if len(minute_df) >= 30 else minute_df
    if pre_end_slice.empty:
        pre_end_slice = minute_df.iloc[-60:-30] if len(minute_df) >= 60 else minute_df.head(0)

    end_vol = float(end_slice["min_volume"].sum())
    pre_end_vol = float(pre_end_slice["min_volume"].sum())
    total_vol = float(minute_df["min_volume"].sum())

    cond_morning_vol = True
    cond_end_vol_ratio = True
    if full_day:
        share = (morning_vol / total_vol) if total_vol > 0 else 0.0
        cond_morning_vol = (share >= float(mp["morning_share_low"])) and (share <= float(mp["morning_share_high"])) if total_vol > 0 else False
        cond_end_vol_ratio = (end_vol >= morning_vol * float(mp["end_vs_morning"])) if morning_vol > 0 else False
    elif mode == "strict":
        cond_morning_vol = False
        cond_end_vol_ratio = False

    cond_vol = cond_vol_total and cond_morning_vol and cond_end_vol_ratio

    high_price = float(minute_df["min_close"].max())
    high_time = str(minute_df.loc[minute_df["min_close"] == high_price, "time"].iloc[0])
    after_high_df = minute_df[minute_df["time"] >= high_time].reset_index(drop=True)
    max_callback = None
    recover_ratio = None
    if len(after_high_df) < 15:
        cond_callback = False
    else:
        low_after = float(after_high_df["min_close"].min())
        max_callback = (high_price - low_after) / high_price if high_price > 0 else None
        callback_low_idx = int(after_high_df["min_close"].idxmin())
        recover_15 = after_high_df.iloc[callback_low_idx : callback_low_idx + 15] if callback_low_idx + 15 < len(after_high_df) else after_high_df.iloc[callback_low_idx:]
        recover_price = float(recover_15["min_close"].max())
        recover_ratio = (recover_price - low_after) / (high_price - low_after) if (high_price > low_after) else 0.0
        cond_callback = (max_callback is not None) and (max_callback <= CALLBACK_MAX) and (recover_ratio >= float(mp["recover_threshold"]))

    end_first_price = float(end_slice["min_close"].iloc[0]) if not end_slice.empty else 0.0
    end_last_price = float(end_slice["min_close"].iloc[-1]) if not end_slice.empty else 0.0
    end_rise = (end_last_price - end_first_price) / end_first_price if end_first_price > 0 else 0.0
    end_vol_ratio = (end_vol / pre_end_vol) if pre_end_vol > 0 else 0.0
    cond_end = (end_rise >= float(mp["end_rise_low"])) and (end_rise <= float(mp["end_rise_high"])) and (end_vol_ratio >= float(mp["end_vol_ratio"]))

    cond_strength = (cond_callback and cond_end) if bool(mp["strength_and"]) else (cond_callback or cond_end)

    selected = bool(cond_vol and cond_ma and cond_not_high and cond_strength)

    next_day = daily.iloc[i + 1]
    next_high = float(next_day["high"])
    next_close = float(next_day["close"])
    base_close = float(today["close"])
    touch_limit = (next_high / base_close - 1) >= limit_up_threshold(symbol) if base_close > 0 else False
    next_ret = (next_close / base_close - 1) if base_close > 0 else None

    return {
        "symbol": symbol,
        "name": name,
        "date": str(day),
        "mode": mode,
        "selected": selected,
        "full_day": full_day,
        "cond_vol": bool(cond_vol),
        "cond_ma": bool(cond_ma),
        "cond_not_high": bool(cond_not_high),
        "cond_callback": bool(cond_callback),
        "cond_end": bool(cond_end),
        "cond_strength": bool(cond_strength),
        "vol_ratio": round(vol_ratio, 4),
        "ma_dist": round(ma_dist, 4) if ma_dist is not None else None,
        "pos_ratio": round(pos_ratio, 4),
        "max_callback": round(float(max_callback), 4) if isinstance(max_callback, (int, float, np.floating)) and not pd.isna(max_callback) else None,
        "recover_ratio": round(float(recover_ratio), 4) if isinstance(recover_ratio, (int, float, np.floating)) and not pd.isna(recover_ratio) else None,
        "end_rise": round(end_rise, 4),
        "end_vol_ratio": round(end_vol_ratio, 4),
        "next_touch_limit": bool(touch_limit),
        "next_ret": round(float(next_ret), 4) if next_ret is not None and not pd.isna(next_ret) else None,
    }


def main():
    print("拉取当日快照，构建回测股票池...")
    spot = ak.stock_zh_a_spot_em()
    spot = spot.rename(columns={"代码": "symbol", "名称": "name", "成交额": "amount", "涨跌幅": "pct_chg"})
    _to_num(spot, ["amount", "pct_chg"])
    if EXCLUDE_ST:
        spot = spot[~spot["name"].str.contains("ST|退", na=False)]
    spot = spot[spot["amount"] >= MIN_AMOUNT]
    spot = spot.sort_values("amount", ascending=False).head(TOP_N)

    records = []
    for _, r in spot.iterrows():
        symbol = str(r["symbol"])
        name = str(r["name"])
        try:
            daily = ak.stock_zh_a_hist(symbol=symbol, period="daily", adjust="qfq")
            if daily is None or daily.empty:
                continue
            daily = daily.rename(columns={"日期": "date", "收盘": "close", "成交量": "volume", "最高": "high", "最低": "low"})
            _to_num(daily, ["close", "volume", "high", "low"])

            minute_all = ak.stock_zh_a_hist_min_em(symbol=symbol, period="1", adjust="")
            if minute_all is None or minute_all.empty or "时间" not in minute_all.columns:
                continue
            minute_all["time_dt"] = pd.to_datetime(minute_all["时间"], errors="coerce")
            minute_all = minute_all[minute_all["time_dt"].notna()].copy()
            minute_all["date_dt"] = minute_all["time_dt"].dt.date
            dates = sorted(minute_all["date_dt"].unique())
            if not dates:
                continue
            today = datetime.date.today()
            dates = [d for d in dates if d < today]
            dates = dates[-MAX_DATES_PER_SYMBOL:]
            for d in dates:
                for mode in MODES:
                    rec = eval_one_day(symbol, name, daily, minute_all, d, mode)
                    if rec is not None:
                        records.append(rec)
        except Exception:
            continue

    df = pd.DataFrame(records)
    if df.empty:
        print("无可用回测样本（可能分钟数据不足或接口异常）")
        return

    print("回测区间(分钟数据可用的最近交易日):", df["date"].min(), "~", df["date"].max())
    print("样本条数:", len(df))
    for mode in MODES:
        sdf = df[df["mode"] == mode]
        selected = sdf[sdf["selected"] == True]
        hit = selected[selected["next_touch_limit"] == True]
        hit_rate = (len(hit) / len(selected)) if len(selected) > 0 else 0.0
        avg_ret = float(selected["next_ret"].mean()) if len(selected) > 0 else float("nan")
        print("\n模式:", mode)
        print("样本条数:", len(sdf))
        print("触发条数:", len(selected))
        print("次日触板条数:", len(hit))
        print("次日触板率:", f"{hit_rate:.2%}")
        print("触发样本次日平均收益(收盘->收盘):", f"{avg_ret:.2%}" if not math.isnan(avg_ret) else "NA")
        for k in ["cond_vol", "cond_ma", "cond_not_high", "cond_callback", "cond_end", "cond_strength"]:
            if k in sdf.columns:
                print(k, int((sdf[k] == True).sum()))
        if not selected.empty:
            print("\n最近触发样本(前20):")
            print(selected.sort_values(["date", "symbol"]).tail(20).to_string(index=False))


if __name__ == "__main__":
    main()
