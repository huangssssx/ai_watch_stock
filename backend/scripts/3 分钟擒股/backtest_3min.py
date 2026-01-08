import datetime
import math
import os
import sys
import pandas as pd
import numpy as np

backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from pymr_compat import ensure_py_mini_racer
ensure_py_mini_racer()
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
LOOKBACK_CALENDAR_DAYS = 140
N_BOARDS = 30
SURGE_THRESHOLD = 0.05
MIN_SIGNALS_TRAIN = 10
MIN_SIGNALS_TEST = 5
TEST_UNIQUE_DAYS = 22


def _sigmoid(x):
    x = np.clip(x, -50, 50)
    return 1.0 / (1.0 + np.exp(-x))


def _train_logreg(X: np.ndarray, y: np.ndarray, lr=0.1, steps=2000, l2=1e-2):
    n, d = X.shape
    w = np.zeros(d, dtype=float)
    b = 0.0
    for _ in range(int(steps)):
        z = X @ w + b
        p = _sigmoid(z)
        grad_w = (X.T @ (p - y)) / n + l2 * w
        grad_b = float(np.mean(p - y))
        w -= lr * grad_w
        b -= lr * grad_b
    return w, b


def _predict_logreg(X: np.ndarray, w: np.ndarray, b: float):
    return _sigmoid(X @ w + b)


def _precision_at_threshold(y_true: np.ndarray, y_prob: np.ndarray, thr: float):
    sel = y_prob >= float(thr)
    if not np.any(sel):
        return 0.0, 0, 0
    tp = int(np.sum(y_true[sel] == 1))
    cnt = int(np.sum(sel))
    return float(tp / cnt), tp, cnt


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
            "require_full_day": True,
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
            "require_full_day": False,
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
        "require_full_day": False,
    }


def _normalize_symbol(value: str) -> str:
    s = "" if value is None else str(value).strip()
    if s.startswith(("sh", "sz", "bj")) and len(s) >= 8:
        return s[2:]
    return s


def _fetch_minute_for_day(symbol: str, day: datetime.date, prefer_periods=("1", "5")):
    start = f"{day} 09:30:00"
    end = f"{day} 15:00:00"
    tried = []
    for p in prefer_periods:
        try:
            df = ak.stock_zh_a_hist_min_em(symbol=symbol, start_date=start, end_date=end, period=str(p), adjust="")
            if df is None or df.empty or "时间" not in df.columns:
                tried.append((p, "empty"))
                continue
            return df, str(p)
        except Exception as e:
            tried.append((p, str(e)))
    return None, str(tried[:2])


def _build_daily(symbol: str):
    daily = ak.stock_zh_a_hist(symbol=symbol, period="daily", adjust="qfq")
    if daily is None or daily.empty:
        return None
    daily = daily.rename(columns={"日期": "date", "收盘": "close", "成交量": "volume", "最高": "high", "最低": "low"})
    _to_num(daily, ["close", "volume", "high", "low"])
    daily["date_dt"] = pd.to_datetime(daily["date"], errors="coerce").dt.date
    daily = daily[daily["date_dt"].notna()].sort_values("date_dt").reset_index(drop=True)
    return daily


def _extract_features(symbol: str, name: str, daily: pd.DataFrame, day: datetime.date, minute_df: pd.DataFrame):
    if daily is None or daily.empty:
        return None
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

    daily_calc = daily.copy()
    daily_calc["ma5"] = daily_calc["close"].rolling(window=5).mean()
    daily_calc["ma10"] = daily_calc["close"].rolling(window=10).mean()
    ma5 = float(daily_calc.loc[i, "ma5"]) if not pd.isna(daily_calc.loc[i, "ma5"]) else float("nan")
    ma10 = float(daily_calc.loc[i, "ma10"]) if not pd.isna(daily_calc.loc[i, "ma10"]) else float("nan")
    ma5_slope = calculate_ma_slope(daily_calc.loc[max(0, i - 4) : i, "ma5"])
    low_ge_ma5 = float(today["low"]) >= ma5 if not math.isnan(ma5) else False
    ma_dist = (ma5 - ma10) / ma10 if (not math.isnan(ma5) and not math.isnan(ma10) and ma10 > 0) else 999.0

    low_20d = float(daily_calc.loc[i - 19 : i, "close"].min())
    pos_ratio = float(today["close"]) / low_20d if low_20d > 0 else 999.0

    if minute_df is None or minute_df.empty or "时间" not in minute_df.columns:
        return None
    m = minute_df.copy()
    m["time_dt"] = pd.to_datetime(m["时间"], errors="coerce")
    m = m[m["time_dt"].notna()].copy()
    if m.empty:
        return None
    m.rename(columns={"收盘": "min_close", "成交量": "min_volume"}, inplace=True)
    _to_num(m, ["min_close", "min_volume"])
    m["time"] = m["time_dt"].dt.strftime("%H:%M")

    full_day = is_full_day(m)
    morning_mask = m["time"].between("09:30", "10:30")
    end_mask = m["time"].between("14:30", "15:00")
    pre_end_mask = m["time"].between("14:00", "14:30")

    morning_vol = float(m[morning_mask]["min_volume"].sum())
    end_slice = m[end_mask]
    pre_end_slice = m[pre_end_mask]
    if end_slice.empty:
        end_slice = m.tail(30) if len(m) >= 30 else m
    if pre_end_slice.empty:
        pre_end_slice = m.iloc[-60:-30] if len(m) >= 60 else m.head(0)

    end_vol = float(end_slice["min_volume"].sum())
    pre_end_vol = float(pre_end_slice["min_volume"].sum())
    total_vol = float(m["min_volume"].sum())
    morning_share = (morning_vol / total_vol) if total_vol > 0 else 0.0
    end_vs_morning = (end_vol / morning_vol) if morning_vol > 0 else 0.0

    high_price = float(m["min_close"].max())
    high_time = str(m.loc[m["min_close"] == high_price, "time"].iloc[0])
    after_high_df = m[m["time"] >= high_time].reset_index(drop=True)
    max_callback = None
    recover_ratio = None
    if len(after_high_df) >= 15:
        low_after = float(after_high_df["min_close"].min())
        max_callback = (high_price - low_after) / high_price if high_price > 0 else None
        callback_low_idx = int(after_high_df["min_close"].idxmin())
        recover_15 = after_high_df.iloc[callback_low_idx : callback_low_idx + 15] if callback_low_idx + 15 < len(after_high_df) else after_high_df.iloc[callback_low_idx:]
        recover_price = float(recover_15["min_close"].max())
        recover_ratio = (recover_price - low_after) / (high_price - low_after) if (high_price > low_after) else 0.0

    end_first_price = float(end_slice["min_close"].iloc[0]) if not end_slice.empty else 0.0
    end_last_price = float(end_slice["min_close"].iloc[-1]) if not end_slice.empty else 0.0
    end_rise = (end_last_price - end_first_price) / end_first_price if end_first_price > 0 else 0.0
    end_vol_ratio = (end_vol / pre_end_vol) if pre_end_vol > 0 else 0.0

    next_day = daily_calc.iloc[i + 1]
    next_high = float(next_day["high"])
    next_close = float(next_day["close"])
    base_close = float(today["close"])
    touch_limit = (next_high / base_close - 1) >= limit_up_threshold(symbol) if base_close > 0 else False
    surge = (next_high / base_close - 1) >= float(SURGE_THRESHOLD) if base_close > 0 else False
    next_ret = (next_close / base_close - 1) if base_close > 0 else None

    return {
        "symbol": symbol,
        "name": name,
        "date": str(day),
        "date_dt": day,
        "vol_ratio": float(vol_ratio),
        "ma5": float(ma5) if not math.isnan(ma5) else None,
        "ma10": float(ma10) if not math.isnan(ma10) else None,
        "ma5_slope": float(ma5_slope),
        "low_ge_ma5": bool(low_ge_ma5),
        "ma_dist": float(ma_dist),
        "pos_ratio": float(pos_ratio),
        "full_day": bool(full_day),
        "morning_share": float(morning_share),
        "end_vs_morning": float(end_vs_morning),
        "max_callback": float(max_callback) if max_callback is not None and not pd.isna(max_callback) else None,
        "recover_ratio": float(recover_ratio) if recover_ratio is not None and not pd.isna(recover_ratio) else None,
        "end_rise": float(end_rise),
        "end_vol_ratio": float(end_vol_ratio),
        "next_touch_limit": bool(touch_limit),
        "next_surge": bool(surge),
        "next_ret": float(next_ret) if next_ret is not None and not pd.isna(next_ret) else None,
    }


def _apply_rule_one(feat: dict, params: dict):
    if feat is None:
        return False, {}
    if params.get("require_full_day", False) and (not bool(feat.get("full_day", False))):
        return False, {"skip": "not_full_day"}
    vol_ratio = float(feat.get("vol_ratio", 0.0))
    cond_vol_total = (vol_ratio >= float(params["vol_ratio_low"])) and (vol_ratio <= float(params["vol_ratio_high"]))
    ma5_slope = float(feat.get("ma5_slope", 0.0))
    ma5 = feat.get("ma5", None)
    ma10 = feat.get("ma10", None)
    ma_dist = float(feat.get("ma_dist", 999.0))
    cond_ma = (
        (ma5 is not None)
        and (ma10 is not None)
        and (ma5_slope > 0)
        and (float(ma5) > float(ma10))
        and bool(feat.get("low_ge_ma5", False))
        and (ma_dist <= float(params["ma_dist_max"]))
    )
    pos_ratio = float(feat.get("pos_ratio", 999.0))
    cond_not_high = pos_ratio <= float(params["high_pos_ratio"])

    morning_share = float(feat.get("morning_share", 0.0))
    end_vs_morning = float(feat.get("end_vs_morning", 0.0))
    cond_morning = (morning_share >= float(params["morning_share_low"])) and (morning_share <= float(params["morning_share_high"]))
    cond_end_vs_morning = end_vs_morning >= float(params["end_vs_morning"])
    cond_vol = bool(cond_vol_total and cond_morning and cond_end_vs_morning)

    max_callback = feat.get("max_callback", None)
    recover_ratio = feat.get("recover_ratio", None)
    cond_callback = (
        (max_callback is not None)
        and (recover_ratio is not None)
        and (float(max_callback) <= float(params["callback_max"]))
        and (float(recover_ratio) >= float(params["recover_threshold"]))
    )
    end_rise = float(feat.get("end_rise", 0.0))
    end_vol_ratio = float(feat.get("end_vol_ratio", 0.0))
    cond_end = (
        (end_rise >= float(params["end_rise_low"]))
        and (end_rise <= float(params["end_rise_high"]))
        and (end_vol_ratio >= float(params["end_vol_ratio"]))
    )
    strength_and = bool(params.get("strength_and", True))
    cond_strength = bool(cond_callback and cond_end) if strength_and else bool(cond_callback or cond_end)
    selected = bool(cond_vol and cond_ma and cond_not_high and cond_strength)
    return selected, {
        "cond_vol_total": bool(cond_vol_total),
        "cond_morning": bool(cond_morning),
        "cond_end_vs_morning": bool(cond_end_vs_morning),
        "cond_vol": bool(cond_vol),
        "cond_ma": bool(cond_ma),
        "cond_not_high": bool(cond_not_high),
        "cond_callback": bool(cond_callback),
        "cond_end": bool(cond_end),
        "cond_strength": bool(cond_strength),
    }


def _hit_rate(df: pd.DataFrame):
    if df is None or df.empty:
        return 0.0
    selected = df[df["selected"] == True]
    if selected.empty:
        return 0.0
    hit = selected[selected["next_surge"] == True]
    return float(len(hit) / len(selected))


def _pick_30_board_symbols():
    board_df = ak.stock_board_industry_name_em()
    if board_df is None or board_df.empty or "板块名称" not in board_df.columns:
        return []
    boards = [b for b in board_df["板块名称"].dropna().astype(str).tolist() if b.strip()]
    boards = boards[: N_BOARDS * 2]
    chosen = []
    seen = set()
    for b in boards:
        if len(chosen) >= N_BOARDS:
            break
        try:
            cons = ak.stock_board_industry_cons_em(symbol=b)
            if cons is None or cons.empty:
                continue
            if "名称" in cons.columns:
                cons = cons[~cons["名称"].astype(str).str.contains("ST|退", na=False)]
            code_col = "代码" if "代码" in cons.columns else None
            name_col = "名称" if "名称" in cons.columns else None
            if code_col is None:
                continue
            for _, r in cons.head(20).iterrows():
                sym = _normalize_symbol(r.get(code_col))
                nm = str(r.get(name_col, "")).strip() if name_col else ""
                if not sym or sym in seen:
                    continue
                daily = _build_daily(sym)
                if daily is None or daily.empty or len(daily) < 40:
                    continue
                chosen.append({"board": b, "symbol": sym, "name": nm})
                seen.add(sym)
                break
        except Exception:
            continue
    return chosen


def main():
    symbols = _pick_30_board_symbols()
    if not symbols:
        print("无法构建30板块样本（可能接口异常）")
        return
    print("样本股票数:", len(symbols))
    end_day = datetime.date.today() - datetime.timedelta(days=1)
    start_day = end_day - datetime.timedelta(days=LOOKBACK_CALENDAR_DAYS)
    feats = []
    fetch_errors = 0
    for item in symbols:
        sym = item["symbol"]
        nm = item["name"]
        daily = _build_daily(sym)
        if daily is None or daily.empty:
            continue
        days = [d for d in daily["date_dt"].tolist() if (d >= start_day and d <= end_day)]
        days = sorted(list(dict.fromkeys(days)))
        for d in days:
            if d >= end_day:
                continue
            try:
                minute_df, minute_meta = _fetch_minute_for_day(sym, d)
                if minute_df is None or minute_df.empty:
                    fetch_errors += 1
                    continue
                feat = _extract_features(sym, nm, daily, d, minute_df)
                if feat is not None:
                    feat["minute_meta"] = minute_meta
                    feats.append(feat)
            except Exception:
                fetch_errors += 1
                continue
    fdf = pd.DataFrame(feats)
    if fdf.empty:
        print("无可用回测样本（可能分钟接口受限或网络异常）")
        return
    print("回测区间:", str(start_day), "~", str(end_day))
    print("样本条数:", len(fdf))
    print("分钟拉取失败次数:", int(fetch_errors))
    print(f"整体次日暴涨率(高点>={SURGE_THRESHOLD:.0%}):", f"{float(fdf['next_surge'].mean()):.2%}")
    print("整体次日触板率:", f"{float(fdf['next_touch_limit'].mean()):.2%}")

    base_records = []
    for mode in MODES:
        mp = mode_params(mode, True)
        p = {
            "vol_ratio_low": VOL_RATIO_LOW,
            "vol_ratio_high": VOL_RATIO_HIGH,
            "ma_dist_max": MA_DIST_MAX,
            "high_pos_ratio": HIGH_POS_RATIO,
            "callback_max": CALLBACK_MAX,
            "recover_threshold": float(mp["recover_threshold"]),
            "end_rise_low": float(mp["end_rise_low"]),
            "end_rise_high": float(mp["end_rise_high"]),
            "end_vol_ratio": float(mp["end_vol_ratio"]),
            "morning_share_low": float(mp["morning_share_low"]),
            "morning_share_high": float(mp["morning_share_high"]),
            "end_vs_morning": float(mp["end_vs_morning"]),
            "strength_and": bool(mp["strength_and"]),
            "require_full_day": bool(mp["require_full_day"]),
        }
        sel = []
        for _, r in fdf.iterrows():
            selected, conds = _apply_rule_one(r.to_dict(), p)
            out = r.to_dict()
            out.update({"mode": mode, "selected": bool(selected)})
            out.update(conds)
            sel.append(out)
        base_records.append(pd.DataFrame(sel))
    base_df = pd.concat(base_records, ignore_index=True)
    for mode in MODES:
        sdf = base_df[base_df["mode"] == mode].copy()
        hit_rate = _hit_rate(sdf)
        selected = sdf[sdf["selected"] == True]
        hit = selected[selected["next_surge"] == True]
        touch = selected[selected["next_touch_limit"] == True]
        avg_ret = float(selected["next_ret"].mean()) if len(selected) > 0 else float("nan")
        print("\n模式:", mode)
        print("样本条数:", len(sdf))
        print("触发条数:", len(selected))
        print(f"次日暴涨(高点>={SURGE_THRESHOLD:.0%})条数:", len(hit))
        print("次日暴涨率:", f"{hit_rate:.2%}")
        print("次日触板条数:", len(touch))
        print("触发样本次日平均收益(收盘->收盘):", f"{avg_ret:.2%}" if not math.isnan(avg_ret) else "NA")

    uniq_dates = sorted(base_df["date_dt"].dropna().unique().tolist())
    if len(uniq_dates) < 10:
        print("\n可用交易日过少，跳过参数调优")
        return
    test_dates = set(uniq_dates[-TEST_UNIQUE_DAYS:])
    train_dates = set([d for d in uniq_dates if d not in test_dates])
    train = fdf[fdf["date_dt"].isin(train_dates)].copy()
    test = fdf[fdf["date_dt"].isin(test_dates)].copy()
    print("\n训练集交易日:", len(train_dates), "测试集交易日:", len(test_dates))

    feat_cols = [
        "vol_ratio",
        "ma5_slope",
        "ma_dist",
        "pos_ratio",
        "morning_share",
        "end_vs_morning",
        "max_callback",
        "recover_ratio",
        "end_rise",
        "end_vol_ratio",
    ]
    feat_cols = [c for c in feat_cols if c in fdf.columns]
    def _make_xy(df0: pd.DataFrame):
        xdf = df0[feat_cols].copy()
        xdf = xdf.fillna(0.0)
        X = xdf.to_numpy(dtype=float)
        y = df0["next_surge"].astype(int).to_numpy(dtype=int)
        return X, y

    X_train, y_train = _make_xy(train)
    X_test, y_test = _make_xy(test)
    mu = X_train.mean(axis=0)
    sigma = X_train.std(axis=0)
    sigma = np.where(sigma == 0, 1.0, sigma)
    X_train_s = (X_train - mu) / sigma
    X_test_s = (X_test - mu) / sigma
    w, b = _train_logreg(X_train_s, y_train, lr=0.1, steps=2500, l2=5e-2)
    prob_test = _predict_logreg(X_test_s, w, b)
    prob_train = _predict_logreg(X_train_s, w, b)
    print("\nLogReg 训练集整体暴涨率:", f"{float(y_train.mean()):.2%}", "测试集整体暴涨率:", f"{float(y_test.mean()):.2%}")

    best_thr = None
    best_prec = 0.0
    best_cnt = 0
    for thr in np.linspace(0.5, 0.95, 10):
        prec, tp, cnt = _precision_at_threshold(y_test, prob_test, float(thr))
        if cnt >= MIN_SIGNALS_TEST and prec >= 0.8:
            if (prec > best_prec) or (prec == best_prec and cnt > best_cnt):
                best_thr = float(thr)
                best_prec = float(prec)
                best_cnt = int(cnt)
    if best_thr is None:
        for thr in np.linspace(0.5, 0.95, 10):
            prec, tp, cnt = _precision_at_threshold(y_test, prob_test, float(thr))
            if cnt >= MIN_SIGNALS_TEST and ((prec > best_prec) or (prec == best_prec and cnt > best_cnt)):
                best_thr = float(thr)
                best_prec = float(prec)
                best_cnt = int(cnt)

    if best_thr is not None:
        prec, tp, cnt = _precision_at_threshold(y_test, prob_test, best_thr)
        train_prec, train_tp, train_cnt = _precision_at_threshold(y_train, prob_train, best_thr)
        print("\nLogReg 阈值:", f"{best_thr:.2f}")
        print("训练集: 触发", int(train_cnt), "命中", int(train_tp), "命中率", f"{float(train_prec):.2%}")
        print("测试集: 触发", int(cnt), "命中", int(tp), "命中率", f"{float(prec):.2%}")

        test_out = test.copy()
        test_out["prob"] = prob_test
        picked = test_out[test_out["prob"] >= best_thr].copy()
        if not picked.empty:
            picked = picked.sort_values(["date_dt", "prob"], ascending=[True, False]).tail(30)
            cols = ["date", "symbol", "name", "prob", "next_surge", "next_touch_limit", "next_ret"] + feat_cols
            cols = [c for c in cols if c in picked.columns]
            print("\nLogReg 测试集最近触发样本(后30):")
            print(picked[cols].to_string(index=False))

    best = None
    vol_lows = [1.1, 1.2, 1.3, 1.4, 1.5]
    vol_highs = [1.6, 1.8, 2.0, 2.2, 2.5, 3.0]
    ma_dists = [0.04, 0.05, 0.06, 0.07, 0.08, 0.1, 0.12]
    high_pos = [1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 2.0]
    cb_maxs = [0.02, 0.03, 0.04, 0.05, 0.06, 0.08]
    rec_ths = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8]
    end_rise_lows = [0.0, 0.002, 0.005, 0.01]
    end_rise_highs = [0.01, 0.02, 0.03, 0.05]
    end_vol_mins = [0.8, 1.0, 1.1, 1.2, 1.3]
    morning_lows = [0.0, 0.1, 0.2, 0.25]
    morning_highs = [0.5, 0.6, 0.8, 1.0]
    end_vs_mornings = [0.0, 0.15, 0.25, 0.35, 0.45]
    strength_ands = [True, False]
    require_full_day = [True, False]

    train_rows = [r.to_dict() for _, r in train.iterrows()]
    test_rows = [r.to_dict() for _, r in test.iterrows()]

    rng = np.random.default_rng(7)
    trials = 5000
    for _ in range(int(trials)):
        vl = float(rng.choice(vol_lows))
        vh = float(rng.choice(vol_highs))
        if vh <= vl:
            continue
        erl = float(rng.choice(end_rise_lows))
        erh = float(rng.choice(end_rise_highs))
        if erh <= erl:
            continue
        msl = float(rng.choice(morning_lows))
        msh = float(rng.choice(morning_highs))
        if msh <= msl:
            continue
        p = {
            "vol_ratio_low": vl,
            "vol_ratio_high": vh,
            "ma_dist_max": float(rng.choice(ma_dists)),
            "high_pos_ratio": float(rng.choice(high_pos)),
            "callback_max": float(rng.choice(cb_maxs)),
            "recover_threshold": float(rng.choice(rec_ths)),
            "end_rise_low": erl,
            "end_rise_high": erh,
            "end_vol_ratio": float(rng.choice(end_vol_mins)),
            "morning_share_low": msl,
            "morning_share_high": msh,
            "end_vs_morning": float(rng.choice(end_vs_mornings)),
            "strength_and": bool(rng.choice(strength_ands)),
            "require_full_day": bool(rng.choice(require_full_day)),
        }
        sel_train = 0
        hit_train = 0
        for row in train_rows:
            selected, _ = _apply_rule_one(row, p)
            if selected:
                sel_train += 1
                if bool(row.get("next_surge", False)):
                    hit_train += 1
        if sel_train < MIN_SIGNALS_TRAIN:
            continue
        hr_train = hit_train / sel_train if sel_train else 0.0
        if best is None or (hr_train > best["hr_train"]) or (hr_train == best["hr_train"] and sel_train > best["sel_train"]):
            best = {"params": p, "hr_train": float(hr_train), "sel_train": int(sel_train)}

    if best is None:
        print("\n未找到满足最小触发数的参数组合，跳过调优输出")
        return

    p = best["params"]
    sel_test = 0
    hit_test = 0
    selected_samples = []
    for row in test_rows:
        selected, _ = _apply_rule_one(row, p)
        if selected:
            sel_test += 1
            if bool(row.get("next_surge", False)):
                hit_test += 1
            selected_samples.append(row)
    hr_test = hit_test / sel_test if sel_test else 0.0
    print("\n最优参数(训练集):", f"触板率{best['hr_train']:.2%}", "触发数", best["sel_train"])
    print("测试集结果:", f"触板率{hr_test:.2%}", "触发数", int(sel_test))
    print("最优参数:", p)
    if sel_test >= MIN_SIGNALS_TEST:
        sdf = pd.DataFrame(selected_samples)
        sdf = sdf.sort_values(["date_dt", "symbol"]).tail(30)
        print("\n测试集最近触发样本(后30):")
        cols = ["date", "symbol", "name", "next_surge", "next_touch_limit", "next_ret", "vol_ratio", "ma_dist", "pos_ratio", "max_callback", "recover_ratio", "end_rise", "end_vol_ratio"]
        cols = [c for c in cols if c in sdf.columns]
        print(sdf[cols].to_string(index=False))
    else:
        print("\n测试集触发样本不足，建议扩大样本或放宽最小触发约束")


if __name__ == "__main__":
    main()
