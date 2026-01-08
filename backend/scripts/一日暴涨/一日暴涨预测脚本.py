import argparse
import datetime
import json
import os
import sys
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from pymr_compat import ensure_py_mini_racer

ensure_py_mini_racer()

import akshare as ak


def _sigmoid(x: np.ndarray) -> np.ndarray:
    x = np.clip(x, -50, 50)
    return 1.0 / (1.0 + np.exp(-x))


def _train_logreg(
    X: np.ndarray,
    y: np.ndarray,
    lr: float = 0.1,
    steps: int = 2500,
    l2: float = 5e-2,
    sample_weight: Optional[np.ndarray] = None,
) -> Tuple[np.ndarray, float]:
    n, d = X.shape
    w = np.zeros(d, dtype=float)
    b = 0.0
    sw = None
    if sample_weight is not None:
        sw = np.asarray(sample_weight, dtype=float).reshape(-1)
        if len(sw) != n:
            sw = None
    denom = float(np.sum(sw)) if sw is not None else float(n)
    if denom <= 0:
        denom = float(n)
    for _ in range(int(steps)):
        z = X @ w + b
        p = _sigmoid(z)
        diff = (p - y).astype(float)
        if sw is not None:
            diff = diff * sw
        grad_w = (X.T @ diff) / denom + l2 * w
        grad_b = float(np.sum(diff) / denom)
        w -= lr * grad_w
        b -= lr * grad_b
    return w, b


def _predict_logreg(X: np.ndarray, w: np.ndarray, b: float) -> np.ndarray:
    return _sigmoid(X @ w + b)


def _precision_at_threshold(y_true: np.ndarray, y_prob: np.ndarray, thr: float) -> Tuple[float, int, int]:
    sel = y_prob >= float(thr)
    if not np.any(sel):
        return 0.0, 0, 0
    tp = int(np.sum(y_true[sel] == 1))
    cnt = int(np.sum(sel))
    return float(tp / cnt), tp, cnt


def _to_num(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _normalize_symbol(value: str) -> str:
    s = "" if value is None else str(value).strip()
    if s.startswith(("sh", "sz", "bj")) and len(s) >= 8:
        return s[2:]
    return s


def _fmt_yyyymmdd(d: datetime.date) -> str:
    return d.strftime("%Y%m%d")


def limit_up_threshold(symbol: str) -> float:
    if symbol.startswith(("300", "301", "688")):
        return 0.195
    if symbol.startswith(("8", "4", "92")):
        return 0.295
    return 0.095


def _safe_trade_day(date0: datetime.date) -> datetime.date:
    try:
        trade_dates_df = ak.tool_trade_date_hist_sina()
        if trade_dates_df is None or trade_dates_df.empty or "trade_date" not in trade_dates_df.columns:
            return date0
        trade_dates = set(trade_dates_df["trade_date"].astype(str).tolist())
        d = date0
        for _ in range(10):
            if d.strftime("%Y-%m-%d") in trade_dates:
                return d
            d = d - datetime.timedelta(days=1)
        return date0
    except Exception:
        return date0


def _fetch_snapshot() -> pd.DataFrame:
    df = ak.stock_zh_a_spot_em()
    if df is None:
        return pd.DataFrame()
    df = df.copy()
    if "名称" in df.columns:
        df = df[~df["名称"].astype(str).str.contains("ST|退", na=False)]
    df.rename(
        columns={
            "代码": "symbol",
            "名称": "name",
            "成交额": "amount",
            "换手率": "turnover",
            "涨跌幅": "pct_chg",
            "最新价": "last",
        },
        inplace=True,
    )
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).map(_normalize_symbol)
    _to_num(df, ["amount", "turnover", "pct_chg", "last"])
    df = df[df.get("symbol").astype(str).str.len() > 0]
    return df


def _pick_universe(snapshot_df: pd.DataFrame, top_n: int, min_amount: float) -> List[dict]:
    if snapshot_df is None or snapshot_df.empty:
        return []
    df = snapshot_df.copy()
    if "amount" in df.columns and df["amount"].notna().any() and float(df["amount"].fillna(0.0).sum()) > 10_000_000_000:
        df = df[df["amount"].fillna(0.0) >= float(min_amount)]
        if "turnover" in df.columns and df["turnover"].notna().any():
            df = df.sort_values(["amount", "turnover"], ascending=[False, False])
        else:
            df = df.sort_values(["amount"], ascending=[False])
    else:
        if "pct_chg" in df.columns and df["pct_chg"].notna().any():
            df = df.sort_values(["pct_chg"], ascending=[False])
        elif "turnover" in df.columns and df["turnover"].notna().any():
            df = df.sort_values(["turnover"], ascending=[False])
    df = df.head(int(top_n))
    out = []
    for _, r in df.iterrows():
        sym = str(r.get("symbol", "")).strip()
        if not sym:
            continue
        out.append({"symbol": sym, "name": str(r.get("name", "")).strip()})
    return out


def _fetch_daily(
    symbol: str,
    start_date: Optional[datetime.date] = None,
    end_date: Optional[datetime.date] = None,
) -> Optional[pd.DataFrame]:
    try:
        if start_date is not None and end_date is not None:
            daily = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=_fmt_yyyymmdd(start_date),
                end_date=_fmt_yyyymmdd(end_date),
                adjust="qfq",
            )
        else:
            daily = ak.stock_zh_a_hist(symbol=symbol, period="daily", adjust="qfq")
    except Exception:
        return None
    if daily is None or daily.empty:
        return None
    daily = daily.rename(
        columns={
            "日期": "date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "成交额": "amount",
        }
    )
    _to_num(daily, ["open", "close", "high", "low", "volume", "amount"])
    daily["date_dt"] = pd.to_datetime(daily["date"], errors="coerce").dt.date
    daily = daily[daily["date_dt"].notna()].sort_values("date_dt").reset_index(drop=True)
    return daily


def _calc_ma_slope(series: pd.Series) -> float:
    s = series.dropna()
    if len(s) < 5:
        return 0.0
    x = np.arange(len(s), dtype=float)
    return float(np.polyfit(x, s.to_numpy(dtype=float), 1)[0])


def _build_feature_frame(symbol: str, name: str, daily: pd.DataFrame) -> Optional[pd.DataFrame]:
    if daily is None or daily.empty or len(daily) < 80:
        return None
    df = daily.copy()
    df["symbol"] = symbol
    df["name"] = name

    df["prev_close"] = df["close"].shift(1)
    df["ret_1"] = (df["close"] / df["prev_close"]) - 1.0
    df["gap"] = (df["open"] / df["prev_close"]) - 1.0
    df["range"] = (df["high"] - df["low"]) / df["prev_close"]
    df["high_ret"] = (df["high"] / df["prev_close"]) - 1.0
    df["pullback"] = (df["high"] - df["close"]) / df["prev_close"]

    df["body"] = (df["close"] - df["open"]).abs() / df["prev_close"]
    df["upper_shadow"] = (df["high"] - df[["open", "close"]].max(axis=1)) / df["prev_close"]
    df["lower_shadow"] = (df[["open", "close"]].min(axis=1) - df["low"]) / df["prev_close"]
    df["close_pos"] = (df["close"] - df["low"]) / ((df["high"] - df["low"]) + 1e-6)

    df["ret_5"] = (df["close"] / df["close"].shift(5)) - 1.0
    df["ret_20"] = (df["close"] / df["close"].shift(20)) - 1.0
    df["ret_3"] = (df["close"] / df["close"].shift(3)) - 1.0

    df["vol_avg_5"] = df["volume"].shift(1).rolling(window=5).mean()
    df["vol_ratio_5"] = df["volume"] / df["vol_avg_5"]
    df["amt_avg_5"] = df["amount"].shift(1).rolling(window=5).mean() if "amount" in df.columns else np.nan
    df["amt_ratio_5"] = df["amount"] / df["amt_avg_5"] if "amount" in df.columns else np.nan

    df["ma5"] = df["close"].rolling(window=5).mean()
    df["ma10"] = df["close"].rolling(window=10).mean()
    df["ma20"] = df["close"].rolling(window=20).mean()
    df["ma60"] = df["close"].rolling(window=60).mean()
    df["ma5_gt_ma10"] = (df["ma5"] > df["ma10"]).astype(int)
    df["ma10_gt_ma20"] = (df["ma10"] > df["ma20"]).astype(int)
    df["dist_ma20"] = (df["close"] / df["ma20"]) - 1.0
    df["dist_ma60"] = (df["close"] / df["ma60"]) - 1.0

    high_20 = df["high"].shift(1).rolling(window=20).max()
    low_20 = df["low"].shift(1).rolling(window=20).min()
    df["dist_high_20"] = (high_20 / df["close"]) - 1.0
    df["pos_20"] = df["close"] / low_20

    low_60 = df["low"].shift(1).rolling(window=60).min()
    df["pos_60"] = df["close"] / low_60

    df["next_high"] = df["high"].shift(-1)
    df["next_close"] = df["close"].shift(-1)
    df["next_ret"] = (df["next_close"] / df["close"]) - 1.0

    thr = limit_up_threshold(symbol)
    df["next_touch_limit"] = ((df["next_high"] / df["close"]) - 1.0) >= float(thr)
    df["limit_thr"] = float(thr)
    df["ret_norm"] = df["ret_1"] / df["limit_thr"]
    df["to_limit"] = df["limit_thr"] - df["ret_1"]
    df["near_limit"] = (df["ret_norm"] >= 0.7).astype(int)

    ma5_slope = []
    ma10_slope = []
    for i in range(len(df)):
        if i < 15:
            ma5_slope.append(np.nan)
            ma10_slope.append(np.nan)
            continue
        ma5_slope.append(_calc_ma_slope(df.loc[max(0, i - 4) : i, "ma5"]))
        ma10_slope.append(_calc_ma_slope(df.loc[max(0, i - 4) : i, "ma10"]))
    df["ma5_slope"] = ma5_slope
    df["ma10_slope"] = ma10_slope

    up = (df["ret_1"] > 0).astype(int)
    streak = []
    cur = 0
    for v in up.fillna(0).tolist():
        if int(v) == 1:
            cur += 1
        else:
            cur = 0
        streak.append(cur)
    df["up_streak"] = streak
    return df


def _pick_board_sample(
    n_boards: int,
    per_board: int,
    end_day: datetime.date,
    fetch_pad_days: int,
    min_history_days: int = 120,
) -> List[dict]:
    try:
        board_df = ak.stock_board_industry_name_em()
    except Exception:
        return []
    if board_df is None or board_df.empty or "板块名称" not in board_df.columns:
        return []
    boards = [b for b in board_df["板块名称"].dropna().astype(str).tolist() if b.strip()]
    boards = boards[: max(int(n_boards) * 3, int(n_boards))]
    chosen: List[dict] = []
    seen = set()
    start_date = end_day - datetime.timedelta(days=max(int(fetch_pad_days), int(min_history_days) + 30))
    for b in boards:
        if len(chosen) >= int(n_boards) * int(per_board):
            break
        try:
            cons = ak.stock_board_industry_cons_em(symbol=b)
        except Exception:
            continue
        if cons is None or cons.empty:
            continue
        if "名称" in cons.columns:
            cons = cons[~cons["名称"].astype(str).str.contains("ST|退", na=False)]
        code_col = "代码" if "代码" in cons.columns else None
        name_col = "名称" if "名称" in cons.columns else None
        if code_col is None:
            continue
        taken = 0
        for _, r in cons.iterrows():
            if taken >= int(per_board):
                break
            sym = _normalize_symbol(r.get(code_col))
            if not sym or sym in seen:
                continue
            nm = str(r.get(name_col, "")).strip() if name_col else ""
            daily = _fetch_daily(sym, start_date=start_date, end_date=end_day)
            if daily is None or daily.empty or len(daily) < int(min_history_days):
                continue
            chosen.append({"symbol": sym, "name": nm})
            seen.add(sym)
            taken += 1
    return chosen


@dataclass
class TrainResult:
    feature_cols: List[str]
    mu: np.ndarray
    sigma: np.ndarray
    w: np.ndarray
    b: float
    threshold: float
    precision_test: float
    trigger_test: int
    hit_test: int
    start_day: datetime.date
    end_day: datetime.date
    test_unique_days: int
    prefilter_json: str


def _prefilter_df(df: pd.DataFrame, p: dict) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    x = df.copy()
    ret_low = float(p.get("ret_low", -1))
    ret_high = float(p.get("ret_high", 1))
    vol_low = float(p.get("vol_low", 0))
    close_pos_low = float(p.get("close_pos_low", 0))
    pullback_high = float(p.get("pullback_high", 1))
    dist_ma20_low = float(p.get("dist_ma20_low", -10))
    dist_ma20_high = float(p.get("dist_ma20_high", 10))
    pos60_high = float(p.get("pos60_high", 999))
    range_high = float(p.get("range_high", 999))
    to_limit_high = float(p.get("to_limit_high", 999))
    streak_high = float(p.get("streak_high", 999))

    m = pd.Series(True, index=x.index)
    if "ret_1" in x.columns:
        m &= (x["ret_1"] >= ret_low) & (x["ret_1"] <= ret_high)
    if "vol_ratio_5" in x.columns:
        m &= x["vol_ratio_5"] >= vol_low
    if "close_pos" in x.columns:
        m &= x["close_pos"] >= close_pos_low
    if "pullback" in x.columns:
        m &= x["pullback"].fillna(0.0) <= pullback_high
    if "dist_ma20" in x.columns:
        m &= (x["dist_ma20"] >= dist_ma20_low) & (x["dist_ma20"] <= dist_ma20_high)
    if "pos_60" in x.columns:
        m &= x["pos_60"] <= pos60_high
    if "range" in x.columns:
        m &= x["range"] <= range_high
    if "to_limit" in x.columns:
        m &= x["to_limit"] <= to_limit_high
    if "up_streak" in x.columns:
        m &= x["up_streak"] <= streak_high

    return x[m].copy()


def _make_xy(df0: pd.DataFrame, feat_cols: List[str]) -> Tuple[np.ndarray, np.ndarray]:
    xdf = df0[feat_cols].copy()
    xdf = xdf.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    X = xdf.to_numpy(dtype=float)
    y = df0["next_touch_limit"].astype(int).to_numpy(dtype=int)
    return X, y


def _train_and_select_threshold(
    dataset: pd.DataFrame,
    test_unique_days: int,
    target_precision: float,
    min_triggers_test: int,
    seed: int,
    pos_weight: float,
    prefilter: Optional[dict],
    lr: float,
    steps: int,
    l2: float,
) -> Optional[TrainResult]:
    if dataset is None or dataset.empty:
        return None
    uniq_dates = sorted(dataset["date_dt"].dropna().unique().tolist())
    if len(uniq_dates) < max(30, int(test_unique_days) + 5):
        return None
    test_dates = set(uniq_dates[-int(test_unique_days) :])
    train_dates = set([d for d in uniq_dates if d not in test_dates])
    train = dataset[dataset["date_dt"].isin(train_dates)].copy()
    test = dataset[dataset["date_dt"].isin(test_dates)].copy()
    if train.empty or test.empty:
        return None
    if prefilter is not None:
        train = _prefilter_df(train, prefilter)
        test = _prefilter_df(test, prefilter)
        if train.empty or test.empty:
            return None

    base_cols = [
        "ret_1",
        "gap",
        "range",
        "high_ret",
        "pullback",
        "body",
        "upper_shadow",
        "lower_shadow",
        "close_pos",
        "ret_3",
        "ret_5",
        "ret_20",
        "vol_ratio_5",
        "amt_ratio_5",
        "ma5_gt_ma10",
        "ma10_gt_ma20",
        "dist_ma20",
        "dist_ma60",
        "ma5_slope",
        "ma10_slope",
        "dist_high_20",
        "pos_20",
        "pos_60",
        "ret_norm",
        "to_limit",
        "near_limit",
        "up_streak",
    ]
    feat_cols = [c for c in base_cols if c in dataset.columns]
    if len(feat_cols) < 8:
        return None

    X_train, y_train = _make_xy(train, feat_cols)
    X_test, y_test = _make_xy(test, feat_cols)

    mu = X_train.mean(axis=0)
    sigma = X_train.std(axis=0)
    sigma = np.where(sigma == 0, 1.0, sigma)
    X_train_s = (X_train - mu) / sigma
    X_test_s = (X_test - mu) / sigma

    rng = np.random.default_rng(int(seed))
    _ = rng.normal(0.0, 0.01, size=X_train_s.shape[1])
    sw = np.where(y_train == 1, float(pos_weight), 1.0)
    w, b = _train_logreg(X_train_s, y_train, lr=float(lr), steps=int(steps), l2=float(l2), sample_weight=sw)

    prob_test = _predict_logreg(X_test_s, w, b)
    best_thr = None
    best_prec = 0.0
    best_cnt = 0
    best_tp = 0

    for thr in np.linspace(0.5, 0.98, 13):
        prec, tp, cnt = _precision_at_threshold(y_test, prob_test, float(thr))
        if cnt < int(min_triggers_test):
            continue
        if prec >= float(target_precision):
            if (prec > best_prec) or (prec == best_prec and cnt > best_cnt):
                best_thr = float(thr)
                best_prec = float(prec)
                best_cnt = int(cnt)
                best_tp = int(tp)
    if best_thr is None:
        for thr in np.linspace(0.5, 0.98, 13):
            prec, tp, cnt = _precision_at_threshold(y_test, prob_test, float(thr))
            if cnt < int(min_triggers_test):
                continue
            if (prec > best_prec) or (prec == best_prec and cnt > best_cnt):
                best_thr = float(thr)
                best_prec = float(prec)
                best_cnt = int(cnt)
                best_tp = int(tp)

    if best_thr is None:
        return None

    return TrainResult(
        feature_cols=feat_cols,
        mu=mu,
        sigma=sigma,
        w=w,
        b=b,
        threshold=float(best_thr),
        precision_test=float(best_prec),
        trigger_test=int(best_cnt),
        hit_test=int(best_tp),
        start_day=min(uniq_dates),
        end_day=max(uniq_dates),
        test_unique_days=int(test_unique_days),
        prefilter_json=json.dumps(prefilter, ensure_ascii=False) if prefilter is not None else "",
    )


def _build_dataset(
    symbols: List[dict],
    start_day: datetime.date,
    end_day: datetime.date,
    fetch_pad_days: int,
) -> pd.DataFrame:
    frames = []
    errors = 0
    fetch_start = start_day - datetime.timedelta(days=max(int(fetch_pad_days), 90))
    fetch_end = end_day + datetime.timedelta(days=2)
    for item in symbols:
        sym = item["symbol"]
        nm = item.get("name", "")
        daily = _fetch_daily(sym, start_date=fetch_start, end_date=fetch_end)
        if daily is None or daily.empty:
            errors += 1
            continue
        df = _build_feature_frame(sym, nm, daily)
        if df is None or df.empty:
            continue
        df = df[(df["date_dt"] >= start_day) & (df["date_dt"] <= end_day)].copy()
        df = df[df["next_high"].notna() & df["close"].notna()]
        if df.empty:
            continue
        frames.append(df)
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if errors:
        print("日线拉取失败数:", int(errors))
    return out


def _predict_for_universe(
    model: TrainResult,
    symbols: List[dict],
    as_of_day: datetime.date,
    fetch_pad_days: int,
) -> pd.DataFrame:
    feats = []
    fetch_start = as_of_day - datetime.timedelta(days=max(int(fetch_pad_days), 90))
    for item in symbols:
        sym = item["symbol"]
        nm = item.get("name", "")
        daily = _fetch_daily(sym, start_date=fetch_start, end_date=as_of_day)
        if daily is None or daily.empty:
            continue
        df = _build_feature_frame(sym, nm, daily)
        if df is None or df.empty:
            continue
        row_df = df[df["date_dt"] == as_of_day].copy()
        if row_df.empty:
            row_df = df[df["date_dt"] <= as_of_day].tail(1).copy()
        if row_df.empty:
            continue
        feats.append(row_df.iloc[0].to_dict())

    if not feats:
        return pd.DataFrame()

    fdf = pd.DataFrame(feats)
    xdf = fdf[model.feature_cols].copy()
    xdf = xdf.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    X = xdf.to_numpy(dtype=float)
    Xs = (X - model.mu) / model.sigma
    prob = _predict_logreg(Xs, model.w, model.b)

    fdf["prob_touch_limit"] = prob
    fdf["picked"] = fdf["prob_touch_limit"] >= float(model.threshold)
    fdf = fdf.sort_values(["prob_touch_limit"], ascending=[False]).reset_index(drop=True)

    cols = [
        "symbol",
        "name",
        "date",
        "close",
        "ret_1",
        "ret_5",
        "ret_20",
        "vol_ratio_5",
        "dist_ma20",
        "dist_high_20",
        "pos_60",
        "prob_touch_limit",
        "picked",
    ]
    cols = [c for c in cols if c in fdf.columns]
    return fdf[cols]


def _render_test_window_report(dataset: pd.DataFrame, model: TrainResult, show_n: int) -> None:
    uniq_dates = sorted(dataset["date_dt"].dropna().unique().tolist())
    if len(uniq_dates) < int(model.test_unique_days) + 1:
        return
    test_dates = set(uniq_dates[-int(model.test_unique_days) :])
    test = dataset[dataset["date_dt"].isin(test_dates)].copy()
    if model.prefilter_json:
        try:
            pre = json.loads(model.prefilter_json)
        except Exception:
            pre = None
        if isinstance(pre, dict):
            test = _prefilter_df(test, pre)
    if test.empty:
        return
    X_test, y_test = _make_xy(test, model.feature_cols)
    Xs = (X_test - model.mu) / model.sigma
    prob = _predict_logreg(Xs, model.w, model.b)
    test["prob_touch_limit"] = prob
    test["picked"] = test["prob_touch_limit"] >= float(model.threshold)
    test["hit"] = test["next_touch_limit"].astype(bool)

    picked = test[test["picked"] == True].copy()
    pos = int(np.sum(y_test == 1))
    tp = int(np.sum((test["picked"] == True) & (test["hit"] == True)))
    fp = int(np.sum((test["picked"] == True) & (test["hit"] == False)))
    fn = int(np.sum((test["picked"] == False) & (test["hit"] == True)))
    tn = int(np.sum((test["picked"] == False) & (test["hit"] == False)))
    prec = float(tp / (tp + fp)) if (tp + fp) > 0 else 0.0
    recall = float(tp / pos) if pos > 0 else 0.0

    print("\n测试窗口明细:")
    print("测试窗口交易日:", int(model.test_unique_days), "样本数:", int(len(test)), "正例数:", int(pos))
    print("混淆矩阵: TP", int(tp), "FP", int(fp), "FN", int(fn), "TN", int(tn))
    print("Precision:", f"{prec:.2%}", "Recall:", f"{recall:.2%}")

    if picked.empty:
        print("测试窗口内无触发样本")
        return

    picked = picked.sort_values(["date_dt", "prob_touch_limit"], ascending=[True, False]).tail(int(show_n))
    cols = [
        "date",
        "symbol",
        "name",
        "prob_touch_limit",
        "hit",
        "next_ret",
        "ret_1",
        "ret_5",
        "vol_ratio_5",
        "dist_ma20",
        "dist_high_20",
        "pos_60",
    ]
    cols = [c for c in cols if c in picked.columns]
    print("\n测试窗口最近触发样本(后%d):" % int(min(show_n, len(picked))))
    print(picked[cols].to_string(index=False))


def _eval_topk_per_day(test: pd.DataFrame, k: int) -> Optional[dict]:
    if test is None or test.empty:
        return None
    if int(k) <= 0:
        return None
    if "date_dt" not in test.columns or "prob_touch_limit" not in test.columns or "next_touch_limit" not in test.columns:
        return None
    df = test.copy()
    df["hit"] = df["next_touch_limit"].astype(bool)
    picked = (
        df.sort_values(["date_dt", "prob_touch_limit"], ascending=[True, False])
        .groupby("date_dt", as_index=False)
        .head(int(k))
    )
    if picked.empty:
        return None
    tp = int(picked["hit"].sum())
    cnt = int(len(picked))
    prec = float(tp / cnt) if cnt > 0 else 0.0
    days = int(picked["date_dt"].nunique())
    return {"k": int(k), "precision": float(prec), "picked": int(cnt), "hit": int(tp), "days": int(days)}


def _split_train_val_test_dates(uniq_dates: List[datetime.date], val_days: int, test_days: int):
    if len(uniq_dates) < int(val_days) + int(test_days) + 10:
        return None
    test_dates = set(uniq_dates[-int(test_days) :])
    val_dates = set(uniq_dates[-int(test_days) - int(val_days) : -int(test_days)])
    train_dates = set([d for d in uniq_dates if (d not in test_dates and d not in val_dates)])
    return train_dates, val_dates, test_dates


def _train_with_val_search(
    dataset: pd.DataFrame,
    val_days: int,
    test_days: int,
    target_precision: float,
    min_triggers_val: int,
    seed: int,
    trials: int,
    pos_weight: float,
) -> Optional[TrainResult]:
    uniq_dates = sorted(dataset["date_dt"].dropna().unique().tolist())
    split = _split_train_val_test_dates(uniq_dates, val_days=val_days, test_days=test_days)
    if split is None:
        return None
    train_dates, val_dates, test_dates = split
    train0 = dataset[dataset["date_dt"].isin(train_dates)].copy()
    val0 = dataset[dataset["date_dt"].isin(val_dates)].copy()
    test0 = dataset[dataset["date_dt"].isin(test_dates)].copy()
    if train0.empty or val0.empty or test0.empty:
        return None

    base_cols = [
        "ret_1",
        "gap",
        "range",
        "high_ret",
        "pullback",
        "body",
        "upper_shadow",
        "lower_shadow",
        "close_pos",
        "ret_3",
        "ret_5",
        "ret_20",
        "vol_ratio_5",
        "amt_ratio_5",
        "ma5_gt_ma10",
        "ma10_gt_ma20",
        "dist_ma20",
        "dist_ma60",
        "ma5_slope",
        "ma10_slope",
        "dist_high_20",
        "pos_20",
        "pos_60",
        "ret_norm",
        "to_limit",
        "near_limit",
        "up_streak",
    ]
    feat_cols = [c for c in base_cols if c in dataset.columns]
    if len(feat_cols) < 10:
        return None

    rng = np.random.default_rng(int(seed))
    best = None
    best_any = None

    for _ in range(int(trials)):
        pre = {
            "ret_low": float(rng.uniform(0.00, 0.10)),
            "ret_high": float(rng.uniform(0.08, 0.30)),
            "vol_low": float(rng.uniform(0.8, 4.0)),
            "close_pos_low": float(rng.uniform(0.40, 0.90)),
            "pullback_high": float(rng.uniform(0.0, 0.12)),
            "dist_ma20_low": float(rng.uniform(-0.15, 0.05)),
            "dist_ma20_high": float(rng.uniform(0.02, 0.25)),
            "pos60_high": float(rng.uniform(1.4, 6.0)),
            "range_high": float(rng.uniform(0.06, 0.35)),
            "to_limit_high": float(rng.uniform(0.00, 0.22)),
            "streak_high": int(rng.integers(2, 12)),
        }
        if pre["ret_high"] <= pre["ret_low"]:
            continue
        if pre["dist_ma20_high"] <= pre["dist_ma20_low"]:
            continue

        train = _prefilter_df(train0, pre)
        val = _prefilter_df(val0, pre)
        if train.empty or val.empty:
            continue

        X_train, y_train = _make_xy(train, feat_cols)
        X_val, y_val = _make_xy(val, feat_cols)
        if len(X_train) < 800 or len(X_val) < 300:
            continue

        mu = X_train.mean(axis=0)
        sigma = X_train.std(axis=0)
        sigma = np.where(sigma == 0, 1.0, sigma)
        X_train_s = (X_train - mu) / sigma
        X_val_s = (X_val - mu) / sigma

        lr = float(rng.uniform(0.03, 0.12))
        steps = int(rng.integers(1600, 4200))
        l2 = float(10 ** rng.uniform(-3.0, -1.2))
        sw = np.where(y_train == 1, float(pos_weight), 1.0)
        w, b = _train_logreg(X_train_s, y_train, lr=lr, steps=steps, l2=l2, sample_weight=sw)
        prob_val = _predict_logreg(X_val_s, w, b)

        best_thr_t = None
        best_prec_t = 0.0
        best_cnt_t = 0
        best_tp_t = 0

        best_thr_a = None
        best_prec_a = 0.0
        best_cnt_a = 0
        best_tp_a = 0

        for thr in np.linspace(0.05, 0.99, 20):
            prec, tp, cnt = _precision_at_threshold(y_val, prob_val, float(thr))
            if cnt < int(min_triggers_val):
                continue
            if (prec > best_prec_a) or (prec == best_prec_a and cnt > best_cnt_a):
                best_thr_a = float(thr)
                best_prec_a = float(prec)
                best_cnt_a = int(cnt)
                best_tp_a = int(tp)
            if prec >= float(target_precision):
                if (prec > best_prec_t) or (prec == best_prec_t and cnt > best_cnt_t):
                    best_thr_t = float(thr)
                    best_prec_t = float(prec)
                    best_cnt_t = int(cnt)
                    best_tp_t = int(tp)

        if best_thr_a is None:
            continue

        cand_any = {
            "score": float(best_prec_a) + 0.001 * float(best_cnt_a),
            "prec_val": float(best_prec_a),
            "cnt_val": int(best_cnt_a),
            "tp_val": int(best_tp_a),
            "thr": float(best_thr_a),
            "prefilter": pre,
            "lr": lr,
            "steps": steps,
            "l2": l2,
        }
        if best_any is None or cand_any["score"] > best_any["score"]:
            best_any = cand_any

        if best_thr_t is not None:
            cand_t = dict(cand_any)
            cand_t.update(
                {
                    "score": float(best_prec_t) + 0.001 * float(best_cnt_t),
                    "prec_val": float(best_prec_t),
                    "cnt_val": int(best_cnt_t),
                    "tp_val": int(best_tp_t),
                    "thr": float(best_thr_t),
                }
            )
            if best is None or cand_t["score"] > best["score"]:
                best = cand_t

    chosen = best if best is not None else best_any
    if chosen is None:
        return None

    pre = chosen["prefilter"]
    train_val = _prefilter_df(pd.concat([train0, val0], ignore_index=True), pre)
    test = _prefilter_df(test0, pre)
    if train_val.empty or test.empty:
        return None

    X_tv, y_tv = _make_xy(train_val, feat_cols)
    X_test, y_test = _make_xy(test, feat_cols)
    mu = X_tv.mean(axis=0)
    sigma = X_tv.std(axis=0)
    sigma = np.where(sigma == 0, 1.0, sigma)
    X_tv_s = (X_tv - mu) / sigma
    X_test_s = (X_test - mu) / sigma
    sw = np.where(y_tv == 1, float(pos_weight), 1.0)
    w, b = _train_logreg(
        X_tv_s,
        y_tv,
        lr=float(chosen["lr"]),
        steps=int(chosen["steps"]),
        l2=float(chosen["l2"]),
        sample_weight=sw,
    )
    prob_test = _predict_logreg(X_test_s, w, b)
    prec_test, tp_test, cnt_test = _precision_at_threshold(y_test, prob_test, float(chosen["thr"]))

    uniq_test = sorted(list(test_dates))
    return TrainResult(
        feature_cols=feat_cols,
        mu=mu,
        sigma=sigma,
        w=w,
        b=b,
        threshold=float(chosen["thr"]),
        precision_test=float(prec_test),
        trigger_test=int(cnt_test),
        hit_test=int(tp_test),
        start_day=min(uniq_dates),
        end_day=max(uniq_dates),
        test_unique_days=int(test_days),
        prefilter_json=json.dumps(pre, ensure_ascii=False),
    )


def run_screener(
    mode: str = "predict",
    n_boards: int = 30,
    per_board: int = 20,
    lookback_days: int = 180,
    test_days: int = 30,
    val_days: int = 60,
    fetch_pad_days: int = 180,
    target_precision: float = 0.8,
    min_triggers_test: int = 8,
    min_triggers_val: int = 12,
    seed: int = 7,
    pos_weight: float = 10.0,
    optimize: int = 1,
    trials: int = 220,
    top_n: int = 900,
    min_amount: float = 50_000_000,
    max_predict_symbols: int = 800,
    as_of: Optional[str] = None,
    only_picked: bool = True,
    max_return: int = 200,
) -> pd.DataFrame:
    if as_of:
        as_of_day = datetime.datetime.strptime(as_of, "%Y-%m-%d").date()
    else:
        as_of_day = _safe_trade_day(datetime.date.today() - datetime.timedelta(days=1))

    end_day = as_of_day
    start_day = end_day - datetime.timedelta(days=int(lookback_days))

    snapshot_df = _fetch_snapshot()
    if snapshot_df is None or snapshot_df.empty:
        print("无法获取全市场快照数据（akshare 接口异常）")
        return pd.DataFrame()

    pred_universe = _pick_universe(snapshot_df, top_n=int(top_n), min_amount=float(min_amount))
    if int(max_predict_symbols) > 0:
        pred_universe = pred_universe[: int(max_predict_symbols)]

    symbols = _pick_board_sample(
        n_boards=int(n_boards),
        per_board=int(per_board),
        end_day=end_day,
        fetch_pad_days=int(fetch_pad_days),
    )
    if not symbols:
        target_cnt = int(n_boards) * int(per_board)
        fallback_pool = _pick_universe(snapshot_df, top_n=max(target_cnt * 3, target_cnt), min_amount=float(min_amount))
        symbols = fallback_pool[:target_cnt]
        if not symbols:
            print("无法构建训练样本（行业板块与快照接口异常）")
            return pd.DataFrame()

    dataset = _build_dataset(
        symbols,
        start_day=start_day,
        end_day=end_day,
        fetch_pad_days=int(fetch_pad_days),
    )
    if dataset is None or dataset.empty:
        print("无可用训练样本（可能网络/接口受限）")
        return pd.DataFrame()

    dataset = dataset.replace([np.inf, -np.inf], np.nan)
    dataset = dataset.dropna(subset=["ret_1", "vol_ratio_5", "next_touch_limit", "date_dt", "range"])
    dataset = dataset[(dataset["ret_1"].abs() <= 0.3) & (dataset["range"].abs() <= 0.5)]
    if dataset.empty:
        print("清洗后训练样本为空")
        return pd.DataFrame()

    if int(optimize) == 1:
        model = _train_with_val_search(
            dataset,
            val_days=int(val_days),
            test_days=int(test_days),
            target_precision=float(target_precision),
            min_triggers_val=int(min_triggers_val),
            seed=int(seed),
            trials=int(trials),
            pos_weight=float(pos_weight),
        )
    else:
        model = _train_and_select_threshold(
            dataset,
            test_unique_days=int(test_days),
            target_precision=float(target_precision),
            min_triggers_test=int(min_triggers_test),
            seed=int(seed),
            pos_weight=float(pos_weight),
            prefilter=None,
            lr=0.08,
            steps=2800,
            l2=5e-2,
        )

    if model is None:
        print("训练失败：可用交易日/样本不足，或特征列缺失")
        return pd.DataFrame()

    if mode == "backtest":
        return pd.DataFrame()

    pred_df = _predict_for_universe(
        model,
        pred_universe,
        as_of_day=as_of_day,
        fetch_pad_days=int(fetch_pad_days),
    )
    if pred_df is None or pred_df.empty:
        print("无可用预测结果（可能日线缺失或接口受限）")
        return pd.DataFrame()

    out = pred_df.copy()
    if only_picked and "picked" in out.columns:
        out = out[out["picked"] == True].copy()

    out["as_of"] = str(as_of_day)
    out = out.reset_index(drop=True)
    if int(max_return) > 0:
        out = out.head(int(max_return)).copy()
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, default="both", choices=["backtest", "predict", "both"])
    parser.add_argument("--n_boards", type=int, default=30)
    parser.add_argument("--per_board", type=int, default=20)
    parser.add_argument("--lookback_days", type=int, default=480)
    parser.add_argument("--test_days", type=int, default=60)
    parser.add_argument("--val_days", type=int, default=60)
    parser.add_argument("--fetch_pad_days", type=int, default=180)
    parser.add_argument("--target_precision", type=float, default=0.8)
    parser.add_argument("--min_triggers_test", type=int, default=8)
    parser.add_argument("--min_triggers_val", type=int, default=12)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--pos_weight", type=float, default=10.0)
    parser.add_argument("--optimize", type=int, default=1)
    parser.add_argument("--trials", type=int, default=220)
    parser.add_argument("--top_n", type=int, default=900)
    parser.add_argument("--min_amount", type=float, default=50_000_000)
    parser.add_argument("--max_predict_symbols", type=int, default=800)
    parser.add_argument("--show_test_triggers", type=int, default=1)
    parser.add_argument("--show_test_n", type=int, default=30)
    parser.add_argument("--topk_per_day", type=int, default=0)
    parser.add_argument("--as_of", type=str, default="")
    args = parser.parse_args()

    if args.as_of:
        as_of_day = datetime.datetime.strptime(args.as_of, "%Y-%m-%d").date()
    else:
        as_of_day = _safe_trade_day(datetime.date.today() - datetime.timedelta(days=1))

    end_day = as_of_day
    start_day = end_day - datetime.timedelta(days=int(args.lookback_days))

    snapshot_df = _fetch_snapshot()
    if snapshot_df is None or snapshot_df.empty:
        print("无法获取全市场快照数据（akshare 接口异常）")
        return

    pred_universe = _pick_universe(snapshot_df, top_n=int(args.top_n), min_amount=float(args.min_amount))
    if int(args.max_predict_symbols) > 0:
        pred_universe = pred_universe[: int(args.max_predict_symbols)]

    symbols = _pick_board_sample(
        n_boards=int(args.n_boards),
        per_board=int(args.per_board),
        end_day=end_day,
        fetch_pad_days=int(args.fetch_pad_days),
    )
    if not symbols:
        target_cnt = int(args.n_boards) * int(args.per_board)
        fallback_pool = _pick_universe(snapshot_df, top_n=max(target_cnt * 3, target_cnt), min_amount=float(args.min_amount))
        symbols = fallback_pool[:target_cnt]
        if not symbols:
            print("无法构建训练样本（行业板块与快照接口异常）")
            return
        print("板块接口异常，已改用流动性样本构建训练集，样本数:", len(symbols))

    dataset = _build_dataset(
        symbols,
        start_day=start_day,
        end_day=end_day,
        fetch_pad_days=int(args.fetch_pad_days),
    )
    if dataset is None or dataset.empty:
        print("无可用训练样本（可能网络/接口受限）")
        return

    dataset = dataset.replace([np.inf, -np.inf], np.nan)
    dataset = dataset.dropna(subset=["ret_1", "vol_ratio_5", "next_touch_limit", "date_dt", "range"])
    dataset = dataset[(dataset["ret_1"].abs() <= 0.3) & (dataset["range"].abs() <= 0.5)]
    if dataset.empty:
        print("清洗后训练样本为空")
        return

    if int(args.optimize) == 1:
        model = _train_with_val_search(
            dataset,
            val_days=int(args.val_days),
            test_days=int(args.test_days),
            target_precision=float(args.target_precision),
            min_triggers_val=int(args.min_triggers_val),
            seed=int(args.seed),
            trials=int(args.trials),
            pos_weight=float(args.pos_weight),
        )
    else:
        model = _train_and_select_threshold(
            dataset,
            test_unique_days=int(args.test_days),
            target_precision=float(args.target_precision),
            min_triggers_test=int(args.min_triggers_test),
            seed=int(args.seed),
            pos_weight=float(args.pos_weight),
            prefilter=None,
            lr=0.08,
            steps=2800,
            l2=5e-2,
        )
    if model is None:
        print("训练失败：可用交易日/样本不足，或特征列缺失")
        return

    if args.mode in ("backtest", "both"):
        base_rate = float(dataset["next_touch_limit"].astype(int).mean())
        print("回测区间:", str(start_day), "~", str(end_day))
        print("训练/回测样本股票数:", len(symbols))
        print("样本条数:", int(len(dataset)))
        print("整体次日触板率:", f"{base_rate:.2%}")
        print("测试集触发:", int(model.trigger_test), "命中:", int(model.hit_test), "命中率:", f"{model.precision_test:.2%}")
        print("阈值:", f"{model.threshold:.2f}", "测试窗口(交易日):", int(model.test_unique_days))
        print("特征:", ",".join(model.feature_cols))
        if model.prefilter_json:
            print("预筛选:", model.prefilter_json)
        if int(args.show_test_triggers) == 1:
            _render_test_window_report(dataset, model, show_n=int(args.show_test_n))
        if int(args.topk_per_day) > 0:
            uniq_dates = sorted(dataset["date_dt"].dropna().unique().tolist())
            test_dates = set(uniq_dates[-int(model.test_unique_days) :]) if len(uniq_dates) >= int(model.test_unique_days) else set()
            test = dataset[dataset["date_dt"].isin(test_dates)].copy()
            if model.prefilter_json:
                try:
                    pre = json.loads(model.prefilter_json)
                except Exception:
                    pre = None
                if isinstance(pre, dict):
                    test = _prefilter_df(test, pre)
            if not test.empty:
                X_test, _y_test = _make_xy(test, model.feature_cols)
                Xs = (X_test - model.mu) / model.sigma
                test["prob_touch_limit"] = _predict_logreg(Xs, model.w, model.b)
                r = _eval_topk_per_day(test, k=int(args.topk_per_day))
                if r is not None:
                    print("TopK/日:", "K", r["k"], "天数", r["days"], "触发", r["picked"], "命中", r["hit"], "命中率", f"{r['precision']:.2%}")

        if args.mode == "backtest":
            return

    print("\n开始全市场预测（基于最新已收盘日）:", str(as_of_day))
    print("预测股票数:", len(pred_universe))
    pred_df = _predict_for_universe(
        model,
        pred_universe,
        as_of_day=as_of_day,
        fetch_pad_days=int(args.fetch_pad_days),
    )
    if pred_df is None or pred_df.empty:
        print("无可用预测结果（可能日线缺失或接口受限）")
        return
    picked = pred_df[pred_df["picked"] == True] if "picked" in pred_df.columns else pred_df.head(0)
    print("触发数量:", int(len(picked)))
    show_n = min(30, len(pred_df))
    print("\nTop 概率(前%d):" % int(show_n))
    print(pred_df.head(show_n).to_string(index=False))
    if not picked.empty:
        print("\n触发名单(最多30):")
        print(picked.head(30).to_string(index=False))


if __name__ == "__main__":
    main()
