import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd


def _project_root() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(here, "..", "..", ".."))


def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _as_yyyymmdd(d: datetime) -> str:
    return d.strftime("%Y%m%d")


def _pick_last_trade_date(pro, end_date: str) -> str:
    start = _as_yyyymmdd(datetime.strptime(end_date, "%Y%m%d") - timedelta(days=30))
    df = pro.trade_cal(exchange="SSE", start_date=start, end_date=end_date, fields="cal_date,is_open")
    if df is None or df.empty:
        raise RuntimeError("trade_cal 返回为空，无法确定最近交易日")
    df = df[df["is_open"].astype(str) == "1"].copy()
    if df.empty:
        raise RuntimeError("trade_cal 无开市日期，无法确定最近交易日")
    dates = sorted(df["cal_date"].astype(str).tolist())
    return str(dates[-1])


def _load_trade_dates(pro, end_date: str, lookback: int) -> list[str]:
    start = _as_yyyymmdd(datetime.strptime(end_date, "%Y%m%d") - timedelta(days=int(lookback) * 3))
    df = pro.trade_cal(exchange="SSE", start_date=start, end_date=end_date, fields="cal_date,is_open")
    if df is None or df.empty:
        raise RuntimeError("trade_cal 返回为空，无法获取交易日序列")
    df = df[df["is_open"].astype(str) == "1"].copy()
    dates = sorted(df["cal_date"].astype(str).tolist())
    if len(dates) < int(lookback):
        return dates
    return dates[-int(lookback) :]


def _load_stock_pool(pro, min_list_days: int) -> pd.DataFrame:
    df = pro.stock_basic(
        exchange="",
        list_status="L",
        fields="ts_code,symbol,name,industry,market,list_date",
    )
    if df is None or df.empty:
        raise RuntimeError("stock_basic 返回为空，无法构建股票池")

    df = df.dropna(subset=["ts_code", "symbol"]).copy()
    df["ts_code"] = df["ts_code"].astype(str).str.strip()
    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["name"] = df.get("name", "").astype(str).str.strip()
    df["industry"] = df.get("industry", "").astype(str).str.strip()
    df["market"] = df.get("market", "").astype(str).str.strip()
    df["list_date"] = df.get("list_date", "").astype(str).str.strip()

    df = df[~df["name"].str.contains("ST", na=False)]

    today = datetime.now().date()
    df["list_dt"] = pd.to_datetime(df["list_date"], format="%Y%m%d", errors="coerce")
    min_dt = today - timedelta(days=int(min_list_days))
    df = df[(df["list_dt"].isna()) | (df["list_dt"].dt.date <= min_dt)]

    df = df.reset_index(drop=True)
    return df


def _safe_call(fn, sleep_s: float, what: str) -> Optional[pd.DataFrame]:
    try:
        df = fn()
        if sleep_s and sleep_s > 0:
            time.sleep(float(sleep_s))
        if df is None:
            print(f"{_now_ts()} {what} 返回 None", flush=True)
            return None
        if getattr(df, "empty", False):
            return df
        return df
    except Exception as e:
        print(f"{_now_ts()} {what} 异常: {type(e).__name__}:{e}", flush=True)
        if sleep_s and sleep_s > 0:
            time.sleep(float(sleep_s))
        return None


def _fetch_daily_all(pro, trade_dates: list[str], sleep_s: float) -> pd.DataFrame:
    frames = []
    for i, td in enumerate(trade_dates, start=1):
        print(f"{_now_ts()} 拉取 daily {i}/{len(trade_dates)} trade_date={td}", flush=True)
        df = _safe_call(
            lambda: pro.daily(
                trade_date=str(td),
                fields="ts_code,trade_date,open,high,low,close,pre_close,pct_chg,vol,amount",
            ),
            sleep_s=sleep_s,
            what=f"daily({td})",
        )
        if df is None or df.empty:
            continue
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    df_all = pd.concat(frames, ignore_index=True)
    return df_all


def _fetch_daily_basic(pro, trade_date: str, sleep_s: float) -> pd.DataFrame:
    fields_full = "ts_code,trade_date,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps_ttm,dv_ttm,circ_mv,total_mv"
    df = _safe_call(
        lambda: pro.daily_basic(trade_date=str(trade_date), fields=fields_full),
        sleep_s=sleep_s,
        what=f"daily_basic({trade_date})",
    )
    if df is None:
        df = pd.DataFrame()
    if df is not None and not df.empty:
        return df

    fields_min = "ts_code,trade_date,turnover_rate,turnover_rate_f,volume_ratio,circ_mv,total_mv"
    df2 = _safe_call(
        lambda: pro.daily_basic(trade_date=str(trade_date), fields=fields_min),
        sleep_s=sleep_s,
        what=f"daily_basic_min({trade_date})",
    )
    if df2 is None:
        return pd.DataFrame()
    return df2


def _fetch_moneyflow_days(pro, trade_dates: list[str], sleep_s: float) -> pd.DataFrame:
    frames = []
    for td in trade_dates:
        df = _safe_call(
            lambda: pro.moneyflow(
                trade_date=str(td),
                fields="ts_code,trade_date,net_mf_amount,buy_lg_amount,buy_elg_amount",
            ),
            sleep_s=sleep_s,
            what=f"moneyflow({td})",
        )
        if df is None or df.empty:
            continue
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    df_all = pd.concat(frames, ignore_index=True)
    return df_all


def _calc_rsi_wilder(close: pd.Series, window: int) -> pd.Series:
    close = pd.to_numeric(close, errors="coerce")
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / float(window), adjust=False, min_periods=window).mean()
    avg_loss = loss.ewm(alpha=1 / float(window), adjust=False, min_periods=window).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi


def _calc_atr(high: pd.Series, low: pd.Series, close: pd.Series, window: int) -> pd.Series:
    high = pd.to_numeric(high, errors="coerce")
    low = pd.to_numeric(low, errors="coerce")
    close = pd.to_numeric(close, errors="coerce")
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / float(window), adjust=False, min_periods=window).mean()
    return atr


def _add_group_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in ("open", "high", "low", "close", "pre_close", "pct_chg", "vol", "amount"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df["trade_date"] = df["trade_date"].astype(str).str.strip()
    df = df.dropna(subset=["ts_code", "trade_date", "close", "high", "low", "vol"])
    df = df.sort_values(["ts_code", "trade_date"], ascending=True).reset_index(drop=True)

    g = df.groupby("ts_code", group_keys=False)

    df["ma5"] = g["close"].transform(lambda s: s.rolling(5, min_periods=5).mean())
    df["ma10"] = g["close"].transform(lambda s: s.rolling(10, min_periods=10).mean())
    df["ma20"] = g["close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    df["ma60"] = g["close"].transform(lambda s: s.rolling(60, min_periods=60).mean())

    df["high10_prev"] = g["high"].transform(lambda s: s.rolling(10, min_periods=10).max().shift(1))
    df["high60_prev"] = g["high"].transform(lambda s: s.rolling(60, min_periods=60).max().shift(1))
    df["low20_prev"] = g["low"].transform(lambda s: s.rolling(20, min_periods=20).min().shift(1))
    df["range20_prev"] = g["close"].transform(
        lambda s: (s.rolling(20, min_periods=20).max().shift(1) / s.rolling(20, min_periods=20).min().shift(1))
    )

    df["vol_ma10_prev"] = g["vol"].transform(lambda s: s.rolling(10, min_periods=10).mean().shift(1))
    df["vol_ma30_prev"] = g["vol"].transform(lambda s: s.rolling(30, min_periods=30).mean().shift(1))
    df["vol_ratio10"] = df["vol"] / df["vol_ma10_prev"].replace(0, np.nan)
    df["vol_dry_ratio"] = df["vol_ma10_prev"] / df["vol_ma30_prev"].replace(0, np.nan)
    df["ret5"] = g["close"].transform(lambda s: s.pct_change(5))
    df["ret20"] = g["close"].transform(lambda s: s.pct_change(20))
    df["rsi14"] = g["close"].transform(lambda s: _calc_rsi_wilder(s, 14))
    df["atr14"] = g.apply(lambda x: _calc_atr(x["high"], x["low"], x["close"], 14)).reset_index(level=0, drop=True)
    df["atr14_pct"] = df["atr14"] / df["close"]
    df["atr14_pct_med10_prev"] = g["atr14_pct"].transform(lambda s: s.rolling(10, min_periods=10).median().shift(1))

    return df


def _industry_hot_filter(latest: pd.DataFrame, top_industries: int, min_stocks: int) -> pd.DataFrame:
    work = latest.copy()
    work = work[work["industry"].notna() & (work["industry"].astype(str).str.strip() != "")]
    if work.empty:
        latest["industry_hot"] = True
        latest["ind_ret5_mean"] = np.nan
        latest["ind_ret5_rank"] = np.nan
        return latest

    agg = work.groupby("industry", as_index=False).agg(
        n=("ts_code", "count"),
        ind_ret5_mean=("ret5", "mean"),
    )
    agg = agg[agg["n"] >= int(min_stocks)].copy()
    if agg.empty:
        latest["industry_hot"] = True
        latest["ind_ret5_mean"] = np.nan
        latest["ind_ret5_rank"] = np.nan
        return latest

    agg["ind_ret5_rank"] = agg["ind_ret5_mean"].rank(ascending=False, method="min")
    agg = agg.sort_values("ind_ret5_rank", ascending=True).head(int(top_industries))
    hot = set(agg["industry"].astype(str).tolist())

    out = latest.merge(agg[["industry", "ind_ret5_mean", "ind_ret5_rank"]], on="industry", how="left")
    out["industry_hot"] = out["industry"].astype(str).isin(hot)
    return out


def _safe_num(x) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")


def _score_washing_in_progress(row: pd.Series, breakout_ratio: float, breakdown_ratio: float) -> tuple[float, str]:
    reasons: list[str] = []
    score = 0.0

    close = _safe_num(row.get("close", np.nan))
    ma20 = _safe_num(row.get("ma20", np.nan))
    high10_prev = _safe_num(row.get("high10_prev", np.nan))
    high60_prev = _safe_num(row.get("high60_prev", np.nan))
    low20_prev = _safe_num(row.get("low20_prev", np.nan))
    range20_prev = _safe_num(row.get("range20_prev", np.nan))
    vol_dry_ratio = _safe_num(row.get("vol_dry_ratio", np.nan))
    vol_ratio10 = _safe_num(row.get("vol_ratio10", np.nan))
    volume_ratio = _safe_num(row.get("volume_ratio", np.nan))
    atr14_pct = _safe_num(row.get("atr14_pct", np.nan))
    atr14_pct_med10_prev = _safe_num(row.get("atr14_pct_med10_prev", np.nan))
    rsi14 = _safe_num(row.get("rsi14", np.nan))
    ret5 = _safe_num(row.get("ret5", np.nan))
    ret20 = _safe_num(row.get("ret20", np.nan))
    turnover_rate = _safe_num(row.get("turnover_rate", np.nan))
    amount = _safe_num(row.get("amount", np.nan))
    net_mf_amount = _safe_num(row.get("net_mf_amount", np.nan))
    net_mf_3d = _safe_num(row.get("net_mf_3d", np.nan))

    if np.isfinite(range20_prev) and range20_prev <= 1.35:
        score += 12
        reasons.append("20日震荡收敛")

    dd60 = np.nan
    if np.isfinite(high60_prev) and high60_prev > 0 and np.isfinite(close):
        dd60 = 1.0 - close / high60_prev
        if 0.15 <= dd60 <= 0.55:
            score += 12
            reasons.append("距60高位回撤充分")
        elif dd60 > 0.55:
            score += 2
            reasons.append("回撤偏深")
        elif dd60 < 0.10:
            score -= 8
            reasons.append("回撤不够")

    breakout_ok = False
    if np.isfinite(high10_prev) and high10_prev > 0 and np.isfinite(close) and close >= high10_prev * float(breakout_ratio):
        breakout_ok = True
        score -= 15
        reasons.append("已疑似突破")
    if not breakout_ok and np.isfinite(high10_prev) and high10_prev > 0:
        score += 10
        reasons.append("未突破10日高点")

    if np.isfinite(ma20) and ma20 > 0 and np.isfinite(close):
        ratio = close / ma20
        if 0.90 <= ratio <= 1.03:
            score += 10
            reasons.append("贴近MA20")
        elif ratio < 0.86:
            score -= 8
            reasons.append("跌破MA20较多")
        elif ratio > 1.08:
            score -= 6
            reasons.append("偏离MA20")

    dry_ok = False
    if np.isfinite(vol_dry_ratio) and vol_dry_ratio <= 0.88:
        dry_ok = True
    if np.isfinite(vol_ratio10) and vol_ratio10 <= 0.95:
        dry_ok = True
    if np.isfinite(volume_ratio) and volume_ratio <= 0.95:
        dry_ok = True
    if dry_ok:
        score += 10
        reasons.append("缩量洗盘")
    if np.isfinite(volume_ratio) and volume_ratio >= 1.6:
        score -= 6
        reasons.append("量比偏热")

    if np.isfinite(atr14_pct) and atr14_pct <= 0.06:
        score += 8
        reasons.append("波动率收敛")
    if np.isfinite(atr14_pct) and np.isfinite(atr14_pct_med10_prev) and atr14_pct <= atr14_pct_med10_prev:
        score += 4

    if np.isfinite(ret5) and -0.06 <= ret5 <= 0.06:
        score += 6
        reasons.append("短期横盘")
    if np.isfinite(ret20) and abs(ret20) <= 0.35:
        score += 4
    elif np.isfinite(ret20) and abs(ret20) > 0.50:
        score -= 6
        reasons.append("中期波动大")

    if np.isfinite(rsi14) and 32 <= rsi14 <= 55:
        score += 6
        reasons.append("RSI偏弱区间")

    if np.isfinite(turnover_rate) and 0.4 <= turnover_rate <= 10.0:
        score += 4
    elif np.isfinite(turnover_rate) and turnover_rate < 0.2:
        score -= 6
        reasons.append("换手过低")
    elif np.isfinite(turnover_rate) and turnover_rate > 18:
        score -= 4
        reasons.append("换手过高")

    if np.isfinite(amount) and amount >= 3e7:
        score += 3
    elif np.isfinite(amount) and amount < 1e7:
        score -= 8
        reasons.append("成交额偏冷")

    if np.isfinite(net_mf_3d):
        if net_mf_3d >= 0:
            score += 4
        else:
            score -= 4
            reasons.append("3日净流出")

    if np.isfinite(net_mf_amount):
        if net_mf_amount >= 0:
            score += 2
        else:
            score -= 2

    if np.isfinite(low20_prev) and low20_prev > 0 and np.isfinite(close) and close <= low20_prev * float(breakdown_ratio):
        score -= 20
        reasons.append("跌破20日低点")

    reason = "；".join(reasons[:6])
    return float(score), reason[:200]


def _prepare_latest_candidates(
    daily_all: pd.DataFrame,
    stock_pool: pd.DataFrame,
    daily_basic: pd.DataFrame,
    moneyflow_days: pd.DataFrame,
    latest_trade_date: str,
    top_industries: int,
    min_industry_stocks: int,
    require_hot_industry: bool,
    breakout_ratio: float,
    breakdown_ratio: float,
    max_range20: float,
    dd60_min: float,
    dd60_max: float,
    min_score: float,
) -> pd.DataFrame:
    if daily_all is None or daily_all.empty:
        return pd.DataFrame()

    daily_all = daily_all.copy()
    daily_all["ts_code"] = daily_all["ts_code"].astype(str).str.strip()

    daily_all = daily_all.merge(stock_pool[["ts_code", "symbol", "name", "industry", "market"]], on="ts_code", how="inner")
    daily_all = _add_group_indicators(daily_all)

    latest = daily_all[daily_all["trade_date"].astype(str) == str(latest_trade_date)].copy()
    if latest.empty:
        latest = daily_all.groupby("ts_code", as_index=False).tail(1).copy()

    if daily_basic is not None and not daily_basic.empty:
        daily_basic = daily_basic.copy()
        daily_basic["ts_code"] = daily_basic["ts_code"].astype(str).str.strip()
        for c in (
            "turnover_rate",
            "turnover_rate_f",
            "volume_ratio",
            "pe",
            "pe_ttm",
            "pb",
            "ps_ttm",
            "dv_ttm",
            "circ_mv",
            "total_mv",
        ):
            if c in daily_basic.columns:
                daily_basic[c] = pd.to_numeric(daily_basic[c], errors="coerce")
        latest = latest.merge(daily_basic, on=["ts_code", "trade_date"], how="left")

    latest["net_mf_amount"] = np.nan
    latest["net_mf_3d"] = np.nan
    if moneyflow_days is not None and not moneyflow_days.empty:
        mf = moneyflow_days.copy()
        mf["ts_code"] = mf["ts_code"].astype(str).str.strip()
        mf["trade_date"] = mf["trade_date"].astype(str).str.strip()
        mf["net_mf_amount"] = pd.to_numeric(mf.get("net_mf_amount", np.nan), errors="coerce")
        mf = mf.dropna(subset=["ts_code", "trade_date"])

        latest = latest.merge(
            mf[mf["trade_date"].astype(str) == str(latest_trade_date)][["ts_code", "net_mf_amount"]],
            on="ts_code",
            how="left",
        )
        mf3 = mf[mf["trade_date"].isin(sorted(mf["trade_date"].unique())[-3:])].groupby("ts_code", as_index=False).agg(
            net_mf_3d=("net_mf_amount", "sum")
        )
        latest = latest.merge(mf3, on="ts_code", how="left")

    latest = _industry_hot_filter(latest, top_industries=int(top_industries), min_stocks=int(min_industry_stocks))
    if bool(require_hot_industry):
        latest = latest[latest["industry_hot"].astype(bool)].copy()

    for c in ("ma20", "high10_prev", "high60_prev", "low20_prev", "range20_prev", "atr14_pct"):
        if c in latest.columns:
            latest[c] = pd.to_numeric(latest[c], errors="coerce")
    latest = latest.dropna(subset=["close", "high", "low", "vol", "ma20", "high10_prev", "high60_prev", "low20_prev", "atr14_pct"])

    latest["dd60"] = 1.0 - pd.to_numeric(latest["close"], errors="coerce") / pd.to_numeric(latest["high60_prev"], errors="coerce").replace(
        0, np.nan
    )

    latest = latest[latest["range20_prev"].astype(float) <= float(max_range20)].copy()
    latest = latest[(latest["dd60"].isna()) | ((latest["dd60"] >= float(dd60_min)) & (latest["dd60"] <= float(dd60_max)))].copy()
    latest = latest[latest["close"] < latest["high10_prev"] * float(breakout_ratio)].copy()
    latest = latest[latest["close"] > latest["low20_prev"] * float(breakdown_ratio)].copy()

    scores = []
    reasons = []
    for _, row in latest.iterrows():
        s, r = _score_washing_in_progress(row, breakout_ratio=float(breakout_ratio), breakdown_ratio=float(breakdown_ratio))
        scores.append(s)
        reasons.append(r)
    latest["score"] = scores
    latest["reason"] = reasons

    latest = latest[pd.to_numeric(latest["score"], errors="coerce") >= float(min_score)].copy()
    latest = latest.sort_values(["score", "amount"], ascending=[False, False])
    return latest


def main() -> None:
    parser = argparse.ArgumentParser(description="全市场扫描：正在洗盘中（缩量震荡）候选池（Tushare Pro）")
    parser.add_argument("--lookback-days", type=int, default=120)
    parser.add_argument("--moneyflow-days", type=int, default=3)
    parser.add_argument("--min-list-days", type=int, default=200)
    parser.add_argument("--top-industries", type=int, default=12)
    parser.add_argument("--min-industry-stocks", type=int, default=12)
    parser.add_argument("--hot-industry", dest="require_hot_industry", action="store_true")
    parser.add_argument("--no-hot-industry", dest="require_hot_industry", action="store_false")
    parser.set_defaults(require_hot_industry=False)
    parser.add_argument("--sleep-s", type=float, default=0.12)
    parser.add_argument("--max-results", type=int, default=200)
    parser.add_argument("--breakout-ratio", type=float, default=1.005)
    parser.add_argument("--breakdown-ratio", type=float, default=0.985)
    parser.add_argument("--max-range20", type=float, default=1.35)
    parser.add_argument("--dd60-min", type=float, default=0.15)
    parser.add_argument("--dd60-max", type=float, default=0.55)
    parser.add_argument("--min-score", type=float, default=26.0)
    parser.add_argument("--out", type=str, default="")
    args = parser.parse_args()

    root = _project_root()
    if root not in sys.path:
        sys.path.insert(0, root)

    try:
        from backend.utils.tushare_client import pro
    except Exception as e:
        raise SystemExit(f"导入 tushare_client 失败: {type(e).__name__}:{e}")

    if pro is None:
        raise SystemExit("pro=None，无法运行（请检查 tushare token/config）")

    end_date = _as_yyyymmdd(datetime.now())
    latest_trade_date = _pick_last_trade_date(pro, end_date=end_date)
    print(f"{_now_ts()} 最近交易日: {latest_trade_date}", flush=True)

    trade_dates = _load_trade_dates(pro, end_date=latest_trade_date, lookback=int(args.lookback_days))
    if not trade_dates:
        raise SystemExit("交易日序列为空，无法拉取日线")

    stock_pool = _load_stock_pool(pro, min_list_days=int(args.min_list_days))
    print(f"{_now_ts()} 股票池: {len(stock_pool)}", flush=True)

    daily_all = _fetch_daily_all(pro, trade_dates=trade_dates, sleep_s=float(args.sleep_s))
    if daily_all is None or daily_all.empty:
        raise SystemExit("daily 拉取为空，无法继续")
    print(f"{_now_ts()} daily 行数: {len(daily_all)}", flush=True)

    daily_basic = _fetch_daily_basic(pro, trade_date=latest_trade_date, sleep_s=float(args.sleep_s))
    if daily_basic is not None and not daily_basic.empty:
        print(f"{_now_ts()} daily_basic 行数: {len(daily_basic)}", flush=True)

    mf_days = max(0, int(args.moneyflow_days))
    moneyflow_dates = trade_dates[-mf_days:] if mf_days > 0 else []
    moneyflow_days = _fetch_moneyflow_days(pro, trade_dates=moneyflow_dates, sleep_s=float(args.sleep_s)) if moneyflow_dates else pd.DataFrame()
    if moneyflow_days is not None and not moneyflow_days.empty:
        print(f"{_now_ts()} moneyflow 行数: {len(moneyflow_days)}", flush=True)

    latest = _prepare_latest_candidates(
        daily_all=daily_all,
        stock_pool=stock_pool,
        daily_basic=daily_basic,
        moneyflow_days=moneyflow_days,
        latest_trade_date=latest_trade_date,
        top_industries=int(args.top_industries),
        min_industry_stocks=int(args.min_industry_stocks),
        require_hot_industry=bool(args.require_hot_industry),
        breakout_ratio=float(args.breakout_ratio),
        breakdown_ratio=float(args.breakdown_ratio),
        max_range20=float(args.max_range20),
        dd60_min=float(args.dd60_min),
        dd60_max=float(args.dd60_max),
        min_score=float(args.min_score),
    )
    if latest is None or latest.empty:
        raise SystemExit("无候选结果（请调整参数或检查数据源）")

    keep_cols = [
        "ts_code",
        "symbol",
        "name",
        "industry",
        "market",
        "industry_hot",
        "ind_ret5_rank",
        "trade_date",
        "score",
        "reason",
        "close",
        "pct_chg",
        "amount",
        "turnover_rate",
        "volume_ratio",
        "vol_ratio10",
        "vol_dry_ratio",
        "ret5",
        "ret20",
        "rsi14",
        "atr14_pct",
        "range20_prev",
        "dd60",
        "ma20",
        "high10_prev",
        "high60_prev",
        "low20_prev",
        "ind_ret5_mean",
        "net_mf_amount",
        "net_mf_3d",
        "pe",
        "pe_ttm",
        "pb",
        "ps_ttm",
        "dv_ttm",
        "total_mv",
        "circ_mv",
    ]
    for c in keep_cols:
        if c not in latest.columns:
            latest[c] = np.nan
    out_df = latest[keep_cols].copy()
    out_df = out_df.head(int(args.max_results)).reset_index(drop=True)

    if str(args.out).strip():
        out_path = str(args.out).strip()
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        out_path = os.path.join(script_dir, f"正在洗盘中_{latest_trade_date}_{datetime.now().strftime('%H%M%S')}.csv")

    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"{_now_ts()} 输出: {out_path} rows={len(out_df)}", flush=True)
    print(out_df.head(30).to_string(index=False), flush=True)


if __name__ == "__main__":
    main()
