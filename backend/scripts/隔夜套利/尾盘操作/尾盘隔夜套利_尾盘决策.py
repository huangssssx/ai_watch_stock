#!/usr/bin/env python3

"""
尾盘隔夜套利：尾盘决策脚本

功能
- 把“盘后候选池”与“尾盘实时强弱”结合，输出一份尾盘可操作的隔夜备选清单 CSV。
- 核心目标：尾盘买入 -> 隔夜持有 -> 次日择机卖出（套利/博弈）时，提高候选的可交易性与次日胜率。

数据来源（尽量用到本项目文档里可用的参数）
- Tushare（主要取前一交易日 / 今日涨跌停价）：
  - daily_basic：换手率/量比/流通市值等
  - moneyflow_*：主力净流入等（优先 moneyflow_dc -> moneyflow_ths -> moneyflow）
  - stk_limit：今日涨停/跌停价（用于风险过滤）
  - limit_list_d（可选）：前一交易日涨停/连板/封单/炸板次数等（情绪强度）
  - margin_detail（可选）：两融明细（融资买入/余额等）
  - top_list（可选）：龙虎榜净买入/净占比等
- pytdx（盘中实时）：
  - get_security_quotes：最新价、五档、涨速、成交额/量、VWAP、买卖盘不平衡等
  - get_security_bars(1分钟)：尾盘短周期动量（可选）

输出（CSV）
- tail_score：尾盘综合分（0-100，越高越优先）
- decision：BUY / WATCH / AVOID（粗粒度决策）
- pass_*：各项关键过滤的布尔结果（点差/盘口强弱/VWAP/动量/跌停风险）

流程（从输入到输出）
1) 读取输入候选（优先从隔夜候选 CSV 取 topk）
2) 计算交易日：asof_trade_date（今天或指定），prev_open_trade_date（上一交易日）
3) 拉取 Tushare 特征（前一交易日为主，今日 stk_limit 用于风险）
4) 拉取 pytdx 尾盘快照（五档/量额/VWAP/涨速）与可选 1 分钟动量特征
5) 合并成一张宽表 -> 计算 tail_score 与 decision -> 落盘 CSV

命令示例
- 默认：自动读取最新“隔夜候选_*.csv”，输出尾盘决策 CSV
  python3 backend/scripts/隔夜套利/尾盘操作/尾盘隔夜套利_尾盘决策.py --auto-input-latest --topk 80

- 强化版：叠加分钟K、昨日涨停信息、两融、龙虎榜（更慢但信息更全）
  python3 backend/scripts/隔夜套利/尾盘操作/尾盘隔夜套利_尾盘决策.py \\
    --auto-input-latest --topk 80 \\
    --with-minute-bars --minute-bars-n 40 \\
    --with-prev-limit --with-margin --with-lhb

- 指定输入文件（不依赖 auto-input-latest）
  python3 backend/scripts/隔夜套利/尾盘操作/尾盘隔夜套利_尾盘决策.py --input-csv backend/scripts/隔夜套利/隔夜候选_YYYYMMDD_xxx.csv

- 只跑一小批并打开调试日志（逐环节耗时/维度/决策统计）
  python3 backend/scripts/隔夜套利/尾盘操作/尾盘隔夜套利_尾盘决策.py --auto-input-latest --topk 20 --with-minute-bars --debug
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict

import pandas as pd


def _now_log_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _log(enabled: bool, msg: str) -> None:
    if not enabled:
        return
    print(f"[{_now_log_ts()}] {msg}", flush=True)


def _shape(df: pd.DataFrame) -> str:
    try:
        return f"{int(df.shape[0])}x{int(df.shape[1])}"
    except Exception:
        return "na"


def _project_root() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(here, "..", "..", "..", ".."))


def _ts_date(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _safe_call(fn, max_retries: int = 2, sleep_s: float = 0.6, **kwargs):
    last = None
    for i in range(max(1, int(max_retries) + 1)):
        try:
            return fn(**kwargs)
        except Exception as e:
            last = e
            if i < max(1, int(max_retries) + 1):
                time.sleep(float(sleep_s))
    raise last


def _last_open_trade_date(pro, ref: Optional[datetime] = None, lookback_days: int = 40) -> str:
    ref = ref or datetime.now()
    end = ref.date()
    start = end - timedelta(days=int(lookback_days))
    df = _safe_call(
        pro.trade_cal,
        exchange="SSE",
        start_date=_ts_date(datetime.combine(start, datetime.min.time())),
        end_date=_ts_date(datetime.combine(end, datetime.min.time())),
        fields="cal_date,is_open",
    )
    if df is None or df.empty:
        raise RuntimeError("trade_cal 返回为空")
    df = df.copy()
    df["cal_date"] = df["cal_date"].astype(str)
    df = df[df["is_open"].astype(int) == 1]
    df = df[df["cal_date"] <= _ts_date(ref)]
    if df.empty:
        raise RuntimeError("trade_cal 无有效开市日期")
    return str(df["cal_date"].max())


def _prev_open_trade_date(pro, trade_date: str, lookback_days: int = 90) -> str:
    trade_date = str(trade_date).strip()
    try:
        ref = datetime.strptime(trade_date, "%Y%m%d")
    except Exception as e:
        raise ValueError(f"trade_date 格式错误: {trade_date}") from e
    end = ref.date()
    start = end - timedelta(days=int(lookback_days))
    df = _safe_call(
        pro.trade_cal,
        exchange="SSE",
        start_date=_ts_date(datetime.combine(start, datetime.min.time())),
        end_date=_ts_date(datetime.combine(end, datetime.min.time())),
        fields="cal_date,is_open",
    )
    if df is None or df.empty:
        raise RuntimeError("trade_cal 返回为空")
    df = df.copy()
    df["cal_date"] = df["cal_date"].astype(str)
    df = df[df["is_open"].astype(int) == 1]
    df = df[df["cal_date"] < trade_date]
    if df.empty:
        raise RuntimeError(f"找不到 {trade_date} 之前的开市日期")
    return str(df["cal_date"].max())


def _to_market_code(ts_code: str) -> Optional[Tuple[int, str]]:
    ts_code = str(ts_code or "").strip()
    if not ts_code:
        return None
    if "." in ts_code:
        code, suf = ts_code.split(".", 1)
        code = code.strip()
        suf = suf.strip().upper()
        if suf == "SZ":
            return 0, code
        if suf == "SH":
            return 1, code
        return None
    if len(ts_code) == 6 and ts_code.startswith("6"):
        return 1, ts_code
    if len(ts_code) == 6:
        return 0, ts_code
    return None


def _chunks(items: List, n: int) -> List[List]:
    n = max(1, int(n))
    return [items[i : i + n] for i in range(0, len(items), n)]


def _rank_pct(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    if s is None or s.isna().all():
        return pd.Series([0.5] * len(s), index=s.index)
    return s.rank(pct=True).fillna(0.5)


def _find_latest_candidates_csv() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    parent = os.path.abspath(os.path.join(here, ".."))
    try:
        files = [f for f in os.listdir(parent) if f.startswith("隔夜候选_") and f.endswith(".csv")]
    except Exception:
        files = []
    if not files:
        return ""
    files.sort(key=lambda x: os.path.getmtime(os.path.join(parent, x)), reverse=True)
    return os.path.join(parent, files[0])


def _fetch_daily_basic_subset(pro, trade_date: str, ts_codes: List[str]) -> pd.DataFrame:
    fields = "ts_code,trade_date,turnover_rate,volume_ratio,total_mv,circ_mv,free_share,float_share"
    df = _safe_call(pro.daily_basic, trade_date=trade_date, fields=fields)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["ts_code"] = df["ts_code"].astype(str)
    keep = set([str(x) for x in ts_codes])
    return df[df["ts_code"].isin(keep)].copy()


def _fetch_moneyflow_subset(pro, trade_date: str, ts_codes: List[str], prefer: str) -> Tuple[pd.DataFrame, str]:
    prefer = str(prefer or "").strip().lower()
    order = []
    if prefer:
        order.append(prefer)
    for t in ["dc", "ths", "moneyflow"]:
        if t not in order:
            order.append(t)

    last_err = None
    for t in order:
        try:
            if t == "dc":
                fields = "ts_code,trade_date,net_amount,net_amount_rate,buy_elg_amount,buy_elg_amount_rate,buy_lg_amount,buy_lg_amount_rate"
                df = _safe_call(pro.moneyflow_dc, trade_date=trade_date, fields=fields)
                if df is None or df.empty:
                    return pd.DataFrame(), "moneyflow_dc"
                df = df.copy()
                df["ts_code"] = df["ts_code"].astype(str)
                df = df[df["ts_code"].isin(set([str(x) for x in ts_codes]))].copy()
                df.rename(
                    columns={
                        "net_amount": "mf_net_amount",
                        "net_amount_rate": "mf_net_amount_rate",
                        "buy_elg_amount": "mf_buy_elg_amount",
                        "buy_elg_amount_rate": "mf_buy_elg_amount_rate",
                        "buy_lg_amount": "mf_buy_lg_amount",
                        "buy_lg_amount_rate": "mf_buy_lg_amount_rate",
                    },
                    inplace=True,
                )
                return df, "moneyflow_dc"
            if t == "ths":
                fields = "ts_code,trade_date,net_amount,net_d5_amount,buy_lg_amount,buy_lg_amount_rate"
                df = _safe_call(pro.moneyflow_ths, trade_date=trade_date, fields=fields)
                if df is None or df.empty:
                    return pd.DataFrame(), "moneyflow_ths"
                df = df.copy()
                df["ts_code"] = df["ts_code"].astype(str)
                df = df[df["ts_code"].isin(set([str(x) for x in ts_codes]))].copy()
                df.rename(
                    columns={
                        "net_amount": "mf_net_amount",
                        "net_d5_amount": "mf_net_d5_amount",
                        "buy_lg_amount": "mf_buy_lg_amount",
                        "buy_lg_amount_rate": "mf_buy_lg_amount_rate",
                    },
                    inplace=True,
                )
                return df, "moneyflow_ths"
            if t == "moneyflow":
                fields = "ts_code,trade_date,net_mf_amount,buy_lg_amount,buy_elg_amount"
                df = _safe_call(pro.moneyflow, trade_date=trade_date, fields=fields)
                if df is None or df.empty:
                    return pd.DataFrame(), "moneyflow"
                df = df.copy()
                df["ts_code"] = df["ts_code"].astype(str)
                df = df[df["ts_code"].isin(set([str(x) for x in ts_codes]))].copy()
                df.rename(
                    columns={
                        "net_mf_amount": "mf_net_amount",
                        "buy_lg_amount": "mf_buy_lg_amount",
                        "buy_elg_amount": "mf_buy_elg_amount",
                    },
                    inplace=True,
                )
                return df, "moneyflow"
        except Exception as e:
            last_err = e
            continue
    if last_err is not None:
        raise last_err
    return pd.DataFrame(), "none"


def _fetch_margin_detail_subset(pro, trade_date: str, ts_codes: List[str], sleep_s: float) -> pd.DataFrame:
    rows = []
    for ts_code in ts_codes:
        try:
            df = _safe_call(
                pro.margin_detail,
                trade_date=trade_date,
                ts_code=str(ts_code),
                fields="ts_code,trade_date,rzye,rzmre,rzche,rqye,rqyl,rqmcl,rzrqye",
            )
            if df is not None and not df.empty:
                rec = df.iloc[0].to_dict()
                rows.append(rec)
        except Exception:
            pass
        if sleep_s and sleep_s > 0:
            time.sleep(float(sleep_s))
    out = pd.DataFrame(rows)
    if not out.empty:
        out["ts_code"] = out["ts_code"].astype(str)
        if "trade_date" in out.columns:
            out.rename(columns={"trade_date": "prev_margin_trade_date"}, inplace=True)
    return out


def _fetch_top_list_subset(pro, trade_date: str, ts_codes: List[str], sleep_s: float) -> pd.DataFrame:
    rows = []
    for ts_code in ts_codes:
        try:
            df = _safe_call(
                pro.top_list,
                trade_date=trade_date,
                ts_code=str(ts_code),
                fields="ts_code,trade_date,turnover_rate,amount,net_amount,net_rate,amount_rate,reason",
            )
            if df is not None and not df.empty:
                rec = df.iloc[0].to_dict()
                rows.append(rec)
        except Exception:
            pass
        if sleep_s and sleep_s > 0:
            time.sleep(float(sleep_s))
    out = pd.DataFrame(rows)
    if not out.empty:
        out["ts_code"] = out["ts_code"].astype(str)
        out.rename(
            columns={
                "trade_date": "prev_lhb_trade_date",
                "turnover_rate": "lhb_turnover_rate",
                "amount": "lhb_amount",
                "net_amount": "lhb_net_amount",
                "net_rate": "lhb_net_rate",
                "amount_rate": "lhb_amount_rate",
                "reason": "lhb_reason",
            },
            inplace=True,
        )
    return out


def _fetch_limit_list_prev_subset(pro, trade_date: str, ts_codes: List[str]) -> pd.DataFrame:
    fields = "ts_code,trade_date,limit_times,open_times,fd_amount,first_time,last_time,limit"
    df = _safe_call(pro.limit_list_d, trade_date=trade_date, fields=fields)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["ts_code"] = df["ts_code"].astype(str)
    df = df[df["ts_code"].isin(set([str(x) for x in ts_codes]))].copy()
    df.rename(
        columns={
            "trade_date": "prev_limit_trade_date",
            "limit_times": "prev_limit_times",
            "open_times": "prev_open_times",
            "fd_amount": "prev_fd_amount",
            "first_time": "prev_first_time",
            "last_time": "prev_last_time",
            "limit": "prev_limit_flag",
        },
        inplace=True,
    )
    return df


def _fetch_stk_limit_subset(pro, trade_date: str, ts_codes: List[str]) -> pd.DataFrame:
    df = _safe_call(pro.stk_limit, trade_date=trade_date, fields="ts_code,trade_date,up_limit,down_limit")
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["ts_code"] = df["ts_code"].astype(str)
    df = df[df["ts_code"].isin(set([str(x) for x in ts_codes]))].copy()
    df.rename(
        columns={
            "trade_date": "today_trade_date",
            "up_limit": "today_up_limit",
            "down_limit": "today_down_limit",
        },
        inplace=True,
    )
    return df


def _tdx_quotes_snapshot(tdx, ts_codes: List[str], chunk_size: int, sleep_s: float) -> pd.DataFrame:
    pairs = []
    keep = []
    for c in ts_codes:
        mc = _to_market_code(c)
        if mc is None:
            continue
        keep.append(c)
        pairs.append(mc)

    rows: List[Dict] = []
    for part in _chunks(list(zip(keep, pairs)), int(chunk_size)):
        req = [p for _, p in part]
        try:
            ret = tdx.get_security_quotes(req)
        except Exception:
            ret = []
        if not isinstance(ret, list):
            ret = []

        for (ts_code, _), q in zip(part, ret):
            if not isinstance(q, dict):
                continue

            price = float(q.get("price") or 0.0)
            last_close = float(q.get("last_close") or 0.0)
            high = float(q.get("high") or 0.0)
            low = float(q.get("low") or 0.0)
            bid1 = float(q.get("bid1") or 0.0)
            ask1 = float(q.get("ask1") or 0.0)
            vol_hand = float(q.get("vol") or 0.0)
            amount_yuan = float(q.get("amount") or 0.0)
            b_vol_hand = float(q.get("b_vol") or 0.0)
            s_vol_hand = float(q.get("s_vol") or 0.0)

            bid_value = 0.0
            ask_value = 0.0
            bid_vol_total = 0.0
            ask_vol_total = 0.0
            for i in range(1, 6):
                bp = float(q.get(f"bid{i}") or 0.0)
                bv = float(q.get(f"bid_vol{i}") or 0.0)
                ap = float(q.get(f"ask{i}") or 0.0)
                av = float(q.get(f"ask_vol{i}") or 0.0)
                bid_value += bp * bv
                ask_value += ap * av
                bid_vol_total += bv
                ask_vol_total += av

            denom = ask_value if ask_value > 0 else 1e-9
            imbalance = bid_value / denom
            if imbalance > 1_000_000.0:
                imbalance = 1_000_000.0
            if price > 0 and bid1 > 0 and ask1 > 0 and ask1 >= bid1:
                spread_bp = (ask1 - bid1) / price * 10000.0
            else:
                spread_bp = float("nan")
            speed_pct = float(q.get("reversed_bytes9") or 0.0) / 100.0
            gap_pct = ((price - last_close) / last_close * 100.0) if last_close > 0 else float("nan")
            vwap = (amount_yuan / (vol_hand * 100.0)) if vol_hand > 0 else float("nan")
            b_ratio = (b_vol_hand / vol_hand) if vol_hand > 0 else float("nan")
            pos_in_range = ((price - low) / (high - low)) if (high > low and price > 0) else float("nan")
            above_vwap = ((price - vwap) / price * 100.0) if (price > 0 and vwap == vwap) else float("nan")

            rows.append(
                {
                    "ts_code": ts_code,
                    "rt_servertime": str(q.get("servertime") or ""),
                    "rt_price": price,
                    "rt_last_close": last_close,
                    "rt_gap_pct": gap_pct,
                    "rt_high": high,
                    "rt_low": low,
                    "rt_pos_in_range": pos_in_range,
                    "rt_speed_pct": speed_pct,
                    "rt_spread_bp": spread_bp,
                    "rt_imbalance": imbalance,
                    "rt_bid1": bid1,
                    "rt_ask1": ask1,
                    "rt_bid_value": bid_value,
                    "rt_ask_value": ask_value,
                    "rt_bid_vol_5": bid_vol_total,
                    "rt_ask_vol_5": ask_vol_total,
                    "rt_vol_hand": vol_hand,
                    "rt_amount_yuan": amount_yuan,
                    "rt_vwap": vwap,
                    "rt_above_vwap_pct": above_vwap,
                    "rt_b_vol_hand": b_vol_hand,
                    "rt_s_vol_hand": s_vol_hand,
                    "rt_b_ratio": b_ratio,
                }
            )

        if sleep_s and sleep_s > 0:
            time.sleep(float(sleep_s))

    df = pd.DataFrame(rows)
    if not df.empty:
        df["ts_code"] = df["ts_code"].astype(str)
    return df


def _tdx_minute_bars_features(tdx, ts_codes: List[str], n: int, sleep_s: float) -> pd.DataFrame:
    n = max(10, int(n))
    rows = []
    for ts_code in ts_codes:
        mc = _to_market_code(ts_code)
        if mc is None:
            continue
        market, code = mc
        try:
            bars = tdx.get_security_bars(8, int(market), str(code), 0, int(n))
        except Exception:
            bars = []
        if not isinstance(bars, list) or not bars:
            if sleep_s and sleep_s > 0:
                time.sleep(float(sleep_s))
            continue

        df = pd.DataFrame(bars)
        for c in ["open", "close", "high", "low", "vol", "amount"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["open", "close"], how="any")
        if df.empty:
            if sleep_s and sleep_s > 0:
                time.sleep(float(sleep_s))
            continue

        df = df.iloc[::-1].reset_index(drop=True)
        o0 = float(df["open"].iloc[0] or 0.0)
        c_last = float(df["close"].iloc[-1] or 0.0)
        ret_n = ((c_last - o0) / o0 * 100.0) if o0 > 0 else float("nan")

        def _ret_last(k: int) -> float:
            k = int(k)
            if len(df) < k + 1:
                return float("nan")
            c0 = float(df["close"].iloc[-k - 1] or 0.0)
            c1 = float(df["close"].iloc[-1] or 0.0)
            return ((c1 - c0) / c0 * 100.0) if c0 > 0 else float("nan")

        ret_5 = _ret_last(5)
        ret_15 = _ret_last(15)

        vol = pd.to_numeric(df.get("vol"), errors="coerce")
        mvol_ratio_5 = float("nan")
        if vol is not None and not vol.isna().all() and len(vol) >= 10:
            v_last5 = float(vol.tail(5).sum())
            v_prev = vol.iloc[:-5]
            v_prev_mean = float(v_prev.mean()) if len(v_prev) > 0 else float("nan")
            if v_prev_mean == v_prev_mean and v_prev_mean > 0:
                mvol_ratio_5 = v_last5 / (v_prev_mean * 5.0)

        green = (df["close"] > df["open"]).sum()
        red = (df["close"] < df["open"]).sum()
        trend_score = (green - red) / max(1, (green + red))
        last_dt = str(df.get("datetime").iloc[-1]) if "datetime" in df.columns else ""

        rows.append(
            {
                "ts_code": str(ts_code),
                "mbar_last_dt": last_dt,
                "mret_n": ret_n,
                "mret_5": ret_5,
                "mret_15": ret_15,
                "mvol_ratio_5": mvol_ratio_5,
                "mtrend_score": trend_score,
                "mbar_n": int(len(df)),
            }
        )
        if sleep_s and sleep_s > 0:
            time.sleep(float(sleep_s))

    out = pd.DataFrame(rows)
    if not out.empty:
        out["ts_code"] = out["ts_code"].astype(str)
    return out


def _score_tail(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for c in [
        "score",
        "rt_gap_pct",
        "rt_pos_in_range",
        "rt_above_vwap_pct",
        "rt_imbalance",
        "rt_spread_bp",
        "rt_b_ratio",
        "rt_amount_yuan",
        "mret_n",
        "mret_5",
        "mret_15",
        "mvol_ratio_5",
        "turnover_rate",
        "volume_ratio",
        "circ_mv",
        "mf_net_amount",
        "mf_net_amount_rate",
        "mf_net_d5_amount",
        "mf_buy_elg_amount",
        "mf_buy_lg_amount",
        "rzmre",
        "rzye",
        "lhb_net_rate",
        "prev_limit_times",
        "prev_open_times",
        "prev_fd_amount",
        "today_up_limit",
        "today_down_limit",
        "rt_price",
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    base_score = _rank_pct(df["score"]) if "score" in df.columns else pd.Series([0.5] * len(df), index=df.index)
    gap = df.get("rt_gap_pct")
    gap_score = (1.0 - _rank_pct((gap - 2.0).abs())) if gap is not None else pd.Series([0.5] * len(df), index=df.index)

    pos = df.get("rt_pos_in_range")
    pos_score = _rank_pct(pos) if pos is not None else pd.Series([0.5] * len(df), index=df.index)

    vwap_above = df.get("rt_above_vwap_pct")
    vwap_score = _rank_pct(vwap_above) if vwap_above is not None else pd.Series([0.5] * len(df), index=df.index)

    imb = df.get("rt_imbalance")
    imb_score = _rank_pct(imb) if imb is not None else pd.Series([0.5] * len(df), index=df.index)

    b_ratio = df.get("rt_b_ratio")
    b_score = _rank_pct(b_ratio) if b_ratio is not None else pd.Series([0.5] * len(df), index=df.index)

    spread = df.get("rt_spread_bp")
    spread_score = (1.0 - _rank_pct(spread)) if spread is not None else pd.Series([0.5] * len(df), index=df.index)

    mret = df.get("mret_n")
    mret_score = _rank_pct(mret) if mret is not None else pd.Series([0.5] * len(df), index=df.index)

    mvolr = df.get("mvol_ratio_5")
    mvol_score = _rank_pct(mvolr) if mvolr is not None else pd.Series([0.5] * len(df), index=df.index)

    mf = df.get("mf_net_amount")
    mf_score = _rank_pct(mf) if mf is not None else pd.Series([0.5] * len(df), index=df.index)

    rz = df.get("rzmre")
    rz_score = _rank_pct(rz) if rz is not None else pd.Series([0.5] * len(df), index=df.index)

    lhb = df.get("lhb_net_rate")
    lhb_score = _rank_pct(lhb) if lhb is not None else pd.Series([0.5] * len(df), index=df.index)

    prev_fd = df.get("prev_fd_amount")
    prev_fd_score = _rank_pct(prev_fd) if prev_fd is not None else pd.Series([0.5] * len(df), index=df.index)

    raw = (
        18.0 * base_score
        + 10.0 * gap_score
        + 12.0 * pos_score
        + 12.0 * vwap_score
        + 10.0 * imb_score
        + 6.0 * b_score
        + 8.0 * spread_score
        + 10.0 * mret_score
        + 6.0 * mvol_score
        + 4.0 * mf_score
        + 2.0 * rz_score
        + 1.0 * lhb_score
        + 1.0 * prev_fd_score
    )
    df["tail_score"] = raw

    df["pass_spread"] = (df.get("rt_spread_bp").fillna(1e9) <= 80.0) if "rt_spread_bp" in df.columns else False
    df["pass_imbalance"] = (df.get("rt_imbalance").fillna(0.0) >= 1.15) if "rt_imbalance" in df.columns else False
    df["pass_vwap"] = (df.get("rt_above_vwap_pct").fillna(-1e9) >= -0.05) if "rt_above_vwap_pct" in df.columns else False
    if "mret_n" in df.columns:
        df["pass_momentum"] = df.get("mret_n").fillna(-1e9) >= -0.05
    else:
        pos = df.get("rt_pos_in_range")
        above = df.get("rt_above_vwap_pct")
        speed = df.get("rt_speed_pct")
        ok_pos = (pos.fillna(0.0) >= 0.65) if pos is not None else True
        ok_above = (above.fillna(-1e9) >= -0.10) if above is not None else True
        ok_speed = (speed.fillna(-1e9) >= -0.10) if speed is not None else True
        df["pass_momentum"] = ok_pos & ok_above & ok_speed

    df["near_down_limit"] = False
    if "today_down_limit" in df.columns and "rt_price" in df.columns:
        dd = df["today_down_limit"]
        px = df["rt_price"]
        df["near_down_limit"] = (dd.notna()) & (px.notna()) & (px <= dd * 1.02)

    df["decision"] = "WATCH"
    buy_mask = (
        (df["tail_score"].fillna(0.0) >= df["tail_score"].fillna(0.0).quantile(0.80))
        & df["pass_spread"].fillna(False)
        & df["pass_imbalance"].fillna(False)
        & df["pass_vwap"].fillna(False)
        & df["pass_momentum"].fillna(False)
        & (~df["near_down_limit"].fillna(False))
    )
    df.loc[buy_mask, "decision"] = "BUY"
    df.loc[df["near_down_limit"].fillna(False), "decision"] = "AVOID"

    df["tail_score"] = pd.to_numeric(df["tail_score"], errors="coerce").fillna(0.0)
    df["tail_score"] = (df["tail_score"] / max(1e-9, float(df["tail_score"].max())) * 100.0).round(2)

    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="尾盘操作：隔夜套利尾盘决策（盘中快照 + 盘后/历史特征）")
    parser.add_argument("--input-csv", type=str, default="")
    parser.add_argument("--auto-input-latest", action="store_true")
    parser.add_argument("--ts-codes", type=str, default="")
    parser.add_argument("--topk", type=int, default=80)
    parser.add_argument("--asof-trade-date", type=str, default="")
    parser.add_argument("--prefer-moneyflow", type=str, default="dc", choices=["dc", "ths", "moneyflow"])
    parser.add_argument("--with-minute-bars", action="store_true")
    parser.add_argument("--minute-bars-n", type=int, default=40)
    parser.add_argument("--with-margin", action="store_true")
    parser.add_argument("--with-lhb", action="store_true")
    parser.add_argument("--with-prev-limit", action="store_true")
    parser.add_argument("--tdx-chunk-size", type=int, default=80)
    parser.add_argument("--tdx-sleep-s", type=float, default=0.2)
    parser.add_argument("--ts-sleep-s", type=float, default=0.15)
    parser.add_argument("--out", type=str, default="")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    root = _project_root()
    if root not in sys.path:
        sys.path.insert(0, root)

    if bool(args.dry_run):
        print("dry_run=1，不请求数据")
        return
    debug = bool(args.debug)
    _log(debug, "start tail_decision")

    try:
        from backend.utils.tushare_client import pro
    except Exception as e:
        raise SystemExit(f"导入 tushare_client 失败: {type(e).__name__}:{e}")
    if pro is None:
        raise SystemExit("pro=None，无法执行（请检查 tushare token/config）")
    _log(debug, "tushare pro ready")

    try:
        from backend.utils.pytdx_client import tdx
    except Exception as e:
        raise SystemExit(f"导入 pytdx_client 失败: {type(e).__name__}:{e}")
    _log(debug, "pytdx tdx ready")

    input_csv = str(args.input_csv).strip()
    if not input_csv and bool(args.auto_input_latest):
        input_csv = _find_latest_candidates_csv()
    _log(debug, f"input_csv={input_csv or 'manual'} topk={int(args.topk)}")

    base_df = pd.DataFrame()
    if input_csv:
        base_df = pd.read_csv(input_csv)
        if "ts_code" not in base_df.columns:
            raise SystemExit("input-csv 缺少 ts_code 列")
        base_df["ts_code"] = base_df["ts_code"].astype(str)
        if "score" in base_df.columns:
            base_df["score"] = pd.to_numeric(base_df["score"], errors="coerce")
            base_df = base_df.sort_values("score", ascending=False, na_position="last")
        base_df = base_df.head(max(1, int(args.topk))).copy()
    else:
        raw = str(args.ts_codes).strip()
        if not raw:
            raise SystemExit("未提供股票列表（用 --input-csv/--auto-input-latest 或 --ts-codes）")
        ts_codes = [x.strip() for x in raw.split(",") if x.strip()]
        base_df = pd.DataFrame({"ts_code": ts_codes[: max(1, int(args.topk))]})
    _log(debug, f"base_df={_shape(base_df)} cols={len(base_df.columns)}")

    ts_codes = base_df["ts_code"].astype(str).dropna().tolist()
    ts_codes = list(dict.fromkeys(ts_codes))
    _log(debug, f"unique ts_codes={len(ts_codes)}")

    today = _ts_date(datetime.now())
    asof = str(args.asof_trade_date).strip()
    if not asof:
        asof = _last_open_trade_date(pro)
    _log(debug, f"asof_trade_date={asof} today={today}")

    prev_open = _prev_open_trade_date(pro, asof)
    _log(debug, f"prev_open_trade_date={prev_open}")

    t0 = time.time()
    daily_basic = _fetch_daily_basic_subset(pro, trade_date=prev_open, ts_codes=ts_codes)
    _log(debug, f"daily_basic(prev_open)={_shape(daily_basic)} elapsed_s={time.time()-t0:.2f}")

    t0 = time.time()
    moneyflow, moneyflow_source = _fetch_moneyflow_subset(
        pro, trade_date=prev_open, ts_codes=ts_codes, prefer=str(args.prefer_moneyflow)
    )
    _log(debug, f"moneyflow(prev_open) source={moneyflow_source} {_shape(moneyflow)} elapsed_s={time.time()-t0:.2f}")

    stk_limit = pd.DataFrame()
    t0 = time.time()
    try:
        stk_limit = _fetch_stk_limit_subset(pro, trade_date=today, ts_codes=ts_codes)
    except Exception:
        stk_limit = pd.DataFrame()
    _log(debug, f"stk_limit(today)={_shape(stk_limit)} elapsed_s={time.time()-t0:.2f}")

    prev_limit = pd.DataFrame()
    if bool(args.with_prev_limit):
        t0 = time.time()
        try:
            prev_limit = _fetch_limit_list_prev_subset(pro, trade_date=prev_open, ts_codes=ts_codes)
        except Exception:
            prev_limit = pd.DataFrame()
        _log(debug, f"prev_limit(prev_open)={_shape(prev_limit)} elapsed_s={time.time()-t0:.2f}")

    margin = pd.DataFrame()
    if bool(args.with_margin):
        t0 = time.time()
        margin = _fetch_margin_detail_subset(
            pro, trade_date=prev_open, ts_codes=ts_codes, sleep_s=float(args.ts_sleep_s)
        )
        _log(debug, f"margin_detail(prev_open)={_shape(margin)} elapsed_s={time.time()-t0:.2f}")

    lhb = pd.DataFrame()
    if bool(args.with_lhb):
        t0 = time.time()
        lhb = _fetch_top_list_subset(pro, trade_date=prev_open, ts_codes=ts_codes, sleep_s=float(args.ts_sleep_s))
        _log(debug, f"top_list(prev_open)={_shape(lhb)} elapsed_s={time.time()-t0:.2f}")

    with tdx:
        t0 = time.time()
        snap = _tdx_quotes_snapshot(
            tdx, ts_codes=ts_codes, chunk_size=int(args.tdx_chunk_size), sleep_s=float(args.tdx_sleep_s)
        )
        _log(debug, f"tdx_quotes={_shape(snap)} elapsed_s={time.time()-t0:.2f}")

        mbar = pd.DataFrame()
        if bool(args.with_minute_bars):
            t0 = time.time()
            mbar = _tdx_minute_bars_features(
                tdx, ts_codes=ts_codes, n=int(args.minute_bars_n), sleep_s=float(args.tdx_sleep_s)
            )
            _log(debug, f"tdx_minute_bars(n={int(args.minute_bars_n)})={_shape(mbar)} elapsed_s={time.time()-t0:.2f}")

    out_df = base_df.copy()
    out_df["asof_trade_date"] = asof
    out_df["prev_open_trade_date"] = prev_open
    out_df["run_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _log(debug, f"merge_start out_df={_shape(out_df)}")

    out_df = out_df.merge(daily_basic, on="ts_code", how="left", suffixes=("", "_db"))
    _log(debug, f"merge daily_basic => out_df={_shape(out_df)}")
    out_df = out_df.merge(moneyflow, on="ts_code", how="left", suffixes=("", "_mf"))
    _log(debug, f"merge moneyflow => out_df={_shape(out_df)}")
    out_df = out_df.merge(stk_limit, on="ts_code", how="left")
    _log(debug, f"merge stk_limit => out_df={_shape(out_df)}")
    if not prev_limit.empty:
        out_df = out_df.merge(prev_limit, on="ts_code", how="left")
        _log(debug, f"merge prev_limit => out_df={_shape(out_df)}")
    if not margin.empty:
        out_df = out_df.merge(margin, on="ts_code", how="left")
        _log(debug, f"merge margin => out_df={_shape(out_df)}")
    if not lhb.empty:
        out_df = out_df.merge(lhb, on="ts_code", how="left")
        _log(debug, f"merge lhb => out_df={_shape(out_df)}")
    out_df = out_df.merge(snap, on="ts_code", how="left")
    _log(debug, f"merge snap => out_df={_shape(out_df)}")
    if not mbar.empty:
        out_df = out_df.merge(mbar, on="ts_code", how="left")
        _log(debug, f"merge mbar => out_df={_shape(out_df)}")

    out_df["moneyflow_source"] = moneyflow_source
    _log(debug, "score_tail start")
    out_df = _score_tail(out_df)
    _log(debug, f"score_tail done decision_counts={out_df['decision'].value_counts(dropna=False).to_dict()}")
    out_df = out_df.sort_values(["decision", "tail_score"], ascending=[True, False], na_position="last")

    out = str(args.out).strip()
    if not out:
        out_dir = os.path.dirname(os.path.abspath(__file__))
        out = os.path.join(out_dir, f"尾盘决策_{asof}_{_now_ts()}.csv")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    out_df.to_csv(out, index=False, encoding="utf-8-sig")

    _log(debug, f"saved out={out} rows={len(out_df)} cols={len(out_df.columns)}")
    print(
        f"asof_trade_date={asof} prev_open_trade_date={prev_open} moneyflow_source={moneyflow_source} rows={len(out_df)} out={out}"
    )
    show_cols = [
        "ts_code",
        "name",
        "tail_score",
        "decision",
        "rt_gap_pct",
        "rt_pos_in_range",
        "rt_above_vwap_pct",
        "rt_imbalance",
        "rt_spread_bp",
        "rt_speed_pct",
        "mret_n",
        "mvol_ratio_5",
        "turnover_rate",
        "volume_ratio",
        "mf_net_amount",
    ]
    show_cols = [c for c in show_cols if c in out_df.columns]
    with pd.option_context("display.max_rows", 80, "display.max_columns", 80, "display.width", 240):
        print(out_df[show_cols].head(min(60, len(out_df))).to_string(index=False))


if __name__ == "__main__":
    main()
