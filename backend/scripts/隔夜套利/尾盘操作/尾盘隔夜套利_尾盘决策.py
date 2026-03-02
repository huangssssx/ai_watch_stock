#!/usr/bin/env python3

"""
尾盘隔夜套利：尾盘决策脚本

功能
- 输出尾盘可操作的隔夜备选清单 CSV（tail_score + decision）。
- 支持两种输入模式：
  - 候选池模式：读取“盘后候选池”CSV + 叠加“尾盘实时强弱”
  - 全市场扫描：买入时点全市场计算并直接输出 topk
- 附带回测模式：用历史日线近似尾盘信号，评估“次日是否给过冲高/盈利机会”

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
- 回测明细：每条记录包含 buy_date/sell_date/buy_price/next_high/hit_next_high 等
- 回测日汇总：按 buy_date 汇总 trades + high_hit（每日命中率曲线）

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
  python3 backend/scripts/隔夜套利/尾盘操作/尾盘隔夜套利_尾盘决策.py  --auto-input-latest --topk 80 --with-minute-bars --minute-bars-n 40 --with-prev-limit --with-margin --with-lhb

- 指定输入文件（不依赖 auto-input-latest）
  python3 backend/scripts/隔夜套利/尾盘操作/尾盘隔夜套利_尾盘决策.py --input-csv backend/scripts/隔夜套利/隔夜候选_YYYYMMDD_xxx.csv

- 全市场扫描（买入时点直接扫全市场输出 topk）
- 14:35~14:45 预扫（建观察池）
- 目的：从全市场先抓一批“可能会进前列”的候选（比如 topk 200），你和 AI 开始盯盘、看题材/盘口/大盘强弱。
- 14:50~14:55 二次扫描（收敛到可买清单）
- 目的：尾盘动量、VWAP 偏离、买卖盘不平衡等信号更接近最终状态，这时做 topk 50 的“买入候选清单”更靠谱。
- 14:56~14:58 最终扫描 + 分批下单（最关键）
- 目的：尽量贴近收盘价完成买入，减少盘中反复造成的信号漂移；同时留出下单/成交时间，避免卡到最后几秒成交不确定。

  python3 backend/scripts/隔夜套利/尾盘操作/尾盘隔夜套利_尾盘决策.py --scan-all --topk 50

- 回测：近 N 个交易日，命中定义为“次日最高价 > 买入价 * (1 + high_hit_pct%)”
  python3 backend/scripts/隔夜套利/尾盘操作/尾盘隔夜套利_尾盘决策.py --backtest --scan-all --topk 50 --bt-last-n 20 --bt-high-hit-pct 0.0 --bt-sell close

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


def _fetch_daily_ohlc_subset(pro, trade_date: str, ts_codes: List[str]) -> pd.DataFrame:
    fields = "ts_code,trade_date,open,high,low,close,pre_close,pct_chg,vol,amount"
    df = _safe_call(pro.daily, trade_date=trade_date, fields=fields)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["ts_code"] = df["ts_code"].astype(str)
    keep = set([str(x) for x in ts_codes])
    return df[df["ts_code"].isin(keep)].copy()


def _proxy_intraday_from_daily(daily_df: pd.DataFrame) -> pd.DataFrame:
    if daily_df is None or daily_df.empty:
        return pd.DataFrame()
    df = daily_df.copy()
    df["ts_code"] = df["ts_code"].astype(str)
    for c in ["open", "high", "low", "close", "pre_close", "pct_chg", "amount"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    close = df.get("close")
    high = df.get("high")
    low = df.get("low")
    pos_in_range = pd.Series([float("nan")] * len(df), index=df.index)
    if close is not None and high is not None and low is not None:
        denom = (high - low).replace(0.0, float("nan"))
        pos_in_range = (close - low) / denom

    df_out = pd.DataFrame(
        {
            "ts_code": df["ts_code"],
            "rt_servertime": "",
            "rt_price": close,
            "rt_last_close": df.get("pre_close"),
            "rt_gap_pct": df.get("pct_chg"),
            "rt_high": high,
            "rt_low": low,
            "rt_pos_in_range": pos_in_range,
            "rt_speed_pct": 0.0,
            "rt_spread_bp": 10.0,
            "rt_imbalance": 2.0,
            "rt_bid1": close,
            "rt_ask1": close,
            "rt_bid_value": 1.0,
            "rt_ask_value": 1.0,
            "rt_bid_vol_5": 1.0,
            "rt_ask_vol_5": 1.0,
            "rt_vol_hand": float("nan"),
            "rt_amount_yuan": pd.to_numeric(df.get("amount"), errors="coerce").fillna(0.0) * 1000.0,
            "rt_vwap": float("nan"),
            "rt_above_vwap_pct": 0.0,
            "rt_b_vol_hand": float("nan"),
            "rt_s_vol_hand": float("nan"),
            "rt_b_ratio": 0.55,
        }
    )
    return df_out


def _trade_dates_in_range(pro, start_date: str, end_date: str) -> List[str]:
    start_date = str(start_date).strip()
    end_date = str(end_date).strip()
    if not start_date or not end_date:
        raise ValueError("start_date/end_date 不能为空")
    df = _safe_call(
        pro.trade_cal,
        exchange="SSE",
        start_date=start_date,
        end_date=end_date,
        fields="cal_date,is_open",
    )
    if df is None or df.empty:
        return []
    df = df.copy()
    df["cal_date"] = df["cal_date"].astype(str)
    df = df[df["is_open"].astype(int) == 1]
    return df["cal_date"].sort_values().tolist()


def _backtest_sell_price_row(row: pd.Series, mode: str) -> float:
    mode = str(mode or "").strip().lower()
    if mode == "open":
        return float(row.get("open") or float("nan"))
    if mode == "high":
        return float(row.get("high") or float("nan"))
    return float(row.get("close") or float("nan"))


def _run_backtest(
    pro,
    scan_all: bool,
    include_st: bool,
    include_suspended: bool,
    start_date: str,
    end_date: str,
    topk: int,
    prefer_moneyflow: str,
    sell_price_mode: str,
    high_hit_pct: float,
    tp_pct: float,
    sl_pct: float,
    out_csv: str,
    debug: bool,
) -> None:
    dates = _trade_dates_in_range(pro, start_date=start_date, end_date=end_date)
    if len(dates) < 2:
        raise SystemExit("回测区间内开市日不足 2 天")

    sb = pd.DataFrame()
    if bool(scan_all):
        sb = _safe_call(pro.stock_basic, exchange="", list_status="L", fields="ts_code,name")
        if sb is None or sb.empty:
            raise SystemExit("stock_basic 返回为空，无法回测 --scan-all")
        sb = sb.copy()
        sb["ts_code"] = sb["ts_code"].astype(str)
        if "name" in sb.columns:
            sb["name"] = sb["name"].astype(str)
        else:
            sb["name"] = ""
        sb = sb[sb["ts_code"].str.endswith((".SZ", ".SH"))].copy()

    st_cache: Dict[str, set] = {}
    susp_cache: Dict[str, set] = {}

    trades: List[Dict] = []
    for i in range(1, len(dates) - 1):
        asof = str(dates[i])
        prev_open = str(dates[i - 1])
        next_open = str(dates[i + 1])

        base_df = pd.DataFrame()
        if bool(scan_all):
            day_sb = sb.copy()
            if not bool(include_st):
                st_set = st_cache.get(asof)
                if st_set is None:
                    try:
                        st = _safe_call(pro.stock_st, trade_date=asof, fields="ts_code")
                        st_set = set(st["ts_code"].astype(str).tolist()) if st is not None and not st.empty else set()
                    except Exception:
                        st_set = set()
                    st_cache[asof] = st_set
                if st_set:
                    day_sb = day_sb[~day_sb["ts_code"].isin(st_set)].copy()

            if not bool(include_suspended):
                susp_set = susp_cache.get(asof)
                if susp_set is None:
                    try:
                        susp = _safe_call(pro.suspend_d, trade_date=asof, suspend_type="S", fields="ts_code")
                        susp_set = (
                            set(susp["ts_code"].astype(str).tolist()) if susp is not None and not susp.empty else set()
                        )
                    except Exception:
                        susp_set = set()
                    susp_cache[asof] = susp_set
                if susp_set:
                    day_sb = day_sb[~day_sb["ts_code"].isin(susp_set)].copy()
            base_df = day_sb[["ts_code", "name"]].drop_duplicates().copy()
        else:
            raise SystemExit("回测目前仅支持 --scan-all（需要全市场股票池）")

        ts_codes = base_df["ts_code"].astype(str).dropna().tolist()
        ts_codes = list(dict.fromkeys(ts_codes))
        if not ts_codes:
            continue

        daily_today = _fetch_daily_ohlc_subset(pro, trade_date=asof, ts_codes=ts_codes)
        if daily_today.empty:
            continue
        proxy_rt = _proxy_intraday_from_daily(daily_today)

        daily_basic = _fetch_daily_basic_subset(pro, trade_date=prev_open, ts_codes=ts_codes)
        moneyflow, moneyflow_source = _fetch_moneyflow_subset(
            pro, trade_date=prev_open, ts_codes=ts_codes, prefer=str(prefer_moneyflow)
        )
        stk_limit = pd.DataFrame()
        try:
            stk_limit = _fetch_stk_limit_subset(pro, trade_date=asof, ts_codes=ts_codes)
        except Exception:
            stk_limit = pd.DataFrame()

        out_df = base_df.copy()
        out_df["asof_trade_date"] = asof
        out_df["prev_open_trade_date"] = prev_open
        out_df["run_at"] = asof
        out_df = out_df.merge(daily_basic, on="ts_code", how="left")
        out_df = out_df.merge(moneyflow, on="ts_code", how="left")
        out_df = out_df.merge(stk_limit, on="ts_code", how="left")
        out_df = out_df.merge(proxy_rt, on="ts_code", how="left")
        out_df["moneyflow_source"] = moneyflow_source

        out_df = _score_tail(out_df)
        decision_order = {"BUY": 0, "WATCH": 1, "AVOID": 2}
        out_df["_decision_rank"] = out_df["decision"].map(decision_order).fillna(9).astype(int)
        out_df = out_df.sort_values(["_decision_rank", "tail_score"], ascending=[True, False], na_position="last")
        out_df.drop(columns=["_decision_rank"], inplace=True)
        out_df = out_df.head(max(1, int(topk))).copy()

        buy_df = out_df[out_df["decision"] == "BUY"].copy()
        if buy_df.empty:
            if debug:
                _log(debug, f"bt {asof}: BUY=0 out={len(out_df)}")
            continue

        buy_codes = buy_df["ts_code"].astype(str).tolist()
        next_daily = _fetch_daily_ohlc_subset(pro, trade_date=next_open, ts_codes=buy_codes)
        if next_daily.empty:
            continue
        next_daily = next_daily.copy()
        next_daily["ts_code"] = next_daily["ts_code"].astype(str)
        next_map = next_daily.set_index("ts_code").to_dict(orient="index")

        buy_today_map = daily_today.set_index("ts_code").to_dict(orient="index")
        for _, r in buy_df.iterrows():
            ts_code = str(r.get("ts_code") or "")
            if not ts_code or ts_code not in buy_today_map or ts_code not in next_map:
                continue
            t0 = buy_today_map[ts_code]
            t1 = next_map[ts_code]
            buy_px = float(t0.get("close") or float("nan"))
            sell_px = _backtest_sell_price_row(pd.Series(t1), sell_price_mode)
            if not (buy_px == buy_px and sell_px == sell_px and buy_px > 0):
                continue

            next_open_px = float(t1.get("open") or float("nan"))
            next_high_px = float(t1.get("high") or float("nan"))
            next_low_px = float(t1.get("low") or float("nan"))
            next_close_px = float(t1.get("close") or float("nan"))

            ret = sell_px / buy_px - 1.0
            mfe = (next_high_px / buy_px - 1.0) if (next_high_px == next_high_px and buy_px > 0) else float("nan")
            mae = (next_low_px / buy_px - 1.0) if (next_low_px == next_low_px and buy_px > 0) else float("nan")
            high_hit_threshold = buy_px * (1.0 + float(high_hit_pct) / 100.0)
            hit_next_high = bool(next_high_px == next_high_px and next_high_px > high_hit_threshold)
            hit_tp = bool(mfe == mfe and mfe >= float(tp_pct) / 100.0)
            hit_sl = bool(mae == mae and mae <= float(sl_pct) / 100.0)

            trades.append(
                {
                    "buy_date": asof,
                    "sell_date": next_open,
                    "ts_code": ts_code,
                    "name": str(r.get("name") or ""),
                    "tail_score": float(r.get("tail_score") or 0.0),
                    "buy_price": buy_px,
                    "sell_price_mode": str(sell_price_mode),
                    "sell_price": sell_px,
                    "ret_pct": ret * 100.0,
                    "next_open": next_open_px,
                    "next_high": next_high_px,
                    "next_low": next_low_px,
                    "next_close": next_close_px,
                    "mfe_pct": mfe * 100.0 if mfe == mfe else float("nan"),
                    "mae_pct": mae * 100.0 if mae == mae else float("nan"),
                    "high_hit_pct": float(high_hit_pct),
                    "high_hit_threshold": high_hit_threshold,
                    "hit_next_high": hit_next_high,
                    "hit_tp": hit_tp,
                    "hit_sl": hit_sl,
                }
            )

        if debug:
            _log(debug, f"bt {asof}: BUY={len(buy_df)} trades_total={len(trades)}")

    if not trades:
        raise SystemExit("回测区间内无交易记录（可能 BUY 条件过严或数据缺失）")

    trades_df = pd.DataFrame(trades)
    trades_df["ret_pct"] = pd.to_numeric(trades_df["ret_pct"], errors="coerce")
    trades_df["hit_next_high"] = trades_df["hit_next_high"].astype(bool)
    trades_df["hit_tp"] = trades_df["hit_tp"].astype(bool)
    trades_df["hit_sl"] = trades_df["hit_sl"].astype(bool)

    win_rate = float((trades_df["ret_pct"] > 0).mean() * 100.0)
    high_hit_rate = float(trades_df["hit_next_high"].mean() * 100.0)
    avg_ret = float(trades_df["ret_pct"].mean())
    med_ret = float(trades_df["ret_pct"].median())
    tp_rate = float(trades_df["hit_tp"].mean() * 100.0)
    sl_rate = float(trades_df["hit_sl"].mean() * 100.0)

    if not out_csv:
        out_dir = os.path.dirname(os.path.abspath(__file__))
        out_csv = os.path.join(out_dir, f"回测_尾盘决策_{start_date}_{end_date}_{_now_ts()}.csv")
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    trades_df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    if str(out_csv).lower().endswith(".csv"):
        daily_csv = out_csv[:-4] + "_daily.csv"
    else:
        daily_csv = out_csv + "_daily.csv"
    daily_df = (
        trades_df.groupby("buy_date", as_index=False)
        .agg(
            trades=("ts_code", "count"),
            high_hit=("hit_next_high", "mean"),
            win_rate=("ret_pct", lambda s: (pd.to_numeric(s, errors="coerce") > 0).mean()),
            avg_ret=("ret_pct", "mean"),
            med_ret=("ret_pct", "median"),
            tp_hit=("hit_tp", "mean"),
            sl_hit=("hit_sl", "mean"),
        )
        .copy()
    )
    daily_df["high_hit"] = (pd.to_numeric(daily_df["high_hit"], errors="coerce") * 100.0).round(2)
    daily_df["win_rate"] = (pd.to_numeric(daily_df["win_rate"], errors="coerce") * 100.0).round(2)
    daily_df["tp_hit"] = (pd.to_numeric(daily_df["tp_hit"], errors="coerce") * 100.0).round(2)
    daily_df["sl_hit"] = (pd.to_numeric(daily_df["sl_hit"], errors="coerce") * 100.0).round(2)
    daily_df["avg_ret"] = pd.to_numeric(daily_df["avg_ret"], errors="coerce").round(3)
    daily_df["med_ret"] = pd.to_numeric(daily_df["med_ret"], errors="coerce").round(3)
    daily_df.to_csv(daily_csv, index=False, encoding="utf-8-sig")

    print(
        f"backtest start={start_date} end={end_date} trades={len(trades_df)} win_rate={win_rate:.2f}% "
        f"high_hit={high_hit_rate:.2f}% high_hit_pct={float(high_hit_pct):.2f}% avg_ret={avg_ret:.3f}% med_ret={med_ret:.3f}% "
        f"tp_hit={tp_rate:.2f}% sl_hit={sl_rate:.2f}% out={out_csv} daily_out={daily_csv}"
    )


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
    parser.add_argument("--scan-all", action="store_true")
    parser.add_argument("--include-st", action="store_true")
    parser.add_argument("--include-suspended", action="store_true")
    parser.add_argument("--topk", type=int, default=80)
    parser.add_argument("--asof-trade-date", type=str, default="")
    parser.add_argument("--backtest", action="store_true")
    parser.add_argument("--bt-start", type=str, default="")
    parser.add_argument("--bt-end", type=str, default="")
    parser.add_argument("--bt-last-n", type=int, default=0)
    parser.add_argument("--bt-sell", type=str, default="close", choices=["open", "close", "high"])
    parser.add_argument("--bt-high-hit-pct", type=float, default=0.0)
    parser.add_argument("--bt-tp-pct", type=float, default=2.0)
    parser.add_argument("--bt-sl-pct", type=float, default=-2.0)
    parser.add_argument("--bt-out", type=str, default="")
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

    if bool(args.backtest):
        start_date = str(args.bt_start).strip()
        end_date = str(args.bt_end).strip()
        bt_last_n = int(getattr(args, "bt_last_n", 0) or 0)

        if (not start_date or not end_date) and bt_last_n > 0:
            anchor = str(args.asof_trade_date).strip()
            ref_dt = None
            if anchor:
                try:
                    ref_dt = datetime.strptime(anchor, "%Y%m%d")
                except Exception:
                    ref_dt = None

            end_dt = None
            try:
                end_dt = datetime.strptime(end_date, "%Y%m%d") if end_date else None
            except Exception:
                end_dt = None

            if ref_dt is not None:
                end_date = _last_open_trade_date(pro, ref=ref_dt)
            else:
                end_date = _last_open_trade_date(pro)

            end_dt = datetime.strptime(end_date, "%Y%m%d")
            probe_start_dt = end_dt - timedelta(days=400)
            probe_start = _ts_date(probe_start_dt)
            dates = _trade_dates_in_range(pro, start_date=probe_start, end_date=end_date)
            need = bt_last_n + 2
            if len(dates) < need:
                raise SystemExit(f"回测近 {bt_last_n} 日失败：区间内开市日不足 {need} 天")
            dates = dates[-need:]
            start_date = str(dates[0])
            end_date = str(dates[-1])
            _log(debug, f"bt_last_n={bt_last_n} resolved start={start_date} end={end_date}")

        if not start_date or not end_date:
            raise SystemExit("--backtest 需要提供 (--bt-start 与 --bt-end) 或 --bt-last-n")

        _run_backtest(
            pro=pro,
            scan_all=bool(args.scan_all),
            include_st=bool(args.include_st),
            include_suspended=bool(args.include_suspended),
            start_date=start_date,
            end_date=end_date,
            topk=int(args.topk),
            prefer_moneyflow=str(args.prefer_moneyflow),
            sell_price_mode=str(args.bt_sell),
            high_hit_pct=float(args.bt_high_hit_pct),
            tp_pct=float(args.bt_tp_pct),
            sl_pct=float(args.bt_sl_pct),
            out_csv=str(args.bt_out).strip(),
            debug=bool(args.debug),
        )
        return

    try:
        from backend.utils.pytdx_client import tdx
    except Exception as e:
        raise SystemExit(f"导入 pytdx_client 失败: {type(e).__name__}:{e}")
    _log(debug, "pytdx tdx ready")

    input_csv = str(args.input_csv).strip()
    if not input_csv and bool(args.auto_input_latest):
        input_csv = _find_latest_candidates_csv()
    _log(debug, f"input_csv={input_csv or 'manual'} topk={int(args.topk)} scan_all={1 if bool(args.scan_all) else 0}")

    if bool(args.scan_all) and (bool(args.with_minute_bars) or bool(args.with_margin) or bool(args.with_lhb)):
        raise SystemExit("--scan-all 不支持 --with-minute-bars/--with-margin/--with-lhb（全市场会非常慢）")

    today = _ts_date(datetime.now())
    asof = str(args.asof_trade_date).strip()
    if not asof:
        asof = _last_open_trade_date(pro)
    _log(debug, f"asof_trade_date={asof} today={today}")

    prev_open = _prev_open_trade_date(pro, asof)
    _log(debug, f"prev_open_trade_date={prev_open}")

    base_df = pd.DataFrame()
    if bool(args.scan_all):
        sb = _safe_call(pro.stock_basic, exchange="", list_status="L", fields="ts_code,name")
        if sb is None or sb.empty:
            raise SystemExit("stock_basic 返回为空，无法 --scan-all")
        sb = sb.copy()
        sb["ts_code"] = sb["ts_code"].astype(str)
        if "name" in sb.columns:
            sb["name"] = sb["name"].astype(str)
        else:
            sb["name"] = ""
        sb = sb[sb["ts_code"].str.endswith((".SZ", ".SH"))].copy()

        if not bool(args.include_st):
            st_set: set[str] = set()
            try:
                st = _safe_call(pro.stock_st, trade_date=asof, fields="ts_code")
                if st is not None and not st.empty:
                    st_set = set(st["ts_code"].astype(str).tolist())
            except Exception:
                st_set = set()
            if st_set:
                sb = sb[~sb["ts_code"].isin(st_set)].copy()

        if not bool(args.include_suspended):
            susp_set: set[str] = set()
            try:
                susp = _safe_call(pro.suspend_d, trade_date=asof, suspend_type="S", fields="ts_code")
                if susp is not None and not susp.empty:
                    susp_set = set(susp["ts_code"].astype(str).tolist())
            except Exception:
                susp_set = set()
            if susp_set:
                sb = sb[~sb["ts_code"].isin(susp_set)].copy()

        base_df = sb[["ts_code", "name"]].drop_duplicates().copy()
    elif input_csv:
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
            raise SystemExit("未提供股票列表（用 --scan-all/--input-csv/--auto-input-latest 或 --ts-codes）")
        ts_codes = [x.strip() for x in raw.split(",") if x.strip()]
        base_df = pd.DataFrame({"ts_code": ts_codes[: max(1, int(args.topk))]})
    _log(debug, f"base_df={_shape(base_df)} cols={len(base_df.columns)}")

    ts_codes = base_df["ts_code"].astype(str).dropna().tolist()
    ts_codes = list(dict.fromkeys(ts_codes))
    _log(debug, f"unique ts_codes={len(ts_codes)}")

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
    decision_order = {"BUY": 0, "WATCH": 1, "AVOID": 2}
    out_df["_decision_rank"] = out_df["decision"].map(decision_order).fillna(9).astype(int)
    out_df = out_df.sort_values(["_decision_rank", "tail_score"], ascending=[True, False], na_position="last")
    out_df.drop(columns=["_decision_rank"], inplace=True)
    if bool(args.scan_all):
        out_df = out_df.head(max(1, int(args.topk))).copy()

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
