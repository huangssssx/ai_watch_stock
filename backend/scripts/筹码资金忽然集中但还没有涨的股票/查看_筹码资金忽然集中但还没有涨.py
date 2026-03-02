import argparse
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timedelta
from glob import glob
from typing import Optional

import pandas as pd
import streamlit as st


_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from backend.utils.tushare_client import pro


OUTPUT_CSV_PATH = os.path.join(os.path.dirname(__file__), "筹码资金忽然集中但还没有涨_latest.csv")


def _in_streamlit_runtime() -> bool:
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
    except Exception:
        return False
    try:
        return get_script_run_ctx() is not None
    except Exception:
        return False


def cache_data(ttl: int, show_spinner: bool = False):
    def _decorator(fn):
        if _in_streamlit_runtime():
            try:
                return st.cache_data(ttl=ttl, show_spinner=show_spinner)(fn)
            except Exception:
                return fn
        return fn

    return _decorator


def _ensure_pro() -> None:
    if pro is None:
        raise RuntimeError("Tushare pro 未初始化成功，请先检查 backend/utils/tushare_client.py 配置与网络连通性")


def _to_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def _fetch_df(fetch_fn, attempts: int = 3, sleep_s: float = 0.6):
    last_exc = None
    last_df = None
    for i in range(int(attempts)):
        try:
            last_df = fetch_fn()
            if last_df is not None and not last_df.empty:
                return last_df
        except Exception as e:
            last_exc = e
        if i < int(attempts) - 1:
            time.sleep(float(sleep_s))
    if last_exc:
        raise last_exc
    return last_df


def _get_recent_trade_dates(n: int, end_date: Optional[str] = None) -> list[str]:
    _ensure_pro()
    end = str(end_date or datetime.now().strftime("%Y%m%d"))
    start = (datetime.strptime(end, "%Y%m%d") - timedelta(days=180)).strftime("%Y%m%d")
    cal = _fetch_df(
        lambda: pro.trade_cal(exchange="SSE", start_date=start, end_date=end, fields="cal_date,is_open"),
        attempts=3,
        sleep_s=0.4,
    )
    if cal is None or cal.empty:
        raise RuntimeError("trade_cal 返回为空，无法确定交易日")
    cal = cal.copy()
    cal["cal_date"] = cal["cal_date"].astype(str)
    cal = cal[cal["is_open"] == 1].sort_values("cal_date")
    dates = cal["cal_date"].tolist()
    if len(dates) < n:
        raise RuntimeError(f"交易日数量不足：需要 {n}，实际 {len(dates)}，请扩大回看窗口或检查 end_date={end}")
    return dates[-n:]


def _pick_latest_chip_file() -> Optional[str]:
    base_dir = os.path.abspath(os.path.join(_PROJECT_ROOT, "backend/data"))
    patterns = [
        os.path.join(base_dir, "all_chip_data_*.csv"),
        os.path.join(base_dir, "chip_data_*.csv"),
    ]
    candidates: list[str] = []
    for p in patterns:
        candidates.extend(glob(p))
    candidates = [p for p in candidates if os.path.isfile(p)]
    if not candidates:
        return None
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0]


@cache_data(ttl=10, show_spinner=False)
def load_latest_results() -> pd.DataFrame:
    if not os.path.exists(OUTPUT_CSV_PATH):
        return pd.DataFrame()
    df = pd.read_csv(OUTPUT_CSV_PATH, dtype={"symbol": str, "ts_code": str, "trade_date": str})
    df = _to_numeric(
        df,
        [
            "close",
            "pct_chg",
            "amount_yi",
            "net_mf_amount",
            "net_mf_ratio",
            "net_mf_ratio_prev_mean",
            "net_mf_ratio_spike",
            "vol_ratio",
        ],
    )
    if "trade_date" in df.columns:
        df["trade_date"] = df["trade_date"].astype(str)
    return df


@cache_data(ttl=600, show_spinner=False)
def fetch_moneyflow_and_price_30d(ts_code: str, end_trade_date: str) -> pd.DataFrame:
    _ensure_pro()
    dates = _get_recent_trade_dates(30, end_date=end_trade_date)
    start_date, end_date = dates[0], dates[-1]

    daily = _fetch_df(
        lambda: pro.daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields="ts_code,trade_date,open,close,pre_close,pct_chg,vol,amount",
        ),
        attempts=3,
        sleep_s=0.4,
    )
    mf = _fetch_df(
        lambda: pro.moneyflow(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields="ts_code,trade_date,net_mf_amount",
        ),
        attempts=3,
        sleep_s=0.4,
    )
    if daily is None or daily.empty:
        return pd.DataFrame()
    daily = daily.copy()
    daily["trade_date"] = daily["trade_date"].astype(str)
    daily = _to_numeric(daily, ["open", "close", "pre_close", "pct_chg", "vol", "amount"])
    daily = daily.dropna(subset=["trade_date", "close", "pct_chg", "vol", "amount"])

    if mf is None or mf.empty:
        out = daily[["trade_date", "close", "pct_chg", "vol", "amount"]].copy()
        out["net_mf_amount"] = pd.NA
        out["amount_wan"] = out["amount"] / 10.0
        out["net_mf_ratio"] = pd.NA
        return out.sort_values("trade_date")

    mf = mf.copy()
    mf["trade_date"] = mf["trade_date"].astype(str)
    mf = _to_numeric(mf, ["net_mf_amount"])
    mf = mf.dropna(subset=["trade_date", "net_mf_amount"])

    out = daily.merge(mf[["trade_date", "net_mf_amount"]], on="trade_date", how="left")
    out["amount_wan"] = out["amount"] / 10.0
    out["net_mf_ratio"] = out["net_mf_amount"] / out["amount_wan"].replace({0.0: pd.NA})
    out = out.sort_values("trade_date").reset_index(drop=True)
    return out


@cache_data(ttl=600, show_spinner=False)
def load_chip_30d_from_local(ts_code: str, end_trade_date: str) -> pd.DataFrame:
    chip_path = _pick_latest_chip_file()
    if not chip_path or not os.path.exists(chip_path):
        return pd.DataFrame()
    df = pd.read_csv(chip_path, dtype={"ts_code": str, "trade_date": str})
    if df is None or df.empty:
        return pd.DataFrame()
    if "ts_code" not in df.columns or "trade_date" not in df.columns:
        return pd.DataFrame()
    sub = df[df["ts_code"].astype(str) == str(ts_code)].copy()
    if sub.empty:
        return pd.DataFrame()
    sub["trade_date"] = sub["trade_date"].astype(str)
    sub = sub[sub["trade_date"] <= str(end_trade_date)]
    if sub.empty:
        return pd.DataFrame()
    sub = _to_numeric(
        sub,
        [
            "his_low",
            "his_high",
            "cost_5pct",
            "cost_15pct",
            "cost_50pct",
            "cost_85pct",
            "cost_95pct",
            "weight_avg",
            "winner_rate",
        ],
    )
    sub = sub.dropna(subset=["trade_date"])
    sub = sub.sort_values("trade_date")
    sub = sub.tail(30).reset_index(drop=True)
    if "cost_95pct" in sub.columns and "cost_5pct" in sub.columns:
        sub["cost_range"] = sub["cost_95pct"] - sub["cost_5pct"]
    if "cost_50pct" in sub.columns and "cost_range" in sub.columns:
        sub["cost_range_pct"] = sub["cost_range"] / sub["cost_50pct"].replace({0.0: pd.NA})
    return sub


@cache_data(ttl=600, show_spinner=False)
def fetch_chip_30d_from_tushare(ts_code: str, end_trade_date: str) -> pd.DataFrame:
    _ensure_pro()
    dates = _get_recent_trade_dates(30, end_date=end_trade_date)
    start_date, end_date = dates[0], dates[-1]

    df = _fetch_df(
        lambda: pro.cyq_perf(
            ts_code=str(ts_code),
            start_date=str(start_date),
            end_date=str(end_date),
        ),
        attempts=3,
        sleep_s=0.5,
    )
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    if "trade_date" in df.columns:
        df["trade_date"] = df["trade_date"].astype(str)

    df = _to_numeric(
        df,
        [
            "his_low",
            "his_high",
            "cost_5pct",
            "cost_15pct",
            "cost_50pct",
            "cost_85pct",
            "cost_95pct",
            "weight_avg",
            "winner_rate",
        ],
    )
    df = df.dropna(subset=["trade_date"])
    if df.empty:
        return pd.DataFrame()
    df = df.sort_values("trade_date").tail(30).reset_index(drop=True)
    if "cost_95pct" in df.columns and "cost_5pct" in df.columns:
        df["cost_range"] = df["cost_95pct"] - df["cost_5pct"]
    if "cost_50pct" in df.columns and "cost_range" in df.columns:
        df["cost_range_pct"] = df["cost_range"] / df["cost_50pct"].replace({0.0: pd.NA})
    return df


def load_chip_30d(ts_code: str, end_trade_date: str) -> pd.DataFrame:
    local = load_chip_30d_from_local(ts_code, end_trade_date)
    if local is not None and not local.empty:
        return local
    try:
        return fetch_chip_30d_from_tushare(ts_code, end_trade_date)
    except Exception:
        return pd.DataFrame()


def _trend_slope(values: list[float]) -> float:
    if len(values) < 3:
        return 0.0
    xs = list(range(len(values)))
    x_mean = sum(xs) / len(xs)
    y_mean = sum(values) / len(values)
    denom = sum((x - x_mean) ** 2 for x in xs) or 1.0
    numer = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, values))
    return float(numer / denom)


def build_commentary(mf: pd.DataFrame, chip: pd.DataFrame) -> dict[str, str]:
    out: dict[str, str] = {}
    if mf is None or mf.empty:
        out["资金派解读"] = "近30日资金流数据缺失，无法做资金流趋势判断。"
        out["交易派解读"] = "近30日行情数据缺失，建议稍后重试。"
        return out

    mf = mf.copy()
    mf = _to_numeric(mf, ["close", "pct_chg", "amount", "net_mf_amount", "net_mf_ratio"])
    mf = mf.dropna(subset=["trade_date", "close"])
    if mf.empty:
        out["资金派解读"] = "近30日资金流数据缺失，无法做资金流趋势判断。"
        out["交易派解读"] = "近30日行情数据缺失，建议稍后重试。"
        return out

    close_first = float(mf["close"].iloc[0])
    close_last = float(mf["close"].iloc[-1])
    price_ret = (close_last / close_first - 1.0) if close_first else 0.0

    mf_amount = mf["net_mf_amount"].dropna()
    mf_ratio = mf["net_mf_ratio"].dropna()

    mf_sum = float(mf_amount.sum()) if not mf_amount.empty else 0.0
    mf_recent5_sum = float(mf_amount.tail(5).sum()) if len(mf_amount) >= 5 else float(mf_amount.sum() or 0.0)
    mf_pos_days = float((mf_amount > 0).mean()) if not mf_amount.empty else 0.0

    ratio_slope = _trend_slope([float(x) for x in mf_ratio.tail(10).tolist()]) if len(mf_ratio) >= 3 else 0.0
    amt_slope = _trend_slope([float(x) for x in mf_amount.tail(10).tolist()]) if len(mf_amount) >= 3 else 0.0

    flat_price = abs(price_ret) <= 0.05
    strong_inflow = mf_sum >= 5000.0 or mf_recent5_sum >= 2500.0
    steady_inflow = mf_pos_days >= 0.6

    lines_fund: list[str] = []
    if strong_inflow and flat_price:
        lines_fund.append("近30日净流入偏强但价格涨幅不大，常见于吸筹/换手阶段或对倒托盘。")
    elif strong_inflow and not flat_price:
        lines_fund.append("近30日资金净流入与价格同向上行，偏向趋势推动型资金。")
    elif mf_sum < 0 and price_ret > 0:
        lines_fund.append("价格上涨但资金净流出，更多像拉升后兑现或边涨边出。")
    elif mf_sum < 0 and price_ret <= 0:
        lines_fund.append("资金与价格同向偏弱，短线风险更高，优先等资金拐点。")
    else:
        lines_fund.append("资金与价格关系不明显，更多依赖结构信号（近5日变化、净流入占比等）。")

    if steady_inflow:
        lines_fund.append("净流入天数占比偏高，资金流更像“持续性”而非单日脉冲。")
    else:
        lines_fund.append("净流入天数占比一般，资金更像“事件脉冲”，需要结合成交额与次日反馈验证。")

    if ratio_slope > 0 and amt_slope > 0:
        lines_fund.append("近10日净流入强度与规模同步走强，属于加速型信号。")
    elif ratio_slope > 0 and amt_slope <= 0:
        lines_fund.append("近10日净流入占比走强但规模未放大，更像精准资金而非大规模推升。")
    elif ratio_slope <= 0 and amt_slope > 0:
        lines_fund.append("近10日规模走强但占比未提升，可能是成交额放大导致的“被稀释”。")
    else:
        lines_fund.append("近10日资金强度未形成明确上升斜率，属于尚未确认的蓄势状态。")

    out["资金派解读"] = "\n".join([f"- {x}" for x in lines_fund])

    lines_trade: list[str] = []
    if price_ret > 0.1:
        lines_trade.append("30日涨幅已较大，更像趋势末端或加速段，追涨需看次日承接。")
    elif abs(price_ret) <= 0.05:
        lines_trade.append("30日整体横盘，若资金持续净流入，后续更容易走“突发放量突破”。")
    else:
        lines_trade.append("30日呈温和波动，关键观察资金拐点能否带来价格重心上移。")
    out["交易派解读"] = "\n".join([f"- {x}" for x in lines_trade])

    if chip is None or chip.empty:
        out["筹码派解读"] = "本地筹码数据缺失或未覆盖该标的，暂不输出筹码解读。"
        out["综合结论"] = "优先以资金流与价格行为为主；筹码部分待数据补齐后再做二次确认。"
        return out

    chip = chip.copy()
    chip = _to_numeric(chip, ["winner_rate", "cost_range", "cost_range_pct", "weight_avg", "cost_50pct"])
    chip = chip.dropna(subset=["trade_date"])
    if chip.empty:
        out["筹码派解读"] = "筹码数据为空，暂不输出筹码解读。"
        out["综合结论"] = "优先以资金流与价格行为为主；筹码部分待数据补齐后再做二次确认。"
        return out

    wr = chip["winner_rate"].dropna()
    cr = chip["cost_range"].dropna() if "cost_range" in chip.columns else pd.Series(dtype=float)
    wr_chg = (float(wr.iloc[-1]) - float(wr.iloc[0])) if len(wr) >= 2 else 0.0
    cr_chg = (float(cr.iloc[-1]) - float(cr.iloc[0])) if len(cr) >= 2 else 0.0
    cr_slope = _trend_slope([float(x) for x in cr.tail(10).tolist()]) if len(cr) >= 3 else 0.0
    wr_slope = _trend_slope([float(x) for x in wr.tail(10).tolist()]) if len(wr) >= 3 else 0.0

    lines_chip: list[str] = []
    if len(cr) >= 2:
        if cr_chg < 0:
            lines_chip.append("成本带收窄，筹码更集中，容易在关键位置形成一致性。")
        elif cr_chg > 0:
            lines_chip.append("成本带走阔，筹码更分散，拉升时更依赖增量资金。")
        else:
            lines_chip.append("成本带宽度变化不大，筹码结构偏稳定。")

        if cr_slope < 0:
            lines_chip.append("近10日成本带持续收敛，偏向“慢慢集中”的过程。")
        elif cr_slope > 0:
            lines_chip.append("近10日成本带扩散，可能在高位换手或分歧加大。")
        else:
            lines_chip.append("近10日成本带斜率不明显，结构变化较弱。")

    if len(wr) >= 2:
        if wr_chg > 10:
            lines_chip.append("获利盘占比明显抬升，短线可能更容易出现获利回吐的波动。")
        elif wr_chg < -10:
            lines_chip.append("获利盘占比明显回落，抛压减轻，但也可能代表趋势尚未走出来。")
        else:
            lines_chip.append("获利盘占比变化温和，结构更像蓄势或震荡整理。")

        if wr_slope > 0:
            lines_chip.append("近10日获利盘占比上行，更多人站在盈利侧，突破时惯性更强。")
        elif wr_slope < 0:
            lines_chip.append("近10日获利盘占比下行，市场信心偏弱，反弹需要放量确认。")
        else:
            lines_chip.append("近10日获利盘占比斜率不明显，更多依赖资金驱动。")

    out["筹码派解读"] = "\n".join([f"- {x}" for x in lines_chip]) if lines_chip else "筹码指标不足，暂不解读。"

    score = 0
    score += 2 if strong_inflow else 0
    score += 1 if steady_inflow else 0
    score += 1 if flat_price else 0
    score += 1 if (ratio_slope > 0 and amt_slope > 0) else 0
    score += 1 if (len(cr) >= 2 and cr_chg < 0) else 0
    score += 1 if (len(wr) >= 2 and wr_slope > 0) else 0

    if score >= 5:
        out["综合结论"] = "资金与筹码同时给出偏强信号，但仍需用“次日是否继续放量且不破关键成本带”做确认。"
    elif score >= 3:
        out["综合结论"] = "属于“可能在酝酿”的标的：更适合等资金持续或价格突破后再跟随。"
    else:
        out["综合结论"] = "信号偏弱或分歧较大：优先观察，不建议在尚未突破前重仓试错。"

    return out


def _safe_dt(s: str) -> str:
    try:
        return pd.to_datetime(str(s), format="%Y%m%d").strftime("%Y-%m-%d")
    except Exception:
        return str(s)


def _silence_streamlit_logs_for_cli() -> None:
    for logger_name in [
        "streamlit",
        "streamlit.runtime.caching.cache_data_api",
        "streamlit.runtime.scriptrunner_utils.script_run_context",
    ]:
        try:
            logging.getLogger(logger_name).setLevel(logging.ERROR)
        except Exception:
            pass


def _dialog(title: str):
    dialog_fn = getattr(st, "dialog", None)
    if callable(dialog_fn):
        try:
            return dialog_fn(title, width="large")
        except TypeError:
            return dialog_fn(title)
    dialog_fn = getattr(st, "experimental_dialog", None)
    if callable(dialog_fn):
        try:
            return dialog_fn(title, width="large")
        except TypeError:
            return dialog_fn(title)

    def _decorator(fn):
        return fn

    return _decorator


def _apply_dialog_width_css(width_ratio: float = 0.8) -> None:
    ratio = float(width_ratio)
    if ratio <= 0:
        ratio = 0.8
    if ratio > 1:
        ratio = 1.0
    vw = ratio * 100.0
    st.markdown(
        f"""
<style>
div[data-testid="stModal"] div[role="dialog"],
div[data-testid="stDialog"] div[role="dialog"] {{
  width: {vw:.4f}vw !important;
  max-width: {vw:.4f}vw !important;
}}
</style>
""",
        unsafe_allow_html=True,
    )


@_dialog("近30日详情")
def show_detail_dialog(ts_code: str, name: str, end_trade_date: str) -> None:
    _apply_dialog_width_css(0.8)
    end_trade_date = str(end_trade_date or "").strip() or datetime.now().strftime("%Y%m%d")
    st.subheader(f"{name}（{ts_code}）")
    st.caption(f"end_trade_date={_safe_dt(end_trade_date)}")

    try:
        with st.spinner("拉取近30日资金流与行情..."):
            mf30 = fetch_moneyflow_and_price_30d(ts_code, end_trade_date)
    except Exception as e:
        st.error(f"资金流数据拉取失败：{type(e).__name__}: {e}")
        st.code(traceback.format_exc())
        return

    if mf30 is None or mf30.empty:
        st.warning("近30日行情/资金流为空。")
        return

    chip_source = "本地"
    chip30 = load_chip_30d_from_local(ts_code, end_trade_date)
    if chip30 is None or chip30.empty:
        try:
            with st.spinner("本地筹码缺失，尝试用 tushare cyq_perf 补齐..."):
                chip30 = fetch_chip_30d_from_tushare(ts_code, end_trade_date)
                chip_source = "tushare"
        except Exception as e:
            chip30 = pd.DataFrame()
            st.info("未找到本地筹码数据（或未覆盖该标的），且 tushare 补齐失败。")
            st.error(f"tushare cyq_perf 拉取失败：{type(e).__name__}: {e}")
            st.code(traceback.format_exc())

    mf_show = mf30.copy()
    mf_show["date"] = pd.to_datetime(mf_show["trade_date"].astype(str), format="%Y%m%d", errors="coerce")
    mf_show = mf_show.dropna(subset=["date"]).set_index("date")

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("收盘价(最新)", f"{float(mf30['close'].iloc[-1]):.2f}")
    kpi2.metric("30日涨跌", f"{(float(mf30['close'].iloc[-1]) / float(mf30['close'].iloc[0]) - 1.0) * 100:.2f}%")
    mf_sum = float(pd.to_numeric(mf30["net_mf_amount"], errors="coerce").fillna(0.0).sum())
    kpi3.metric("30日净流入(万元)", f"{mf_sum:.0f}")
    kpi4.metric("近5日净流入(万元)", f"{float(pd.to_numeric(mf30['net_mf_amount'], errors='coerce').fillna(0.0).tail(5).sum()):.0f}")

    c_left, c_right = st.columns([1.1, 0.9])
    with c_left:
        st.markdown("**资金流与价格**")
        chart_df = mf_show[["close", "net_mf_amount", "net_mf_ratio"]].copy()
        st.line_chart(chart_df, use_container_width=True, height=320)
        with st.expander("查看近30日明细"):
            st.dataframe(mf30, use_container_width=True, hide_index=True)

    with c_right:
        st.markdown("**解说**")
        commentary = build_commentary(mf30, chip30)
        for title in ["资金派解读", "交易派解读", "筹码派解读", "综合结论"]:
            if title not in commentary:
                continue
            st.markdown(f"**{title}**")
            st.markdown(commentary[title])

    st.divider()
    st.subheader(f"近30日筹码结构（来源：{chip_source}）")
    if chip30 is None or chip30.empty:
        st.info("未找到可用的筹码数据（本地与 tushare 均为空或失败）。")
        return

    chip_show = chip30.copy()
    chip_show["date"] = pd.to_datetime(chip_show["trade_date"].astype(str), format="%Y%m%d", errors="coerce")
    chip_show = chip_show.dropna(subset=["date"]).set_index("date")

    chip_cols = [c for c in ["winner_rate", "cost_range", "weight_avg", "cost_50pct"] if c in chip_show.columns]
    if chip_cols:
        st.line_chart(chip_show[chip_cols], use_container_width=True, height=320)
    with st.expander("查看筹码明细"):
        st.dataframe(chip30, use_container_width=True, hide_index=True)


def cli_main() -> None:
    _silence_streamlit_logs_for_cli()

    parser = argparse.ArgumentParser()
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--ts-code", type=str, default="")
    parser.add_argument("--trade-date", type=str, default="")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--keep-proxy", action="store_true", default=False)
    args = parser.parse_args()

    if not bool(args.keep_proxy):
        for k in [
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
        ]:
            os.environ.pop(k, None)
        os.environ["NO_PROXY"] = "*"
        os.environ["no_proxy"] = "*"

    try:
        pro._DataApi__timeout = int(args.timeout)
    except Exception:
        pass

    df = load_latest_results()
    if df is None or df.empty:
        raise SystemExit(f"未找到结果文件或文件为空：{OUTPUT_CSV_PATH}")

    trade_date = ""
    if "trade_date" in df.columns and df["trade_date"].notna().any():
        trade_date = str(df["trade_date"].dropna().iloc[0])

    print(f"结果文件：{OUTPUT_CSV_PATH}")
    print(f"trade_date={trade_date} 行数={len(df)}")
    show_cols = [
        c
        for c in [
            "symbol",
            "name",
            "ts_code",
            "trade_date",
            "close",
            "pct_chg",
            "amount_yi",
            "net_mf_amount",
            "net_mf_ratio",
            "net_mf_ratio_spike",
            "vol_ratio",
        ]
        if c in df.columns
    ]
    view = df[show_cols].head(max(1, int(args.top_n))).copy()
    with pd.option_context("display.max_rows", None, "display.max_columns", None, "display.width", 200):
        print(view.to_string(index=False))

    ts_code = str(args.ts_code).strip()
    if not ts_code:
        print("")
        print("如需查看单只股票的近30日详情：")
        print(
            "python3 backend/scripts/筹码资金忽然集中但还没有涨的股票/查看_筹码资金忽然集中但还没有涨.py "
            "--ts-code 002307.SZ"
        )
        return

    end_trade_date = str(args.trade_date).strip() or trade_date or datetime.now().strftime("%Y%m%d")
    picked_name = ""
    if "ts_code" in df.columns and "name" in df.columns:
        m = df[df["ts_code"].astype(str) == ts_code]
        if not m.empty:
            picked_name = str(m["name"].iloc[0] or "")
            if not str(args.trade_date).strip() and "trade_date" in m.columns:
                end_trade_date = str(m["trade_date"].iloc[0] or end_trade_date)

    print("")
    print("=" * 80)
    print(f"详情：{picked_name}（{ts_code}） end_trade_date={end_trade_date}")
    print("=" * 80)

    mf30 = fetch_moneyflow_and_price_30d(ts_code, end_trade_date)
    chip30 = load_chip_30d_from_local(ts_code, end_trade_date)
    chip_source = "本地"
    if chip30 is None or chip30.empty:
        print("筹码：本地数据缺失或未覆盖该标的，尝试用 tushare cyq_perf 补齐...")
        try:
            chip30 = fetch_chip_30d_from_tushare(ts_code, end_trade_date)
            chip_source = "tushare"
        except Exception as e:
            chip30 = pd.DataFrame()
            print(f"筹码：tushare cyq_perf 拉取失败：{type(e).__name__}: {e}")

    if mf30 is None or mf30.empty:
        raise SystemExit("近30日行情/资金流为空（tushare daily/moneyflow 无返回）。")

    close_first = float(pd.to_numeric(mf30["close"], errors="coerce").dropna().iloc[0])
    close_last = float(pd.to_numeric(mf30["close"], errors="coerce").dropna().iloc[-1])
    ret30 = (close_last / close_first - 1.0) * 100 if close_first else 0.0
    mf_sum = float(pd.to_numeric(mf30["net_mf_amount"], errors="coerce").fillna(0.0).sum())
    mf_5sum = float(pd.to_numeric(mf30["net_mf_amount"], errors="coerce").fillna(0.0).tail(5).sum())
    print(f"收盘价(最新)：{close_last:.2f}  30日涨跌：{ret30:.2f}%")
    print(f"30日净流入(万元)：{mf_sum:.0f}  近5日净流入(万元)：{mf_5sum:.0f}")

    print("")
    commentary = build_commentary(mf30, chip30)
    for title in ["资金派解读", "交易派解读", "筹码派解读", "综合结论"]:
        if title not in commentary:
            continue
        print(f"[{title}]")
        print(commentary[title])
        print("")

    print("资金流与行情（近30日）：")
    mf_cols = [c for c in ["trade_date", "close", "pct_chg", "amount", "net_mf_amount", "net_mf_ratio"] if c in mf30.columns]
    mf_view = mf30[mf_cols].tail(30).copy()
    with pd.option_context("display.max_rows", None, "display.max_columns", None, "display.width", 200):
        print(mf_view.to_string(index=False))

    if chip30 is not None and not chip30.empty:
        print("")
        print(f"筹码（近30日，来源：{chip_source}）：")
        chip_cols = [c for c in ["trade_date", "winner_rate", "cost_5pct", "cost_50pct", "cost_95pct", "cost_range", "weight_avg"] if c in chip30.columns]
        chip_view = chip30[chip_cols].tail(30).copy()
        with pd.option_context("display.max_rows", None, "display.max_columns", None, "display.width", 200):
            print(chip_view.to_string(index=False))
    else:
        print("")
        print("筹码：本地数据缺失或未覆盖该标的。")


def main() -> None:
    st.set_page_config(page_title="筹码资金忽然集中但还没有涨", layout="wide")
    st.title("筹码资金忽然集中但还没有涨：最新结果")

    with st.sidebar:
        st.subheader("数据源")
        st.write(f"结果文件：{OUTPUT_CSV_PATH}")
        top_n = st.slider("展示条数", min_value=20, max_value=300, value=120, step=10)
        st.caption("点击任意一行的“查看30日”按钮，会以弹窗展示详情。")
        if st.button("刷新结果文件"):
            load_latest_results.clear()
            st.rerun()

    df = load_latest_results()
    if df is None or df.empty:
        st.warning("未找到结果文件或文件为空。请先运行筛选脚本生成最新结果。")
        st.stop()

    if "trade_date" in df.columns and df["trade_date"].notna().any():
        st.caption(f"最新 trade_date：{_safe_dt(str(df['trade_date'].dropna().iloc[0]))}，行数：{len(df)}")
    else:
        st.caption(f"行数：{len(df)}")

    show_cols = [
        c
        for c in [
            "symbol",
            "name",
            "trade_date",
            "close",
            "pct_chg",
            "amount_yi",
            "net_mf_amount",
            "net_mf_ratio",
            "net_mf_ratio_spike",
            "vol_ratio",
            "ts_code",
        ]
        if c in df.columns
    ]
    view = df[show_cols].head(int(top_n)).copy()
    st.dataframe(view, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("点击查看近30日变化")

    for i, row in view.iterrows():
        ts_code = str(row.get("ts_code", "") or "")
        if not ts_code:
            continue
        name = str(row.get("name", "") or "")
        symbol = str(row.get("symbol", "") or "")
        trade_date = str(row.get("trade_date", "") or "")
        pct_chg = row.get("pct_chg", pd.NA)
        net_mf_ratio_spike = row.get("net_mf_ratio_spike", pd.NA)

        c1, c2, c3, c4, c5, c6 = st.columns([1.0, 2.5, 1.2, 1.2, 1.2, 1.2])
        c1.write(symbol)
        c2.write(name)
        c3.write(_safe_dt(trade_date))
        c4.write("" if pd.isna(pct_chg) else f"{float(pct_chg):.2f}%")
        c5.write("" if pd.isna(net_mf_ratio_spike) else f"{float(net_mf_ratio_spike):.2f}x")
        if c6.button("查看30日", key=f"view_{i}_{ts_code}"):
            show_detail_dialog(ts_code, name, trade_date)


if __name__ == "__main__":
    if _in_streamlit_runtime():
        main()
    else:
        cli_main()
