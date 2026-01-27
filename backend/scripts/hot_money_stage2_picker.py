import akshare as ak
import pandas as pd
import numpy as np
import datetime
import time
import traceback
import sys
import os


_TS_REQ_COUNT = 0
_TS_WINDOW_START = None
_TS_MAX_PER_MIN = 240.0
_TS_WINDOW_SECONDS = 60.0


def _rate_limit_tushare():
    global _TS_REQ_COUNT, _TS_WINDOW_START
    now = time.time()
    if _TS_WINDOW_START is None:
        _TS_WINDOW_START = now
        _TS_REQ_COUNT = 0
    elapsed = now - _TS_WINDOW_START
    if elapsed >= _TS_WINDOW_SECONDS:
        _TS_WINDOW_START = now
        _TS_REQ_COUNT = 0
    elif _TS_REQ_COUNT >= _TS_MAX_PER_MIN:
        sleep_time = _TS_WINDOW_SECONDS - elapsed + 0.05
        if sleep_time > 0:
            time.sleep(sleep_time)
        _TS_WINDOW_START = time.time()
        _TS_REQ_COUNT = 0
    _TS_REQ_COUNT += 1


def _resolve_project_root():
    start_paths = []
    if "__file__" in globals():
        start_paths.append(os.path.abspath(__file__))
    start_paths.append(os.getcwd())
    for start in start_paths:
        cur = os.path.abspath(start)
        if os.path.isfile(cur):
            cur = os.path.dirname(cur)
        while True:
            if os.path.exists(os.path.join(cur, "backend", "stock_watch.db")):
                return cur
            parent = os.path.dirname(cur)
            if parent == cur:
                break
            cur = parent
    return os.getcwd()


project_root = _resolve_project_root()
if project_root not in sys.path:
    sys.path.append(project_root)

try:
    from backend.utils.tushare_client import pro
    import tushare as ts
except ImportError:
    print("无法导入 tushare_client，请从项目根目录运行或检查环境。")
    pro = None
    ts = None


def get_ts_code(code: str) -> str:
    code = str(code).strip()
    if len(code) == 6 and code.isdigit():
        if code.startswith("6"):
            return f"{code}.SH"
        if code.startswith("0") or code.startswith("3"):
            return f"{code}.SZ"
        if code.startswith("8") or code.startswith("4"):
            return f"{code}.BJ"
    return code


def fetch_daily_history(code: str, name: str, sector: str):
    if pro is None:
        return None
    try:
        ts_code = get_ts_code(code)
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=200)).strftime("%Y%m%d")
        _rate_limit_tushare()
        df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df is None or df.empty or len(df) < 40:
            return None
        hist = df.sort_values("trade_date", ascending=True)
        if len(hist) < 40:
            return None
        return {
            "code": code,
            "name": name,
            "sector": sector,
            "hist": hist,
        }
    except Exception as e:
        print(f"fetch_daily_history 失败 {code}: {e}")
        print(traceback.format_exc())
        return None


def analyze_stock(record: dict) -> dict:
    hist = record["hist"].copy()
    hist["close"] = pd.to_numeric(hist["close"], errors="coerce")
    hist["open"] = pd.to_numeric(hist["open"], errors="coerce")
    hist["high"] = pd.to_numeric(hist["high"], errors="coerce")
    hist["low"] = pd.to_numeric(hist["low"], errors="coerce")
    hist["vol"] = pd.to_numeric(hist["vol"], errors="coerce")
    hist = hist.dropna(subset=["close", "open", "high", "low", "vol"])
    if len(hist) < 40:
        raise ValueError("有效K线不足")

    window_60 = hist.tail(60) if len(hist) >= 60 else hist
    high_60 = float(window_60["high"].max())
    low_60 = float(window_60["low"].min())
    last_close = float(window_60["close"].iloc[-1])
    if high_60 == low_60:
        rpp = 0.5
    else:
        rpp = (last_close - low_60) / (high_60 - low_60)
    rpp = float(max(0.0, min(1.0, rpp)))

    vols = window_60["vol"]
    vol_ma = vols.rolling(20).mean()
    vol_ratio = vols / vol_ma
    vol_ratio = vol_ratio.replace([np.inf, -np.inf], np.nan).fillna(0)
    last_vol_ratio = float(vol_ratio.iloc[-1])

    recent_window = window_60.tail(10)
    vol_ma_recent = vol_ma.reindex(recent_window.index)
    recent_ratio = recent_window["vol"] / vol_ma_recent
    recent_ratio = recent_ratio.replace([np.inf, -np.inf], np.nan).fillna(0)
    if high_60 == low_60:
        rpp_recent = pd.Series(0.5, index=recent_window.index)
    else:
        rpp_recent = (recent_window["close"] - low_60) / (high_60 - low_60)
    surge_mask = (
        (recent_window["close"] > recent_window["open"])
        & (recent_ratio >= 2.0)
        & (rpp_recent <= 0.5)
    )
    surge_days = recent_window.loc[surge_mask]
    has_surge = not surge_days.empty
    last_surge_date = ""
    last_surge_ratio = 0.0
    if has_surge:
        last_surge = surge_days.iloc[-1]
        last_surge_date = str(last_surge["trade_date"])
        last_surge_ratio = float(recent_ratio.loc[last_surge.name])

    closes_10 = hist["close"].tail(10)
    if len(closes_10) >= 2:
        diff = closes_10.diff().dropna()
        path = float(diff.abs().sum())
        net = float(abs(closes_10.iloc[-1] - closes_10.iloc[0]))
        trending = float(net / path) if path > 0 else 0.0
    else:
        trending = 0.0

    if len(hist) > 25:
        lookback = hist.iloc[-25:-3]
    else:
        lookback = hist.iloc[:-3]
    if lookback.empty:
        platform_high = float(window_60["high"].max())
    else:
        platform_high = float(lookback["close"].max())
    current_close = float(hist["close"].iloc[-1])
    if platform_high > 0:
        gap_to_platform_pct = float((platform_high - current_close) / platform_high * 100)
    else:
        gap_to_platform_pct = 0.0

    score = 0.0
    if has_surge:
        score += 40.0
    score += max(0.0, min(last_vol_ratio, 4.0) / 4.0 * 20.0)
    score += max(0.0, min(trending, 1.0) * 20.0)
    if rpp < 0.4:
        score += (0.4 - rpp) / 0.4 * 20.0

    return {
        "symbol": record["code"],
        "name": record["name"],
        "sector": record["sector"],
        "close": round(current_close, 2),
        "rpp": round(rpp, 3),
        "last_vol_ratio": round(last_vol_ratio, 2),
        "has_surge": bool(has_surge),
        "last_surge_date": last_surge_date,
        "last_surge_ratio": round(last_surge_ratio, 2),
        "trending": round(trending, 3),
        "platform_high": round(platform_high, 2),
        "gap_to_platform_pct": round(gap_to_platform_pct, 2),
        "score": round(score, 2),
    }


def _plot_stock_panel(symbol: str, name: str, platform_high: float, days: int = 80):
    if pro is None:
        return
    try:
        ts_code = get_ts_code(symbol)
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=200)).strftime("%Y%m%d")
        _rate_limit_tushare()
        df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df is None or df.empty:
            return
        df = df.sort_values("trade_date", ascending=True)
        df = df.tail(days)
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.set_index("trade_date")
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["vol"] = pd.to_numeric(df["vol"], errors="coerce")

        price_df = pd.DataFrame(
            {
                "close": df["close"],
                "platform": platform_high,
            },
            index=df.index,
        )
        vol_df = pd.DataFrame(
            {
                "volume": df["vol"],
            },
            index=df.index,
        )
        latest_close = float(df["close"].iloc[-1])
        broken = latest_close >= platform_high
        import streamlit as st
        import altair as alt

        title = f"{symbol} {name} | 状态: {'已突破平台线' if broken else '未突破平台线'}"
        with st.expander(title, expanded=False):
            plot_df = price_df.reset_index().rename(columns={"trade_date": "date"})
            plot_df = plot_df.melt(
                id_vars="date",
                value_vars=["close", "platform"],
                var_name="series",
                value_name="value",
            )
            y_min = float(plot_df["value"].min())
            y_max = float(plot_df["value"].max())
            if y_max == y_min:
                y_min -= 0.5
                y_max += 0.5
            else:
                margin = (y_max - y_min) * 0.2
                y_min -= margin
                y_max += margin

            base = alt.Chart(plot_df).encode(
                x="date:T",
                y=alt.Y("value:Q", scale=alt.Scale(domain=[y_min, y_max])),
                color=alt.Color(
                    "series:N",
                    scale=alt.Scale(
                        domain=["close", "platform"], range=["#1f77b4", "#ff7f0e"]
                    ),
                ),
            )
            # 线 + 点，防止数据点太少连不成线
            line_chart = base.mark_line() + base.mark_point(size=30)
            line_chart = line_chart.encode(
                tooltip=["date:T", "series:N", "value:Q"],
            ).properties(height=300)

            st.altair_chart(line_chart, use_container_width=True)
            st.bar_chart(vol_df, height=150)
    except Exception:
        return


def run_streamlit_dashboard():
    import streamlit as st

    st.set_page_config(page_title="资金热点选股平台突破观察", layout="wide")
    st.title("资金热点选股 - 第二阶段选标的与平台突破检查")
    run_button = st.button("运行选股并检查所有标的是否突破平台线")
    if not run_button:
        return
    df = main()
    if df is None or df.empty:
        st.warning("当前无入选标的")
        return
    st.subheader("入选标的一览")
    st.dataframe(df)
    for _, row in df.iterrows():
        symbol = str(row.get("symbol", "")).strip()
        name = str(row.get("name", "")).strip()
        platform_high = float(row.get("platform_high", 0.0))
        if not symbol or platform_high <= 0:
            continue
        _plot_stock_panel(symbol, name, platform_high)


def main():
    start_time = time.time()
    print("启动资金热点选股第二阶段（选标的）扫描...")
    if pro is None:
        print("Tushare 未初始化，退出。")
        return

    try:
        sectors = ak.stock_board_industry_name_em()
    except Exception as e:
        print(f"获取板块列表失败: {e}")
        print(traceback.format_exc())
        return

    if sectors is None or sectors.empty:
        print("未获取到板块数据。")
        return

    sectors = sectors.copy()
    sectors = sectors[~sectors["板块名称"].astype(str).str.contains("ST")]
    sectors = sectors.sort_values(by="涨跌幅", ascending=False)
    top_sectors = sectors.head(8)
    sector_list = top_sectors["板块名称"].tolist()
    print("热点板块:", sector_list)

    candidates = []
    for sector in sector_list:
        try:
            cons = ak.stock_board_industry_cons_em(symbol=sector)
            if cons is not None and not cons.empty:
                df_cons = cons.copy()
                if "最新价" in df_cons.columns:
                    df_cons["最新价"] = pd.to_numeric(df_cons["最新价"], errors="coerce")
                    df_cons = df_cons[df_cons["最新价"] >= 3]
                if "成交额" in df_cons.columns:
                    df_cons["成交额"] = pd.to_numeric(df_cons["成交额"], errors="coerce")
                    df_cons = df_cons[df_cons["成交额"] >= 50000000]
                for _, row in df_cons.iterrows():
                    code = str(row.get("代码", "")).strip().zfill(6)
                    name = str(row.get("名称", "")).strip()
                    if code and name:
                        candidates.append(
                            {"code": code, "name": name, "sector": sector}
                        )
            time.sleep(0.3)
        except Exception as e:
            print(f"获取板块成分股失败 {sector}: {e}")
            print(traceback.format_exc())

    if not candidates:
        print("候选池为空。")
        return

    print(f"候选池股票数: {len(candidates)}")
    analyzed_rows = []
    total = len(candidates)
    stat_total_analyzed = 0
    stat_pass_gap = 0
    stat_pass_trend = 0
    stat_pass_vol = 0
    stat_pass_surge_or_strongvol = 0
    for idx, item in enumerate(candidates):
        data = fetch_daily_history(item["code"], item["name"], item["sector"])
        if data is not None:
            try:
                metrics = analyze_stock(data)
                stat_total_analyzed += 1
                cond_gap = -5.0 <= metrics["gap_to_platform_pct"] <= 20.0
                cond_trend = metrics["trending"] >= 0.18
                cond_vol = metrics["last_vol_ratio"] >= 1.3
                cond_surge = metrics["has_surge"]
                cond_surge_or_strongvol = cond_surge or metrics["last_vol_ratio"] >= 2.0
                if cond_gap:
                    stat_pass_gap += 1
                if cond_gap and cond_trend:
                    stat_pass_trend += 1
                if cond_gap and cond_trend and cond_vol:
                    stat_pass_vol += 1
                if cond_gap and cond_trend and cond_vol and cond_surge_or_strongvol:
                    stat_pass_surge_or_strongvol += 1
                    analyzed_rows.append(metrics)
            except Exception as e:
                print(f"分析个股失败 {item['code']}: {e}")
                print(traceback.format_exc())
        if idx % 20 == 0:
            print(f"进度: {idx}/{total}")

    print(
        f"统计: 有效个股 {stat_total_analyzed}, 价位条件通过 {stat_pass_gap}, "
        f"趋势条件通过 {stat_pass_trend}, 量能条件通过 {stat_pass_vol}, "
        f"综合条件通过 {stat_pass_surge_or_strongvol}"
    )

    if not analyzed_rows:
        print("无符合条件的标的。")
        return

    df = pd.DataFrame(analyzed_rows)
    df = df.sort_values(by="score", ascending=False).reset_index(drop=True)
    print("Top 标的（仅展示前 50 条）：")
    print(df.head(50).to_string(index=False))
    print(f"完成，耗时 {time.time() - start_time:.2f} 秒。")
    return df


if __name__ == "__main__":
    if "--streamlit" in sys.argv:
        run_streamlit_dashboard()
    else:
        main()
