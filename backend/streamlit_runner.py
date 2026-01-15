import os
import sys
try:
    getattr(sys.stderr, 'flush', lambda: None)()
except Exception:
    try:
        sys.stderr = open(os.devnull, 'w')
    except Exception:
        pass
os.environ.setdefault('TQDM_DISABLE', '1')
_backend_dir = os.path.dirname(os.path.abspath(__file__))
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)
from pymr_compat import ensure_py_mini_racer
ensure_py_mini_racer()

import streamlit as st
import os
import sys
import akshare as ak
import pandas as pd
import json
import datetime
import time
import traceback

try:
    sys.stderr = open(os.devnull, "w")
except Exception:
    pass

# --- Configuration ---
st.set_page_config(page_title="大盘全景看板", layout="wide")

# --- Styles ---
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
    }
    .metric-value {
        font-size: 24px;
        font-weight: bold;
    }
    .metric-delta {
        font-size: 14px;
    }
    .stButton>button {
        width: 100%;
    }
</style>
""", unsafe_allow_html=True)

# --- Data Fetching (Cached) ---
@st.cache_data(ttl=60)
def load_market_data(run_token: str):
    data = {}
    logs = []

    def _try_call(label: str, fn, retries: int = 2, sleep_s: float = 0.6):
        last_err = None
        for i in range(retries + 1):
            t0 = time.time()
            try:
                v = fn()
                dt = time.time() - t0
                logs.append(f"[OK] {label} {dt:.2f}s")
                return v
            except Exception as e:
                dt = time.time() - t0
                last_err = e
                logs.append(f"[ERR] {label} {dt:.2f}s {repr(e)}")
                logs.append(traceback.format_exc())
                if i < retries:
                    time.sleep(sleep_s)
        raise last_err

    # 1. Indices (Sina is fast and stable)
    try:
        df_index = _try_call("stock_zh_index_spot_sina", lambda: ak.stock_zh_index_spot_sina())
        # Filter Key Indices
        targets = ["上证指数", "深证成指", "创业板指", "科创50"] 
        # Note: Sina names might vary slightly, e.g. "上证指数"
        if isinstance(df_index, pd.DataFrame) and (not df_index.empty) and ("名称" in df_index.columns):
            data['indices'] = df_index[df_index['名称'].isin(targets)].copy()
        else:
            data['indices'] = pd.DataFrame()
    except Exception as e:
        st.error(f"指数数据获取失败: {e}")
        data['indices'] = pd.DataFrame()

    # 2. Northbound Funds
    try:
        df_north = _try_call("stock_hsgt_fund_flow_summary_em", lambda: ak.stock_hsgt_fund_flow_summary_em())
        if isinstance(df_north, pd.DataFrame):
            data['hsgt'] = df_north.copy()
        else:
            data['hsgt'] = pd.DataFrame()
        # Usually row 0 is Northbound (沪股通+深股通 sum is not directly given, need to sum)
        # Structure: 沪股通(North), 港股通(South), 深股通(North), 港股通(South)
        # We need rows where "资金方向" == "北向"
        if not df_north.empty and '资金方向' in df_north.columns:
            data['north'] = df_north[df_north['资金方向'] == '北向'].copy()
            data['south'] = df_north[df_north['资金方向'] == '南向'].copy()
        else:
            data['north'] = pd.DataFrame()
            data['south'] = pd.DataFrame()
    except Exception as e:
        # Fallback
        data['hsgt'] = pd.DataFrame()
        data['north'] = pd.DataFrame()
        data['south'] = pd.DataFrame()

    # 3. Market Summary (Breadth)
    try:
        sse = _try_call("stock_sse_summary", lambda: ak.stock_sse_summary())
        szse = _try_call("stock_szse_summary", lambda: ak.stock_szse_summary())
        data['sse'] = sse
        data['szse'] = szse
    except:
        pass

    # 4. Sectors
    try:
        sectors = _try_call("stock_board_industry_name_em", lambda: ak.stock_board_industry_name_em())
        data['sectors'] = sectors
    except:
        data['sectors'] = pd.DataFrame()

    data["_logs"] = "\n".join(logs[-120:])
    return data

# --- UI Layout ---

col_header_1, col_header_2 = st.columns([3, 1])
with col_header_1:
    st.title("📊 A股大盘全景监测")
    st.caption(f"最后更新: {datetime.datetime.now().strftime('%H:%M:%S')}")

with col_header_2:
    if st.button("🔄 立即刷新数据"):
        st.session_state["_market_run_token"] = str(time.time())
        st.rerun()

if "_market_run_token" not in st.session_state:
    st.session_state["_market_run_token"] = str(time.time())

if "_market_first_enter_done" not in st.session_state:
    st.session_state["_market_first_enter_done"] = True
    st.session_state["_market_run_token"] = str(time.time())

# Load Data
with st.spinner("正在连接行情中心..."):
    market_data = load_market_data(st.session_state["_market_run_token"])

with st.expander("运行日志", expanded=False):
    st.code(market_data.get("_logs", ""), language="text")

with st.expander("导出数据(JSON)", expanded=False):
    indices_df_export = market_data.get("indices", pd.DataFrame())
    hsgt_df_export = market_data.get("hsgt", pd.DataFrame())
    sectors_df_export = market_data.get("sectors", pd.DataFrame())

    include_full_sectors = st.checkbox("包含全量行业列表", value=False)
    export_payload = {
        "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "indices": indices_df_export.to_dict(orient="records") if isinstance(indices_df_export, pd.DataFrame) else [],
        "fund_flow": hsgt_df_export.to_dict(orient="records") if isinstance(hsgt_df_export, pd.DataFrame) else [],
        "sectors_top10": [],
        "sectors_bottom10": [],
        "sectors": [],
    }

    if isinstance(sectors_df_export, pd.DataFrame) and (not sectors_df_export.empty) and ("涨跌幅" in sectors_df_export.columns):
        top_10_export = sectors_df_export.sort_values(by="涨跌幅", ascending=False).head(10)
        bottom_10_export = sectors_df_export.sort_values(by="涨跌幅", ascending=True).head(10)
        export_payload["sectors_top10"] = top_10_export.to_dict(orient="records")
        export_payload["sectors_bottom10"] = bottom_10_export.to_dict(orient="records")
        if include_full_sectors:
            export_payload["sectors"] = sectors_df_export.to_dict(orient="records")

    export_json = json.dumps(export_payload, ensure_ascii=False, indent=2, default=str)
    st.download_button(
        "⬇️ 导出 JSON",
        data=export_json,
        file_name=f"market_dashboard_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        mime="application/json",
        use_container_width=True,
    )
    st.code(export_json, language="json")

# --- Section 1: Key Indices ---
st.subheader("核心指数")
cols = st.columns(4)
indices_df = market_data.get('indices', pd.DataFrame())

if not indices_df.empty:
    # Sort to ensure order if possible, or just iterate
    # Target order: SH, SZ, CYB, KC50
    target_order = ["上证指数", "深证成指", "创业板指", "科创50"]
    
    for i, name in enumerate(target_order):
        row = indices_df[indices_df['名称'] == name]
        if not row.empty:
            price = pd.to_numeric(row.iloc[0]['最新价'], errors='coerce')
            change = pd.to_numeric(row.iloc[0]['涨跌幅'], errors='coerce')
            with cols[i]:
                st.metric(label=name, value=f"{price:.2f}" if pd.notna(price) else "-", delta=f"{change:.2f}%" if pd.notna(change) else None)
else:
    st.warning("暂无指数数据")

st.divider()

# --- Section 2: Market Sentiment & Funds ---
col_fund, col_breadth = st.columns([1, 2])

with col_fund:
    st.subheader("💸 资金风向")
    hsgt_df = market_data.get('hsgt', pd.DataFrame())
    if not hsgt_df.empty:
        direction = st.segmented_control(
            "资金方向",
            options=["北向", "南向", "全部"],
            default="北向",
            label_visibility="collapsed",
        )
        if direction == "北向":
            df_flow = hsgt_df[hsgt_df.get('资金方向', '') == '北向'].copy()
        elif direction == "南向":
            df_flow = hsgt_df[hsgt_df.get('资金方向', '') == '南向'].copy()
        else:
            df_flow = hsgt_df.copy()

        total_in = float("nan")
        total_buy = float("nan")
        try:
            if '资金净流入' in df_flow.columns:
                total_in = pd.to_numeric(df_flow['资金净流入'], errors='coerce').sum(min_count=1)
            if '成交净买额' in df_flow.columns:
                total_buy = pd.to_numeric(df_flow['成交净买额'], errors='coerce').sum(min_count=1)
        except:
            pass

        status_hint = ""
        try:
            if '交易状态' in df_flow.columns:
                status_vals = [str(x) for x in pd.unique(df_flow['交易状态'].dropna()).tolist()]
                if status_vals:
                    status_hint = f"交易状态: {', '.join(status_vals)}"
        except:
            pass

        if pd.isna(total_buy) and pd.isna(total_in):
            st.info("资金接口返回为空或字段无法解析")
        elif pd.notna(total_buy):
            st.metric(
                f"{direction}成交净买额(合计)",
                f"{total_buy:.2f}",
                delta="流入" if total_buy > 0 else "流出"
            )
        elif pd.notna(total_in):
            st.metric(
                f"{direction}资金净流入(合计)",
                f"{total_in:.2f}",
                delta="流入" if total_in > 0 else "流出"
            )

        if direction == "北向" and (pd.notna(total_buy) and abs(float(total_buy)) < 1e-9) and status_hint:
            st.caption(f"提示：当前北向数据为 0，{status_hint}（可能休市/上游暂无数据）")
        elif status_hint:
            st.caption(status_hint)

        show_cols = [c for c in ['交易日', '板块', '资金方向', '交易状态', '资金净流入', '成交净买额', '当日资金余额'] if c in df_flow.columns]
        if show_cols:
            st.dataframe(df_flow[show_cols], hide_index=True)
        else:
            st.dataframe(df_flow, hide_index=True)
    else:
        st.info("资金数据不可用")

with col_breadth:
    st.subheader("🌡️ 市场温度")
    # Calculate approximate Up/Down from Summary if available, or just verify
    # SSE Summary has '上市股票' but not Up/Down count directly. 
    # SZSE Summary also general.
    # To get exact Up/Down, we need a snapshot or estimate.
    # Let's use Sectors as a proxy for heat.
    
    sectors = market_data.get('sectors', pd.DataFrame())
    if not sectors.empty:
        up_sectors = len(sectors[sectors['涨跌幅'] > 0])
        down_sectors = len(sectors[sectors['涨跌幅'] < 0])
        total_sectors = len(sectors)
        
        st.write(f"行业板块涨跌分布: 🟥 {up_sectors} 涨 / 🟩 {down_sectors} 跌")
        
        # Simple progress bar for sentiment
        sentiment_score = up_sectors / total_sectors if total_sectors > 0 else 0.5
        st.progress(sentiment_score, text=f"市场情绪 (行业维度): {int(sentiment_score*100)}%")
    else:
        st.info("板块数据不可用")

st.divider()

# --- Section 3: Sector Performance ---
st.subheader("🚀 行业热度榜")

sectors = market_data.get('sectors', pd.DataFrame())
if not sectors.empty:
    # Top 10 Gainers
    top_10 = sectors.sort_values(by="涨跌幅", ascending=False).head(10)
    # Bottom 10 Losers
    bottom_10 = sectors.sort_values(by="涨跌幅", ascending=True).head(10)
    
    col_top, col_bottom = st.columns(2)
    
    with col_top:
        st.markdown("**涨幅 Top 10**")
        df_up = top_10[['板块名称', '涨跌幅']].set_index('板块名称')
        st.bar_chart(df_up, height=380)
        
    with col_bottom:
        st.markdown("**跌幅 Top 10**")
        df_down = bottom_10[['板块名称', '涨跌幅']].set_index('板块名称')
        st.bar_chart(df_down, height=380)
else:
    st.error("无法加载行业数据")

