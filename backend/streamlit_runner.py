import streamlit as st
import pandas as pd
import numpy as np
import akshare as ak
import datetime

# ==========================================
# 0. 页面配置与全局设置
# ==========================================
st.set_page_config(
    page_title="个股 PE-Band 估值分析工具",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 隐藏部分默认样式
hide_streamlit_style = """
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
</style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

# ==========================================
# 1. 核心逻辑函数封装 (带缓存)
# ==========================================

@st.cache_data(ttl=3600)  # 设置缓存有效期为1小时
def get_stock_price(symbol, lookback_days):
    """
    [Data Fetch] 获取日线行情
    """
    end_date = datetime.datetime.now().strftime("%Y%m%d")
    # 多拉取一年数据，确保开头有财务数据覆盖
    start_date = (datetime.datetime.now() - datetime.timedelta(days=lookback_days + 365)).strftime("%Y%m%d")
    
    try:
        df_price = ak.stock_zh_a_hist(symbol=symbol, start_date=start_date, end_date=end_date, adjust="qfq")
        if df_price is None or df_price.empty:
            return None
        
        df_price = df_price[['日期', '收盘']].rename(columns={'日期': 'date', '收盘': 'close'})
        df_price['date'] = pd.to_datetime(df_price['date'])
        df_price = df_price.sort_values('date')
        return df_price
    except Exception as e:
        st.error(f"行情数据获取失败: {e}")
        return None

@st.cache_data(ttl=3600)
def get_financial_data(symbol):
    """
    [Data Fetch] 获取并清洗财务EPS数据 (TTM + 异常值处理)
    """
    try:
        df_abstract = ak.stock_financial_abstract(symbol=symbol)
        if df_abstract is None or df_abstract.empty:
            return None, "未找到财务数据"

        # 1. 模糊匹配寻找 EPS 行
        df_abstract['指标'] = df_abstract['指标'].astype(str)
        target_keywords = ["基本每股收益", "每股收益(基本)", "每股收益", "归属母公司股东的净利润"]
        
        target_row = None
        row_name = ""
        for kw in target_keywords:
            mask = df_abstract['指标'].str.contains(kw)
            if mask.any():
                target_row = df_abstract[mask].iloc[0]
                row_name = kw
                break
        
        if target_row is None:
            return None, "未找到EPS相关指标"

        # 2. TTM 年化处理
        date_cols = [c for c in df_abstract.columns if c.isdigit() and len(c) == 8]
        eps_records = []
        
        for d_col in date_cols:
            try:
                dt = pd.to_datetime(d_col, format='%Y%m%d')
                val = float(target_row[d_col])
                
                # --- TTM 年化算法 ---
                month = dt.month
                annual_eps = val 
                if month == 3: annual_eps = val * 4
                elif month == 6: annual_eps = val * 2
                elif month == 9: annual_eps = val / 3 * 4
                
                if annual_eps > 0.001:
                    eps_records.append({'date': dt, 'eps': annual_eps})
            except:
                continue
        
        df_fin = pd.DataFrame(eps_records).sort_values(by='date')

        # 3. 异常值剔除 (3-Sigma)
        if len(df_fin) > 8:
            mean_eps = df_fin['eps'].mean()
            std_eps = df_fin['eps'].std()
            upper = mean_eps + 3 * std_eps
            lower = mean_eps - 3 * std_eps
            df_fin = df_fin[(df_fin['eps'] <= upper) & (df_fin['eps'] >= lower)]
            
        return df_fin, row_name
        
    except Exception as e:
        st.error(f"财务数据解析失败: {e}")
        return None, str(e)

def calculate_pe_band(df_price, df_fin, pe_list, lookback_days):
    """
    [Core Calc] 合并数据并计算PE通道
    """
    # Merge Asof
    df_merge = pd.merge_asof(df_price, df_fin, on='date', direction='backward')
    df_merge['eps'] = df_merge['eps'].ffill()
    df_merge = df_merge.dropna(subset=['eps'])
    
    # Calculate Bands
    for pe in pe_list:
        df_merge[f"PE {pe}x"] = df_merge['eps'] * pe
        
    # Crop Data
    df_final = df_merge.tail(lookback_days).copy()
    return df_final

# ==========================================
# 2. UI 布局与交互逻辑
# ==========================================

# --- Sidebar: 参数设置区 ---
with st.sidebar:
    st.header("⚙️ 参数配置")
    
    input_symbol = st.text_input("股票代码 (Symbol)", value="002371", help="输入A股代码，如 600519 或 002371")
    
    st.subheader("估值通道设置")
    pe1 = st.number_input("低估线 (Low PE)", value=20, step=1)
    pe2 = st.number_input("中枢线 (Mid PE)", value=30, step=1)
    pe3 = st.number_input("高估线 (High PE)", value=40, step=1)
    target_pe_list = [pe1, pe2, pe3]
    
    lookback = st.slider("回溯天数 (Lookback)", min_value=100, max_value=2000, value=500, step=100)
    
    run_btn = st.button("🚀 开始分析", type="primary")

# --- Main: 主界面逻辑 ---
st.title(f"📊 A股深度估值分析工具")
st.caption("数据来源: AkShare开源接口 | 模型: TTM动态市盈率 + 3-Sigma清洗")

if run_btn:
    with st.spinner(f"正在拉取 {input_symbol} 的数据，请稍候..."):
        # 1. 获取数据
        df_price = get_stock_price(input_symbol, lookback)
        df_fin, idx_name = get_financial_data(input_symbol)
        
        if df_price is not None and df_fin is not None:
            # 2. 核心计算
            df_result = calculate_pe_band(df_price, df_fin, target_pe_list, lookback)
            
            if df_result.empty:
                st.warning("⚠️ 计算结果为空，请检查股票代码或调整回溯时间。")
            else:
                # 3.1 核心指标看板 (Metrics)
                latest = df_result.iloc[-1]
                curr_price = latest['close']
                curr_eps = latest['eps']
                curr_pe = curr_price / curr_eps
                
                # 估值状态判定
                if curr_pe < target_pe_list[0]:
                    status = "🟢 极度低估"
                    delta_color = "normal" 
                elif curr_pe < target_pe_list[1]:
                    status = "🟡 相对低估"
                    delta_color = "off"
                elif curr_pe < target_pe_list[2]:
                    status = "🟠 相对高估"
                    delta_color = "inverse"
                else:
                    status = "🔴 极度高估"
                    delta_color = "inverse"

                st.markdown("### 📌 核心指标摘要")
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("最新收盘价", f"¥{curr_price:.2f}")
                col2.metric("当前 TTM PE", f"{curr_pe:.2f} x", delta=f"{curr_pe - target_pe_list[1]:.1f} (vs 中枢)", delta_color="inverse")
                col3.metric("年化 EPS", f"¥{curr_eps:.2f}", help=f"基于指标: {idx_name}")
                col4.metric("估值状态", status)
                
                st.divider()

                # 3.2 交互式图表 (Chart)
                st.markdown(f"### 📈 PE-Band 走势图 ({input_symbol})")
                
                # 整理绘图数据：将 date 设为索引，只保留需要绘制的列
                chart_cols = ['close'] + [f"PE {pe}x" for pe in target_pe_list]
                chart_data = df_result.set_index('date')[chart_cols]
                
                # 使用 Streamlit 原生图表 (简单、美观)
                st.line_chart(
                    chart_data,
                    color=["#1890ff", "#52c41a", "#faad14", "#f5222d"], # 蓝(股价), 绿(低), 黄(中), 红(高)
                    use_container_width=True,
                    height=500
                )
                
                # 3.3 详细数据展示 (Data)
                with st.expander("🔍 查看详细历史数据 (Data Table)"):
                    st.dataframe(
                        df_result.style.format({
                            "close": "{:.2f}", 
                            "eps": "{:.4f}",
                            f"PE {pe1}x": "{:.2f}",
                            f"PE {pe2}x": "{:.2f}",
                            f"PE {pe3}x": "{:.2f}"
                        }),
                        use_container_width=True
                    )
                
                # 3.4 智能评语 (Log)
                st.info(f"""
                **💡 智能分析报告**:
                当前 **{input_symbol}** 的股价为 **{curr_price}** 元，对应的动态市盈率为 **{curr_pe:.2f}** 倍。
                相较于设定的估值中枢 (**{target_pe_list[1]}倍 PE**)，当前处于 **{status}** 区域。
                
                *注：EPS数据已剔除3-Sigma极端异常值，并基于最新财报进行TTM年化处理。*
                """)
                
        else:
            st.error("数据拉取失败，请检查股票代码是否正确，或稍后重试。")

else:
    # 初始引导页
    st.info("👈 请在左侧边栏输入参数，并点击【开始分析】按钮")