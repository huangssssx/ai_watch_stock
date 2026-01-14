# -*- coding: utf-8 -*-
import os
import sys
_backend_dir = os.path.dirname(os.path.abspath(__file__))
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)
from pymr_compat import ensure_py_mini_racer
ensure_py_mini_racer()
import streamlit as st
import pandas as pd
import akshare as ak
import altair as alt
from datetime import datetime
import traceback
import logging
import sys
import io

# ==============================================================================
# 0. 日志配置 (增强版)
# ==============================================================================
# 创建一个 StringIO 对象来捕获日志流，以便在 UI 上显示
log_capture_string = io.StringIO()

# 配置日志记录器
logger = logging.getLogger("StockApp")
logger.setLevel(logging.INFO)

# 清除旧的处理器，防止 Streamlit 重载导致重复打印
if logger.hasHandlers():
    logger.handlers.clear()

# 1. 控制台处理器 (打印到终端)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

# 2. 字符串流处理器 (用于在 UI 显示)
stream_handler = logging.StreamHandler(log_capture_string)
stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(stream_handler)

def log_info(msg):
    """统一日志记录入口"""
    logger.info(msg)

def log_error(msg):
    """统一错误记录入口"""
    logger.error(msg)

# ==============================================================================
# 1. 页面基础配置
# ==============================================================================
st.set_page_config(
    page_title="A股行业资金流向看板 (修正版)",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 兼容性检查
try:
    st_version = st.__version__
    major, minor, patch = map(int, st_version.split('.')[:3])
    if major < 1 or (major == 1 and minor < 35):
        st.error(f"⚠️ 检测到您的 Streamlit 版本 ({st_version}) 较旧。建议升级到 1.35.0+")
except:
    pass

# ==============================================================================
# 2. 核心逻辑区 - 数据获取与处理
# ==============================================================================
class DataManager:
    """数据管理类：负责数据的获取、清洗与缓存"""
    
    @staticmethod
    def _safe_numeric(series):
        """辅助函数：安全地将含有单位(万/亿)的字符串转为数值"""
        def convert(x):
            if pd.isna(x) or x == "": return 0.0
            if isinstance(x, (int, float)): return float(x)
            x = str(x).replace("元", "").replace(",", "")
            factor = 1.0
            if "万" in x:
                factor = 10000.0
                x = x.replace("万", "")
            elif "亿" in x:
                factor = 100000000.0
                x = x.replace("亿", "")
            try:
                return float(x) * factor
            except:
                return 0.0
        return series.apply(convert)

    @staticmethod
    @st.cache_data(ttl=300)
    def get_sector_flow_rank():
        """获取行业资金流向排名数据"""
        log_info("🚀 [Start] 开始调用 ak.stock_sector_fund_flow_rank()...")
        try:
            with st.spinner("正在从 AkShare 拉取行业数据..."):
                df = ak.stock_sector_fund_flow_rank()
                
            if df is None or df.empty:
                log_error("❌ [Error] 接口返回数据为空 (None or Empty)")
                st.warning("接口返回数据为空")
                return pd.DataFrame()

            log_info(f"✅ [Fetch] 原始数据获取成功，形状: {df.shape}")

            # 数据清洗
            df = df.dropna(how='all').drop_duplicates()
            
            # 列名兼容性处理
            col_mapping = {
                "名称": "行业名称",
                "今日主力净流入-净额": "主力净流入",
                "今日主力净流入-净占比": "主力净流入-净占比"
            }
            df = df.rename(columns=col_mapping)
            
            # 检查列名是否映射成功
            if "行业名称" not in df.columns:
                log_error(f"❌ [Error] 缺少 '行业名称' 列，当前列名: {list(df.columns)}")
                return pd.DataFrame()

            # 类型转换
            num_cols = ["主力净流入", "主力净流入-净占比"]
            for col in num_cols:
                if col in df.columns:
                    df[col] = DataManager._safe_numeric(df[col])
            
            # 排序
            df = df.sort_values(by="主力净流入", ascending=False).reset_index(drop=True)
            return df

        except Exception as e:
            err_msg = traceback.format_exc()
            log_error(f"❌ [Exception] 获取行业数据发生异常:\n{err_msg}")
            return pd.DataFrame()

    @staticmethod
    @st.cache_data(ttl=600)
    def get_sector_details(sector_name):
        """
        获取指定行业的成分股列表
        使用 ak.stock_board_industry_cons_em 接口 (稳健)
        """
        log_info(f"🚀 [Start] 获取板块成分股: {sector_name}")
        try:
            # 使用用户指定的接口
            df = ak.stock_board_industry_cons_em(symbol=sector_name)
            
            if df is not None and not df.empty:
                log_info(f"✅ [Fetch] 成分股获取成功，行数: {len(df)}")
                # 筛选核心列
                cols_to_keep = ['代码', '名称', '最新价', '涨跌幅', '成交额', '换手率', '市盈率-动态']
                # 兼容不同版本返回的列名
                existing_cols = [c for c in cols_to_keep if c in df.columns]
                df = df[existing_cols]
                
                # 简单数值处理
                if '成交额' in df.columns:
                    df['成交额'] = DataManager._safe_numeric(df['成交额'])
                    
                return df
            else:
                log_error(f"❌ [Error] 板块 [{sector_name}] 返回数据为空")
                return pd.DataFrame()
        except Exception as e:
            log_error(f"❌ [Exception] 获取成分股失败: {str(e)}")
            return pd.DataFrame()

# ==============================================================================
# 3. UI 组件区
# ==============================================================================

if hasattr(st, "dialog"):
    @st.dialog("板块个股详情", width="large")
    def show_stock_list_dialog(sector_name):
        _render_stock_list(sector_name)
else:
    def show_stock_list_dialog(sector_name):
        st.sidebar.markdown("---")
        st.sidebar.subheader(f"📌 {sector_name} - 个股详情")
        _render_stock_list(sector_name)

def _render_stock_list(sector_name):
    """抽离的渲染逻辑"""
    st.caption(f"当前板块：{sector_name} (数据源: 东方财富-板块成份)")
    
    with st.spinner(f"正在加载 {sector_name} 的股票列表..."):
        df_stocks = DataManager.get_sector_details(sector_name)
    
    if df_stocks.empty:
        st.warning(f"⚠️ 未能获取到 [{sector_name}] 的成分股数据，请稍后重试。")
    else:
        # 配置列显示格式
        column_cfg = {
            "代码": st.column_config.TextColumn("代码"),
            "名称": st.column_config.TextColumn("名称"),
            "最新价": st.column_config.NumberColumn("最新价", format="%.2f"),
            "涨跌幅": st.column_config.NumberColumn("涨跌幅", format="%.2f%%"),
            "成交额": st.column_config.NumberColumn("成交额", format="￥%.0f"),
            "换手率": st.column_config.NumberColumn("换手率", format="%.2f%%"),
            "市盈率-动态": st.column_config.NumberColumn("PE(动)", format="%.1f"),
        }
        
        st.dataframe(
            df_stocks,
            use_container_width=True,
            hide_index=True,
            column_config=column_cfg
        )

# ==============================================================================
# 4. 主程序入口
# ==============================================================================
def main():
    # --- 侧边栏 ---
    with st.sidebar:
        st.header("⚙️ 参数配置")
        top_n = st.slider("展示行业数量", 10, 50, 20)
        refresh_btn = st.button("🔄 刷新数据")
        
        if refresh_btn:
            st.cache_data.clear()
            st.rerun()

    st.title("🚀 A股行业资金流向透视")
    
    # 1. 获取主榜单数据
    df_all = DataManager.get_sector_flow_rank()
    
    if df_all.empty:
        st.error("数据加载失败，请检查网络或稍后重试。")
        st.stop()

    # 2. 截取 Top N
    df_view = df_all.head(top_n).copy()

    # --- 核心交互图表 (Altair) ---
    st.subheader(f"📊 热门行业资金流向 (Top {top_n})")
    st.info("👆 点击下方的柱状图，可查看该行业的成分股列表")

    # 定义基础图表
    base = alt.Chart(df_view).encode(
        x=alt.X('行业名称', sort=None, title="行业板块"),
        y=alt.Y('主力净流入', title="主力净流入(元)"),
        tooltip=['行业名称', '主力净流入', '主力净流入-净占比']
    ).properties(height=450)

    # [关键修复] 定义具名选择器，用于捕获点击事件
    # name='select_sector' 是必须的，这样在 event.selection 中才能通过这个名字取值
    click_selection = alt.selection_point(name='select_sector', fields=['行业名称'], on='click')

    # 绘制柱状图，并绑定选择器
    bars = base.mark_bar().encode(
        # 选中时完全不透明，未选中时半透明
        opacity=alt.condition(click_selection, alt.value(1.0), alt.value(0.3)),
        color=alt.condition(
            alt.datum['主力净流入'] > 0,
            alt.value("#f5222d"),  # 红
            alt.value("#52c41a")   # 绿
        )
    ).add_params(click_selection)

    # 渲染图表，on_select="rerun" 触发生效
    try:
        event = st.altair_chart(bars, use_container_width=True, on_select="rerun")
    except TypeError:
        st.altair_chart(bars, use_container_width=True)
        st.error("您的 Streamlit 版本不支持 on_select，请升级到 1.35.0 以上。")
        return

    # --- 处理点击事件 ---
    # [关键修复] 之前的 AttributeError 是因为使用了 event.selection.rows
    # 正确的做法是根据选择器名称 ('select_sector') 从字典中取出数据
    if event.selection and 'select_sector' in event.selection:
        selection_list = event.selection['select_sector']
        
        if selection_list and len(selection_list) > 0:
            # 获取被点击的行业名称
            sector_data = selection_list[0]
            sector_name = sector_data.get("行业名称")
            
            if sector_name:
                log_info(f"🖱️ 用户点击了: {sector_name}")
                # 弹出模态窗口
                show_stock_list_dialog(sector_name)

    # --- 底部数据预览 ---
    with st.expander("查看榜单源数据"):
        st.dataframe(df_view)

if __name__ == "__main__":
    main()