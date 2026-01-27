import streamlit as st
import pandas as pd
import datetime
import time
import os
import sys

# Ensure backend package can be found
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.append(project_root)

# Import tushare client from backend utils
from backend.utils.tushare_client import pro as ts_pro

# No need for local init_tushare anymore
# DEFAULT_TOKEN = '...' 
# DEFAULT_URL = '...'

def get_latest_trade_date(pro, date_str):
    """Get the latest trading date <= date_str"""
    df = pro.trade_cal(exchange='', start_date='20230101', end_date=date_str, is_open='1')
    if not df.empty:
        df_sorted = df.sort_values('cal_date')
        return df_sorted['cal_date'].values[-1]
    return date_str

def get_previous_trade_dates(pro, end_date, n=3):
    """Get n trading dates ending with end_date"""
    # Get a range sufficiently large to find n trading days
    start_lookback = (pd.to_datetime(end_date) - datetime.timedelta(days=20)).strftime('%Y%m%d')
    df = pro.trade_cal(exchange='', start_date=start_lookback, end_date=end_date, is_open='1')
    if not df.empty:
        df = df.sort_values('cal_date')
    if len(df) >= n:
        return df['cal_date'].values[-n:]
    return df['cal_date'].values

@st.cache_data(ttl=3600)
def get_sw_industries():
    # Fetch L1 industries. SW2021 is the current standard.
    try:
        df = ts_pro.index_classify(level='L1', src='SW2021')
        return df
    except Exception as e:
        st.error(f"Error fetching industries: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_hot_sectors(trade_date, top_n=3):
    try:
        # sw_daily returns daily performance for SW industries
        # We need to fetch for all L1 industries. 
        # Tushare sw_daily allows fetching by date.
        df = ts_pro.sw_daily(trade_date=trade_date)
        
        # Filter for L1 industries only if mixed (usually sw_daily returns what you ask or all)
        # But sw_daily by date returns all SW indexes (L1, L2, L3).
        # We need to filter for L1.
        # Let's get L1 codes first.
        l1_df = get_sw_industries()
        if l1_df is None or l1_df.empty or 'index_code' not in l1_df.columns:
            st.warning("申万行业列表为空或缺少 index_code 字段")
            return pd.DataFrame()
        l1_codes = set(l1_df['index_code'].values)
        
        # Filter df where ts_code is in l1_codes
        df_l1 = df[df['ts_code'].isin(l1_codes)].copy()
        
        if df_l1.empty:
             # Fallback: maybe the user provided date is not valid or no data yet.
             return pd.DataFrame()

        # Sort by pct_change descending
        df_sorted = df_l1.sort_values(by='pct_change', ascending=False)
        return df_sorted.head(top_n)
    except Exception as e:
        st.error(f"Error fetching hot sectors: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_sector_stocks(sector_code):
    try:
        # index_member_all gets constituents
        # Note: API parameter might be l1_code, l2_code etc.
        # We assume sector_code is L1.
        df = ts_pro.index_member_all(l1_code=sector_code, is_new='Y')
        if df is None or df.empty or 'ts_code' not in df.columns:
            return []
        return df['ts_code'].unique().tolist()
    except Exception as e:
        st.error(f"Error fetching sector stocks: {e}")
        return []

@st.cache_data(ttl=600)
def get_filtered_stocks_v2(stock_list, trade_date_str):
    results = []
    debug_logs = []
    if not stock_list:
        debug_logs.append("板块成分股为空")
        return pd.DataFrame(), debug_logs
    dates = get_previous_trade_dates(ts_pro, trade_date_str, n=3)
    if len(dates) < 3:
        st.warning("Not enough trading days data to perform analysis.")
        return pd.DataFrame(), debug_logs
    
    start_date = dates[0]
    end_date = dates[-1]
    debug_logs.append(f"基准交易日={trade_date_str}")
    debug_logs.append(f"近3个交易日={list(dates)}")
    debug_logs.append(f"日线区间={start_date}~{end_date}")
    debug_logs.append(f"成分股数={len(stock_list)}")
    
    batch_size = 50
    progress_bar = st.progress(0)
    
    for i in range(0, len(stock_list), batch_size):
        batch_stocks = stock_list[i:i+batch_size]
        for stock in batch_stocks:
            def _safe_daily():
                last_error = None
                for _ in range(3):
                    try:
                        time.sleep(0.15)
                        print(f"请求股票 {stock} 日线数据, 日期范围: {start_date} 到 {end_date}")
                        return ts_pro.daily(
                            ts_code=stock,
                            start_date=start_date,
                            end_date=end_date,
                            fields="ts_code,trade_date,close,pre_close,pct_chg,vol"
                        )
                    except Exception as e:
                        last_error = e
                        msg = str(e)
                        if "请求过于频繁" in msg or "频繁" in msg:
                            digits = "".join([c for c in msg if c.isdigit()])
                            wait_seconds = int(digits) if digits else 2
                            time.sleep(min(wait_seconds + 1, 30))
                            continue
                        time.sleep(0.5)
                if last_error is not None:
                    debug_logs.append(f"{stock} 请求失败: {last_error}")
                return None

            try:
                df_stock = _safe_daily()
                print(f"获取到股票 {stock} 日线数据: {df_stock}")

                if df_stock is None or df_stock.empty:
                    debug_logs.append(f"{stock} 日线数据为空")
                    continue
                if 'trade_date' not in df_stock.columns or 'close' not in df_stock.columns:
                    debug_logs.append(f"{stock} 日线缺少必要列")
                    continue
                df_stock = df_stock.sort_values(by='trade_date')

                if len(df_stock) < 3:
                    continue

                closes = df_stock['close'].values
                dates_stock = df_stock['trade_date'].values

                p_t2 = closes[-3]
                p_t1 = closes[-2]
                p_t = closes[-1]

                if p_t1 < p_t2 and p_t > p_t1:
                    pct_change = df_stock.iloc[-1]['pct_chg'] if 'pct_chg' in df_stock.columns else ((df_stock.iloc[-1]['close'] / (df_stock.iloc[-1]['pre_close'] if 'pre_close' in df_stock.columns else df_stock.iloc[-2]['close'])) - 1) * 100
                    vol_t = df_stock.iloc[-1]['vol'] if 'vol' in df_stock.columns else 0
                    vol_t1 = df_stock.iloc[-2]['vol'] if 'vol' in df_stock.columns else 0
                    vol_change = vol_t / (vol_t1 + 1)

                    results.append({
                        'ts_code': stock,
                        'close': p_t,
                        'pct_chg': pct_change,
                        'prev_pct_chg': df_stock.iloc[-2]['pct_chg'] if 'pct_chg' in df_stock.columns else None,
                        'vol_ratio': vol_change,
                        'latest_date': dates_stock[-1]
                    })
            except Exception as e:
                debug_logs.append(f"{stock} 处理异常: {e}")
        
        progress_bar.progress(min((i + batch_size) / len(stock_list), 1.0))
        
    progress_bar.empty()
    
    if not results:
        return pd.DataFrame(), debug_logs
        
    res_df = pd.DataFrame(results)
    
    # Fetch stock names
    if not res_df.empty:
        try:
            stock_basics = ts_pro.stock_basic(ts_code=','.join(res_df['ts_code'].tolist()), fields='ts_code,name,industry')
            res_df = pd.merge(res_df, stock_basics, on='ts_code', how='left')
            if 'name' not in res_df.columns:
                if 'name_y' in res_df.columns:
                    res_df['name'] = res_df['name_y']
                elif 'name_x' in res_df.columns:
                    res_df['name'] = res_df['name_x']
            if 'name' not in res_df.columns:
                res_df['name'] = res_df['ts_code']
        except:
            pass

    # Sort logic: 
    # Prioritize stocks with moderate rise (1-5%) and volume support (vol_ratio > 1).
    # Limit up stocks (pct_chg > 9.5) might be hard to buy or prone to opening low.
    # We sort by Vol Ratio * Pct Change (just a heuristic).
    # Or simply by Pct Change desc.
    
    res_df = res_df.sort_values(by='pct_chg', ascending=False)
    
    return res_df, debug_logs

# Main App
st.title("🔥 热门板块反弹选股助手")

with st.sidebar:
    st.header("设置")
    
    # Default to today
    today = datetime.datetime.now().strftime('%Y%m%d')
    date_input = st.date_input("选择日期", datetime.datetime.now())
    selected_date_str = date_input.strftime('%Y%m%d')
    preview_trade_date = None
    if ts_pro:
        try:
            preview_trade_date = get_latest_trade_date(ts_pro, selected_date_str)
        except Exception:
            preview_trade_date = None
    st.caption(f"分析基准交易日: {preview_trade_date or '-'}")
    
    run_btn = st.button("🔍 寻找热门板块")

if run_btn or 'hot_sectors' in st.session_state:
    if not ts_pro:
        st.error("Tushare 初始化失败，请检查 backend/utils/tushare_client.py 配置")
    else:
        trade_date_now = get_latest_trade_date(ts_pro, selected_date_str)
        st.info(f"使用交易日期: {trade_date_now} (基于选择日期 {selected_date_str})")

        should_refresh = (
            run_btn
            or ('hot_sectors' not in st.session_state)
            or (st.session_state.get('base_date_str') != selected_date_str)
            or (st.session_state.get('trade_date') != trade_date_now)
        )

        if should_refresh:
            if st.session_state.get('base_date_str') != selected_date_str or st.session_state.get('trade_date') != trade_date_now:
                st.session_state.pop('selected_sector_code', None)
                st.session_state.pop('selected_sector_name', None)

            with st.spinner(f"正在获取 {selected_date_str} 的申万一级行业数据..."):
                hot_sectors = get_hot_sectors(trade_date_now)
                if hot_sectors.empty:
                    st.warning("未找到板块数据，请检查日期或配置。")
                    st.session_state.pop('hot_sectors', None)
                    st.session_state.pop('trade_date', None)
                    st.session_state.pop('base_date_str', None)
                else:
                    st.session_state['hot_sectors'] = hot_sectors
                    st.session_state['trade_date'] = trade_date_now
                    st.session_state['base_date_str'] = selected_date_str

        if 'hot_sectors' in st.session_state:
            hot_sectors = st.session_state['hot_sectors']
            trade_date = st.session_state['trade_date']
            
            st.subheader(f"📈 Top 3 热门板块 ({trade_date})")
            
            # Display nicely
            display_cols = ['index_code', 'index_name', 'pct_change', 'close', 'vol']
            
            # Create columns for the top 3
            cols = st.columns(3)
            
            selected_sector = None
            
            # Reset iterator
            for i in range(len(hot_sectors)):
                with cols[i]:
                    row = hot_sectors.iloc[i]
                    name_val = row['name'] if 'name' in hot_sectors.columns else (row['index_name'] if 'index_name' in hot_sectors.columns else row['ts_code'])
                    value_val = f"{row['close']:.2f}" if 'close' in hot_sectors.columns else (f"{row['pct_change']:.2f}%" if 'pct_change' in hot_sectors.columns else "")
                    delta_val = f"{row['pct_change']:.2f}%" if 'pct_change' in hot_sectors.columns else ""
                    st.metric(label=name_val, value=value_val, delta=delta_val)
                    if st.button(f"查看 {name_val} 成分股", key=f"btn_{row['ts_code']}"):
                        st.session_state['selected_sector_code'] = row['ts_code']
                        st.session_state['selected_sector_name'] = name_val
                        # Rerun to show details below
                        st.rerun()

            # Step 2: Selected Sector Analysis
            if 'selected_sector_code' in st.session_state:
                sector_code = st.session_state['selected_sector_code']
                sector_name = st.session_state['selected_sector_name']
                
                st.divider()
                st.subheader(f"🔍 {sector_name} - 潜力反弹个股")
                st.markdown("筛选条件：**前两天收盘价先跌后涨** (V型反弹)")
                
                with st.spinner(f"正在分析 {sector_name} 的成分股..."):
                    # Get stocks
                    stocks = get_sector_stocks(sector_code)
                    st.write(f"板块成分股数量: {len(stocks)}")
                    
                    # Filter
                    filtered_df, debug_logs = get_filtered_stocks_v2(stocks, trade_date)
                    
                    if filtered_df.empty:
                        st.info("未找到符合筛选条件的股票。")
                        with st.expander("查看诊断日志"):
                            for log in debug_logs:
                                st.text(log)
                    else:
                        st.success(f"找到 {len(filtered_df)} 只符合条件的股票")
                        
                        # Display columns
                        if 'name' not in filtered_df.columns:
                            if 'name_y' in filtered_df.columns:
                                filtered_df = filtered_df.copy()
                                filtered_df['name'] = filtered_df['name_y']
                            elif 'name_x' in filtered_df.columns:
                                filtered_df = filtered_df.copy()
                                filtered_df['name'] = filtered_df['name_x']
                            else:
                                filtered_df = filtered_df.copy()
                                filtered_df['name'] = filtered_df['ts_code']
                        for col in ['prev_pct_chg', 'vol_ratio']:
                            if col not in filtered_df.columns:
                                filtered_df = filtered_df.copy()
                                filtered_df[col] = None
                        show_df = filtered_df[['ts_code', 'name', 'close', 'pct_chg', 'prev_pct_chg', 'vol_ratio']]
                        show_df.columns = ['代码', '名称', '现价', '今日涨幅(%)', '昨日涨幅(%)', '量比(今日/昨日)']
                        
                        st.dataframe(show_df.style.highlight_max(axis=0, subset=['今日涨幅(%)']), use_container_width=True)
                        with st.expander("查看诊断日志"):
                            if not debug_logs:
                                st.text("无")
                            else:
                                for log in debug_logs:
                                    st.text(log)
                        
                        st.markdown("""
                        **排序说明**: 
                        目前按 **今日涨幅** 降序排列。
                        - 较高的今日涨幅配合量比放大，通常意味着反弹力度强。
                        - 建议关注 **量比 > 1.0** 且 **涨幅在 3%-7%** 之间的个股，既确认了反弹又未透支上涨空间。
                        """)
