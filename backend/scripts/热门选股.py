import sys
from pathlib import Path
import akshare as ak
import streamlit as st
import altair as alt

backend_dir = Path(__file__).resolve().parents[1]
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from utils.tushare_client import ts, pro
import datetime

if pro is None:
    raise SystemExit("Tushare pro 未初始化")

try:
    # current_date = datetime.datetime.now().strftime("%Y%m%d")

    # 行业资金流
    # Index(['序号', '名称', '今日涨跌幅', '今日主力净流入-净额', '今日主力净流入-净占比', '今日超大单净流入-净额', 
    #    '今日超大单净流入-净占比', '今日大单净流入-净额', '今日大单净流入-净占比', '今日中单净流入-净额',
    #    '今日中单净流入-净占比', '今日小单净流入-净额', '今日小单净流入-净占比', '今日主力净流入最大股'],
    #   dtype='object')


    st.title('Streamlit 柱状图基础示例')
    df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流")
    df.columns = df.columns.str.strip()
    df.sort_values(by="今日主力净流入-净额", ascending=False, inplace=True)
    st.subheader('显示原始数据')
    st.dataframe(df)

    # 使用 Altair 自定义图表以显示所有 X 轴标签
    st.subheader('今日主力净流入-净额 柱状图')
    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X('名称', sort='-y', axis=alt.Axis(labelAngle=-90)),
        y='今日主力净流入-净额',
        tooltip=['名称', '今日主力净流入-净额']
    ).interactive()
    st.altair_chart(chart, use_container_width=True)

    
    # # 行业资金流详情
    # df = ak.stock_sector_fund_flow_summary(symbol="证券", indicator="今日")
    # # print(df)

    # # 概念资金流详情
    # df = ak.stock_individual_fund_flow(stock="600094", market="sh")
    # print(df.columns)

except Exception as e:
    print(f"stock_board_concept_name_em 失败：{e}")


