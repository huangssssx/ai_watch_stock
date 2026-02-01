import akshare as ak
import pandas as pd
import numpy as np
import time
import datetime
import traceback
import sys
import os
import sqlite3
import json

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
    print("❌ Failed to import tushare_client. Ensure you are running from project root or backend is in python path.")
    pro = None

import tushare as ts

#设置你的token，登录tushare在个人用户中心里拷贝
ts.set_token('68656717691d5af91958a8b613652ab7fe532c9f7827dd09609aefd0')

# 获取所有上市股票的基本信息
df = pro.stock_basic(
    exchange='', 
    list_status='L', 
    fields='ts_code,symbol,name,area,industry,market,list_date'
)


'''
获取所有上市股票的涨幅排行列表
'''
step = 50
stock_code_list = df['ts_code'].tolist()
# stock_code_list = stock_code_list[:100]
# 按照 step 个进行分割为二维数组
stock_step_code_list = [stock_code_list[i:i+step] for i in range(0, len(stock_code_list), step)]
print(stock_step_code_list)

# ['NAME', 'TS_CODE', 'DATE', 'TIME', 'OPEN', 'PRE_CLOSE', 'PRICE', 'HIGH',
#        'LOW', 'BID', 'ASK', 'VOLUME', 'AMOUNT', 'B1_V', 'B1_P', 'B2_V', 'B2_P',
#        'B3_V', 'B3_P', 'B4_V', 'B4_P', 'B5_V', 'B5_P', 'A1_V', 'A1_P', 'A2_V',
#        'A2_P', 'A3_V', 'A3_P', 'A4_V', 'A4_P', 'A5_V', 'A5_P'], 
# 全市场当前 tick 数据
df_all = pd.DataFrame() 
for stock_100_code in stock_step_code_list:  
    stock_code_str = ','.join(stock_100_code)
    df = ts.realtime_quote(ts_code=stock_code_str,src="sina")
    df_all = pd.concat([df_all, df], axis=0,ignore_index=True)

# 获取所有股票的涨幅数据
df_all['change'] = df_all['PRICE'] - df_all['PRE_CLOSE']
# 计算涨幅百分比
df_all['change_percent'] = df_all['change'] / df_all['PRE_CLOSE'] * 100

# 截取 change_percent在 3%～5% 之间的股票
df_all = df_all[(df_all['change_percent'] >= 3) & (df_all['change_percent'] <= 5)]

print(df_all[["TS_CODE", "NAME", "PRICE", "PRE_CLOSE", "change", "change_percent"]].sort_values(by="change_percent", ascending=False))


'''
去掉所有量比小于 1.2 的股票
做什么：用“当日累计量”构造代理量比：今日累计成交量 / 近N日平均日成交量
接口：
ts.realtime_quote(ts_code=..., src='sina')
pro.daily(ts_code=..., start_date=..., end_date=...)
'''
# 计算近N日平均日成交量
pro.daily(ts_code=stock_code_str, start_date='20230101', end_date='20230105')



# df_all = df_all[df_all['VOLUME'] >= df_all['PRE_VOLUME'] * 1.2]
