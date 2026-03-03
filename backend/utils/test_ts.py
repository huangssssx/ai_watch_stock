import tushare as ts
#tushare版本 1.4.24
token = "f5187841c7d5663c97cd3a4125214b8fa7f7866fa32fb2ea93e9bebfebba"

pro = ts.pro_api(token)

pro._DataApi__token = token # 保证有这个代码，不然不可以获取
pro._DataApi__http_url = 'http://lianghua.nanyangqiankun.top'  # 保证有这个代码，不然不可以获取

# #  正常使用（与官方API完全一致）
df = pro.ths_hot(trade_date='20260302', market='热股', fields='ts_code,ts_name,hot,concept')
#  ts_code 去重
df = df.drop_duplicates(subset=['ts_code'])


print(df)