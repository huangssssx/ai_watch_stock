import akshare as ak
import pandas as pd

print("获取实时行情数据...")
df_spot = ak.stock_zh_a_spot_em()

print(f"\n总股票数: {len(df_spot)}")
print(f"\n列名: {df_spot.columns.tolist()}")
print(f"\n前5行数据:")
print(df_spot.head())

print(f"\n成交额列的基本信息:")
print(f"  成交额数据类型: {df_spot['成交额'].dtype}")
print(f"  成交额前10个值:")
print(df_spot['成交额'].head(10))
print(f"\n成交额统计:")
print(df_spot['成交额'].describe())

print(f"\n过滤ST股票前:")
print(f"  总数: {len(df_spot)}")
df_spot = df_spot[~df_spot['名称'].str.contains("ST|退")]
print(f"  过滤ST后: {len(df_spot)}")

print(f"\n检查成交额>100000000的股票数:")
count = (df_spot['成交额'] > 100000000).sum()
print(f"  符合条件的股票数: {count}")

if count == 0:
    print("\n⚠️ 没有股票的成交额>1亿！")
    print(f"\n成交额的最大值: {df_spot['成交额'].max()}")
    print(f"成交额的前20大值:")
    print(df_spot.nlargest(20, '成交额')[['代码', '名称', '成交额', '成交量']])
