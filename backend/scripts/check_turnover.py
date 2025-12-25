import akshare as ak
import pandas as pd

print("获取实时行情数据...")
df_spot = ak.stock_zh_a_spot_em()

print(f"\n总股票数: {len(df_spot)}")

# 基础过滤
df_spot = df_spot[~df_spot['名称'].str.contains("ST|退")]
print(f"过滤ST后: {len(df_spot)}")

print(f"\n换手率数据检查:")
print(f"  换手率数据类型: {df_spot['换手率'].dtype}")
print(f"  换手率非空数量: {df_spot['换手率'].notna().sum()}")
print(f"  换手率>0的数量: {(df_spot['换手率'] > 0).sum()}")
print(f"\n换手率Top20:")
top20 = df_spot.nlargest(20, '换手率')[['代码', '名称', '换手率', '最新价', '成交量', '成交额']]
print(top20)

print(f"\n成交量数据检查:")
print(f"  成交量数据类型: {df_spot['成交量'].dtype}")
print(f"  成交量非空数量: {df_spot['成交量'].notna().sum()}")

print(f"\n成交额数据检查:")
print(f"  成交额数据类型: {df_spot['成交额'].dtype}")
print(f"  成交额非空数量: {df_spot['成交额'].notna().sum()}")
