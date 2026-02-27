import akshare as ak
import time
# 批量延时获取筹码集中度
for symbol in ["601918"]:
    stock_cyq_em_df = ak.stock_cyq_em(symbol=symbol, adjust="qfq")
    # stock_cyq_em_df.to_json(f"{symbol}_筹码集中度.json", index=False)
    stock_cyq_em_df.to_csv(f"{symbol}_筹码集中度.csv", index=False)
    time.sleep(1)
    print(stock_cyq_em_df)
