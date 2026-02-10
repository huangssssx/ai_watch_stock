import akshare as ak

stock_cyq_em_df = ak.stock_cyq_em(symbol="603813", adjust="qfq")
stock_cyq_em_df.to_json("603813_筹码集中度.json", index=False)
print(stock_cyq_em_df)