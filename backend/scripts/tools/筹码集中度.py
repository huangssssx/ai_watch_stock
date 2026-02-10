import akshare as ak

stock_cyq_em_df = ak.stock_cyq_em(symbol="000779", adjust="qfq")
# stock_cyq_em_df.to_json("000779_筹码集中度.json", index=False)
stock_cyq_em_df.to_csv("000779_筹码集中度.csv", index=False)
print(stock_cyq_em_df)