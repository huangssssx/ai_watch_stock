import akshare as ak

# 获取个股实时行情快照，其中包含买一到买五、卖一到卖五的价格和数量
stock_bid_ask_df = ak.stock_individual_info_em(symbol="000001")
print(stock_bid_ask_df)