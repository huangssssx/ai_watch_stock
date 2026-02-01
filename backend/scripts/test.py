import akshare as ak
import pandas as pd
import tushare as ts
ts.set_token('68656717691d5af91958a8b613652ab7fe532c9f7827dd09609aefd0')
def get_realtime_order_book(symbol="600519"):
    """
    获取 A 股股票的实时五档盘口数据
    :param symbol: 股票代码 (不带后缀)
    """
    print(f"正在获取 {symbol} 的实时五档盘口 (stock_bid_ask_em)...")
    try:
        # 获取五档委单数据
        df = ts.realtime_quote(ts_code=f"{symbol}.SH")
        
        # 为了方便观察，我们将返回的 DataFrame 进行简单的格式化输出
        # 该接口返回两列：item (项目名称) 和 value (数值)
        if not df.empty:
            return df
        else:
            print("未能获取到数据。")
            return None
    except Exception as e:
        print(f"接口调用失败: {e}")
        return None

if __name__ == "__main__":
    # 示例：获取贵州茅台的五档盘口
    order_book = get_realtime_order_book("600519")
    if order_book is not None:
        print(order_book)
        # 将order_book转换为本地的 cvs 文件
        order_book.to_csv("order_book.csv", index=False)