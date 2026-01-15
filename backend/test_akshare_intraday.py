import akshare as ak
import datetime

def test_fetch_intraday(symbol):
    print(f"Testing intraday fetch for {symbol}...")
    try:
        clean_symbol = symbol.lower().replace("sh", "").replace("sz", "")
        # akshare 分时数据接口: stock_zh_a_minute
        # period='1' 表示 1 分钟数据
        # adjust='qfq' 前复权
        df = ak.stock_zh_a_minute(symbol=clean_symbol, period='1', adjust='qfq')
        
        if df is None or df.empty:
            print("No data found")
            return

        # 过滤出最近一天的数据
        # df 的列通常是: day, open, high, low, close, volume
        # day 格式通常是 "2023-10-27 15:00:00"
        
        last_time = df.iloc[-1]['day']
        print(f"Last time: {last_time}")
        
        # 假设 last_time 是字符串，我们需要解析日期
        # 简单取最后 240 个点（4小时交易时间 * 60分钟）作为“当日”数据近似
        # 或者解析日期精确过滤
        
        recent_data = df.tail(240) 
        
        print("Data columns:", df.columns)
        print("First row of recent:", recent_data.iloc[0])
        print("Last row of recent:", recent_data.iloc[-1])
        print("Success!")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_fetch_intraday("sh600519") 
