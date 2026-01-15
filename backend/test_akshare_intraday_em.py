import akshare as ak
import datetime

def test_fetch_intraday_em(symbol):
    print(f"Testing EM intraday fetch for {symbol}...")
    try:
        clean_symbol = symbol.lower().replace("sh", "").replace("sz", "")
        # stock_zh_a_hist_min_em
        # period='1'
        # adjust='qfq'
        df = ak.stock_zh_a_hist_min_em(symbol=clean_symbol, period='1', adjust='qfq')
        
        if df is None or df.empty:
            print("No data found")
            return
        
        # columns: 时间, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 最新价
        print("Data columns:", df.columns)
        
        # Get latest date string
        last_dt = df.iloc[-1]['时间'] # Format likely '2025-01-15 15:00:00'
        print(f"Last datetime: {last_dt}")
        
        # Filter for the last date only
        last_date_str = last_dt.split(" ")[0]
        today_data = df[df['时间'].str.startswith(last_date_str)]
        
        print(f"Rows for {last_date_str}: {len(today_data)}")
        print("First row:", today_data.iloc[0])
        print("Success!")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_fetch_intraday_em("600519") 
