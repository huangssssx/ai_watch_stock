from pytdx.hq import TdxHq_API
import pandas as pd

# 配置
IP = '180.153.18.170'
PORT = 7709
MARKET_SZ = 0  # 深圳市场
MARKET_SH = 1  # 上海市场
STOCK_CODE = '000001'  # 平安银行

def get_minute_data():
    api = TdxHq_API()
    try:
        print(f"正在连接服务器 {IP}:{PORT} ...")
        if api.connect(IP, PORT):
            print("✅ 连接成功")
            
            # 获取实时分时数据 (get_minute_time_data 可能存在解析问题，尝试用1分钟K线替代)
            print(f"正在获取 {STOCK_CODE} 的1分钟K线数据 (Category=8)...")
            # category: 8=1分钟K线, market: 0=SZ, code: 000001, start: 0, count: 240
            data = api.get_security_bars(8, MARKET_SZ, STOCK_CODE, 0, 240)
            
            if data:
                print("原始数据示例 (前2条):")
                print(data[:2])
                
                # 转换为DataFrame方便查看
                df = api.to_df(data)
                print(f"成功获取 {len(df)} 条K线数据：")
                print(df.head(5))
                print("...")
                print(df.tail(5))
                
                # 保存到文件
                output_file = f"minute_kline_{STOCK_CODE}.csv"
                df.to_csv(output_file, index=False)
                print(f"\n完整数据已保存至: {output_file}")
            else:
                print("⚠️ 未获取到K线数据，尝试获取原始分时数据...")
                data = api.get_minute_time_data(MARKET_SZ, STOCK_CODE)
                if data:
                    print("获取到原始分时数据 (可能存在解析异常):")
                    print(data[:5])
            
            api.disconnect()
        else:
            print("❌ 连接失败")
    except Exception as e:
        print(f"❌ 发生错误: {str(e)}")

if __name__ == '__main__':
    get_minute_data()
