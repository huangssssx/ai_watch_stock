import akshare as ak
import pandas as pd
import time

def verify_data():
    print("=== 开始验证 Akshare 数据接口 (修正版) ===")
    
    # 1. 核心指数
    print("\n[1/4] 获取核心指数 (stock_zh_index_spot_em)...")
    try:
        start_time = time.time()
        indices = ak.stock_zh_index_spot_em(symbol="主要指数")
        print(f"耗时: {time.time() - start_time:.2f}s")
        # 筛选上证、深证、创业板
        targets = ["上证指数", "深证成指", "创业板指", "科创50"]
        filtered = indices[indices['名称'].isin(targets)]
        print(filtered[['名称', '最新价', '涨跌幅']])
    except Exception as e:
        print(f"FAILED: {e}")

    # 2. 北向资金
    print("\n[2/4] 获取北向资金 (stock_hsgt_fund_flow_summary_em)...")
    try:
        start_time = time.time()
        north = ak.stock_hsgt_fund_flow_summary_em()
        print(f"耗时: {time.time() - start_time:.2f}s")
        print(north) 
    except Exception as e:
        print(f"FAILED: {e}")
        # Try min flow as fallback
        try:
            print("尝试 stock_hsgt_fund_min_em...")
            north_min = ak.stock_hsgt_fund_min_em(symbol="北向资金")
            print(north_min.tail())
        except Exception as e2:
            print(f"Fallback FAILED: {e2}")

    # 3. 行业板块
    print("\n[3/4] 获取行业板块 (stock_board_industry_name_em)...")
    try:
        start_time = time.time()
        sectors = ak.stock_board_industry_name_em()
        print(f"耗时: {time.time() - start_time:.2f}s")
        print(f"获取到 {len(sectors)} 个板块")
        print(sectors[['板块名称', '最新价', '涨跌幅']].head())
    except Exception as e:
        print(f"FAILED: {e}")

    # 4. 市场概况 (涨跌分布)
    print("\n[4/4] 获取市场概况 (stock_sse_summary / stock_szse_summary)...")
    try:
        start_time = time.time()
        sse = ak.stock_sse_summary()
        szse = ak.stock_szse_summary()
        print(f"耗时: {time.time() - start_time:.2f}s")
        print("上证概况:", sse.iloc[0]['股票'] if not sse.empty else "Empty")
        print("深证概况:", szse.iloc[0]['数量'] if not szse.empty else "Empty")
    except Exception as e:
        print(f"FAILED: {e}")
        
    print("\n=== 验证结束 ===")

if __name__ == "__main__":
    verify_data()
