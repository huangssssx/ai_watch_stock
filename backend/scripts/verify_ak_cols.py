import akshare as ak
import pandas as pd

def check_columns():
    print("Checking ak.stock_individual_fund_flow_rank(indicator='今日')...")
    try:
        df_flow = ak.stock_individual_fund_flow_rank(indicator="今日")
        print("Flow Columns:", df_flow.columns.tolist())
        print("First row:", df_flow.iloc[0].to_dict())
    except Exception as e:
        print(f"Error fetching flow rank: {e}")

    print("\nChecking ak.stock_zh_a_spot_em()...")
    try:
        df_spot = ak.stock_zh_a_spot_em()
        print("Spot Columns:", df_spot.columns.tolist())
        print("First row:", df_spot.iloc[0].to_dict())
    except Exception as e:
        print(f"Error fetching spot data: {e}")

    print("\nChecking ak.stock_sector_fund_flow_rank(indicator='今日')...")
    try:
        df_sector = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流")
        print("Sector Columns:", df_sector.columns.tolist())
        print("First row:", df_sector.iloc[0].to_dict())
    except Exception as e:
        print(f"Error fetching sector flow: {e}")

if __name__ == "__main__":
    check_columns()
