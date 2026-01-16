import sys
import os
import json

# Add backend directory to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.dirname(current_dir)
sys.path.append(backend_dir)

from database import SessionLocal
from models import IndicatorDefinition

def add_indicator():
    db = SessionLocal()
    
    # Check if exists
    name = "中短线-震荡与反转 (KDJ/WR/BIAS)"
    existing = db.query(IndicatorDefinition).filter(IndicatorDefinition.name == name).first()
    if existing:
        print(f"Indicator '{name}' already exists with ID {existing.id}.")
        db.close()
        return

    # Python code for the indicator logic
    # Note: This code runs inside the data_fetcher context where 'ak' (akshare), 'pd', 'talib', 'context' are available
    python_code = r"""
symbol = context.get("symbol", "")
if not symbol:
    df = pd.DataFrame([{"提示": "未获取到股票代码"}])
else:
    import datetime
    today = datetime.datetime.now()
    # Get enough history for 24-day MA + buffer
    start_str = (today - datetime.timedelta(days=150)).strftime("%Y%m%d")
    end_str = today.strftime("%Y%m%d")

    try:
        # Fetch daily data (qfq)
        df = ak.stock_zh_a_hist(
            symbol=symbol, period="daily", start_date=start_str, end_date=end_str, adjust="qfq"
        )
    except Exception:
        df = pd.DataFrame()

    if df is None or df.empty:
        df = pd.DataFrame([{"提示": "未获取到日线数据", "股票代码": symbol}])
    else:
        # Preprocess
        df = df.sort_values(by="日期").reset_index(drop=True)
        # Ensure numeric
        for c in ["收盘", "最高", "最低", "成交量"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        
        close = df["收盘"]
        high = df["最高"]
        low = df["最低"]
        
        # 1. KDJ (9, 3, 3)
        # RSV = (Close - MinLow9) / (MaxHigh9 - MinLow9) * 100
        low_min = low.rolling(9).min()
        high_max = high.rolling(9).max()
        rsv = (close - low_min) / (high_max - low_min) * 100
        
        # Initialize K, D with 50 (standard practice or just let ewm handle it)
        # Pandas ewm with alpha=1/3 is equivalent to: Y_t = 1/3 * X_t + 2/3 * Y_{t-1}
        # This matches the common definition: K = 1/3 * RSV + 2/3 * PrevK
        df['K'] = rsv.ewm(alpha=1/3, adjust=False).mean()
        df['D'] = df['K'].ewm(alpha=1/3, adjust=False).mean()
        df['J'] = 3 * df['K'] - 2 * df['D']
        
        # 2. WR (Williams %R) - 14 days
        # WR = (MaxHigh14 - Close) / (MaxHigh14 - MinLow14) * 100
        # Note: WR is usually 0 to 100 (inverted scale often used, but here we calculate raw value)
        # Typically WR > 80 is oversold, < 20 is overbought. 
        # Wait, standard formula: (HighN - C) / (HighN - LowN) * 100.
        # So if C is close to High, WR is close to 0 (Overbought).
        # If C is close to Low, WR is close to 100 (Oversold).
        h14 = high.rolling(14).max()
        l14 = low.rolling(14).min()
        df['WR14'] = (h14 - close) / (h14 - l14) * 100
        
        # 3. BIAS (6, 12, 24)
        ma6 = close.rolling(6).mean()
        ma12 = close.rolling(12).mean()
        ma24 = close.rolling(24).mean()
        
        df['BIAS6'] = (close - ma6) / ma6 * 100
        df['BIAS12'] = (close - ma12) / ma12 * 100
        df['BIAS24'] = (close - ma24) / ma24 * 100
        
        # Format output
        df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
        
        # Keep last 5 rows
        df = df.tail(5).copy()
        
        # Round
        for c in ['K', 'D', 'J', 'WR14', 'BIAS6', 'BIAS12', 'BIAS24']:
            df[c] = df[c].round(2)
            
        # Select Columns
        cols = ["日期", "收盘", "K", "D", "J", "WR14", "BIAS6", "BIAS12", "BIAS24"]
        final_cols = [c for c in cols if c in df.columns]
        df = df[final_cols]
"""

    new_indicator = IndicatorDefinition(
        name=name,
        akshare_api="stock_zh_a_hist", # Not strictly used as python_code overrides it, but good for ref
        params_json=json.dumps({"symbol": "{symbol}"}),
        post_process_json=None,
        python_code=python_code,
        is_pinned=False
    )
    
    try:
        db.add(new_indicator)
        db.commit()
        db.refresh(new_indicator)
        print(f"Successfully added indicator: {new_indicator.name} (ID: {new_indicator.id})")
    except Exception as e:
        print(f"Error adding indicator: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    add_indicator()
