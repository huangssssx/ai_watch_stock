
import sys
import os
import pandas as pd
import akshare as ak
import datetime
import numpy as np

# Add the project root to sys.path
sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from backend.models import RuleScript

# Database path
DB_PATH = 'sqlite:///backend/stock_watch.db'

def test_db_script(symbol="600519"):
    engine = create_engine(DB_PATH)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # 1. Get the script from DB
        rule = session.query(RuleScript).filter(RuleScript.name.like('%触底反弹监控%')).first()
        if not rule:
            print("Rule not found in DB")
            return
            
        print(f"Testing Rule: {rule.name}")
        script_content = rule.code
        
        # 2. Prepare environment variables
        local_vars = {
            "symbol": symbol,
            "ak": ak,
            "pd": pd,
            "np": np,
            "datetime": datetime
        }
        
        # 3. Execute the script
        exec(script_content, {}, local_vars)
        
        # 4. Check results
        triggered = local_vars.get("triggered", False)
        signal = local_vars.get("signal", "WAIT")
        message = local_vars.get("message", "")
        
        print(f"\n--- Result for {symbol} ---")
        print(f"Triggered: {triggered}")
        print(f"Signal: {signal}")
        print(f"Message: {message}")
        print("-" * 30)
        
        return triggered, message

    except Exception as e:
        print(f"Error executing script: {e}")
    finally:
        session.close()

def batch_verify():
    # Test on a few stocks
    test_symbols = ["600519", "300750", "002594", "601127"]
    print("Starting Batch Verification...")
    
    for sym in test_symbols:
        test_db_script(sym)

if __name__ == "__main__":
    batch_verify()
