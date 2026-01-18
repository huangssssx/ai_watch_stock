
import sys
import os

# Add the project root to sys.path
sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from backend.models import RuleScript, Base

# Database path
DB_PATH = 'sqlite:///backend/stock_watch.db'

def get_rule_script():
    engine = create_engine(DB_PATH)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Search for the script
        # Using like to be safer, though user said "触底反弹监控"
        rules = session.query(RuleScript).filter(RuleScript.name.like('%触底反弹监控%')).all()
        
        if not rules:
            print("No rule found matching '触底反弹监控'")
            return

        for rule in rules:
            print(f"--- Rule Found: {rule.name} ---")
            print(f"Description: {rule.description}")
            print("Code:")
            print(rule.code)
            print("-" * 20)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    get_rule_script()
