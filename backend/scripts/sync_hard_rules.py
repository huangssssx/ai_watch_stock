import sys
import os
from sqlalchemy.orm import Session

# Add backend to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database import SessionLocal
from models import RuleScript

def sync_rule(db: Session, name: str, description: str, file_path: str):
    print(f"Syncing rule: {name}...")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            code = f.read()
            
        rule = db.query(RuleScript).filter(RuleScript.name == name).first()
        if rule:
            print(f"Updating existing rule: {name}")
            rule.code = code
            rule.description = description
        else:
            print(f"Creating new rule: {name}")
            rule = RuleScript(
                name=name,
                description=description,
                code=code,
                is_pinned=False
            )
            db.add(rule)
        
        db.commit()
        print(f"Successfully synced: {name}")
        
    except Exception as e:
        print(f"Error syncing {name}: {e}")
        db.rollback()

if __name__ == "__main__":
    db = SessionLocal()
    scripts_dir = os.path.dirname(__file__)
    
    # 1. U-Turn Down (Akshare)
    sync_rule(
        db, 
        "掉头向下预警 (Akshare)", 
        "【推荐】股价原本向上，今日跌破MA5且收阴 (Akshare实时数据+日线)", 
        os.path.join(scripts_dir, "u_turn_down_ak.py")
    )
    
    # 2. Rebound (Akshare)
    sync_rule(
        db, 
        "反弹预警 (Akshare)", 
        "【推荐】股价原本向下，今日站上MA5且收阳 (Akshare实时数据+日线)", 
        os.path.join(scripts_dir, "rebound_alert_ak.py")
    )
    
    db.close()
