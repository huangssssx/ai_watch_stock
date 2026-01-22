from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from database import get_db
import models
import schemas

router = APIRouter(prefix="/rules", tags=["rules"])

@router.get("/", response_model=List[schemas.RuleScript])
def read_rules(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    rules = db.query(models.RuleScript).offset(skip).limit(limit).all()
    return rules

@router.post("/", response_model=schemas.RuleScript)
def create_rule(rule: schemas.RuleScriptCreate, db: Session = Depends(get_db)):
    db_rule = models.RuleScript(**rule.dict())
    db.add(db_rule)
    db.commit()
    db.refresh(db_rule)
    return db_rule

@router.put("/{rule_id}", response_model=schemas.RuleScript)
def update_rule(rule_id: int, rule_update: schemas.RuleScriptUpdate, db: Session = Depends(get_db)):
    db_rule = db.query(models.RuleScript).filter(models.RuleScript.id == rule_id).first()
    if not db_rule:
        raise HTTPException(status_code=404, detail="Rule script not found")
    
    update_data = rule_update.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_rule, key, value)
    
    db.commit()
    db.refresh(db_rule)
    return db_rule

@router.delete("/{rule_id}")
def delete_rule(rule_id: int, db: Session = Depends(get_db)):
    db_rule = db.query(models.RuleScript).filter(models.RuleScript.id == rule_id).first()
    if not db_rule:
        raise HTTPException(status_code=404, detail="Rule script not found")
    
    # Check if used by any stock
    if db.query(models.Stock).filter(models.Stock.rule_script_id == rule_id).first():
         raise HTTPException(status_code=400, detail="Cannot delete rule script that is in use by a stock")

    db.delete(db_rule)
    db.commit()
    return {"ok": True}

@router.post("/{rule_id}/test", response_model=schemas.RuleTestResponse)
def test_rule(rule_id: int, payload: schemas.RuleTestPayload, db: Session = Depends(get_db)):
    """
    Test a rule script against a specific stock symbol (without saving/linking).
    """
    db_rule = db.query(models.RuleScript).filter(models.RuleScript.id == rule_id).first()
    if not db_rule:
        raise HTTPException(status_code=404, detail="Rule script not found")
    
    script_code = db_rule.code
    symbol = payload.symbol
    
    if not script_code:
        return {"triggered": False, "log": "Empty script"}

    # Execute script
    try:
        from pymr_compat import ensure_py_mini_racer
        ensure_py_mini_racer()
        import akshare as ak
        import pandas as pd
        import numpy as np
        import datetime
        import time
        import io
        import sys
        
        from utils.tushare_client import ts, pro
        
        # Capture stdout
        old_stdout = sys.stdout
        new_stdout = io.StringIO()
        sys.stdout = new_stdout
        
        # Sandbox
        local_scope = {
            "ak": ak,
            "ts": ts,
            "pro": pro,
            "pd": pd,
            "np": np,
            "datetime": datetime,
            "time": time,
            "symbol": symbol,
            "triggered": False,
            "message": ""
        }
        
        try:
            exec(script_code, {}, local_scope)
            triggered = bool(local_scope.get("triggered", False))
            message = str(local_scope.get("message", ""))
            signal = local_scope.get("signal", None)
        except Exception as e:
            print(f"Error executing script: {e}")
            triggered = False
            message = f"Error: {e}"
            signal = None
            
        output_log = new_stdout.getvalue()
        sys.stdout = old_stdout
        
        return {
            "triggered": triggered,
            "message": message,
            "log": output_log,
            "signal": signal
        }
        
    except Exception as e:
        return {
            "triggered": False,
            "message": f"System Error: {e}",
            "log": ""
        }
