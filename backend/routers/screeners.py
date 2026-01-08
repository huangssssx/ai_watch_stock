from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional, Any
from pydantic import BaseModel
import datetime
import json

from database import get_db
from models import StockScreener, ScreenerResult
from services.screener_service import execute_screener_script, update_screener_job

router = APIRouter(prefix="/screeners", tags=["screeners"])

class ScreenerCreate(BaseModel):
    name: str
    description: Optional[str] = None
    script_content: Optional[str] = ""
    cron_expression: Optional[str] = None
    is_active: bool = False

class ScreenerUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    script_content: Optional[str] = None
    cron_expression: Optional[str] = None
    is_active: Optional[bool] = None

@router.get("/")
def list_screeners(db: Session = Depends(get_db)):
    return db.query(StockScreener).all()

@router.post("/")
def create_screener(screener: ScreenerCreate, db: Session = Depends(get_db)):
    db_screener = StockScreener(
        name=screener.name,
        description=screener.description,
        script_content=screener.script_content,
        cron_expression=screener.cron_expression,
        is_active=screener.is_active
    )
    db.add(db_screener)
    db.commit()
    db.refresh(db_screener)
    
    update_screener_job(db_screener.id, db_screener.cron_expression, db_screener.is_active)
    
    return db_screener

@router.put("/{screener_id}")
def update_screener(screener_id: int, screener: ScreenerUpdate, db: Session = Depends(get_db)):
    db_screener = db.query(StockScreener).filter(StockScreener.id == screener_id).first()
    if not db_screener:
        raise HTTPException(status_code=404, detail="Screener not found")
    
    if screener.name is not None: db_screener.name = screener.name
    if screener.description is not None: db_screener.description = screener.description
    if screener.script_content is not None: db_screener.script_content = screener.script_content
    if screener.cron_expression is not None: db_screener.cron_expression = screener.cron_expression
    if screener.is_active is not None: db_screener.is_active = screener.is_active
    
    db.commit()
    db.refresh(db_screener)
    
    update_screener_job(db_screener.id, db_screener.cron_expression, db_screener.is_active)
    
    return db_screener

@router.delete("/{screener_id}")
def delete_screener(screener_id: int, db: Session = Depends(get_db)):
    db_screener = db.query(StockScreener).filter(StockScreener.id == screener_id).first()
    if not db_screener:
        raise HTTPException(status_code=404, detail="Screener not found")
        
    # Stop job
    update_screener_job(screener_id, "", False)
    
    db.delete(db_screener)
    db.commit()
    return {"ok": True}

@router.post("/{screener_id}/run")
def run_screener_now(screener_id: int, db: Session = Depends(get_db)):
    screener = db.query(StockScreener).filter(StockScreener.id == screener_id).first()
    if not screener:
        raise HTTPException(status_code=404, detail="Screener not found")
        
    success, data, log = execute_screener_script(screener.script_content)
    
    # Update status
    screener.last_run_at = datetime.datetime.now()
    screener.last_run_status = "success" if success else "failed"
    screener.last_run_log = log
    
    if success:
        data_to_save = data if data is not None else []
        result_entry = ScreenerResult(
            screener_id=screener.id,
            result_json=json.dumps(data_to_save, ensure_ascii=False),
            count=len(data_to_save),
        )
        db.add(result_entry)
    
    db.commit()
    
    return {
        "success": success,
        "log": log,
        "count": len(data) if data else 0,
        "data": data[:100] if data else [] # Return first 100 rows for preview
    }

@router.get("/{screener_id}/results")
def get_screener_results(screener_id: int, limit: int = 10, db: Session = Depends(get_db)):
    results = db.query(ScreenerResult).filter(ScreenerResult.screener_id == screener_id).order_by(ScreenerResult.run_at.desc()).limit(limit).all()
    return results
