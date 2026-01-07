from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from database import get_db
import models
import schemas
import json
import datetime
from services.monitor_service import process_stock, update_stock_job, analyze_stock_manual

router = APIRouter(prefix="/stocks", tags=["stocks"])

@router.get("/", response_model=List[schemas.Stock])
def read_stocks(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    stocks = db.query(models.Stock).offset(skip).limit(limit).all()
    return stocks

@router.post("/", response_model=schemas.Stock)
def create_stock(stock: schemas.StockCreate, db: Session = Depends(get_db)):
    data = stock.dict()
    
    if not data.get("monitoring_schedule"):
        default_schedule = [
            {"start": "09:30", "end": "11:30"},
            {"start": "13:00", "end": "15:00"}
        ]
        data["monitoring_schedule"] = json.dumps(default_schedule)

    indicator_ids = data.pop("indicator_ids", None) or []
    db_stock = models.Stock(**data)
    if indicator_ids:
        indicators = (
            db.query(models.IndicatorDefinition)
            .filter(models.IndicatorDefinition.id.in_(indicator_ids))
            .all()
        )
        db_stock.indicators = indicators
    db.add(db_stock)
    db.commit()
    db.refresh(db_stock)
    return db_stock

@router.put("/{stock_id}", response_model=schemas.Stock)
def update_stock(stock_id: int, stock_update: schemas.StockUpdate, db: Session = Depends(get_db)):
    db_stock = db.query(models.Stock).filter(models.Stock.id == stock_id).first()
    if not db_stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    
    update_data = stock_update.dict(exclude_unset=True)
    
    indicator_ids = None
    if "indicator_ids" in update_data:
        indicator_ids = update_data.pop("indicator_ids")

    for key, value in update_data.items():
        setattr(db_stock, key, value)

    if indicator_ids is not None:
        if len(indicator_ids) == 0:
            db_stock.indicators = []
        else:
            indicators = (
                db.query(models.IndicatorDefinition)
                .filter(models.IndicatorDefinition.id.in_(indicator_ids))
                .all()
            )
            db_stock.indicators = indicators
    
    db.commit()
    db.refresh(db_stock)
    
    # Update scheduler if monitoring status or interval changed
    if "is_monitoring" in update_data or "interval_seconds" in update_data:
        update_stock_job(db_stock.id, db_stock.interval_seconds, db_stock.is_monitoring)
        
    return db_stock

@router.delete("/{stock_id}")
def delete_stock(stock_id: int, db: Session = Depends(get_db)):
    db_stock = db.query(models.Stock).filter(models.Stock.id == stock_id).first()
    if not db_stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    
    # Stop monitoring
    update_stock_job(stock_id, 0, False)
    
    db.delete(db_stock)
    db.commit()
    return {"ok": True}

@router.post("/{stock_id}/test-run", response_model=schemas.StockTestRunResponse)
def test_run_stock(stock_id: int, send_alerts: bool = True, bypass_checks: bool = True, db: Session = Depends(get_db)):
    result = process_stock(
        stock_id,
        bypass_checks=bypass_checks,
        send_alerts=send_alerts,
        is_test=False,
        return_result=True,
        db=db,
    )
    if not result:
        raise HTTPException(status_code=500, detail="Test run failed")
    return result

@router.get("/{stock_id}/ai-watch-config", response_model=schemas.StockAIWatchConfig)
def get_ai_watch_config(stock_id: int, db: Session = Depends(get_db)):
    config = db.query(models.StockAIWatchConfig).filter(models.StockAIWatchConfig.stock_id == stock_id).first()
    if not config:
        # Return default
        return schemas.StockAIWatchConfig(
            id=0,
            stock_id=stock_id,
            indicator_ids="[]",
            custom_prompt="",
            analysis_history="[]",
            updated_at=None
        )
    return config

@router.post("/{stock_id}/ai-watch-config", response_model=schemas.StockAIWatchConfig)
def save_ai_watch_config(stock_id: int, config_in: schemas.StockAIWatchConfigBase, db: Session = Depends(get_db)):
    config = db.query(models.StockAIWatchConfig).filter(models.StockAIWatchConfig.stock_id == stock_id).first()
    if not config:
        config = models.StockAIWatchConfig(stock_id=stock_id)
        db.add(config)
    
    config.indicator_ids = config_in.indicator_ids
    config.custom_prompt = config_in.custom_prompt
    # Don't touch history here
    
    db.commit()
    db.refresh(config)
    return config

@router.post("/{stock_id}/ai-watch-analyze")
def run_ai_watch_analyze(stock_id: int, request: schemas.AIWatchAnalyzeRequest, db: Session = Depends(get_db)):
    # 1. Run Analysis
    result = analyze_stock_manual(
        stock_id=stock_id,
        indicator_ids=request.indicator_ids,
        custom_prompt=request.custom_prompt,
        ai_provider_id=request.ai_provider_id,
        db=db
    )
    
    if not result.get("ok"):
        raise HTTPException(status_code=500, detail=result.get("error", "Unknown error"))

    # 2. Save History
    config = db.query(models.StockAIWatchConfig).filter(models.StockAIWatchConfig.stock_id == stock_id).first()
    if not config:
        config = models.StockAIWatchConfig(
            stock_id=stock_id,
            indicator_ids=json.dumps(request.indicator_ids),
            custom_prompt=request.custom_prompt
        )
        db.add(config)
    else:
        # Update preferences too
        config.indicator_ids = json.dumps(request.indicator_ids)
        config.custom_prompt = request.custom_prompt

    # Append History
    try:
        history = json.loads(config.analysis_history) if config.analysis_history else []
        if not isinstance(history, list):
            history = []
    except:
        history = []
    
    new_entry = {
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "result": result
    }
    history.insert(0, new_entry)
    history = history[:3] # Keep last 3
    
    config.analysis_history = json.dumps(history, ensure_ascii=False)
    
    db.commit()
    
    return result
