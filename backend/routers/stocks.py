from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from database import get_db
import models
import schemas
import json
from services.monitor_service import update_stock_job
from services.data_fetcher import data_fetcher
from services.ai_service import ai_service

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
def test_run_stock(stock_id: int, db: Session = Depends(get_db)):
    db_stock = db.query(models.Stock).filter(models.Stock.id == stock_id).first()
    if not db_stock:
        raise HTTPException(status_code=404, detail="Stock not found")

    if not db_stock.ai_provider_id:
        raise HTTPException(status_code=400, detail="No AI provider configured")

    ai_config = db.query(models.AIConfig).filter(models.AIConfig.id == db_stock.ai_provider_id).first()
    if not ai_config:
        raise HTTPException(status_code=404, detail="AI Config not found")

    context = {"symbol": db_stock.symbol}
    data_parts = []
    for indicator in db_stock.indicators:
        data = data_fetcher.fetch(indicator.akshare_api, indicator.params_json, context)
        data_parts.append(f"--- Indicator: {indicator.name} ---\n{data}\n")
    full_data = "\n".join(data_parts)

    data_char_limit = ai_config.max_tokens if ai_config.max_tokens else 100000
    data_truncated = len(full_data) > data_char_limit
    data_for_prompt = full_data[:data_char_limit] if data_truncated else full_data

    prompt_template = db_stock.prompt_template or "Analyze the trend."
    system_prompt = (
        "You are a professional stock analyst. Analyze the provided data and return the result in strictly formatted JSON. "
        "The JSON must contain 'type' (string: 'info', 'warning', 'error') and 'message' (string)."
    )
    user_prompt = f"""Task: Analyze the following stock data based on the instructions.

Instructions:
{prompt_template}

Data:
{data_for_prompt}

Return strictly JSON format: {{"type": "...", "message": "..."}}"""

    config_dict = {"api_key": ai_config.api_key, "base_url": ai_config.base_url, "model_name": ai_config.model_name}
    ai_reply = ai_service.chat(user_prompt, config_dict, system_prompt=system_prompt)

    return {
        "ok": True,
        "stock_id": db_stock.id,
        "stock_symbol": db_stock.symbol,
        "model_name": ai_config.model_name,
        "base_url": ai_config.base_url,
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        "ai_reply": ai_reply,
        "data_truncated": data_truncated,
        "data_char_limit": data_char_limit if data_truncated else None,
    }
