from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from database import get_db
import models
import schemas
import json
from services.data_fetcher import data_fetcher

router = APIRouter(prefix="/indicators", tags=["indicators"])

@router.get("/", response_model=List[schemas.IndicatorDefinition])
def read_indicators(skip: int = 0, limit: int = 200, db: Session = Depends(get_db)):
    items = db.query(models.IndicatorDefinition).offset(skip).limit(limit).all()
    return items

@router.post("/", response_model=schemas.IndicatorDefinition)
def create_indicator(indicator: schemas.IndicatorDefinitionCreate, db: Session = Depends(get_db)):
    db_indicator = models.IndicatorDefinition(**indicator.dict())
    db.add(db_indicator)
    db.commit()
    db.refresh(db_indicator)
    return db_indicator

@router.put("/{indicator_id}", response_model=schemas.IndicatorDefinition)
def update_indicator(indicator_id: int, indicator: schemas.IndicatorDefinitionUpdate, db: Session = Depends(get_db)):
    db_indicator = (
        db.query(models.IndicatorDefinition)
        .filter(models.IndicatorDefinition.id == indicator_id)
        .first()
    )
    if not db_indicator:
        raise HTTPException(status_code=404, detail="Indicator not found")
    
    update_data = indicator.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_indicator, key, value)
        
    db.commit()
    db.refresh(db_indicator)
    return db_indicator

@router.delete("/{indicator_id}")
def delete_indicator(indicator_id: int, db: Session = Depends(get_db)):
    db_indicator = (
        db.query(models.IndicatorDefinition)
        .filter(models.IndicatorDefinition.id == indicator_id)
        .first()
    )
    if not db_indicator:
        raise HTTPException(status_code=404, detail="Indicator not found")
    db.delete(db_indicator)
    db.commit()
    return {"ok": True}

@router.post("/{indicator_id}/test", response_model=schemas.IndicatorTestResponse)
def test_indicator(indicator_id: int, payload: schemas.IndicatorTestRequest, db: Session = Depends(get_db)):
    db_indicator = (
        db.query(models.IndicatorDefinition)
        .filter(models.IndicatorDefinition.id == indicator_id)
        .first()
    )
    if not db_indicator:
        raise HTTPException(status_code=404, detail="Indicator not found")

    context = {"symbol": payload.symbol, "name": payload.name or ""}
    raw = data_fetcher.fetch(
        db_indicator.akshare_api,
        db_indicator.params_json,
        context,
        db_indicator.post_process_json,
        db_indicator.python_code,
    )

    if isinstance(raw, str) and (raw.startswith("Error") or raw.startswith("No data returned")):
        return {
            "ok": False,
            "indicator_id": db_indicator.id,
            "indicator_name": db_indicator.name,
            "symbol": payload.symbol,
            "raw": raw,
            "parsed": None,
            "error": raw,
        }

    parsed = None
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = None

    return {
        "ok": True,
        "indicator_id": db_indicator.id,
        "indicator_name": db_indicator.name,
        "symbol": payload.symbol,
        "raw": raw,
        "parsed": parsed,
        "error": None,
    }
