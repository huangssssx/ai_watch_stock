from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional, Any
from database import get_db
import models
import schemas
import json
from services.data_fetcher import data_fetcher

router = APIRouter(prefix="/indicators", tags=["indicators"])

def _normalize_optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        v = value.strip()
        return v if v else None
    return str(value).strip() or None

def _validate_indicator_payload(python_code: Optional[str]) -> None:
    if not python_code or not python_code.strip():
        raise HTTPException(status_code=400, detail="python_code is required")

@router.get("/", response_model=List[schemas.IndicatorDefinition])
def read_indicators(skip: int = 0, limit: int = 200, db: Session = Depends(get_db)):
    items = db.query(models.IndicatorDefinition).offset(skip).limit(limit).all()
    return items

@router.post("/", response_model=schemas.IndicatorDefinition)
def create_indicator(indicator: schemas.IndicatorDefinitionCreate, db: Session = Depends(get_db)):
    payload = indicator.dict()
    
    # Enforce Pure Python Mode
    payload["akshare_api"] = None
    payload["params_json"] = None
    payload["post_process_json"] = None
    payload["python_code"] = _normalize_optional_str(payload.get("python_code"))

    _validate_indicator_payload(payload.get("python_code"))

    db_indicator = models.IndicatorDefinition(**payload)
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
    
    # Enforce Pure Python Mode - ignore legacy fields from input and ensure they are cleared if somehow set
    if "akshare_api" in update_data:
        update_data["akshare_api"] = None
    if "params_json" in update_data:
        update_data["params_json"] = None
    if "post_process_json" in update_data:
        update_data["post_process_json"] = None
        
    if "python_code" in update_data:
        update_data["python_code"] = _normalize_optional_str(update_data.get("python_code"))

    candidate_python_code = update_data.get("python_code", _normalize_optional_str(db_indicator.python_code))
    _validate_indicator_payload(candidate_python_code)

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
    
    # Use fetch which now only supports script mode
    raw = data_fetcher.fetch(
        api_name=None,
        params_json=None,
        context=context,
        post_process_json=None,
        python_code=db_indicator.python_code,
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
