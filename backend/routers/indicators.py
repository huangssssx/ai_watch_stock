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

def _validate_indicator_payload(akshare_api: Optional[str], params_json: Optional[str], python_code: Optional[str]) -> None:
    if akshare_api:
        if not params_json:
            raise HTTPException(status_code=400, detail="params_json is required when akshare_api is provided")
        try:
            json.loads(params_json)
        except Exception:
            raise HTTPException(status_code=400, detail="params_json must be valid JSON")
        return
    if not python_code:
        raise HTTPException(status_code=400, detail="python_code is required when akshare_api is empty")

@router.get("/", response_model=List[schemas.IndicatorDefinition])
def read_indicators(skip: int = 0, limit: int = 200, db: Session = Depends(get_db)):
    items = db.query(models.IndicatorDefinition).offset(skip).limit(limit).all()
    return items

@router.post("/", response_model=schemas.IndicatorDefinition)
def create_indicator(indicator: schemas.IndicatorDefinitionCreate, db: Session = Depends(get_db)):
    payload = indicator.dict()
    payload["akshare_api"] = _normalize_optional_str(payload.get("akshare_api"))
    payload["params_json"] = _normalize_optional_str(payload.get("params_json"))
    payload["post_process_json"] = _normalize_optional_str(payload.get("post_process_json"))
    payload["python_code"] = _normalize_optional_str(payload.get("python_code"))

    _validate_indicator_payload(payload.get("akshare_api"), payload.get("params_json"), payload.get("python_code"))

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
    if "akshare_api" in update_data:
        update_data["akshare_api"] = _normalize_optional_str(update_data.get("akshare_api"))
    if "params_json" in update_data:
        update_data["params_json"] = _normalize_optional_str(update_data.get("params_json"))
    if "post_process_json" in update_data:
        update_data["post_process_json"] = _normalize_optional_str(update_data.get("post_process_json"))
    if "python_code" in update_data:
        update_data["python_code"] = _normalize_optional_str(update_data.get("python_code"))

    candidate_akshare_api = update_data.get("akshare_api", _normalize_optional_str(db_indicator.akshare_api))
    candidate_params_json = update_data.get("params_json", _normalize_optional_str(db_indicator.params_json))
    candidate_python_code = update_data.get("python_code", _normalize_optional_str(db_indicator.python_code))
    _validate_indicator_payload(candidate_akshare_api, candidate_params_json, candidate_python_code)

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
