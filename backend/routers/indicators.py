from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from database import get_db
import models
import schemas

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

