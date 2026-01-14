from fastapi import APIRouter, Depends, HTTPException, Query, Body
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from database import get_db
import models
import schemas
from services.news_service import news_service

router = APIRouter(prefix="/news", tags=["news"])

@router.get("/latest", response_model=List[schemas.StockNews])
def get_latest_news(limit: int = 50, db: Session = Depends(get_db)):
    return db.query(models.StockNews).order_by(models.StockNews.publish_time.desc()).limit(limit).all()

@router.post("/fetch")
def fetch_news(limit: int = 50, db: Session = Depends(get_db)):
    count = news_service.fetch_market_news(db, limit)
    return {"ok": True, "count": count}

class NewsAnalyzeRequest(BaseModel):
    ai_config_id: Optional[int] = None
    custom_prompt: Optional[str] = None
    limit: Optional[int] = None

@router.post("/analyze")
def analyze_news(
    payload: Optional[NewsAnalyzeRequest] = Body(default=None),
    ai_config_id: Optional[int] = Query(default=None),
    custom_prompt: Optional[str] = Query(default=None),
    limit: Optional[int] = Query(default=None),
    db: Session = Depends(get_db),
):
    resolved_ai_id = payload.ai_config_id if payload is not None else ai_config_id
    resolved_prompt = payload.custom_prompt if payload is not None else custom_prompt
    resolved_limit = payload.limit if payload is not None else limit
    result = news_service.analyze_news_raw(db, resolved_ai_id, custom_prompt=resolved_prompt, limit=resolved_limit or 30)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error"))
    return result

@router.get("/analysis/latest", response_model=List[schemas.SentimentAnalysis])
def get_latest_analysis(limit: int = 10, db: Session = Depends(get_db)):
    return db.query(models.SentimentAnalysis).order_by(models.SentimentAnalysis.timestamp.desc()).limit(limit).all()
