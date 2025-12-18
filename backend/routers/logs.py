from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from database import get_db
import models
import schemas

router = APIRouter(prefix="/logs", tags=["logs"])

@router.get("/", response_model=List[schemas.Log])
def read_logs(stock_id: int = None, limit: int = 50, db: Session = Depends(get_db)):
    query = db.query(models.Log)
    if stock_id:
        query = query.filter(models.Log.stock_id == stock_id)
    
    # Order by timestamp desc
    logs = query.order_by(models.Log.timestamp.desc()).limit(limit).all()
    return logs
