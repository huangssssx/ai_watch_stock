from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from database import get_db
import models
import schemas
from services.ai_service import ai_service

router = APIRouter(prefix="/ai-configs", tags=["ai-configs"])

@router.get("/", response_model=List[schemas.AIConfig])
def read_ai_configs(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    configs = db.query(models.AIConfig).offset(skip).limit(limit).all()
    return configs

@router.post("/", response_model=schemas.AIConfig)
def create_ai_config(config: schemas.AIConfigCreate, db: Session = Depends(get_db)):
    db_config = models.AIConfig(**config.dict())
    db.add(db_config)
    db.commit()
    db.refresh(db_config)
    return db_config

@router.delete("/{config_id}")
def delete_ai_config(config_id: int, db: Session = Depends(get_db)):
    db_config = db.query(models.AIConfig).filter(models.AIConfig.id == config_id).first()
    if not db_config:
        raise HTTPException(status_code=404, detail="Config not found")
    db.delete(db_config)
    db.commit()
    return {"ok": True}

@router.post("/{config_id}/test", response_model=schemas.AIConfigTestResponse)
def test_ai_config(config_id: int, payload: schemas.AIConfigTestRequest, db: Session = Depends(get_db)):
    db_config = db.query(models.AIConfig).filter(models.AIConfig.id == config_id).first()
    if not db_config:
        raise HTTPException(status_code=404, detail="Config not found")

    config_dict = {
        "api_key": db_config.api_key,
        "base_url": db_config.base_url,
        "model_name": db_config.model_name,
    }

    system_prompt = payload.prompt_template or None
    user_message = payload.data_context or "hello"

    try:
        reply = ai_service.chat(user_message, config_dict, system_prompt=system_prompt)
        return {"ok": True, "parsed": {"type": "info", "message": "联通成功"}, "raw": reply}
    except Exception as e:
        error_text = str(e)
        return {"ok": False, "parsed": {"type": "error", "message": f"AI Error: {error_text}"}, "raw": error_text}
