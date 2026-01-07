from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from database import get_db
from models import ResearchScript
from schemas import ResearchScript as ResearchScriptSchema, ResearchScriptCreate, ResearchScriptUpdate, ResearchRunRequest, ResearchRunResponse
from services.research_service import execute_research_script
from services.streamlit_service import update_streamlit_code, get_streamlit_url
import datetime

router = APIRouter(prefix="/research", tags=["research"])

@router.get("/", response_model=List[ResearchScriptSchema])
def get_scripts(db: Session = Depends(get_db)):
    return db.query(ResearchScript).order_by(ResearchScript.updated_at.desc()).all()

@router.post("/", response_model=ResearchScriptSchema)
def create_script(script: ResearchScriptCreate, db: Session = Depends(get_db)):
    db_script = ResearchScript(**script.dict())
    db.add(db_script)
    db.commit()
    db.refresh(db_script)
    return db_script

@router.put("/{script_id}", response_model=ResearchScriptSchema)
def update_script(script_id: int, script: ResearchScriptUpdate, db: Session = Depends(get_db)):
    db_script = db.query(ResearchScript).filter(ResearchScript.id == script_id).first()
    if not db_script:
        raise HTTPException(status_code=404, detail="Script not found")
    
    for key, value in script.dict(exclude_unset=True).items():
        setattr(db_script, key, value)
    
    db.commit()
    db.refresh(db_script)
    return db_script

@router.delete("/{script_id}")
def delete_script(script_id: int, db: Session = Depends(get_db)):
    db_script = db.query(ResearchScript).filter(ResearchScript.id == script_id).first()
    if not db_script:
        raise HTTPException(status_code=404, detail="Script not found")
    
    db.delete(db_script)
    db.commit()
    return {"ok": True}

@router.post("/run", response_model=ResearchRunResponse)
def run_script(request: ResearchRunRequest):
    success, result, chart, log, error = execute_research_script(request.script_content)
    return {
        "success": success,
        "log": log,
        "result": result,
        "chart": chart,
        "error": error
    }

@router.post("/streamlit/run")
def run_streamlit(request: ResearchRunRequest):
    """
    Updates the Streamlit script file and returns the Streamlit URL.
    Streamlit server (running in background) will auto-reload.
    """
    url = update_streamlit_code(request.script_content)
    return {"url": url}
