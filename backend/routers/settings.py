from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Dict, Any
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from database import get_db
import models
import schemas

router = APIRouter(
    prefix="/settings",
    tags=["settings"],
    responses={404: {"description": "Not found"}},
)

@router.get("/email", response_model=schemas.EmailConfig)
def get_email_config(db: Session = Depends(get_db)):
    config = db.query(models.SystemConfig).filter(models.SystemConfig.key == "email_config").first()
    if not config or not config.value:
        return schemas.EmailConfig()
    try:
        data = json.loads(config.value)
        return schemas.EmailConfig(**data)
    except:
        return schemas.EmailConfig()

@router.put("/email", response_model=schemas.EmailConfig)
def update_email_config(config: schemas.EmailConfig, db: Session = Depends(get_db)):
    db_config = db.query(models.SystemConfig).filter(models.SystemConfig.key == "email_config").first()
    value_str = config.json()
    
    if not db_config:
        db_config = models.SystemConfig(key="email_config", value=value_str)
        db.add(db_config)
    else:
        db_config.value = value_str
    
    db.commit()
    return config

@router.post("/email/test")
def test_email_config(config: schemas.EmailConfig):
    try:
        msg = MIMEMultipart()
        msg["From"] = config.sender_email
        msg["To"] = config.receiver_email
        msg["Subject"] = "AI Watch Stock - Test Email"
        msg.attach(MIMEText("This is a test email from AI Watch Stock system.", "plain"))

        server = smtplib.SMTP(config.smtp_server, config.smtp_port)
        server.starttls()
        server.login(config.sender_email, config.sender_password)
        server.send_message(msg)
        server.quit()
        return {"ok": True, "message": "Test email sent successfully"}
    except Exception as e:
        return {"ok": False, "message": str(e)}

@router.get("/global-prompt", response_model=schemas.GlobalPromptConfig)
def get_global_prompt(db: Session = Depends(get_db)):
    config = db.query(models.SystemConfig).filter(models.SystemConfig.key == "global_prompt").first()
    if not config or not config.value:
        return schemas.GlobalPromptConfig()
    try:
        data = json.loads(config.value)
        return schemas.GlobalPromptConfig(**data)
    except:
        return schemas.GlobalPromptConfig()

@router.put("/global-prompt", response_model=schemas.GlobalPromptConfig)
def update_global_prompt(config: schemas.GlobalPromptConfig, db: Session = Depends(get_db)):
    db_config = db.query(models.SystemConfig).filter(models.SystemConfig.key == "global_prompt").first()
    value_str = config.json()
    
    if not db_config:
        db_config = models.SystemConfig(key="global_prompt", value=value_str)
        db.add(db_config)
    else:
        db_config.value = value_str
    
    db.commit()
    return config

@router.get("/alert-rate-limit", response_model=schemas.AlertRateLimitConfig)
def get_alert_rate_limit_config(db: Session = Depends(get_db)):
    config = db.query(models.SystemConfig).filter(models.SystemConfig.key == "alert_rate_limit").first()
    if not config or not config.value:
        return schemas.AlertRateLimitConfig()
    try:
        data = json.loads(config.value)
        return schemas.AlertRateLimitConfig(**data)
    except:
        return schemas.AlertRateLimitConfig()

@router.put("/alert-rate-limit", response_model=schemas.AlertRateLimitConfig)
def update_alert_rate_limit_config(config: schemas.AlertRateLimitConfig, db: Session = Depends(get_db)):
    db_config = db.query(models.SystemConfig).filter(models.SystemConfig.key == "alert_rate_limit").first()
    value_str = config.json()

    if not db_config:
        db_config = models.SystemConfig(key="alert_rate_limit", value=value_str)
        db.add(db_config)
    else:
        db_config.value = value_str

    db.commit()
    return config
