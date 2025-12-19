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
        msg["Subject"] = "AI Watch Stock - 邮件配置测试"
        body = """
<div style="font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial; font-size:14px; color:#111827; line-height:1.6;">
  <div style="font-size:18px; font-weight:700; margin-bottom:12px;">邮件配置测试</div>
  <div style="margin-bottom:12px; color:#374151;">这是一封来自 AI Watch Stock 的测试邮件，用于验证 SMTP 配置是否可用。</div>
  <div style="margin-bottom:8px; color:#6B7280;">字体颜色示例（按时间紧急程度）：</div>
  <div><span style="color:#DC2626; font-weight:800;">紧急</span>：建议立即处理/短线信号</div>
  <div><span style="color:#D97706; font-weight:800;">一般</span>：1-3 天内处理</div>
  <div><span style="color:#6B7280; font-weight:800;">不紧急</span>：中线/长线周期</div>
</div>
""".strip()
        msg.attach(MIMEText(body, "html"))

        server = smtplib.SMTP(config.smtp_server, config.smtp_port)
        server.starttls()
        server.login(config.sender_email, config.sender_password)
        server.send_message(msg)
        server.quit()
        return {"ok": True, "message": "测试邮件发送成功"}
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
