from fastapi import FastAPI, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from database import SessionLocal, engine, Base
from models import AlertRule, AlertEvent, AlertNotification, IndicatorConfig, IndicatorCollection
from datetime import datetime
import logging
from fastapi.middleware.cors import CORSMiddleware
import requests
from typing import List, Dict, Optional
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import json
from sqlalchemy import text
import os
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
import re
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("API")

import fcntl
import sys
import atexit

# ... imports ...
# Initialize DB
def _drop_legacy_tables():
    try:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS strategies"))
            conn.execute(text("DROP TABLE IF EXISTS alert_logs"))
    except Exception as e:
        logger.warning(f"Drop legacy tables failed: {e}")

_drop_legacy_tables()
Base.metadata.create_all(bind=engine)

app = FastAPI(title="AI Watch Stock")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

scheduler: Optional[BackgroundScheduler] = None
scheduler_lock_file = None

def alert_event_to_dict(a):
    return {
        "id": a.id,
        "rule_id": a.rule_id,
        "symbol": a.symbol,
        "timestamp": a.timestamp.isoformat() if a.timestamp else None,
        "message": a.message,
        "level": a.level,
    }

def alert_notification_to_dict(n):
    return {
        "id": n.id,
        "rule_id": n.rule_id,
        "message": n.message,
        "level": n.level,
        "triggered_at": n.triggered_at.isoformat() if n.triggered_at else None,
        "last_notified_at": n.last_notified_at.isoformat() if n.last_notified_at else None,
        "is_cleared": n.is_cleared,
    }

@app.post("/alert_rules/")
def create_alert_rule(rule: dict, db: Session = Depends(get_db)):
    r = AlertRule(
        name=rule.get("name", "Rule"),
        symbol=rule.get("symbol", ""),
        period=rule.get("period", "1"),
        provider=rule.get("provider", "em"),
        condition=rule.get("condition", ""),
        message=rule.get("message", ""),
        level=rule.get("level", "WARNING"),
        enabled=rule.get("enabled", True),
    )
    db.add(r)
    db.commit()
    db.refresh(r)
    return {"id": r.id}

@app.get("/alert_rules/")
def list_alert_rules(skip: int = 0, limit: int = 50, db: Session = Depends(get_db)):
    items = db.query(AlertRule).offset(skip).limit(limit).all()
    return [
        {
            "id": r.id,
            "name": r.name,
            "symbol": r.symbol,
            "period": r.period,
            "provider": r.provider,
            "condition": r.condition,
            "message": r.message,
            "level": r.level,
            "enabled": r.enabled,
            "last_checked_at": r.last_checked_at.isoformat() if r.last_checked_at else None,
            "last_triggered_at": r.last_triggered_at.isoformat() if r.last_triggered_at else None,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
    for r in items]

@app.put("/alert_rules/{rule_id}")
def update_alert_rule(rule_id: int, payload: dict, db: Session = Depends(get_db)):
    r = db.query(AlertRule).filter(AlertRule.id == rule_id).first()
    if not r:
        raise HTTPException(status_code=404, detail="AlertRule not found")
    for k in ["name","symbol","period","provider","condition","message","level","enabled"]:
        if k in payload:
            setattr(r, k, payload[k])
    db.commit()
    db.refresh(r)
    return {"id": r.id}

@app.post("/alert_rules/batch")
def batch_create_alert_rules(payload: dict, db: Session = Depends(get_db)):
    created_ids: List[int] = []
    rules = payload.get("rules")
    if isinstance(rules, list):
        for rd in rules:
            r = AlertRule(
                name=rd.get("name", "Rule"),
                symbol=rd.get("symbol", ""),
                period=str(rd.get("period", "1")),
                provider=rd.get("provider", "em"),
                condition=rd.get("condition", ""),
                message=rd.get("message", ""),
                level=rd.get("level", "WARNING"),
                enabled=bool(rd.get("enabled", True)),
            )
            db.add(r)
            db.commit()
            db.refresh(r)
            created_ids.append(r.id)
        return {"created": created_ids}
    paste = payload.get("paste", "")
    items: List[Dict] = []
    if isinstance(paste, str) and paste.strip():
        try:
            data = json.loads(paste)
            if isinstance(data, list):
                items = data
        except Exception:
            lines = [ln.strip() for ln in paste.splitlines() if ln.strip()]
            for ln in lines:
                parts = ln.split()
                if len(parts) >= 3:
                    symbol = parts[0]
                    op = parts[1]
                    thr = parts[2]
                    period = "1"
                    msg = ""
                    if len(parts) >= 4 and parts[3].isdigit():
                        period = parts[3]
                        if len(parts) >= 5:
                            msg = " ".join(parts[4:])
                    elif len(parts) >= 4:
                        msg = " ".join(parts[3:])
                    cond = ""
                    try:
                        tval = float(thr)
                        if op in ("跌到", "<=", "le", "lt"):
                            cond = f"price <= {tval}"
                        elif op in ("涨到", ">=", "ge", "gt"):
                            cond = f"price >= {tval}"
                    except Exception:
                        cond = ""
                    items.append({
                        "name": f"{symbol} {op} {thr}",
                        "symbol": symbol,
                        "period": period,
                        "provider": "em",
                        "condition": cond,
                        "message": msg or f"{symbol} 价格{op} {thr}",
                        "level": "WARNING",
                        "enabled": True,
                    })
    if not items:
        raise HTTPException(status_code=400, detail="No rules provided")
    for rd in items:
        r = AlertRule(
            name=rd.get("name", "Rule"),
            symbol=rd.get("symbol", ""),
            period=str(rd.get("period", "1")),
            provider=rd.get("provider", "em"),
            condition=rd.get("condition", ""),
            message=rd.get("message", ""),
            level=rd.get("level", "WARNING"),
            enabled=bool(rd.get("enabled", True)),
        )
        db.add(r)
        db.commit()
        db.refresh(r)
        created_ids.append(r.id)
    return {"created": created_ids}

@app.delete("/alert_rules/batch")
def batch_delete_alert_rules(payload: dict, db: Session = Depends(get_db)):
    ids = payload.get("ids") if isinstance(payload, dict) else None
    delete_all = bool(payload.get("all")) if isinstance(payload, dict) else False
    if delete_all:
        count = db.query(AlertRule).delete(synchronize_session=False)
        db.commit()
        return {"deleted": count}
    if not ids or not isinstance(ids, list):
        raise HTTPException(status_code=400, detail="ids list required")
    q = db.query(AlertRule).filter(AlertRule.id.in_(ids))
    count = q.count()
    q.delete(synchronize_session=False)
    db.commit()
    return {"deleted": count}

@app.delete("/alert_rules/{rule_id}")
def delete_alert_rule(rule_id: int, db: Session = Depends(get_db)):
    r = db.query(AlertRule).filter(AlertRule.id == rule_id).first()
    if not r:
        raise HTTPException(status_code=404, detail="AlertRule not found")
    db.delete(r)
    db.commit()
    return {"deleted": True}

@app.get("/alerts/")
def list_alert_events(
    since_id: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    q = db.query(AlertEvent)
    if since_id and since_id > 0:
        q = q.filter(AlertEvent.id > since_id)
    items = q.order_by(AlertEvent.id.desc()).limit(limit).all()
    return [alert_event_to_dict(a) for a in items]

@app.delete("/alerts/batch")
def batch_delete_alert_events(payload: dict, db: Session = Depends(get_db)):
    ids = payload.get("ids") if isinstance(payload, dict) else None
    delete_all = bool(payload.get("all")) if isinstance(payload, dict) else False
    if delete_all:
        count = db.query(AlertEvent).delete(synchronize_session=False)
        db.commit()
        return {"deleted": count}
    if not ids or not isinstance(ids, list):
        raise HTTPException(status_code=400, detail="ids list required")
    q = db.query(AlertEvent).filter(AlertEvent.id.in_(ids))
    count = q.count()
    q.delete(synchronize_session=False)
    db.commit()
    return {"deleted": count}

@app.get("/alert_notifications/")
def list_alert_notifications(
    skip: int = 0,
    limit: int = 100,
    include_cleared: bool = False,
    db: Session = Depends(get_db),
):
    q = db.query(AlertNotification)
    if not include_cleared:
        q = q.filter(AlertNotification.is_cleared == False)
    items = q.order_by(AlertNotification.id.desc()).offset(skip).limit(limit).all()
    return [alert_notification_to_dict(n) for n in items]

@app.put("/alert_notifications/{notif_id}")
def update_alert_notification(notif_id: int, payload: dict, db: Session = Depends(get_db)):
    n = db.query(AlertNotification).filter(AlertNotification.id == notif_id).first()
    if not n:
        raise HTTPException(status_code=404, detail="AlertNotification not found")
    for k in ["last_notified_at", "is_cleared"]:
        if k in payload:
            if k == "last_notified_at" and isinstance(payload[k], str):
                try:
                    val = payload[k]
                    if val.endswith("Z"):
                        val = val.replace("Z", "+00:00")
                    n.last_notified_at = datetime.fromisoformat(val)
                except Exception:
                    n.last_notified_at = datetime.utcnow()
            elif k == "is_cleared":
                n.is_cleared = bool(payload[k])
    db.commit()
    db.refresh(n)
    return {"id": n.id}

@app.post("/alert_notifications/clear_all")
def clear_all_alert_notifications(db: Session = Depends(get_db)):
    q = db.query(AlertNotification).filter(AlertNotification.is_cleared == False)
    count = q.count()
    for n in q.all():
        n.is_cleared = True
    db.commit()
    return {"cleared": count}

@app.get("/indicator_configs/")
def list_indicator_configs(db: Session = Depends(get_db)):
    items = db.query(IndicatorConfig).order_by(IndicatorConfig.id.desc()).all()
    res = []
    for i in items:
        res.append({
            "id": i.id,
            "name": i.name,
            "api_name": i.api_name,
            "params": json.loads(i.params) if i.params else {},
            "description": i.description,
            "created_at": i.created_at.isoformat() if i.created_at else None,
        })
    return res

@app.post("/indicator_configs/")
def create_indicator_config(payload: dict, db: Session = Depends(get_db)):
    c = IndicatorConfig(
        name=payload.get("name", "New Indicator"),
        api_name=payload.get("api_name", ""),
        params=json.dumps(payload.get("params", {})),
        description=payload.get("description", ""),
    )
    db.add(c)
    db.commit()
    db.refresh(c)
    return {"id": c.id}

@app.put("/indicator_configs/{config_id}")
def update_indicator_config(config_id: int, payload: dict, db: Session = Depends(get_db)):
    c = db.query(IndicatorConfig).filter(IndicatorConfig.id == config_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="IndicatorConfig not found")
    
    if "name" in payload:
        c.name = payload["name"]
    if "api_name" in payload:
        c.api_name = payload["api_name"]
    if "params" in payload:
        c.params = json.dumps(payload["params"])
    if "description" in payload:
        c.description = payload["description"]
        
    db.commit()
    return {"id": c.id}

@app.delete("/indicator_configs/{config_id}")
def delete_indicator_config(config_id: int, db: Session = Depends(get_db)):
    c = db.query(IndicatorConfig).filter(IndicatorConfig.id == config_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="IndicatorConfig not found")
    db.delete(c)
    db.commit()
    return {"deleted": True}

# --- Indicator Collections ---

@app.get("/indicator_collections/")
def list_indicator_collections(db: Session = Depends(get_db)):
    items = db.query(IndicatorCollection).order_by(IndicatorCollection.id.desc()).all()
    res = []
    for i in items:
        res.append({
            "id": i.id,
            "name": i.name,
            "description": i.description,
            "indicator_ids": json.loads(i.indicator_ids) if i.indicator_ids else [],
            "created_at": i.created_at.isoformat() if i.created_at else None,
        })
    return res

@app.post("/indicator_collections/")
def create_indicator_collection(payload: dict, db: Session = Depends(get_db)):
    c = IndicatorCollection(
        name=payload.get("name", "New Collection"),
        description=payload.get("description", ""),
        indicator_ids=json.dumps(payload.get("indicator_ids", [])),
    )
    db.add(c)
    db.commit()
    db.refresh(c)
    return {"id": c.id}

@app.put("/indicator_collections/{collection_id}")
def update_indicator_collection(collection_id: int, payload: dict, db: Session = Depends(get_db)):
    c = db.query(IndicatorCollection).filter(IndicatorCollection.id == collection_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="IndicatorCollection not found")
    
    if "name" in payload:
        c.name = payload["name"]
    if "description" in payload:
        c.description = payload["description"]
    if "indicator_ids" in payload:
        c.indicator_ids = json.dumps(payload["indicator_ids"])
        
    db.commit()
    return {"id": c.id}

@app.delete("/indicator_collections/{collection_id}")
def delete_indicator_collection(collection_id: int, db: Session = Depends(get_db)):
    c = db.query(IndicatorCollection).filter(IndicatorCollection.id == collection_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="IndicatorCollection not found")
    db.delete(c)
    db.commit()
    return {"deleted": True}

from datetime import datetime, timedelta

def _resolve_dynamic_params(params: dict):
    """
    Resolve dynamic date variables in params.
    Supported variables:
    {{today}} -> YYYYMMDD
    {{today-N}} -> YYYYMMDD (N days ago)
    {{today+N}} -> YYYYMMDD (N days later)
    """
    now = datetime.now()
    
    for k, v in params.items():
        if isinstance(v, str) and "{{" in v and "}}" in v:
            try:
                # Simple regex or string manipulation
                if "{{today}}" in v:
                    params[k] = v.replace("{{today}}", now.strftime("%Y%m%d"))
                elif "{{today-" in v:
                    # Extract N
                    import re
                    match = re.search(r"{{today-(\d+)}}", v)
                    if match:
                        days = int(match.group(1))
                        target_date = now - timedelta(days=days)
                        params[k] = v.replace(match.group(0), target_date.strftime("%Y%m%d"))
                elif "{{today+" in v:
                    import re
                    match = re.search(r"{{today\+(\d+)}}", v)
                    if match:
                        days = int(match.group(1))
                        target_date = now + timedelta(days=days)
                        params[k] = v.replace(match.group(0), target_date.strftime("%Y%m%d"))
            except Exception as e:
                logger.warning(f"Failed to resolve dynamic param {k}={v}: {e}")
    return params

@app.post("/indicator_collections/{collection_id}/run")
def run_indicator_collection(collection_id: int, payload: dict, db: Session = Depends(get_db)):
    """
    Run all indicators in a collection with provided dynamic parameters (symbol, etc.)
    Payload: { "symbol": "600498", "start_date": "...", "end_date": "...", "adjust": "..." }
    """
    col = db.query(IndicatorCollection).filter(IndicatorCollection.id == collection_id).first()
    if not col:
        raise HTTPException(status_code=404, detail="IndicatorCollection not found")
    
    ids = json.loads(col.indicator_ids) if col.indicator_ids else []
    if not ids:
        return {"results": {}}
    
    indicators = db.query(IndicatorConfig).filter(IndicatorConfig.id.in_(ids)).all()
    
    results = {}
    
    for ind in indicators:
        try:
            params = json.loads(ind.params) if ind.params else {}
            
            # 1. Resolve dynamic date variables in original params first
            params = _resolve_dynamic_params(params)
            
            # 2. Apply overrides (only if provided in payload)
            for k, v in payload.items():
                if not v: continue # Skip empty overrides
                
                # Direct match
                if k in params:
                    params[k] = v
                
                # Heuristic for symbol/stock/code
                if k == "symbol":
                    if "stock" in params: params["stock"] = v
                    if "code" in params: params["code"] = v
                    if "symbol" in params: params["symbol"] = v
            
            # Call Proxy
            # We use internal requests to alaya.zone directly to save overhead of self-calling proxy endpoint
            # But wait, our proxy logic handles error logging nicely.
            # Let's just use requests directly to alaya.zone
            
            # Smart Adjustment for Minute Data Interfaces
            if "_min_" in ind.api_name:
                # 1. Fix period='daily' -> '1'
                if params.get("period") == "daily":
                    params["period"] = "1"
                
                # 2. Fix date format YYYYMMDD -> YYYY-MM-DD HH:MM:SS
                # AkShare min interfaces often need specific start/end time format
                if "start_date" in params and len(str(params["start_date"])) == 8:
                    s = str(params["start_date"])
                    params["start_date"] = f"{s[0:4]}-{s[4:6]}-{s[6:8]} 09:30:00"
                
                if "end_date" in params and len(str(params["end_date"])) == 8:
                    s = str(params["end_date"])
                    params["end_date"] = f"{s[0:4]}-{s[4:6]}-{s[6:8]} 15:00:00"

            logger.info(f"Collection Run: {ind.name} ({ind.api_name}) params={params}")
            resp = requests.get(f"http://alaya.zone:3001/akshare/{ind.api_name}", params=params, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                results[ind.name] = data.get("data", data)
            else:
                results[ind.name] = {"error": f"Status {resp.status_code}", "detail": resp.text}
                
        except Exception as e:
            logger.error(f"Collection Run Error {ind.name}: {e}")
            results[ind.name] = {"error": str(e)}
            
    return {"results": results}

@app.get("/proxy/akshare/{api_name}")
def proxy_akshare_get(api_name: str, request: Request):
    # Forward query params
    params = dict(request.query_params)
    try:
        resp = requests.get(f"http://alaya.zone:3001/akshare/{api_name}", params=params, timeout=30)
        return resp.json()
    except Exception as e:
        logger.error(f"Proxy GET failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/proxy/akshare/proxy")
async def proxy_akshare_post(request: Request):
    try:
        body = await request.json()
        resp = requests.post("http://alaya.zone:3001/akshare/proxy", json=body, timeout=30)
        return resp.json()
    except Exception as e:
        logger.error(f"Proxy POST failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health(db: Session = Depends(get_db)):
    rules_enabled = db.query(AlertRule).filter(AlertRule.enabled == True).count()
    notif_unread = db.query(AlertNotification).filter(AlertNotification.is_cleared == False).count()
    return {
        "status": "ok",
        "time": datetime.utcnow().isoformat(),
        "scheduler_running": scheduler is not None,
        "rules_enabled_count": rules_enabled,
        "notifications_unread_count": notif_unread,
    }

def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").lower()
    if s.startswith("sh") or s.startswith("sz"):
        return s
    if s.startswith("60") or s.startswith("68") or s.startswith("688") or s.startswith("900") or s.startswith("730") or s.startswith("700"):
        return f"sh{s}"
    return f"sz{s}"

def _strip_prefix(symbol: str) -> str:
    s = (symbol or "").lower()
    if s.startswith("sh") or s.startswith("sz"):
        return s[2:]
    return s

def fetch_minute_latest_em(symbol: str, period: str, ds: str, adjust: str = ""):
    base_symbol = _strip_prefix(symbol)
    d = f"{ds[0:4]}-{ds[4:6]}-{ds[6:8]}"
    start = f"{d} 09:30:00"
    end = f"{d} 15:00:00"
    params = {"symbol": base_symbol, "start_date": start, "end_date": end, "period": period}
    if adjust:
        params["adjust"] = adjust
    resp = requests.get("http://alaya.zone:3001/akshare/stock_zh_a_hist_min_em", params=params, timeout=10)
    if resp.status_code != 200:
        return None
    raw = resp.json()
    rows = raw.get("data", []) if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
    if not rows:
        return None
    r = rows[-1]
    return {
        "时间": r.get("时间") or r.get("day"),
        "开盘": r.get("开盘") or r.get("open"),
        "最高": r.get("最高") or r.get("high"),
        "最低": r.get("最低") or r.get("low"),
        "收盘": r.get("收盘") or r.get("close"),
        "成交量": r.get("成交量") or r.get("volume"),
    }

def eval_condition(expr: str, latest: dict):
    ctx = {
        "时间": latest.get("时间"),
        "开盘": latest.get("开盘"),
        "最高": latest.get("最高"),
        "最低": latest.get("最低"),
        "收盘": latest.get("收盘"),
        "成交量": latest.get("成交量"),
        "price": latest.get("收盘"),
        "volume": latest.get("成交量"),
    }
    try:
        return bool(eval(expr, {"__builtins__": None}, ctx))
    except Exception:
        return False

def check_alert_rules_once():
    db = SessionLocal()
    try:
        ds = datetime.now().strftime("%Y%m%d")
        rules = db.query(AlertRule).filter(AlertRule.enabled == True).all()
        logger.info(f"AlertRule tick: rules={len(rules)}")
        for r in rules:
            latest = fetch_minute_latest_em(r.symbol, r.period, ds)
            logger.info(f"Rule {r.id} symbol={r.symbol} latest={latest}")
            r.last_checked_at = datetime.utcnow()
            if latest and eval_condition(r.condition or "", latest):
                msg = f"[Rule {r.name}] {r.message}"
                evt = AlertEvent(
                    rule_id=r.id,
                    symbol=r.symbol,
                    message=msg,
                    level=r.level,
                    timestamp=datetime.utcnow()
                )
                db.add(evt)
                notif = AlertNotification(
                    rule_id=r.id,
                    message=msg,
                    level=r.level,
                    triggered_at=datetime.utcnow(),
                    is_cleared=False,
                )
                db.add(notif)
                _send_alert_email(f"规则告警 {r.symbol}", msg)
                r.last_triggered_at = datetime.utcnow()
                # Auto-close rule after first trigger
                r.enabled = False
                logger.info(f"Rule {r.id} triggered")
            db.commit()
    finally:
        db.close()

def _parse_recipients(s: str) -> List[str]:
    if not s:
        return []
    return [x.strip() for x in re.split(r"[;,]", s) if x.strip()]

def _send_alert_email(subject: str, body: str):
    to_env = os.environ.get("ALERT_EMAIL_TO", "").strip()
    recipients = _parse_recipients(to_env)
    if not recipients:
        return
    host = os.environ.get("SMTP_HOST", "").strip()
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER", "").strip()
    password = os.environ.get("SMTP_PASS", "").strip()
    use_ssl = os.environ.get("SMTP_SSL", "false").strip().lower() in ("1", "true", "yes")
    sender = os.environ.get("EMAIL_FROM", user or "").strip()
    if not host or not user or not password or not sender:
        logger.warning("Alert email not sent: SMTP config incomplete")
        return
    msg = MIMEText(body, "plain", "utf-8")
    msg["From"] = formataddr(("AI Watch Stock", sender))
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    try:
        if use_ssl or port == 465:
            server = smtplib.SMTP_SSL(host, port, timeout=10)
        else:
            server = smtplib.SMTP(host, port, timeout=10)
            server.ehlo()
            server.starttls()
        server.login(user, password)
        server.sendmail(sender, recipients, msg.as_string())
        server.quit()
        logger.info(f"Alert email sent to {recipients}")
    except Exception as e:
        logger.error(f"Send alert email failed: {e}")

@app.on_event("startup")
def on_startup():
    global scheduler, scheduler_lock_file
    try:
        scheduler_lock_file = open("scheduler.lock", "w")
        fcntl.flock(scheduler_lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        
        if scheduler is None:
            scheduler = BackgroundScheduler()
            scheduler.add_job(check_alert_rules_once, IntervalTrigger(minutes=1))
            scheduler.start()
            logger.info("AlertRule scheduler started (lock acquired)")
    except IOError:
        logger.info("AlertRule scheduler not started (lock held by another worker)")
        if scheduler_lock_file:
            try:
                scheduler_lock_file.close()
            except Exception:
                pass
            scheduler_lock_file = None

@app.on_event("shutdown")
def on_shutdown():
    global scheduler, scheduler_lock_file
    if scheduler:
        scheduler.shutdown(wait=False)
        scheduler = None
        logger.info("AlertRule scheduler stopped")
    
    if scheduler_lock_file:
        try:
            fcntl.flock(scheduler_lock_file, fcntl.LOCK_UN)
            scheduler_lock_file.close()
        except Exception:
            pass
        scheduler_lock_file = None
