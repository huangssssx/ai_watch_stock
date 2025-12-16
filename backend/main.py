from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from backend.database import SessionLocal, engine, Base
from backend import models
from backend.engine import StrategyEngine
import asyncio
from datetime import datetime
import logging
from fastapi.middleware.cors import CORSMiddleware
import requests
from typing import List, Dict, Optional
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("API")

# Initialize DB
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

# Global running tasks
running_tasks = {}
scheduler: Optional[BackgroundScheduler] = None

def strategy_to_dict(s):
    return {
        "id": s.id,
        "name": s.name,
        "symbol": s.symbol,
        "content": s.content,
        "status": s.status,
        "created_at": s.created_at.isoformat() if s.created_at else None,
    }

def alert_to_dict(a):
    return {
        "id": a.id,
        "strategy_id": a.strategy_id,
        "timestamp": a.timestamp.isoformat() if a.timestamp else None,
        "message": a.message,
        "level": a.level,
    }

@app.post("/strategies/")
def create_strategy(strategy: dict, db: Session = Depends(get_db)):
    # strategy dict should match Strategy model
    # Expecting { "name": "...", "symbol": "...", "content": {...} }
    db_strat = models.Strategy(
        name=strategy.get("name", "Untitled"),
        symbol=strategy.get("symbol", ""),
        content=strategy.get("content", {}),
        status="stopped"
    )
    db.add(db_strat)
    db.commit()
    db.refresh(db_strat)
    return strategy_to_dict(db_strat)

@app.get("/strategies/")
def read_strategies(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    items = db.query(models.Strategy).offset(skip).limit(limit).all()
    return [strategy_to_dict(s) for s in items]

@app.get("/strategies/{strategy_id}")
def read_strategy(strategy_id: int, db: Session = Depends(get_db)):
    strat = db.query(models.Strategy).filter(models.Strategy.id == strategy_id).first()
    if not strat:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return strategy_to_dict(strat)

@app.put("/strategies/{strategy_id}")
def update_strategy(strategy_id: int, strategy: dict, db: Session = Depends(get_db)):
    db_strat = db.query(models.Strategy).filter(models.Strategy.id == strategy_id).first()
    if not db_strat:
        raise HTTPException(status_code=404, detail="Strategy not found")
    
    if strategy.get("name"): db_strat.name = strategy["name"]
    if strategy.get("symbol"): db_strat.symbol = strategy["symbol"]
    if strategy.get("content"): db_strat.content = strategy["content"]
    
    db.commit()
    db.refresh(db_strat)
    return strategy_to_dict(db_strat)

@app.post("/monitor/start/{strategy_id}")
async def start_monitor(strategy_id: int, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    strat = db.query(models.Strategy).filter(models.Strategy.id == strategy_id).first()
    if not strat:
        raise HTTPException(status_code=404, detail="Strategy not found")
    
    if strat.status == "running":
        return {"message": "Already running"}
    
    strat.status = "running"
    db.commit()
    
    # Start background loop
    # Check if task is already in dictionary (maybe from restart)
    if strategy_id in running_tasks:
        running_tasks[strategy_id].cancel()
    
    task = asyncio.create_task(monitor_loop(strategy_id))
    running_tasks[strategy_id] = task
    
    return {"message": "Monitoring started"}

@app.post("/monitor/stop/{strategy_id}")
def stop_monitor(strategy_id: int, db: Session = Depends(get_db)):
    strat = db.query(models.Strategy).filter(models.Strategy.id == strategy_id).first()
    if not strat:
        raise HTTPException(status_code=404, detail="Strategy not found")
    
    strat.status = "stopped"
    db.commit()
    
    if strategy_id in running_tasks:
        running_tasks[strategy_id].cancel()
        del running_tasks[strategy_id]
    
    return {"message": "Monitoring stopped"}

@app.get("/alerts/{strategy_id}")
def get_alerts(strategy_id: int, db: Session = Depends(get_db)):
    items = (
        db.query(models.AlertLog)
        .filter(models.AlertLog.strategy_id == strategy_id)
        .order_by(models.AlertLog.timestamp.desc())
        .limit(50)
        .all()
    )
    return [alert_to_dict(a) for a in items]

@app.put("/alerts/{alert_id}")
def update_alert(alert_id: int, payload: dict, db: Session = Depends(get_db)):
    alert = db.query(models.AlertLog).filter(models.AlertLog.id == alert_id).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    if "message" in payload:
        alert.message = payload["message"]
    if "level" in payload:
        alert.level = payload["level"]
    db.commit()
    db.refresh(alert)
    return alert_to_dict(alert)

@app.post("/alert_rules/")
def create_alert_rule(rule: dict, db: Session = Depends(get_db)):
    r = models.AlertRule(
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
    items = db.query(models.AlertRule).offset(skip).limit(limit).all()
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
    r = db.query(models.AlertRule).filter(models.AlertRule.id == rule_id).first()
    if not r:
        raise HTTPException(status_code=404, detail="AlertRule not found")
    for k in ["name","symbol","period","provider","condition","message","level","enabled"]:
        if k in payload:
            setattr(r, k, payload[k])
    db.commit()
    db.refresh(r)
    return {"id": r.id}

@app.delete("/alert_rules/{rule_id}")
def delete_alert_rule(rule_id: int, db: Session = Depends(get_db)):
    r = db.query(models.AlertRule).filter(models.AlertRule.id == rule_id).first()
    if not r:
        raise HTTPException(status_code=404, detail="AlertRule not found")
    db.delete(r)
    db.commit()
    return {"deleted": True}

@app.post("/alert_rules/batch")
def batch_create_alert_rules(payload: dict, db: Session = Depends(get_db)):
    created_ids: List[int] = []
    # Preferred: JSON array under 'rules'
    rules = payload.get("rules")
    if isinstance(rules, list):
        for rd in rules:
            r = models.AlertRule(
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
    # Fallback: parse 'paste' content (JSON array or line-based)
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
                    # Optional period and message
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
        r = models.AlertRule(
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
        count = db.query(models.AlertRule).delete(synchronize_session=False)
        db.commit()
        return {"deleted": count}
    if not ids or not isinstance(ids, list):
        raise HTTPException(status_code=400, detail="ids list required")
    q = db.query(models.AlertRule).filter(models.AlertRule.id.in_(ids))
    count = q.count()
    q.delete(synchronize_session=False)
    db.commit()
    return {"deleted": count}

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
        rules = db.query(models.AlertRule).filter(models.AlertRule.enabled == True).all()
        logger.info(f"AlertRule tick: rules={len(rules)}")
        for r in rules:
            latest = fetch_minute_latest_em(r.symbol, r.period, ds)
            logger.info(f"Rule {r.id} symbol={r.symbol} latest={latest}")
            r.last_checked_at = datetime.utcnow()
            if latest and eval_condition(r.condition or "", latest):
                log = models.AlertLog(
                    strategy_id=0,
                    message=f"[Rule {r.name}] {r.message}",
                    level=r.level,
                    timestamp=datetime.utcnow()
                )
                db.add(log)
                r.last_triggered_at = datetime.utcnow()
                # Auto-close rule after first trigger
                r.enabled = False
                logger.info(f"Rule {r.id} triggered")
            db.commit()
    finally:
        db.close()

@app.get("/data/minute")
def get_minute_data(
    symbol: str,
    period: str = "1",
    date: str = None,
    provider: str = None,
    adjust: str = "",
    latest: bool = False,
    today_only: bool = False,
):
    """
    Fetch intraday minute K-line via AkShare proxy
    """
    try:
        ds = date if date else datetime.now().strftime("%Y%m%d")
        order = [provider] if provider else ["em"]
        last_error = None
        for prov in order:
            if prov == "sina":
                api = "stock_zh_a_minute"
                params = {"symbol": _normalize_symbol(symbol), "period": period}
                if adjust:
                    params["adjust"] = adjust
                try:
                    resp = requests.get("http://alaya.zone:3001/akshare/" + api, params=params, timeout=10)
                    if resp.status_code != 200:
                        last_error = f"sina http {resp.status_code}"
                        continue
                    raw = resp.json()
                    rows = raw.get("data", []) if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
                    # Normalize keys to Chinese for chart consumption
                    normalized = []
                    for r in rows:
                        if isinstance(r, dict):
                            if "day" in r:
                                normalized.append({
                                    "时间": r.get("day"),
                                    "开盘": r.get("open"),
                                    "最高": r.get("high"),
                                    "最低": r.get("low"),
                                    "收盘": r.get("close"),
                                    "成交量": r.get("volume"),
                                })
                            else:
                                normalized.append(r)
                    if normalized:
                        logger.info(f"Minute data fetched: provider=sina, symbol={symbol}, period={period}, rows={len(normalized)}")
                        logger.info(f"Minute data head: {normalized[:5]}")
                        # Derive actual trading date from first row
                        actual_date = ds
                        try:
                            t0 = normalized[0].get("时间")
                            if isinstance(t0, str) and " " in t0:
                                actual_date = t0.split(" ")[0].replace("-", "")
                        except Exception:
                            pass
                        # Optional filters
                        if today_only:
                            dstr = f"{actual_date[0:4]}-{actual_date[4:6]}-{actual_date[6:8]}"
                            normalized = [r for r in normalized if isinstance(r.get("时间"), str) and r.get("时间").startswith(dstr)]
                        if latest and normalized:
                            normalized = [normalized[-1]]
                        return {"code": 200, "data": normalized, "date": actual_date, "provider": "sina"}
                except Exception as e:
                    last_error = f"sina err {e}"
                    continue
            elif prov == "em":
                api = "stock_zh_a_hist_min_em"
                base_symbol = _strip_prefix(symbol)
                # Build start/end based on date
                d = f"{ds[0:4]}-{ds[4:6]}-{ds[6:8]}"
                start = f"{d} 09:30:00"
                end = f"{d} 15:00:00"
                params = {"symbol": base_symbol, "start_date": start, "end_date": end, "period": period}
                if adjust:
                    params["adjust"] = adjust
                try:
                    resp = requests.get("http://alaya.zone:3001/akshare/" + api, params=params, timeout=10)
                    if resp.status_code != 200:
                        last_error = f"em http {resp.status_code}"
                        continue
                    raw = resp.json()
                    rows = raw.get("data", []) if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
                    # EM already uses Chinese keys; keep only required columns
                    normalized = []
                    for r in rows:
                        if isinstance(r, dict):
                            normalized.append({
                                "时间": r.get("时间") or r.get("day"),
                                "开盘": r.get("开盘") or r.get("open"),
                                "最高": r.get("最高") or r.get("high"),
                                "最低": r.get("最低") or r.get("low"),
                                "收盘": r.get("收盘") or r.get("close"),
                                "成交量": r.get("成交量") or r.get("volume"),
                            })
                    if normalized:
                        logger.info(f"Minute data fetched: provider=em, symbol={symbol}, period={period}, rows={len(normalized)}")
                        logger.info(f"Minute data head: {normalized[:5]}")
                        # Optional filters
                        if today_only:
                            dstr = f"{ds[0:4]}-{ds[4:6]}-{ds[6:8]}"
                            normalized = [r for r in normalized if isinstance(r.get("时间"), str) and r.get("时间").startswith(dstr)]
                        if latest and normalized:
                            normalized = [normalized[-1]]
                        return {"code": 200, "data": normalized, "date": ds, "provider": "em"}
                except Exception as e:
                    last_error = f"em err {e}"
                    continue
        raise HTTPException(status_code=500, detail=f"Minute data fetch failed. Last error: {last_error}")
    except Exception as e:
        logger.error(f"Minute data fetch failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.on_event("startup")
def on_startup():
    global scheduler
    if scheduler is None:
        scheduler = BackgroundScheduler()
        scheduler.add_job(check_alert_rules_once, IntervalTrigger(minutes=1))
        scheduler.start()
        logger.info("AlertRule scheduler started")

@app.on_event("shutdown")
def on_shutdown():
    global scheduler
    if scheduler:
        scheduler.shutdown(wait=False)
        scheduler = None
        logger.info("AlertRule scheduler stopped")

async def monitor_loop(strategy_id: int):
    logger.info(f"Starting monitor for {strategy_id}")
    
    while True:
        db = SessionLocal()
        try:
            logger.info(f"Tick start strategy={strategy_id} at {datetime.utcnow().isoformat()}")
            strat = db.query(models.Strategy).filter(models.Strategy.id == strategy_id).first()
            if not strat or strat.status != "running":
                logger.info(f"Stopping monitor for {strategy_id} (Status changed)")
                break
            
            try:
                engine = StrategyEngine(strat.content)
                alerts = engine.run_once()
                
                if alerts:
                    logger.info(f"Strategy {strategy_id} generated {len(alerts)} alerts")
                    for alert in alerts:
                        log = models.AlertLog(
                            strategy_id=strategy_id,
                            message=f"[{alert['scenario']}] {alert['message']}",
                            level="WARNING",
                            timestamp=datetime.utcnow()
                        )
                        db.add(log)
                    db.commit()
                else:
                    logger.info(f"Strategy {strategy_id} no alerts")
                
            except Exception as e:
                logger.error(f"Monitor error for {strategy_id}: {e}")
            
        except Exception as e:
            logger.error(f"DB Error in monitor loop: {e}")
        finally:
            db.close()
            
        try:
            await asyncio.sleep(5) # Poll every 5 seconds
        except asyncio.CancelledError:
            logger.info("Monitor cancelled")
            break
