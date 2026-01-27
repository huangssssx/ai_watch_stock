from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from database import get_db
import models
import schemas
import json
import datetime
from services.monitor_service import process_stock, update_stock_job, analyze_stock_manual, fetch_stock_indicators_data
import akshare as ak
from utils.ak_fallback import get_a_minute_data

router = APIRouter(prefix="/stocks", tags=["stocks"])

@router.get("/", response_model=List[schemas.Stock])
def read_stocks(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    stocks = db.query(models.Stock).order_by(models.Stock.is_pinned.desc(), models.Stock.id.asc()).offset(skip).limit(limit).all()
    return stocks

@router.post("/", response_model=schemas.Stock)
def create_stock(stock: schemas.StockCreate, db: Session = Depends(get_db)):
    data = stock.dict()
    
    if not data.get("monitoring_schedule"):
        default_schedule = [
            {"start": "09:30", "end": "11:30"},
            {"start": "13:00", "end": "15:00"}
        ]
        data["monitoring_schedule"] = json.dumps(default_schedule)

    indicator_ids = data.pop("indicator_ids", None) or []
    db_stock = models.Stock(**data)
    if indicator_ids:
        indicators = (
            db.query(models.IndicatorDefinition)
            .filter(models.IndicatorDefinition.id.in_(indicator_ids))
            .all()
        )
        db_stock.indicators = indicators
    db.add(db_stock)
    db.commit()
    db.refresh(db_stock)
    return db_stock

@router.put("/{stock_id}", response_model=schemas.Stock)
def update_stock(stock_id: int, stock_update: schemas.StockUpdate, db: Session = Depends(get_db)):
    db_stock = db.query(models.Stock).filter(models.Stock.id == stock_id).first()
    if not db_stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    
    update_data = stock_update.dict(exclude_unset=True)
    
    indicator_ids = None
    if "indicator_ids" in update_data:
        indicator_ids = update_data.pop("indicator_ids")

    for key, value in update_data.items():
        setattr(db_stock, key, value)

    if indicator_ids is not None:
        if len(indicator_ids) == 0:
            db_stock.indicators = []
        else:
            indicators = (
                db.query(models.IndicatorDefinition)
                .filter(models.IndicatorDefinition.id.in_(indicator_ids))
                .all()
            )
            db_stock.indicators = indicators
    
    db.commit()
    db.refresh(db_stock)
    
    # Update scheduler if monitoring status or interval changed
    if "is_monitoring" in update_data or "interval_seconds" in update_data:
        update_stock_job(db_stock.id, db_stock.interval_seconds, db_stock.is_monitoring)
        
    return db_stock

@router.delete("/{stock_id}")
def delete_stock(stock_id: int, db: Session = Depends(get_db)):
    db_stock = db.query(models.Stock).filter(models.Stock.id == stock_id).first()
    if not db_stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    
    # Stop monitoring
    update_stock_job(stock_id, 0, False)
    
    db.delete(db_stock)
    db.commit()
    return {"ok": True}

@router.post("/{stock_id}/test-run", response_model=schemas.StockTestRunResponse)
def test_run_stock(stock_id: int, send_alerts: bool = True, bypass_checks: bool = True, db: Session = Depends(get_db)):
    result = process_stock(
        stock_id,
        bypass_checks=bypass_checks,
        send_alerts=send_alerts,
        is_test=False,
        return_result=True,
        db=db,
    )
    if not result:
        raise HTTPException(status_code=500, detail="Test run failed")
    return result

@router.get("/{stock_id}/ai-watch-config", response_model=schemas.StockAIWatchConfig)
def get_ai_watch_config(stock_id: int, db: Session = Depends(get_db)):
    config = db.query(models.StockAIWatchConfig).filter(models.StockAIWatchConfig.stock_id == stock_id).first()
    if not config:
        # Return default
        return schemas.StockAIWatchConfig(
            id=0,
            stock_id=stock_id,
            indicator_ids="[]",
            custom_prompt="",
            ai_provider_id=None,
            analysis_history="[]",
            updated_at=None
        )
    return config

@router.post("/{stock_id}/ai-watch-config", response_model=schemas.StockAIWatchConfig)
def save_ai_watch_config(stock_id: int, config_in: schemas.StockAIWatchConfigBase, db: Session = Depends(get_db)):
    config = db.query(models.StockAIWatchConfig).filter(models.StockAIWatchConfig.stock_id == stock_id).first()
    if not config:
        config = models.StockAIWatchConfig(stock_id=stock_id)
        db.add(config)
    
    config.indicator_ids = config_in.indicator_ids
    config.custom_prompt = config_in.custom_prompt
    config.ai_provider_id = config_in.ai_provider_id
    # Don't touch history here
    
    db.commit()
    db.refresh(config)
    return config

@router.post("/{stock_id}/ai-watch-analyze")
def run_ai_watch_analyze(stock_id: int, request: schemas.AIWatchAnalyzeRequest, db: Session = Depends(get_db)):
    # 1. Run Analysis
    result = analyze_stock_manual(
        stock_id=stock_id,
        indicator_ids=request.indicator_ids,
        custom_prompt=request.custom_prompt,
        ai_provider_id=request.ai_provider_id,
        db=db
    )
    
    if not result.get("ok"):
        raise HTTPException(status_code=500, detail=result.get("error", "Unknown error"))

    # 2. Save History
    config = db.query(models.StockAIWatchConfig).filter(models.StockAIWatchConfig.stock_id == stock_id).first()
    if not config:
        config = models.StockAIWatchConfig(
            stock_id=stock_id,
            indicator_ids=json.dumps(request.indicator_ids),
            custom_prompt=request.custom_prompt,
            ai_provider_id=request.ai_provider_id
        )
        db.add(config)
    else:
        # Update preferences too
        config.indicator_ids = json.dumps(request.indicator_ids)
        config.custom_prompt = request.custom_prompt
        config.ai_provider_id = request.ai_provider_id

    # Append History
    try:
        history = json.loads(config.analysis_history) if config.analysis_history else []
        if not isinstance(history, list):
            history = []
    except:
        history = []
    
    new_entry = {
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "result": result
    }
    history.insert(0, new_entry)
    history = history[:3] # Keep last 3
    
    config.analysis_history = json.dumps(history, ensure_ascii=False)
    
    db.commit()
    
    return result

@router.post("/{stock_id}/preview-indicators")
def preview_stock_indicators(stock_id: int, request: schemas.AIWatchAnalyzeRequest, db: Session = Depends(get_db)):
    # Note: reusing AIWatchAnalyzeRequest for indicator_ids, ignoring prompt/ai fields
    
    # 1. Fetch Data
    result = fetch_stock_indicators_data(
        stock_id=stock_id,
        indicator_ids=request.indicator_ids,
        db=db
    )
    
    if not result.get("ok"):
        raise HTTPException(status_code=500, detail=result.get("error", "Unknown error"))
        
    # 2. Save Preference (reuse StockAIWatchConfig so user sees same selection in both tools)
    config = db.query(models.StockAIWatchConfig).filter(models.StockAIWatchConfig.stock_id == stock_id).first()
    if not config:
        config = models.StockAIWatchConfig(
            stock_id=stock_id,
            indicator_ids=json.dumps(request.indicator_ids),
            custom_prompt=""
        )
        db.add(config)
    else:
        config.indicator_ids = json.dumps(request.indicator_ids)
    
    db.commit()
    
    return result

@router.get("/{symbol}/daily")
def get_stock_daily_data(symbol: str):
    """
    Get intraday minute-level data for a stock symbol (latest trading day).
    Note: Endpoint name kept as 'daily' to avoid frontend refactor, but returns intraday data.
    """
    try:
        # Clean symbol (remove sh/sz prefix if exists)
        clean_symbol = symbol.lower().replace("sh", "").replace("sz", "")
        
        # Fetch minute data (period='1' means 1-minute interval)
        # adjust='qfq' is usually good, but for intraday pure price might be better? 
        # Actually for intraday comparison, qfq is fine or no adjust.
        # stock_zh_a_hist_min_em returns recent data.
        df = get_a_minute_data(symbol=clean_symbol, period='1', adjust='qfq')
        
        if df is None or df.empty:
            return {"ok": False, "error": "No data found"}
            
        # Filter for the latest date
        # '时间' column format: '2025-01-15 09:30:00'
        last_dt = df.iloc[-1]['时间']
        last_date_str = last_dt.split(" ")[0]
        today_data = df[df['时间'].str.startswith(last_date_str)]
        
        # Format columns
        # akshare returns: 时间, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 均价
        result = []
        for _, row in today_data.iterrows():
            # Extract time part HH:MM
            dt_str = row["时间"]
            time_str = dt_str.split(" ")[1][:5] # '09:30:00' -> '09:30'
            
            result.append({
                "date": dt_str,      # Full datetime for reference
                "time": time_str,    # HH:MM for X-axis
                "open": row["开盘"],
                "close": row["收盘"],
                "high": row["最高"],
                "low": row["最低"],
                "volume": row["成交量"],
                "avg": row["均价"]
            })
            
        return {
            "ok": True, 
            "data": result, 
            "info": {
                "date": last_date_str,
                "symbol": symbol
            }
        }
        
    except Exception as e:
        return {"ok": False, "error": str(e)}

@router.get("/{symbol}/history")
def get_stock_history_data(symbol: str, period: str = "daily"):
    """
    Get stock history data (k-line).
    period: daily, weekly, monthly
    """
    try:
        clean_symbol = symbol.lower().replace("sh", "").replace("sz", "")
        
        # Default start date ~2 years ago for daily, more for weekly/monthly
        end_date = datetime.date.today().strftime("%Y%m%d")
        days_back = 730
        if period == "weekly":
            days_back = 1800 # ~5 years
        elif period == "monthly":
            days_back = 3650 # ~10 years
            
        start_date = (datetime.date.today() - datetime.timedelta(days=days_back)).strftime("%Y%m%d")
        
        df = ak.stock_zh_a_hist(
            symbol=clean_symbol, 
            period=period, 
            start_date=start_date, 
            end_date=end_date, 
            adjust="qfq"
        )
        
        if df is None or df.empty:
            return {"ok": False, "error": "No data found"}
            
        result = []
        for _, row in df.iterrows():
            result.append({
                "date": row["日期"],
                "open": row["开盘"],
                "close": row["收盘"],
                "high": row["最高"],
                "low": row["最低"],
                "volume": row["成交量"]
            })
            
        return {"ok": True, "data": result}
        
    except Exception as e:
        return {"ok": False, "error": str(e)}
