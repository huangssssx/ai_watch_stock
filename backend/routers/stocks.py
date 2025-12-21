from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from database import get_db
import models
import schemas
import json
from jinja2 import Template
from services.monitor_service import update_stock_job
from services.data_fetcher import data_fetcher
from services.ai_service import ai_service

router = APIRouter(prefix="/stocks", tags=["stocks"])

@router.get("/", response_model=List[schemas.Stock])
def read_stocks(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    stocks = db.query(models.Stock).offset(skip).limit(limit).all()
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
def test_run_stock(stock_id: int, db: Session = Depends(get_db)):
    db_stock = db.query(models.Stock).filter(models.Stock.id == stock_id).first()
    if not db_stock:
        raise HTTPException(status_code=404, detail="Stock not found")

    if not db_stock.ai_provider_id:
        raise HTTPException(status_code=400, detail="No AI provider configured")

    ai_config = db.query(models.AIConfig).filter(models.AIConfig.id == db_stock.ai_provider_id).first()
    if not ai_config:
        raise HTTPException(status_code=404, detail="AI Config not found")

    context = {"symbol": db_stock.symbol, "name": db_stock.name}
    data_parts = []
    for indicator in db_stock.indicators:
        data = data_fetcher.fetch(indicator.akshare_api, indicator.params_json, context, indicator.post_process_json, indicator.python_code)
        data_parts.append(f"--- Indicator: {indicator.name} ---\n{data}\n")
    full_data = "\n".join(data_parts)

    data_char_limit = ai_config.max_tokens if ai_config.max_tokens else 100000
    data_truncated = len(full_data) > data_char_limit
    data_for_prompt = full_data[:data_char_limit] if data_truncated else full_data

    prompt_template = ""
    
    # Load Global Prompt
    global_prompt = ""
    account_info = ""
    global_prompt_config = db.query(models.SystemConfig).filter(models.SystemConfig.key == "global_prompt").first()
    if global_prompt_config and global_prompt_config.value:
        try:
            # Try to render jinja2 template
            template = Template(global_prompt_config.value)
            global_prompt = template.render(symbol=db_stock.symbol, name=db_stock.name)
        except Exception as e:
            print(f"Error rendering global prompt: {e}")
            global_prompt = global_prompt_config.value

    # Load Account Info
    account_config = db.query(models.SystemConfig).filter(models.SystemConfig.key == "account_info").first()
    if account_config and account_config.value:
         account_info = account_config.value

    # Stock specific prompt overrides global prompt if exists? 
    # Or we can combine them. For now let's append stock specific prompt to analysis instructions.
    if db_stock.prompt_template:
        prompt_template = db_stock.prompt_template
    else:
        prompt_template = global_prompt

    system_prompt = (
        "你是一位拥有20年经验的资深量化基金经理，擅长短线博弈和趋势跟踪。"
        "你的任务是根据提供的股票实时数据和技术指标，给出当前时间点明确的、可执行的交易指令。"
        "\n\n"
        "【分析原则】\n"
        "1. 客观：只基于提供的数据说话，不要幻想未提供的新闻。\n"
        "2. 果断：必须给出明确的方向（买入/卖出/观望），禁止模棱两可。\n"
        "3. 风控：任何开仓建议必须包含止损位。\n"
        "\n\n"
        "【输出要求】\n"
        "请严格只输出一个合法的 JSON 对象，不要包含 Markdown 代码块标记（如 ```json），格式如下：\n"
        "{\n"
        "  \"type\": \"info\" | \"warning\" | \"error\",  // info=正常分析, warning=数据不足或风险极高, error=无法分析\n"
        "  \"signal\": \"STRONG_BUY\" | \"BUY\" | \"WAIT\" | \"SELL\" | \"STRONG_SELL\", // 明确的信号\n"
        "  \"action_advice\": \"...\", // 一句话的大白话操作建议，例如：'现价25.5元立即买入，目标27元'\n"
        "  \"suggested_position\": \"...\", // 建议仓位，例如：'3成仓' 或 '空仓观望'\n"
        "  \"duration\": \"...\", // 建议持仓时间，例如：'短线T+1' 或 '中线持股2周'\n"
        "  \"support_pressure\": {\"support\": 价格, \"pressure\": 价格}, // 支撑压力位\n"
        "  \"stop_loss_price\": 价格, // 严格的止损价格\n"
        "  \"message\": \"...\" // 详细的逻辑分析摘要，解释为什么这么做，不超过100字\n"
        "}"
    )

    import datetime
    current_time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    user_prompt = f"""
    Current Time: {current_time_str}
    Stock Symbol: {context.get('symbol', 'Unknown')}
    Stock Name: {context.get('name', '')}

    User Account Info:
    {(account_info or '').strip() or '-'}
    
    Task: Analyze the following market data and generate an investment decision JSON.
    
    Analysis Instructions (Strategy):
    {prompt_template}
    
    Real-time Indicators Data:
    {data_for_prompt}
    
    Remember: Be decisive. If the signal is strictly strictly strictly unclear, allow 'WAIT'. Otherwise, give a direction.
    Return strictly JSON format.
    """

    config_dict = {
        "api_key": ai_config.api_key, 
        "base_url": ai_config.base_url, 
        "model_name": ai_config.model_name,
        "temperature": getattr(ai_config, "temperature", 0.1)
    }
    ai_reply_str = ai_service.chat(user_prompt, config_dict, system_prompt=system_prompt)
    
    # Try to parse AI reply as JSON
    try:
        clean_reply = ai_reply_str.replace("```json", "").replace("```", "").strip()
        ai_reply = json.loads(clean_reply)
        if "signal" not in ai_reply:
            ai_reply["signal"] = "WAIT"
    except json.JSONDecodeError:
        ai_reply = {
            "type": "error",
            "message": "AI parsing failed: " + ai_reply_str,
            "signal": "WAIT"
        }

    return {
        "ok": True,
        "stock_id": db_stock.id,
        "stock_symbol": db_stock.symbol,
        "model_name": ai_config.model_name,
        "base_url": ai_config.base_url,
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        "ai_reply": ai_reply,
        "data_truncated": data_truncated,
        "data_char_limit": data_char_limit if data_truncated else None,
    }
