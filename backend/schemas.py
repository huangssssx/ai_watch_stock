from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime

# AI Config
class AIConfigBase(BaseModel):
    name: str
    provider: str
    base_url: str
    api_key: str
    model_name: str
    is_active: bool = True

class AIConfigCreate(AIConfigBase):
    pass

class AIConfig(AIConfigBase):
    id: int
    class Config:
        orm_mode = True

class AIConfigTestRequest(BaseModel):
    prompt_template: Optional[str] = None
    data_context: Optional[str] = None

class AIConfigTestResponse(BaseModel):
    ok: bool
    parsed: Dict[str, Any]
    raw: str

# Indicator Definition
class IndicatorDefinitionBase(BaseModel):
    name: str
    akshare_api: str
    params_json: str

class IndicatorDefinitionCreate(IndicatorDefinitionBase):
    pass

class IndicatorDefinition(IndicatorDefinitionBase):
    id: int
    class Config:
        orm_mode = True

# Stock
class StockBase(BaseModel):
    symbol: str
    name: str
    interval_seconds: int = 300
    prompt_template: Optional[str] = None
    ai_provider_id: Optional[int] = None

class StockCreate(StockBase):
    indicator_ids: Optional[List[int]] = None

class StockUpdate(BaseModel):
    name: Optional[str] = None
    interval_seconds: Optional[int] = None
    is_monitoring: Optional[bool] = None
    prompt_template: Optional[str] = None
    ai_provider_id: Optional[int] = None
    indicator_ids: Optional[List[int]] = None

class Stock(StockBase):
    id: int
    is_monitoring: bool
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    indicators: List[IndicatorDefinition] = []
    
    class Config:
        orm_mode = True

# Log
class LogBase(BaseModel):
    stock_id: int
    raw_data: str
    ai_response: str
    ai_analysis: Dict[str, Any]
    is_alert: bool

class Log(LogBase):
    id: int
    timestamp: datetime
    class Config:
        orm_mode = True
