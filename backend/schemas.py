from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime

try:
    from pydantic import ConfigDict
except Exception:
    ConfigDict = None

class ORMModel(BaseModel):
    if ConfigDict is not None:
        model_config = ConfigDict(from_attributes=True)
    else:
        class Config:
            orm_mode = True

# AI Config
class AIConfigBase(BaseModel):
    name: str
    provider: str
    base_url: str
    api_key: str
    model_name: str
    temperature: Optional[float] = 0.1
    max_tokens: Optional[int] = 100000
    is_active: bool = True

class AIConfigCreate(AIConfigBase):
    pass

class AIConfigUpdate(BaseModel):
    name: Optional[str] = None
    provider: Optional[str] = None
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    model_name: Optional[str] = None
    max_tokens: Optional[int] = None
    is_active: Optional[bool] = None

class AIConfig(AIConfigBase, ORMModel):
    id: int

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
    post_process_json: Optional[str] = None

class IndicatorDefinitionCreate(IndicatorDefinitionBase):
    pass

class IndicatorDefinitionUpdate(BaseModel):
    name: Optional[str] = None
    akshare_api: Optional[str] = None
    params_json: Optional[str] = None
    post_process_json: Optional[str] = None

class IndicatorDefinition(IndicatorDefinitionBase, ORMModel):
    id: int

# Stock
class StockBase(BaseModel):
    symbol: str
    name: str
    interval_seconds: int = 300
    monitoring_schedule: Optional[str] = None
    prompt_template: Optional[str] = None
    ai_provider_id: Optional[int] = None
    only_trade_days: Optional[bool] = True

class StockCreate(StockBase):
    indicator_ids: Optional[List[int]] = None

class StockUpdate(BaseModel):
    name: Optional[str] = None
    interval_seconds: Optional[int] = None
    is_monitoring: Optional[bool] = None
    monitoring_schedule: Optional[str] = None
    prompt_template: Optional[str] = None
    ai_provider_id: Optional[int] = None
    indicator_ids: Optional[List[int]] = None
    only_trade_days: Optional[bool] = None

class Stock(StockBase, ORMModel):
    id: int
    is_monitoring: bool
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    indicators: List[IndicatorDefinition] = []

class StockTestRunResponse(BaseModel):
    ok: bool
    stock_id: int
    stock_symbol: str
    model_name: str
    base_url: str
    system_prompt: str
    user_prompt: str
    ai_reply: Dict[str, Any]  # Changed from str to Dict
    data_truncated: bool
    data_char_limit: Optional[int] = None

# Log
class LogBase(BaseModel):
    stock_id: int
    raw_data: str
    ai_response: str
    ai_analysis: Dict[str, Any]
    is_alert: bool

class Log(LogBase, ORMModel):
    id: int
    timestamp: datetime
    stock: Optional[Stock] = None

# System Config
class EmailConfig(BaseModel):
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    sender_email: str = ""
    sender_password: str = ""
    receiver_email: str = ""

class GlobalPromptConfig(BaseModel):
    prompt_template: str = ""
    account_info: str = ""

class AlertRateLimitConfig(BaseModel):
    enabled: bool = False
    max_per_hour_per_stock: int = 0
    allowed_signals: List[str] = ["STRONG_BUY", "BUY", "SELL", "STRONG_SELL"]
    allowed_urgencies: List[str] = ["紧急", "一般", "不紧急"]
    suppress_duplicates: bool = True
    bypass_rate_limit_for_strong_signals: bool = True

class SystemConfigBase(BaseModel):
    key: str
    value: str

class SystemConfig(SystemConfigBase, ORMModel):
    updated_at: Optional[datetime]
