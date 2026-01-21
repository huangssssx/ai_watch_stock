from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
import pydantic

try:
    from pydantic import ConfigDict
except Exception:
    ConfigDict = None

class ORMModel(BaseModel):
    if str(getattr(pydantic, "__version__", "")).startswith("2") and ConfigDict is not None:
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

class AIConfig(ORMModel, AIConfigBase):
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
    akshare_api: Optional[str] = None
    params_json: Optional[str] = None
    post_process_json: Optional[str] = None
    python_code: Optional[str] = None
    is_pinned: bool = False

class IndicatorDefinitionCreate(IndicatorDefinitionBase):
    pass

class IndicatorDefinitionUpdate(BaseModel):
    name: Optional[str] = None
    akshare_api: Optional[str] = None
    params_json: Optional[str] = None
    post_process_json: Optional[str] = None
    python_code: Optional[str] = None
    is_pinned: Optional[bool] = None

class IndicatorDefinition(ORMModel, IndicatorDefinitionBase):
    id: int

class IndicatorTestRequest(BaseModel):
    symbol: str
    name: Optional[str] = None

class IndicatorTestResponse(BaseModel):
    ok: bool
    indicator_id: int
    indicator_name: str
    symbol: str
    raw: str
    parsed: Optional[Any] = None
    error: Optional[str] = None

# Stock
class StockBase(BaseModel):
    symbol: str
    name: str
    interval_seconds: int = 300
    monitoring_schedule: Optional[str] = None
    prompt_template: Optional[str] = None
    remark: Optional[str] = None
    ai_provider_id: Optional[int] = None
    only_trade_days: Optional[bool] = True
    monitoring_mode: Optional[str] = "ai_only"
    rule_script_id: Optional[int] = None
    is_pinned: bool = False

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
    monitoring_mode: Optional[str] = None
    rule_script_id: Optional[int] = None
    remark: Optional[str] = None
    is_pinned: Optional[bool] = None

class Stock(ORMModel, StockBase):
    id: int
    is_monitoring: bool
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    indicators: List[IndicatorDefinition] = []

class StockTestRunResponse(BaseModel):
    ok: bool
    run_id: Optional[str] = None
    stock_id: int
    stock_symbol: str
    monitoring_mode: Optional[str] = None
    skipped_reason: Optional[str] = None
    error: Optional[str] = None

    script_triggered: Optional[bool] = None
    script_message: Optional[str] = None
    script_log: Optional[str] = None

    model_name: Optional[str] = None
    base_url: Optional[str] = None
    system_prompt: Optional[str] = None
    user_prompt: Optional[str] = None
    ai_reply: Optional[Dict[str, Any]] = None
    raw_response: Optional[str] = None

    data_truncated: Optional[bool] = None
    data_char_limit: Optional[int] = None

    fetch_ok: Optional[int] = None
    fetch_error: Optional[int] = None
    fetch_errors: Optional[List[Dict[str, Any]]] = None
    ai_duration_ms: Optional[int] = None

    is_alert: Optional[bool] = None
    alert_attempted: Optional[bool] = None
    alert_suppressed: Optional[bool] = None
    alert_result: Optional[Any] = None
    prompt_source: Optional[str] = None

# Log
class LogBase(BaseModel):
    stock_id: int
    raw_data: str
    ai_response: str
    ai_analysis: Dict[str, Any]
    is_alert: bool

class Log(ORMModel, LogBase):
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
    bypass_rate_limit_for_strong_signals: bool = True
    if str(getattr(pydantic, "__version__", "")).startswith("2") and ConfigDict is not None:
        model_config = ConfigDict(extra="ignore")
    else:
        class Config:
            extra = "ignore"

class SystemConfigBase(BaseModel):
    key: str
    value: str

class SystemConfig(ORMModel, SystemConfigBase):
    updated_at: Optional[datetime]

# Research Script
class ResearchScriptBase(BaseModel):
    title: str
    description: Optional[str] = None
    script_content: str
    is_pinned: bool = False

class ResearchScriptCreate(ResearchScriptBase):
    pass

class ResearchScriptUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    script_content: Optional[str] = None
    is_pinned: Optional[bool] = None

class ResearchScript(ORMModel, ResearchScriptBase):
    id: int
    last_run_at: Optional[datetime] = None
    last_run_status: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

class ResearchRunRequest(BaseModel):
    script_content: str

class ResearchRunResponse(BaseModel):
    success: bool
    log: str
    result: Optional[List[Dict[str, Any]]] = None # For table
    chart: Optional[Dict[str, Any]] = None # For chart
    error: Optional[str] = None

# Rule Script
class RuleScriptBase(BaseModel):
    name: str
    description: Optional[str] = None
    code: str
    is_pinned: bool = False

class RuleScriptCreate(RuleScriptBase):
    pass

class RuleScriptUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    code: Optional[str] = None
    is_pinned: Optional[bool] = None

class RuleScript(ORMModel, RuleScriptBase):
    id: int
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

class RuleTestPayload(BaseModel):
    symbol: str

class RuleTestResponse(BaseModel):
    triggered: bool
    message: str
    log: str
    signal: Optional[str] = None

# AI Watch Config
class StockAIWatchConfigBase(BaseModel):
    indicator_ids: str = "[]"
    custom_prompt: Optional[str] = ""
    ai_provider_id: Optional[int] = None

class StockAIWatchConfig(ORMModel, StockAIWatchConfigBase):
    id: int
    stock_id: int
    analysis_history: str = "[]"
    updated_at: Optional[datetime]

class AIWatchAnalyzeRequest(BaseModel):
    indicator_ids: List[int]
    custom_prompt: str
    ai_provider_id: Optional[int] = None

# Stock News
class StockNewsBase(BaseModel):
    title: str
    content: str
    source: Optional[str] = None
    publish_time: Optional[datetime] = None
    url: Optional[str] = None
    related_stock_codes: Optional[str] = None

class StockNews(ORMModel, StockNewsBase):
    id: int
    created_at: Optional[datetime] = None

# Sentiment Analysis
class SentimentAnalysisBase(BaseModel):
    target_type: str = "market"
    target_value: str = "global"
    sentiment_score: float = 0.0
    policy_orientation: Optional[str] = None
    trading_signal: str = "WAIT"
    summary: Optional[str] = None
    raw_response: Optional[str] = None
    ai_provider_id: Optional[int] = None

class SentimentAnalysis(ORMModel, SentimentAnalysisBase):
    id: int
    timestamp: Optional[datetime] = None
