from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, Text, DateTime, JSON, Table, Float
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base

stock_indicators = Table(
    "stock_indicators",
    Base.metadata,
    Column("stock_id", Integer, ForeignKey("stocks.id"), primary_key=True),
    Column("indicator_id", Integer, ForeignKey("indicator_definitions.id"), primary_key=True),
)

class Stock(Base):
    __tablename__ = "stocks"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, unique=True, index=True)
    name = Column(String, default="")
    is_monitoring = Column(Boolean, default=False)
    interval_seconds = Column(Integer, default=300)  # Default 5 minutes
    monitoring_schedule = Column(Text, nullable=True) # JSON list of {start: "HH:MM", end: "HH:MM"}
    only_trade_days = Column(Boolean, default=True) # Only monitor on trading days
    
    # Configuration for this stock
    prompt_template = Column(Text, nullable=True) # Custom prompt for this stock
    remark = Column(Text, nullable=True) # User remarks/notes
    ai_provider_id = Column(Integer, ForeignKey("ai_configs.id"), nullable=True)
    
    # Monitoring Strategy
    monitoring_mode = Column(String, default="ai_only") # ai_only, script_only, hybrid
    rule_script_id = Column(Integer, ForeignKey("rule_scripts.id"), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    indicators = relationship("IndicatorDefinition", secondary=stock_indicators, back_populates="stocks")
    logs = relationship("Log", back_populates="stock", cascade="all, delete-orphan")
    ai_config = relationship("AIConfig")
    rule_script = relationship("RuleScript")
    ai_watch_config = relationship("StockAIWatchConfig", uselist=False, back_populates="stock")

class StockAIWatchConfig(Base):
    __tablename__ = "stock_ai_watch_configs"

    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"), unique=True, index=True)
    indicator_ids = Column(Text, default="[]") # JSON list of IDs
    custom_prompt = Column(Text, default="")
    ai_provider_id = Column(Integer, ForeignKey("ai_configs.id"), nullable=True)
    analysis_history = Column(Text, default="[]") # JSON list of last 3 results
    
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

    stock = relationship("Stock", back_populates="ai_watch_config")

class RuleScript(Base):
    __tablename__ = "rule_scripts"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    description = Column(String, nullable=True)
    code = Column(Text, default="") # Python code
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

class IndicatorDefinition(Base):
    __tablename__ = "indicator_definitions"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    akshare_api = Column(String)
    params_json = Column(String)
    post_process_json = Column(Text, nullable=True)
    python_code = Column(Text, nullable=True)

    stocks = relationship("Stock", secondary=stock_indicators, back_populates="indicators")

class AIConfig(Base):
    __tablename__ = "ai_configs"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True) # e.g. "SiliconFlow", "DeepSeek"
    provider = Column(String) # e.g. "openai" (compatible)
    base_url = Column(String)
    api_key = Column(String)
    model_name = Column(String)
    temperature = Column(Float, default=0.1)
    max_tokens = Column(Integer, default=100000) # Max tokens/chars context limit
    
    is_active = Column(Boolean, default=True)

class Log(Base):
    __tablename__ = "logs"

    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"))
    timestamp = Column(DateTime(timezone=True), server_default=func.now())
    
    raw_data = Column(Text) # JSON/Text of fetched indicators
    ai_response = Column(Text) # The raw response from AI
    ai_analysis = Column(JSON) # Parsed JSON: {type, message}
    is_alert = Column(Boolean, default=False)
    
    stock = relationship("Stock", back_populates="logs")

class KnowledgeBase(Base):
    __tablename__ = "knowledge_base"

    id = Column(Integer, primary_key=True, index=True)
    content = Column(Text)
    tags = Column(String) # Comma separated tags
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class SystemConfig(Base):
    __tablename__ = "system_configs"

    key = Column(String, primary_key=True, index=True) # e.g., "email_config", "global_prompt"
    value = Column(Text) # JSON string or plain text
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

class StockScreener(Base):
    __tablename__ = "stock_screeners"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    description = Column(String, nullable=True)
    script_content = Column(Text, default="")
    cron_expression = Column(String, nullable=True)  # e.g., "0 15 * * *"
    is_active = Column(Boolean, default=False)
    
    last_run_at = Column(DateTime(timezone=True), nullable=True)
    last_run_status = Column(String, default="pending") # pending, success, failed
    last_run_log = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    results = relationship("ScreenerResult", back_populates="screener", cascade="all, delete-orphan")

class ScreenerResult(Base):
    __tablename__ = "screener_results"

    id = Column(Integer, primary_key=True, index=True)
    screener_id = Column(Integer, ForeignKey("stock_screeners.id"))
    run_at = Column(DateTime(timezone=True), server_default=func.now())
    result_json = Column(Text) # JSON list of dicts
    count = Column(Integer, default=0)

    screener = relationship("StockScreener", back_populates="results")

class ResearchScript(Base):
    __tablename__ = "research_scripts"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, unique=True, index=True)
    description = Column(String, nullable=True)
    script_content = Column(Text, default="")
    
    last_run_at = Column(DateTime(timezone=True), nullable=True)
    last_run_status = Column(String, default="pending") # pending, success, failed
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

class StockNews(Base):
    __tablename__ = "stock_news"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, index=True)
    content = Column(Text)
    source = Column(String, nullable=True) # e.g. "CLS", "EastMoney"
    publish_time = Column(DateTime(timezone=True), nullable=True)
    url = Column(String, nullable=True)
    
    # Optional: if we want to associate with specific stocks (many-to-many is better but string is simpler for now)
    related_stock_codes = Column(String, nullable=True) 

    created_at = Column(DateTime(timezone=True), server_default=func.now())

class SentimentAnalysis(Base):
    __tablename__ = "sentiment_analysis"

    id = Column(Integer, primary_key=True, index=True)
    
    # Target: "market" (general), "sector" (specific sector), "stock" (specific stock)
    target_type = Column(String, default="market", index=True)
    target_value = Column(String, default="global") # e.g., "semiconductor", "600519"

    timestamp = Column(DateTime(timezone=True), server_default=func.now())
    
    # AI Results
    sentiment_score = Column(Float, default=0.0) # -1.0 to 1.0
    policy_orientation = Column(Text, nullable=True) # Description of policy trend
    trading_signal = Column(String, default="WAIT") # BUY, SELL, WAIT
    summary = Column(Text, nullable=True)
    raw_response = Column(Text, nullable=True)

    # Config used
    ai_provider_id = Column(Integer, ForeignKey("ai_configs.id"), nullable=True)
