from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, Text, DateTime, JSON, Table
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
    
    # Configuration for this stock
    prompt_template = Column(Text, nullable=True) # Custom prompt for this stock
    ai_provider_id = Column(Integer, ForeignKey("ai_configs.id"), nullable=True)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    indicators = relationship("IndicatorDefinition", secondary=stock_indicators, back_populates="stocks")
    logs = relationship("Log", back_populates="stock", cascade="all, delete-orphan")
    ai_config = relationship("AIConfig")

class IndicatorDefinition(Base):
    __tablename__ = "indicator_definitions"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    akshare_api = Column(String)
    params_json = Column(String)

    stocks = relationship("Stock", secondary=stock_indicators, back_populates="indicators")

class AIConfig(Base):
    __tablename__ = "ai_configs"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True) # e.g. "SiliconFlow", "DeepSeek"
    provider = Column(String) # e.g. "openai" (compatible)
    base_url = Column(String)
    api_key = Column(String)
    model_name = Column(String)
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
