from sqlalchemy import Column, Integer, String, Text, DateTime, JSON, Boolean
from backend.database import Base
from datetime import datetime

class Strategy(Base):
    __tablename__ = "strategies"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100))
    symbol = Column(String(20))
    content = Column(JSON)  # The Strategy DSL
    status = Column(String(20), default="stopped") # stopped, running
    created_at = Column(DateTime, default=datetime.utcnow)

class AlertLog(Base):
    __tablename__ = "alert_logs"

    id = Column(Integer, primary_key=True, index=True)
    strategy_id = Column(Integer)
    timestamp = Column(DateTime, default=datetime.utcnow)
    message = Column(Text)
    level = Column(String(20), default="INFO")

class AlertRule(Base):
    __tablename__ = "alert_rules"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100))
    symbol = Column(String(20))
    period = Column(String(5), default="1")
    provider = Column(String(10), default="em")
    condition = Column(Text)
    message = Column(Text)
    level = Column(String(20), default="WARNING")
    enabled = Column(Boolean, default=True)
    last_checked_at = Column(DateTime)
    last_triggered_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
