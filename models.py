from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean
from database import Base
from datetime import datetime

class AlertRule(Base):
    __tablename__ = "alert_rules"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100))
    symbol = Column(String(20))
    stock_name = Column(String(100), nullable=True)
    period = Column(String(5), default="1")
    provider = Column(String(10), default="em")
    condition = Column(Text)
    message = Column(Text)
    level = Column(String(20), default="WARNING")
    enabled = Column(Boolean, default=True)
    last_checked_at = Column(DateTime)
    last_triggered_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)

class AlertEvent(Base):
    __tablename__ = "alert_events"

    id = Column(Integer, primary_key=True, index=True)
    rule_id = Column(Integer)
    symbol = Column(String(20))
    message = Column(Text)
    level = Column(String(20), default="WARNING")
    timestamp = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)

class AlertNotification(Base):
    __tablename__ = "alert_notifications"

    id = Column(Integer, primary_key=True, index=True)
    rule_id = Column(Integer)
    message = Column(Text)
    level = Column(String(20), default="WARNING")
    triggered_at = Column(DateTime, default=datetime.utcnow)
    last_notified_at = Column(DateTime, nullable=True)
    is_cleared = Column(Boolean, default=False)

class IndicatorConfig(Base):
    __tablename__ = "indicator_configs"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100))
    api_name = Column(String(100))
    params = Column(Text)  # JSON string
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class IndicatorCollection(Base):
    __tablename__ = "indicator_collections"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100))
    description = Column(Text, nullable=True)
    indicator_ids = Column(Text)  # JSON list of integers, e.g. "[1, 2, 5]"
    last_run_params = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
