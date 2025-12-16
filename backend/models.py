from sqlalchemy import Column, Integer, String, Text, DateTime, JSON
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
