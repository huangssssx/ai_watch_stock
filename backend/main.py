from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import sys
from sqlalchemy import inspect, text

_backend_dir = os.path.dirname(os.path.abspath(__file__))
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)

from routers import stocks, ai_configs, logs, indicators, settings, screeners, research, rules, news
from services.monitor_service import start_scheduler
from services.screener_service import restore_screener_jobs
from services.streamlit_service import start_streamlit, stop_streamlit
from database import Base, engine
import models

app = FastAPI(title="AI Stock Watcher")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(stocks.router)
app.include_router(ai_configs.router)
app.include_router(logs.router)
app.include_router(indicators.router)
app.include_router(settings.router)
app.include_router(screeners.router)
app.include_router(research.router)
app.include_router(rules.router)
app.include_router(news.router)

def ensure_db_schema():
    Base.metadata.create_all(bind=engine)
    inspector = inspect(engine)
    if "indicator_definitions" in inspector.get_table_names():
        cols = {c["name"] for c in inspector.get_columns("indicator_definitions")}
        if "post_process_json" not in cols:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE indicator_definitions ADD COLUMN post_process_json TEXT"))

    if "ai_configs" in inspector.get_table_names():
        cols = {c["name"] for c in inspector.get_columns("ai_configs")}
        if "temperature" not in cols:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE ai_configs ADD COLUMN temperature FLOAT DEFAULT 0.1"))

    if "stocks" in inspector.get_table_names():
        cols = {c["name"] for c in inspector.get_columns("stocks")}
        if "only_trade_days" not in cols:
            with engine.begin() as conn:
                # Add column, default True (1 in SQLite/MySQL usually works with BOOLEAN)
                conn.execute(text("ALTER TABLE stocks ADD COLUMN only_trade_days BOOLEAN DEFAULT 1"))

@app.on_event("startup")
def startup_event():
    ensure_db_schema()
    start_scheduler()
    restore_screener_jobs()
    start_streamlit()

@app.on_event("shutdown")
def shutdown_event():
    stop_streamlit()

@app.get("/")
def read_root():
    return {"message": "AI Stock Watcher API is running"}
