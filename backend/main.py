from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import stocks, ai_configs, logs, indicators
from services.monitor_service import start_scheduler

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

@app.on_event("startup")
def startup_event():
    start_scheduler()

@app.get("/")
def read_root():
    return {"message": "AI Stock Watcher API is running"}
