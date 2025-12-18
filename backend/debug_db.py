
from database import SessionLocal
from models import Stock, Log, AIConfig

db = SessionLocal()

print("--- Stocks ---")
stocks = db.query(Stock).all()
for s in stocks:
    print(f"ID: {s.id}, Symbol: {s.symbol}, Monitoring: {s.is_monitoring}, Interval: {s.interval_seconds}, AI Provider: {s.ai_provider_id}")

print("\n--- AI Configs ---")
configs = db.query(AIConfig).all()
for c in configs:
    print(f"ID: {c.id}, Name: {c.name}")

print("\n--- Logs (Last 5) ---")
logs = db.query(Log).order_by(Log.timestamp.desc()).limit(5).all()
for l in logs:
    print(f"Time: {l.timestamp}, Stock: {l.stock_id}, Alert: {l.is_alert}, Message: {l.ai_analysis.get('message') if l.ai_analysis else 'None'}")

db.close()
