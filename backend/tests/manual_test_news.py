import sys
import os
import datetime

# Add backend to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import SessionLocal, engine, Base
from services.news_service import news_service
from models import StockNews, SentimentAnalysis, AIConfig

def test_news_module():
    print("Initializing DB...")
    Base.metadata.create_all(bind=engine)
    
    db = SessionLocal()
    try:
        # 1. Test Fetch
        print("\n--- Testing News Fetch ---")
        count = news_service.fetch_market_news(db, limit=5)
        print(f"Fetched {count} news items.")
        
        latest_news = db.query(StockNews).order_by(StockNews.publish_time.desc()).limit(3).all()
        for n in latest_news:
            print(f"[{n.publish_time}] {n.title} (Source: {n.source})")
        
        if count == 0 and len(latest_news) == 0:
            print("Warning: No news fetched. This might be due to akshare API issues or network.")
        
        # 2. Test Analysis (Only if AI config exists)
        print("\n--- Testing Sentiment Analysis ---")
        ai_config = db.query(AIConfig).filter(AIConfig.is_active == True).first()
        if ai_config:
            print(f"Found Active AI Config: {ai_config.name} ({ai_config.model_name})")
            print("Running analysis (this calls the LLM)...")
            
            result = news_service.analyze_sentiment(db, ai_config.id)
            if result.get("ok"):
                data = result["data"]
                print("Analysis Success:")
                print(f"Sentiment Score: {data.get('sentiment_score')}")
                print(f"Signal: {data.get('trading_signal')}")
                print(f"Policy: {data.get('policy_orientation')}")
            else:
                print(f"Analysis Failed: {result.get('error')}")
        else:
            print("No active AI config found. Skipping analysis test.")
            
    finally:
        db.close()

if __name__ == "__main__":
    test_news_module()
