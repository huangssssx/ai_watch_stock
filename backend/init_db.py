import os
from database import engine, Base
from models import Stock, IndicatorDefinition, AIConfig, Log, KnowledgeBase

def init_db():
    db_path = os.path.join(os.path.dirname(__file__), "stock_watch.db")
    if os.path.exists(db_path):
        os.remove(db_path)
    Base.metadata.create_all(bind=engine)
    print("Database tables created.")

if __name__ == "__main__":
    init_db()
