import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

def _pick_sqlite_db_path() -> str:
    backend_dir = os.path.dirname(os.path.abspath(__file__))
    cwd_path = os.path.join(os.getcwd(), "stock_watch.db")
    backend_path = os.path.join(backend_dir, "stock_watch.db")

    candidates = []
    if os.path.exists(cwd_path):
        candidates.append(cwd_path)
    if os.path.exists(backend_path) and backend_path != cwd_path:
        candidates.append(backend_path)

    if len(candidates) == 0:
        return backend_path
    if len(candidates) == 1:
        return candidates[0]

    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0]

SQLALCHEMY_DATABASE_URL = f"sqlite:///{_pick_sqlite_db_path()}"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
