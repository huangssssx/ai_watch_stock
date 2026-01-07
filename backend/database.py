import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

def _pick_sqlite_db_path() -> str:
    backend_dir = os.path.dirname(os.path.abspath(__file__))
    backend_path = os.path.join(backend_dir, "stock_watch.db")

    if not os.path.exists(backend_path):
        raise RuntimeError(
            f"DB 文件不存在：{backend_path}。"
            "为避免隐式新建导致数据丢失，已中止启动。"
            "请先恢复/放置原始 backend/stock_watch.db。"
        )

    return backend_path

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
