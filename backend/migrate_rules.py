from sqlalchemy import create_engine, text
from database import SQLALCHEMY_DATABASE_URL

def migrate():
    engine = create_engine(SQLALCHEMY_DATABASE_URL)
    with engine.connect() as conn:
        # 1. Create rule_scripts table
        print("Creating rule_scripts table...")
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS rule_scripts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR,
            description VARCHAR,
            code TEXT,
            created_at DATETIME DEFAULT (CURRENT_TIMESTAMP),
            updated_at DATETIME
        );
        """))
        
        # Create index on name
        try:
            conn.execute(text("CREATE UNIQUE INDEX ix_rule_scripts_name ON rule_scripts (name)"))
        except Exception as e:
            print(f"Index might already exist: {e}")

        # 2. Add columns to stocks table
        # SQLite does not support IF NOT EXISTS for columns, so we try and catch
        print("Adding columns to stocks table...")
        try:
            conn.execute(text("ALTER TABLE stocks ADD COLUMN monitoring_mode VARCHAR DEFAULT 'ai_only'"))
            print("Added monitoring_mode column")
        except Exception as e:
            print(f"monitoring_mode column might already exist: {e}")
            
        try:
            conn.execute(text("ALTER TABLE stocks ADD COLUMN rule_script_id INTEGER REFERENCES rule_scripts(id)"))
            print("Added rule_script_id column")
        except Exception as e:
            print(f"rule_script_id column might already exist: {e}")

        conn.commit()
        print("Migration complete.")

if __name__ == "__main__":
    migrate()
