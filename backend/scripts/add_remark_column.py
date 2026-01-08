import sqlite3
import os

def add_column():
    backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(backend_dir, "stock_watch.db")
    
    if not os.path.exists(db_path):
        print(f"Database not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute("ALTER TABLE stocks ADD COLUMN remark TEXT")
        conn.commit()
        print("Successfully added 'remark' column to 'stocks' table.")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower() or "no such table" in str(e).lower():
            print(f"Operation skipped: {e}")
        else:
            print(f"Error adding column: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    add_column()
