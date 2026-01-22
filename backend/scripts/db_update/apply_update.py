import sqlite3
import os

DB_PATH = "backend/stock_watch.db"
SCRIPT_DIR = "backend/scripts/db_update"

MAPPING = {
    5: "valley_sniper_safe.py",
    6: "chase_heat_safe.py",
    7: "wash_markup_safe.py"
}

def update_db():
    if not os.path.exists(DB_PATH):
        print(f"❌ Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        for script_id, filename in MAPPING.items():
            file_path = os.path.join(SCRIPT_DIR, filename)
            if not os.path.exists(file_path):
                print(f"⚠️ Script file not found: {file_path}, skipping ID {script_id}")
                continue

            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            cursor.execute(
                "UPDATE stock_screeners SET script_content = ? WHERE id = ?",
                (content, script_id)
            )
            
            if cursor.rowcount > 0:
                print(f"✅ Updated script ID {script_id} ({filename})")
            else:
                print(f"⚠️ No row found for script ID {script_id}")

        conn.commit()
        print("🎉 All updates applied successfully!")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error updating database: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    update_db()
