import sqlite3
import os

DB_PATH = "../stock_watch.db"

def add_column_if_not_exists(cursor, table, column, col_type="BOOLEAN", default=0):
    try:
        # Check if column exists
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [info[1] for info in cursor.fetchall()]
        if column not in columns:
            print(f"Adding column {column} to {table}...")
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type} DEFAULT {default}")
            print(f"Added {column} to {table}.")
        else:
            print(f"Column {column} already exists in {table}.")
    except Exception as e:
        print(f"Error adding column {column} to {table}: {e}")

def main():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    tables = [
        "stock_screeners",
        "rule_scripts",
        "research_scripts",
        "indicator_definitions"
    ]

    for table in tables:
        add_column_if_not_exists(cursor, table, "is_pinned", "BOOLEAN", 0)

    conn.commit()
    conn.close()
    print("Migration completed.")

if __name__ == "__main__":
    main()
