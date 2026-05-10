import sqlite3
import os

db_path = "turni.db"
if not os.path.exists(db_path):
    print("Database not found")
else:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Check if table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='team_operatori'")
    if not cursor.fetchone():
        print("Table team_operatori not found")
    else:
        cursor.execute("SELECT id, nome, username FROM team_operatori")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    conn.close()
