import sqlite3
import os

db_path = "turni.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute("PRAGMA table_info(team_operatori)")
rows = cursor.fetchall()
for row in rows:
    print(row)
conn.close()
