import sqlite3
import os

db_path = "turni.db"
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

print("--- TURNI OPERATORE 2 ---")
cursor.execute("SELECT * FROM team_turni WHERE operatore_id=2 AND data LIKE '2026-05-%'")
rows = cursor.fetchall()
for r in rows:
    print(dict(r))

conn.close()
