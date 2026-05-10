import sqlite3
import os

db_path = "turni.db"
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

print("--- UTENTI ---")
cursor.execute("SELECT id, username, nome FROM utenti")
users = cursor.fetchall()
for u in users:
    print(dict(u))

print("\n--- OPERATORI ---")
cursor.execute("SELECT id, nome, posizione, linked_user_id FROM team_operatori")
ops = cursor.fetchall()
for o in ops:
    print(dict(o))

conn.close()
