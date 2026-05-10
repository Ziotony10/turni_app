import sqlite3

def run():
    conn = sqlite3.connect("turni.db")
    cursor = conn.cursor()
    
    try:
        cursor.execute("ALTER TABLE team_swap_requests ADD COLUMN from_turno TEXT DEFAULT ''")
        print("Aggiunta colonna from_turno")
    except sqlite3.OperationalError as e:
        print(f"from_turno: {e}")
        
    try:
        cursor.execute("ALTER TABLE team_swap_requests ADD COLUMN to_turno TEXT DEFAULT ''")
        print("Aggiunta colonna to_turno")
    except sqlite3.OperationalError as e:
        print(f"to_turno: {e}")
        
    conn.commit()
    conn.close()

if __name__ == '__main__':
    run()
