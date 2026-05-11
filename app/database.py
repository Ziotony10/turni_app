import sqlite3
import os
import time
from fastapi import HTTPException

from app.config import (
    USE_PG, DATABASE_URL, DB_PATH,
    SQLITE_BUSY_TIMEOUT_MS, SQLITE_LOG_BUSY_TIMEOUT_MS,
    PG_CONNECT_TIMEOUT_SEC, PG_STATEMENT_TIMEOUT_MS, PG_LOCK_TIMEOUT_MS, PG_IDLE_IN_TX_TIMEOUT_MS,
    INITIAL_ADMIN_USERNAME, INITIAL_ADMIN_PASSWORD, INITIAL_ADMIN_NAME
)

if USE_PG:
    import psycopg2
    import psycopg2.extras
    import psycopg2.pool

# ─── Connection Pool (PostgreSQL) ──────────────────────────────────────────────
_pg_pool = None

def get_pg_pool():
    global _pg_pool
    if _pg_pool is None and USE_PG:
        _pg_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=12,  # Supabase free supporta fino a 15 connessioni dirette, 200 via pooler 6543
            dsn=DATABASE_URL,
            connect_timeout=PG_CONNECT_TIMEOUT_SEC,
            application_name="turni_app",
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=3,
            options=(
                f"-c statement_timeout={PG_STATEMENT_TIMEOUT_MS} "
                f"-c lock_timeout={PG_LOCK_TIMEOUT_MS} "
                f"-c idle_in_transaction_session_timeout={PG_IDLE_IN_TX_TIMEOUT_MS}"
            ),
        )
    return _pg_pool

# ─── DB helpers ────────────────────────────────────────────────────────────────
def _open_sqlite_connection(timeout_ms: int):
    conn = sqlite3.connect(DB_PATH, timeout=max(timeout_ms / 1000, 0.1))
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {timeout_ms}")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def get_db():
    if USE_PG:
        pool = get_pg_pool()
        for attempt in range(10):
            try:
                conn = pool.getconn()
                conn.cursor_factory = psycopg2.extras.RealDictCursor
                try:
                    conn.rollback()                 # pulisce eventuale stato residuo
                    conn.cursor().execute("SELECT 1")
                except Exception:
                    try:
                        pool.putconn(conn, close=True)
                    except Exception:
                        pass
                    time.sleep(0.1)
                    continue
                return conn
            except psycopg2.pool.PoolError:
                if attempt < 9:
                    time.sleep(0.3)
                else:
                    raise HTTPException(status_code=503, detail="Server occupato, riprova tra un momento")
    return _open_sqlite_connection(SQLITE_BUSY_TIMEOUT_MS)

def release_db(conn, discard: bool = False):
    """Rilascia la connessione al pool (PG) o la chiude (SQLite)."""
    if USE_PG:
        if getattr(conn, "_pool_returned", False):
            return
        try:
            is_closed = getattr(conn, "closed", 1) != 0
            if not discard and not is_closed:
                try:
                    conn.rollback()
                except Exception:
                    discard = True
            get_pg_pool().putconn(conn, close=(discard or is_closed))
        except Exception:
            pass
    else:
        conn.close()

def q(sql):
    if not USE_PG:
        return sql
    sql = sql.replace("?", "%s")
    sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
    sql = sql.replace("DEFAULT CURRENT_TIMESTAMP", "DEFAULT NOW()")
    return sql

def ex(conn, sql, params=()):
    if USE_PG:
        try:
            cur = conn.cursor()
            cur.execute(q(sql), params)
            return cur
        except (psycopg2.DatabaseError, psycopg2.InterfaceError):
            try:
                get_pg_pool().putconn(conn, close=True)
                conn._pool_returned = True
            except Exception:
                pass
            raise HTTPException(503, "Connessione database temporaneamente non disponibile, riprova tra un momento")
    return conn.execute(q(sql), params)

def fetchall(conn, sql, params=()):
    cur = ex(conn, sql, params)
    rows = cur.fetchall()
    return [dict(r) for r in rows]

def fetchone(conn, sql, params=()):
    cur = ex(conn, sql, params)
    row = cur.fetchone()
    return dict(row) if row else None

def get_limit_placeholder():
    return "%s" if USE_PG else "?"

# ─── Init DB ───────────────────────────────────────────────────────────────────
def init_db():
    conn = get_db()
    try:
        if not USE_PG:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
        # ── Core tables ────────────────────────────────────────────────────────
        ex(conn, """CREATE TABLE IF NOT EXISTS utenti (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            nome TEXT,
            password_hash TEXT NOT NULL,
            is_admin INTEGER DEFAULT 0,
            is_editor INTEGER DEFAULT 0,
            is_team_editor INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

        ex(conn, """CREATE TABLE IF NOT EXISTS turni (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            data TEXT NOT NULL,
            turno TEXT, ora_inizio TEXT, ora_fine TEXT,
            ore_diurne REAL DEFAULT 0, ore_notturne REAL DEFAULT 0,
            strao_diurno REAL DEFAULT 0, strao_notturno REAL DEFAULT 0,
            strao_fest_diurno REAL DEFAULT 0, strao_fest_notturno REAL DEFAULT 0,
            reperibilita TEXT, note TEXT,
            UNIQUE(user_id, data))""")

        ex(conn, """CREATE TABLE IF NOT EXISTS impostazioni (
            user_id INTEGER NOT NULL,
            chiave TEXT NOT NULL,
            valore TEXT,
            PRIMARY KEY (user_id, chiave))""")

        ex(conn, """CREATE TABLE IF NOT EXISTS tabelle_turni (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            tipo TEXT NOT NULL,
            num_settimane INTEGER NOT NULL,
            turni_json TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

        # ── Login / accessi logs ────────────────────────────────────────────────
        ex(conn, """CREATE TABLE IF NOT EXISTS log_accessi (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT,
            esito TEXT,
            ip TEXT,
            user_agent TEXT,
            timestamp TEXT DEFAULT CURRENT_TIMESTAMP)""")

        ex(conn, """CREATE TABLE IF NOT EXISTS login_page_visits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ip_address TEXT,
            user_agent TEXT,
            referrer TEXT,
            is_bot INTEGER DEFAULT 0,
            timestamp TEXT DEFAULT CURRENT_TIMESTAMP)""")

        ex(conn, """CREATE TABLE IF NOT EXISTS feedback_utenti (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            nome TEXT,
            messaggio TEXT NOT NULL,
            pagina TEXT,
            letto INTEGER DEFAULT 0,
            letto_da TEXT,
            letto_il TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

        # ── Team tables ─────────────────────────────────────────────────────────
        ex(conn, """CREATE TABLE IF NOT EXISTS team_operatori (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            posizione INTEGER NOT NULL,
            linked_user_id INTEGER,
            attivo INTEGER DEFAULT 1)""")

        ex(conn, """CREATE TABLE IF NOT EXISTS team_turni (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            operatore_id INTEGER NOT NULL,
            turno_base TEXT,
            turno_var TEXT,
            flags TEXT DEFAULT '',
            flags_base TEXT DEFAULT '',
            flags_var TEXT DEFAULT '',
            modificato_da TEXT,
            modificato_il TEXT,
            UNIQUE(data, operatore_id))""")

        ex(conn, """CREATE TABLE IF NOT EXISTS team_colonne_destra (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT UNIQUE NOT NULL,
            -- rep1, rep2, rep3: Identificano i reperibili del giorno.
            -- rep1: Primo reperibile (primo ad essere chiamato in caso di necessità/malattia)
            -- rep2: Secondo reperibile
            -- rep3: Terzo reperibile
            rep1 TEXT, rep2 TEXT, rep3 TEXT,
            fest_m1 TEXT, fest_m2 TEXT, fest_p1 TEXT, fest_p2 TEXT)""")

        ex(conn, """CREATE TABLE IF NOT EXISTS team_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_modifica TEXT NOT NULL,
            utente TEXT NOT NULL,
            data_turno TEXT NOT NULL,
            operatore_nome TEXT,
            campo TEXT,
            vecchio_valore TEXT,
            nuovo_valore TEXT,
            flags TEXT)""")

        ex(conn, """CREATE TABLE IF NOT EXISTS team_ferie_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            operatore_id INTEGER NOT NULL,
            data TEXT NOT NULL,
            tipo TEXT NOT NULL DEFAULT 'ferie', -- 'ferie', 'ex-fest', 'rot'
            note TEXT,
            stato TEXT NOT NULL DEFAULT 'pending',
            requested_by TEXT,
            reviewed_by TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(operatore_id, data))""")

        ex(conn, """CREATE TABLE IF NOT EXISTS team_notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            title TEXT DEFAULT 'Notifica',
            messaggio TEXT NOT NULL,
            link TEXT,
            letto INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

        ex(conn, """CREATE TABLE IF NOT EXISTS team_swap_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            richiedente_id INTEGER NOT NULL,
            collega_id INTEGER NOT NULL,
            data TEXT NOT NULL,
            from_turno TEXT DEFAULT '',
            to_turno TEXT DEFAULT '',
            from_col TEXT DEFAULT '',
            to_col TEXT DEFAULT '',
            stato TEXT NOT NULL DEFAULT 'pending_target', -- 'pending_target', 'pending_editor', 'approved', 'rejected', 'cancelled'
            created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

        ex(conn, """CREATE TABLE IF NOT EXISTS team_ferie_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            actor_username TEXT,
            user_id INTEGER,
            username TEXT,
            operatore_id INTEGER,
            operatore_nome TEXT,
            data_turno TEXT NOT NULL,
            action TEXT NOT NULL,
            status_from TEXT,
            status_to TEXT)""")

        ex(conn, """CREATE TABLE IF NOT EXISTS team_template_weekly (
            giorno_settimana INTEGER NOT NULL,
            posizione INTEGER NOT NULL,
            turno_base TEXT,
            turno_var TEXT,
            flags TEXT DEFAULT '',
            PRIMARY KEY (giorno_settimana, posizione))""")

        ex(conn, """CREATE TABLE IF NOT EXISTS team_template_reperibili_weekly (
            giorno_settimana INTEGER PRIMARY KEY,
            -- Identificano la posizione (1-based) dell'operatore che deve fare reperibilità in quel giorno
            -- rep1_pos: Primo reperibile, rep2_pos: Secondo, ecc.
            rep1_pos INTEGER,
            rep2_pos INTEGER,
            rep3_pos INTEGER,
            fest_m1_pos INTEGER,
            fest_m2_pos INTEGER,
            fest_p1_pos INTEGER,
            fest_p2_pos INTEGER)""")

        ex(conn, """CREATE TABLE IF NOT EXISTS team_template_config (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            start_date TEXT,
            end_date TEXT)""")

        # Indici per le query piu' frequenti
        ex(conn, "CREATE INDEX IF NOT EXISTS idx_log_accessi_timestamp ON log_accessi(timestamp DESC)")
        ex(conn, "CREATE INDEX IF NOT EXISTS idx_login_page_visits_timestamp ON login_page_visits(timestamp DESC)")
        ex(conn, "CREATE INDEX IF NOT EXISTS idx_feedback_utenti_created_at ON feedback_utenti(created_at DESC)")
        ex(conn, "CREATE INDEX IF NOT EXISTS idx_feedback_utenti_letto ON feedback_utenti(letto, created_at DESC)")
        ex(conn, "CREATE INDEX IF NOT EXISTS idx_team_log_data_modifica ON team_log(data_modifica DESC)")
        ex(conn, "CREATE INDEX IF NOT EXISTS idx_team_log_data_turno ON team_log(data_turno)")
        ex(conn, "CREATE INDEX IF NOT EXISTS idx_team_ferie_requests_status_operatore_data ON team_ferie_requests(stato, operatore_id, data)")
        ex(conn, "CREATE INDEX IF NOT EXISTS idx_team_ferie_requests_user_data ON team_ferie_requests(user_id, data)")
        ex(conn, "CREATE INDEX IF NOT EXISTS idx_team_ferie_log_created_at ON team_ferie_log(created_at DESC)")
        ex(conn, "CREATE INDEX IF NOT EXISTS idx_team_ferie_log_operatore_data ON team_ferie_log(operatore_id, data_turno)")

        # ── Migrations / defaults ─────────────────────────────────────
        if not USE_PG:
            u_cols = [r[1] for r in conn.execute("PRAGMA table_info(utenti)").fetchall()]
            for col in ["is_admin", "is_editor", "is_team_editor"]:
                if col not in u_cols:
                    conn.execute(f"ALTER TABLE utenti ADD COLUMN {col} INTEGER DEFAULT 0")

            cols = [r[1] for r in conn.execute("PRAGMA table_info(turni)").fetchall()]
            if "user_id" not in cols:
                conn.execute("ALTER TABLE turni ADD COLUMN user_id INTEGER NOT NULL DEFAULT 1")
            for col, typ in [("ora_inizio","TEXT"),("ora_fine","TEXT"),("ore_diurne","REAL"),
                              ("ore_notturne","REAL"),("strao_fest_diurno","REAL"),("strao_fest_notturno","REAL")]:
                if col not in cols:
                    conn.execute(f"ALTER TABLE turni ADD COLUMN {col} {typ} DEFAULT 0")

            team_cols = [r[1] for r in conn.execute("PRAGMA table_info(team_turni)").fetchall()]
            for col in ["flags_base", "flags_var"]:
                if col not in team_cols:
                    conn.execute(f"ALTER TABLE team_turni ADD COLUMN {col} TEXT DEFAULT ''")

            op_cols = [r[1] for r in conn.execute("PRAGMA table_info(team_operatori)").fetchall()]
            if "linked_user_id" not in op_cols:
                conn.execute("ALTER TABLE team_operatori ADD COLUMN linked_user_id INTEGER")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_team_operatori_active_link ON team_operatori(attivo, linked_user_id)")

            tpl_rep_cols = [r[1] for r in conn.execute("PRAGMA table_info(team_template_reperibili_weekly)").fetchall()]
            for col in ["fest_m1_pos", "fest_m2_pos", "fest_p1_pos", "fest_p2_pos"]:
                if col not in tpl_rep_cols:
                    conn.execute(f"ALTER TABLE team_template_reperibili_weekly ADD COLUMN {col} INTEGER")

            ferie_req_cols = [r[1] for r in conn.execute("PRAGMA table_info(team_ferie_requests)").fetchall()]
            if "tipo" not in ferie_req_cols:
                conn.execute("ALTER TABLE team_ferie_requests ADD COLUMN tipo TEXT NOT NULL DEFAULT 'ferie'")
            if "note" not in ferie_req_cols:
                conn.execute("ALTER TABLE team_ferie_requests ADD COLUMN note TEXT")

            swap_cols = [r[1] for r in conn.execute("PRAGMA table_info(team_swap_requests)").fetchall()]
            for col in ["from_col", "to_col"]:
                if col not in swap_cols:
                    conn.execute(f"ALTER TABLE team_swap_requests ADD COLUMN {col} TEXT DEFAULT ''")

            notif_cols = [r[1] for r in conn.execute("PRAGMA table_info(team_notifications)").fetchall()]
            if "title" not in notif_cols:
                conn.execute("ALTER TABLE team_notifications ADD COLUMN title TEXT DEFAULT 'Notifica'")
            if "link" not in notif_cols:
                conn.execute("ALTER TABLE team_notifications ADD COLUMN link TEXT")

            if "flags_base" in [r[1] for r in conn.execute("PRAGMA table_info(team_turni)").fetchall()] and "flags_var" in [r[1] for r in conn.execute("PRAGMA table_info(team_turni)").fetchall()]:
                conn.execute("""
                    UPDATE team_turni
                    SET
                      flags_base = CASE
                        WHEN COALESCE(flags_base,'') = '' AND COALESCE(flags_var,'') = '' AND COALESCE(turno_var,'') = '' THEN COALESCE(flags,'')
                        ELSE COALESCE(flags_base,'')
                      END,
                      flags_var = CASE
                        WHEN COALESCE(flags_base,'') = '' AND COALESCE(flags_var,'') = '' AND COALESCE(turno_var,'') <> '' THEN COALESCE(flags,'')
                        ELSE COALESCE(flags_var,'')
                      END
                    WHERE COALESCE(flags,'') <> ''
                """)

            imp_cols = [r[1] for r in conn.execute("PRAGMA table_info(impostazioni)").fetchall()]
            if "user_id" not in imp_cols:
                conn.execute("ALTER TABLE impostazioni RENAME TO impostazioni_old")
                conn.execute("""CREATE TABLE impostazioni (
                    user_id INTEGER NOT NULL, chiave TEXT NOT NULL, valore TEXT,
                    PRIMARY KEY (user_id, chiave))""")
                conn.execute("INSERT INTO impostazioni SELECT 1, chiave, valore FROM impostazioni_old")
                conn.execute("DROP TABLE impostazioni_old")

            # Default operatori se vuota
            if fetchone(conn, "SELECT COUNT(*) as cnt FROM team_operatori")["cnt"] == 0:
                for i in range(1, 14):
                    conn.execute("INSERT INTO team_operatori (nome, posizione, attivo) VALUES (?,?,1)",
                                 (f"Operatore {i}", i))

            # Admin iniziale opzionale tramite env
            if fetchone(conn, "SELECT COUNT(*) as cnt FROM utenti")["cnt"] == 0 and INITIAL_ADMIN_USERNAME and INITIAL_ADMIN_PASSWORD:
                from app.security import hash_password
                hashed = hash_password(INITIAL_ADMIN_PASSWORD)
                conn.execute(
                    "INSERT INTO utenti (username, nome, password_hash, is_admin, is_editor, is_team_editor) VALUES (?,?,?,1,1,1)",
                    (INITIAL_ADMIN_USERNAME, INITIAL_ADMIN_NAME, hashed))

        else:
            # PostgreSQL: aggiungi colonne se mancano
            for col_def in [
                "ALTER TABLE utenti ADD COLUMN IF NOT EXISTS is_admin INTEGER DEFAULT 0",
                "ALTER TABLE utenti ADD COLUMN IF NOT EXISTS is_editor INTEGER DEFAULT 0",
                "ALTER TABLE utenti ADD COLUMN IF NOT EXISTS is_team_editor INTEGER DEFAULT 0",
                "ALTER TABLE team_operatori ADD COLUMN IF NOT EXISTS linked_user_id INTEGER",
                "ALTER TABLE team_turni ADD COLUMN IF NOT EXISTS flags_base TEXT DEFAULT ''",
                "ALTER TABLE team_turni ADD COLUMN IF NOT EXISTS flags_var TEXT DEFAULT ''",
                "ALTER TABLE team_template_reperibili_weekly ADD COLUMN IF NOT EXISTS fest_m1_pos INTEGER",
                "ALTER TABLE team_template_reperibili_weekly ADD COLUMN IF NOT EXISTS fest_m2_pos INTEGER",
                "ALTER TABLE team_template_reperibili_weekly ADD COLUMN IF NOT EXISTS fest_p1_pos INTEGER",
                "ALTER TABLE team_template_reperibili_weekly ADD COLUMN IF NOT EXISTS fest_p2_pos INTEGER",
                "ALTER TABLE team_ferie_requests ADD COLUMN IF NOT EXISTS tipo TEXT NOT NULL DEFAULT 'ferie'",
                "ALTER TABLE team_ferie_requests ADD COLUMN IF NOT EXISTS note TEXT",
                "ALTER TABLE team_swap_requests ADD COLUMN IF NOT EXISTS from_col TEXT DEFAULT ''",
                "ALTER TABLE team_swap_requests ADD COLUMN IF NOT EXISTS to_col TEXT DEFAULT ''",
                "ALTER TABLE team_notifications ADD COLUMN IF NOT EXISTS title TEXT DEFAULT 'Notifica'",
                "ALTER TABLE team_notifications ADD COLUMN IF NOT EXISTS link TEXT",
            ]:
                try:
                    ex(conn, col_def)
                except:
                    conn.rollback()
            try:
                ex(conn, "CREATE INDEX IF NOT EXISTS idx_team_operatori_active_link ON team_operatori(attivo, linked_user_id)")
            except:
                conn.rollback()
            try:
                ex(conn, """
                    UPDATE team_turni
                    SET
                      flags_base = CASE
                        WHEN COALESCE(flags_base,'') = '' AND COALESCE(flags_var,'') = '' AND COALESCE(turno_var,'') = '' THEN COALESCE(flags,'')
                        ELSE COALESCE(flags_base,'')
                      END,
                      flags_var = CASE
                        WHEN COALESCE(flags_base,'') = '' AND COALESCE(flags_var,'') = '' AND COALESCE(turno_var,'') <> '' THEN COALESCE(flags,'')
                        ELSE COALESCE(flags_var,'')
                      END
                    WHERE COALESCE(flags,'') <> ''
                """)
            except:
                conn.rollback()
            if (fetchone(conn, "SELECT COUNT(*) as cnt FROM utenti") or {}).get("cnt", 0) == 0 and INITIAL_ADMIN_USERNAME and INITIAL_ADMIN_PASSWORD:
                from app.security import hash_password
                ex(conn,
                   "INSERT INTO utenti (username, nome, password_hash, is_admin, is_editor, is_team_editor) VALUES (?,?,?,?,?,?)",
                   (INITIAL_ADMIN_USERNAME, INITIAL_ADMIN_NAME, hash_password(INITIAL_ADMIN_PASSWORD), 1, 1, 1))

        conn.commit()
    finally:
        release_db(conn)
