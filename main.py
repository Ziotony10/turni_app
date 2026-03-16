from fastapi import FastAPI, Depends, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from typing import Optional
import sqlite3, os
from datetime import date, datetime, timedelta
from jose import JWTError, jwt
from passlib.context import CryptContext
import secrets

app = FastAPI(title="Gestione Turni")
DB_PATH    = "turni.db"
DATABASE_URL = os.environ.get("DATABASE_URL")
USE_PG     = bool(DATABASE_URL)

if USE_PG:
    import psycopg2, psycopg2.extras

# ─── Sicurezza ────────────────────────────────────────────────────────────────
SECRET_KEY   = os.environ.get("JWT_SECRET", secrets.token_hex(32))
ALGORITHM    = "HS256"
TOKEN_EXPIRE = 60 * 24

pwd_context   = CryptContext(schemes=["sha256_crypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

# ─── Config turni ─────────────────────────────────────────────────────────────
TURNO_ORARI = {
    "M":  (7*60,  15*60), "M1": (8*60,  16*60),
    "M2": (9*60,  17*60), "M3": (11*60, 19*60),
    "P":  (15*60, 23*60), "N":  (23*60, 7*60),
}
TURNI_CONFIG = {
    "M":   {"lavorativo": True,  "label": "Mattino 7-15",     "colore": "#3B82F6"},
    "M1":  {"lavorativo": True,  "label": "Mattino 1  8-16",  "colore": "#2563EB"},
    "M2":  {"lavorativo": True,  "label": "Mattino 2  9-17",  "colore": "#1D4ED8"},
    "M3":  {"lavorativo": True,  "label": "Mattino 3 11-19",  "colore": "#1E40AF"},
    "P":   {"lavorativo": True,  "label": "Pomeriggio 15-23", "colore": "#F59E0B"},
    "N":   {"lavorativo": True,  "label": "Notte 23-7",       "colore": "#6366F1"},
    "RC":  {"lavorativo": False, "label": "Riposo Comp.",     "colore": "#10B981"},
    "R":   {"lavorativo": False, "label": "Riposo Dom.",      "colore": "#EF4444"},
    "ROT": {"lavorativo": False, "label": "Rid. Orario",      "colore": "#8B5CF6"},
    "RF":  {"lavorativo": False, "label": "Riposo Festivo",   "colore": "#EC4899"},
    "MAL": {"lavorativo": False, "label": "Malattia",         "colore": "#F97316"},
    "F":   {"lavorativo": False, "label": "Ferie",            "colore": "#14B8A6"},
    "F-P": {"lavorativo": False, "label": "Ferie su P",       "colore": "#84CC16"},
    "F-N": {"lavorativo": False, "label": "Ferie su N",       "colore": "#06B6D4"},
}
FESTIVITA = {
    "2025-01-01","2025-01-06","2025-04-20","2025-04-21","2025-04-25",
    "2025-05-01","2025-06-02","2025-08-15","2025-11-01","2025-12-07",
    "2025-12-08","2025-12-25","2025-12-26",
    "2026-01-01","2026-01-06","2026-04-05","2026-04-06","2026-04-25",
    "2026-05-01","2026-06-02","2026-08-15","2026-11-01","2026-12-07",
    "2026-12-08","2026-12-25","2026-12-26",
    "2027-01-01","2027-01-06","2027-03-28","2027-03-29","2027-04-25",
    "2027-05-01","2027-06-02","2027-08-15","2027-11-01","2027-12-07",
    "2027-12-08","2027-12-25","2027-12-26",
}
NOTTE_ASSENZA = {"F": 0.0, "F-P": 3.0, "F-N": 7.0}

IMPOSTAZIONI_DEFAULTS = {
    "retribuzione_totale": "2573.39", "tariffa_nott_50": "7.53974",
    "tariffa_dom": "8.39811", "tariffa_nott_ord": "5.27782",
    "tariffa_strao_fer_d": "22.61922", "tariffa_strao_fer_n": "24.12716",
    "tariffa_strao_fest_d": "24.12716", "tariffa_strao_fest_n": "24.36517",
    "tariffa_rep_feriale": "15.26", "tariffa_rep_semifestiva": "32.99",
    "tariffa_rep_festiva": "53.13", "indennita_turno": "279.66",
    "trattenuta_sindacato": "18.86", "trattenuta_regionale": "50.00",
    "trattenuta_pegaso": "33.90", "aliquota_inps": "9.19", "detrazioni_annue": "1955.00",
    "tariffa_fest_riposo": "98.97654",  # Festività in giorno di riposo (tariffa base)
}

# ─── DB helpers ───────────────────────────────────────────────────────────────
def get_db():
    if USE_PG:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        return conn
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def q(sql):
    """Adatta la query da SQLite a PostgreSQL."""
    if not USE_PG:
        return sql
    sql = sql.replace("?", "%s")
    sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
    sql = sql.replace("DEFAULT CURRENT_TIMESTAMP", "DEFAULT NOW()")
    return sql

def ex(conn, sql, params=()):
    if USE_PG:
        cur = conn.cursor()
        cur.execute(q(sql), params)
        return cur
    return conn.execute(q(sql), params)

def fetchall(conn, sql, params=()):
    cur = ex(conn, sql, params)
    rows = cur.fetchall()
    return [dict(r) for r in rows]

def fetchone(conn, sql, params=()):
    cur = ex(conn, sql, params)
    row = cur.fetchone()
    return dict(row) if row else None

# ─── Calcolo ore ──────────────────────────────────────────────────────────────
def split_dn(start, end):
    if end <= start: end += 1440
    notturni = [(0,360),(1200,1440),(1440,1800)]
    nott = 0
    for ns, ne in notturni:
        s, e = max(start,ns), min(end,ne)
        if e > s: nott += e - s
    tot = end - start
    return round((tot-nott)/60,2), round(nott/60,2)

def to_min(s):
    try:
        h, m = s.strip().split(":")
        return int(h)*60 + int(m)
    except: return None

def calcola_ore(turno, ora_inizio, ora_fine, data_str):
    r = {"ore_diurne":0.0,"ore_notturne":0.0,"strao_diurno":0.0,
         "strao_notturno":0.0,"strao_fest_diurno":0.0,"strao_fest_notturno":0.0}
    ei = to_min(ora_inizio) if ora_inizio else None
    ef = to_min(ora_fine)   if ora_fine   else None
    std = TURNO_ORARI.get(turno)

    # Controlla se il giorno è festivo da CALENDARIO (non domenica)
    # La domenica aziendale è R, non un festivo automatico
    try:
        festivo = data_str in FESTIVITA
    except:
        festivo = False

    if turno == "R":
        if ei is not None and ef is not None:
            d,n = split_dn(ei,ef); r["strao_fest_diurno"]=d; r["strao_fest_notturno"]=n
        return r
    if turno == "RC":
        if ei is not None and ef is not None:
            d,n = split_dn(ei,ef); r["strao_diurno"]=d; r["strao_notturno"]=n
        return r
    if not std:
        if ei is not None and ef is not None:
            d,n = split_dn(ei,ef); r["strao_diurno"]=d; r["strao_notturno"]=n
        return r

    si, sf = std

    # ── Giorno FESTIVO con turno lavorativo → tutto strao festivo ──────────────
    if festivo:
        ini = ei if ei is not None else si
        fin = ef if ef is not None else sf
        d,n = split_dn(ini, fin)
        r["strao_fest_diurno"] = d; r["strao_fest_notturno"] = n
        return r

    # ── Giorno normale ─────────────────────────────────────────────────────────
    if ei is None and ef is None:
        d,n = split_dn(si,sf); r["ore_diurne"]=d; r["ore_notturne"]=n
        return r

    ini = ei if ei is not None else si
    fin = ef if ef is not None else sf
    sfn = sf if sf > si else sf+1440
    fn  = fin if fin > ini else fin+1440

    oi, of = max(ini,si), min(fn,sfn)
    if of > oi:
        d,n = split_dn(oi,of); r["ore_diurne"]+=d; r["ore_notturne"]+=n
    if ini < si:
        d,n = split_dn(ini,si); r["strao_diurno"]+=d; r["strao_notturno"]+=n
    if fn > sfn:
        d,n = split_dn(sfn,fn); r["strao_diurno"]+=d; r["strao_notturno"]+=n
    return r

def calcola_tipo_rep(turno, data_str):
    if turno == "RC": return "semifestiva"
    if turno == "R": return "festiva"
    if data_str in FESTIVITA: return "festiva"
    if TURNI_CONFIG.get(turno,{}).get("lavorativo"): return "feriale"
    return ""

# ─── Init DB ──────────────────────────────────────────────────────────────────
def init_db():
    conn = get_db()
    try:
        ex(conn, """CREATE TABLE IF NOT EXISTS utenti (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            nome TEXT,
            password_hash TEXT NOT NULL,
            is_admin INTEGER DEFAULT 0,
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

        if not USE_PG:
            # Migrazione utenti: aggiungi is_admin se manca
            u_cols = [r[1] for r in conn.execute("PRAGMA table_info(utenti)").fetchall()]
            if "is_admin" not in u_cols:
                conn.execute("ALTER TABLE utenti ADD COLUMN is_admin INTEGER DEFAULT 0")
            # Imposta antonino.adragna come admin
            conn.execute("UPDATE utenti SET is_admin=1 WHERE username='antonino.adragna'")

            cols = [r[1] for r in conn.execute("PRAGMA table_info(turni)").fetchall()]
            if "user_id" not in cols:
                conn.execute("ALTER TABLE turni ADD COLUMN user_id INTEGER NOT NULL DEFAULT 1")
            for col, typ in [("ora_inizio","TEXT"),("ora_fine","TEXT"),("ore_diurne","REAL"),
                             ("ore_notturne","REAL"),("strao_fest_diurno","REAL"),("strao_fest_notturno","REAL")]:
                if col not in cols:
                    conn.execute(f"ALTER TABLE turni ADD COLUMN {col} {typ} DEFAULT 0")
            imp_cols = [r[1] for r in conn.execute("PRAGMA table_info(impostazioni)").fetchall()]
            if "user_id" not in imp_cols:
                conn.execute("ALTER TABLE impostazioni RENAME TO impostazioni_old")
                conn.execute("""CREATE TABLE impostazioni (
                    user_id INTEGER NOT NULL, chiave TEXT NOT NULL, valore TEXT,
                    PRIMARY KEY (user_id, chiave))""")
                conn.execute("INSERT INTO impostazioni SELECT 1, chiave, valore FROM impostazioni_old")
                conn.execute("DROP TABLE impostazioni_old")
        else:
            # PostgreSQL: aggiungi colonna is_admin se manca
            try:
                ex(conn, "ALTER TABLE utenti ADD COLUMN IF NOT EXISTS is_admin INTEGER DEFAULT 0")
            except: conn.rollback()
            # Imposta antonino.adragna come admin
            ex(conn, "UPDATE utenti SET is_admin=1 WHERE username='antonino.adragna'")

        conn.commit()
    finally:
        conn.close()

init_db()

# ─── Auth helpers ─────────────────────────────────────────────────────────────
def hash_password(pwd):    return pwd_context.hash(pwd)
def verify_password(p, h): return pwd_context.verify(p, h)

def create_token(user_id, username, is_admin=False):
    exp = datetime.utcnow() + timedelta(minutes=TOKEN_EXPIRE)
    return jwt.encode({"sub": str(user_id), "username": username, "is_admin": is_admin, "exp": exp}, SECRET_KEY, algorithm=ALGORITHM)

def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        uid = int(payload.get("sub"))
        if not uid: raise HTTPException(401, "Token non valido")
        return {"id": uid, "username": payload.get("username"), "is_admin": payload.get("is_admin", False)}
    except JWTError:
        raise HTTPException(401, "Token non valido o scaduto")

def require_admin(user=Depends(get_current_user)):
    if not user.get("is_admin"):
        raise HTTPException(403, "Accesso riservato agli amministratori")
    return user

def get_user_settings(user_id, conn):
    rows = fetchall(conn, "SELECT chiave, valore FROM impostazioni WHERE user_id=?", (user_id,))
    result = {k: float(v) for k, v in IMPOSTAZIONI_DEFAULTS.items()}
    for r in rows:
        try: result[r["chiave"]] = float(r["valore"])
        except: pass
    return result

# ─── Modelli ──────────────────────────────────────────────────────────────────
class RegisterInput(BaseModel):
    username: str; password: str; nome: Optional[str] = None

class TurnoInput(BaseModel):
    turno:        Optional[str]  = None
    ora_inizio:   Optional[str]  = None
    ora_fine:     Optional[str]  = None
    reperibilita: Optional[bool] = False
    note:         Optional[str]  = None

class ImpostazioniInput(BaseModel):
    valori: dict

# ─── Auth endpoints ───────────────────────────────────────────────────────────
@app.post("/api/auth/register")
def register(payload: RegisterInput):
    if len(payload.username) < 3: raise HTTPException(400, "Username troppo corto (min 3)")
    if len(payload.password) < 6: raise HTTPException(400, "Password troppo corta (min 6)")
    conn = get_db()
    try:
        ex(conn, "INSERT INTO utenti (username, nome, password_hash) VALUES (?,?,?)",
           (payload.username.strip().lower(), payload.nome or payload.username, hash_password(payload.password)))
        conn.commit()
        user = fetchone(conn, "SELECT id, is_admin FROM utenti WHERE username=?", (payload.username.strip().lower(),))
        # Imposta admin se è antonino.adragna
        if payload.username.strip().lower() == "antonino.adragna":
            ex(conn, "UPDATE utenti SET is_admin=1 WHERE username=?", (payload.username.strip().lower(),))
            conn.commit()
            user["is_admin"] = 1
        token = create_token(user["id"], payload.username.strip().lower(), bool(user.get("is_admin")))
        return {"access_token": token, "token_type": "bearer", "username": payload.username, "is_admin": bool(user.get("is_admin"))}
    except (psycopg2.errors.UniqueViolation if USE_PG else sqlite3.IntegrityError):
        conn.rollback()
        raise HTTPException(400, "Username già esistente")
    finally:
        conn.close()

@app.post("/api/auth/login")
def login(form: OAuth2PasswordRequestForm = Depends()):
    conn = get_db()
    user = fetchone(conn, "SELECT * FROM utenti WHERE username=?", (form.username.strip().lower(),))
    conn.close()
    if not user or not verify_password(form.password, user["password_hash"]):
        raise HTTPException(401, "Credenziali non corrette")
    token = create_token(user["id"], user["username"], bool(user.get("is_admin")))
    return {"access_token": token, "token_type": "bearer", "username": user["username"], "nome": user["nome"], "is_admin": bool(user.get("is_admin"))}

@app.get("/api/auth/me")
def me(current_user=Depends(get_current_user)):
    conn = get_db()
    user = fetchone(conn, "SELECT id, username, nome, is_admin FROM utenti WHERE id=?", (current_user["id"],))
    conn.close()
    if not user:
        raise HTTPException(401, "Utente non trovato")
    # antonino.adragna è sempre admin indipendentemente dal valore nel DB
    is_admin = bool(user.get("is_admin")) or user["username"] == "antonino.adragna"
    # Aggiorna il DB se necessario
    if user["username"] == "antonino.adragna" and not user.get("is_admin"):
        try:
            conn2 = get_db()
            ex(conn2, "UPDATE utenti SET is_admin=1 WHERE username='antonino.adragna'")
            conn2.commit()
            conn2.close()
        except: pass
    return {"id": user["id"], "username": user["username"], "nome": user["nome"], "is_admin": is_admin}

# ─── Admin: gestione utenti ───────────────────────────────────────────────────
@app.get("/api/admin/utenti")
def get_utenti(admin=Depends(require_admin)):
    conn = get_db()
    rows = fetchall(conn, "SELECT id, username, nome, is_admin, created_at FROM utenti ORDER BY created_at")
    conn.close()
    return rows

@app.post("/api/admin/utenti/{user_id}/admin")
def toggle_admin(user_id: int, admin=Depends(require_admin)):
    conn = get_db()
    user = fetchone(conn, "SELECT is_admin, username FROM utenti WHERE id=?", (user_id,))
    if not user: raise HTTPException(404, "Utente non trovato")
    # Non puoi rimuovere i tuoi stessi privilegi
    if user["username"] == "antonino.adragna":
        raise HTTPException(400, "Non puoi modificare l'admin principale")
    new_val = 0 if user["is_admin"] else 1
    ex(conn, "UPDATE utenti SET is_admin=? WHERE id=?", (new_val, user_id))
    conn.commit(); conn.close()
    return {"ok": True, "is_admin": bool(new_val)}

@app.delete("/api/admin/utenti/{user_id}")
def delete_user(user_id: int, admin=Depends(require_admin)):
    conn = get_db()
    user = fetchone(conn, "SELECT username FROM utenti WHERE id=?", (user_id,))
    if not user: raise HTTPException(404, "Utente non trovato")
    if user["username"] == "antonino.adragna":
        raise HTTPException(400, "Non puoi eliminare l'admin principale")
    ex(conn, "DELETE FROM turni WHERE user_id=?", (user_id,))
    ex(conn, "DELETE FROM impostazioni WHERE user_id=?", (user_id,))
    ex(conn, "DELETE FROM utenti WHERE id=?", (user_id,))
    conn.commit(); conn.close()
    return {"ok": True}

# ─── Admin: tabelle turni ─────────────────────────────────────────────────────
@app.get("/api/admin/tabelle")
def get_tabelle(user=Depends(get_current_user)):
    conn = get_db()
    rows = fetchall(conn, "SELECT id, nome, tipo, num_settimane, created_at FROM tabelle_turni ORDER BY tipo, nome")
    conn.close()
    return rows

@app.get("/api/admin/tabelle/{tab_id}")
def get_tabella(tab_id: int, user=Depends(get_current_user)):
    conn = get_db()
    row = fetchone(conn, "SELECT * FROM tabelle_turni WHERE id=?", (tab_id,))
    conn.close()
    if not row: raise HTTPException(404, "Tabella non trovata")
    import json
    row["turni_json"] = json.loads(row["turni_json"])
    return row

class TabellaTurniInput(BaseModel):
    nome: str
    tipo: str  # "telefonisti" | "capisala"
    num_settimane: int
    turni: list  # lista di liste: [[lun,mar,...,dom], [lun,...], ...]

@app.post("/api/admin/tabelle")
def create_tabella(payload: TabellaTurniInput, admin=Depends(require_admin)):
    import json
    conn = get_db()
    ex(conn, "INSERT INTO tabelle_turni (nome, tipo, num_settimane, turni_json) VALUES (?,?,?,?)",
       (payload.nome, payload.tipo, payload.num_settimane, json.dumps(payload.turni)))
    conn.commit(); conn.close()
    return {"ok": True}

@app.put("/api/admin/tabelle/{tab_id}")
def update_tabella(tab_id: int, payload: TabellaTurniInput, admin=Depends(require_admin)):
    import json
    conn = get_db()
    ex(conn, "UPDATE tabelle_turni SET nome=?, tipo=?, num_settimane=?, turni_json=? WHERE id=?",
       (payload.nome, payload.tipo, payload.num_settimane, json.dumps(payload.turni), tab_id))
    conn.commit(); conn.close()
    return {"ok": True}

@app.delete("/api/admin/tabelle/{tab_id}")
def delete_tabella(tab_id: int, admin=Depends(require_admin)):
    conn = get_db()
    ex(conn, "DELETE FROM tabelle_turni WHERE id=?", (tab_id,))
    conn.commit(); conn.close()
    return {"ok": True}

# ─── Applica tabella turni ────────────────────────────────────────────────────
class ApplicaTabella(BaseModel):
    tab_id: int
    data_inizio: str       # "YYYY-MM-DD"
    data_fine: Optional[str] = None   # "YYYY-MM-DD" — se assente usa fine anno
    settimana_inizio: int  # quale settimana della tabella inizia (1-based)
    giorno_inizio: int     # quale giorno della settimana inizia (0=lun, 6=dom)
    anno_fine: int         # anno di fine (per retrocompatibilità)

@app.post("/api/tabella/applica")
def applica_tabella(payload: ApplicaTabella, user=Depends(get_current_user)):
    import json
    from datetime import timedelta
    conn = get_db()
    tab = fetchone(conn, "SELECT * FROM tabelle_turni WHERE id=?", (payload.tab_id,))
    if not tab: conn.close(); raise HTTPException(404, "Tabella non trovata")

    settimane = json.loads(tab["turni_json"])  # lista di settimane, ogni sett. è lista di 7 turni
    num_sett = len(settimane)

    data_inizio = date.fromisoformat(payload.data_inizio)
    # Usa data_fine se fornita, altrimenti fine anno
    if payload.data_fine:
        data_fine = date.fromisoformat(payload.data_fine)
    else:
        data_fine = date(payload.anno_fine, 12, 31)

    # Calcola posizione iniziale nella tabella
    sett_idx = (payload.settimana_inizio - 1) % num_sett  # 0-based
    giorno_idx = payload.giorno_inizio                     # 0=lun, 6=dom

    # Itera dal giorno di inizio fino al 31 dicembre
    data_cur = data_inizio
    cur_sett = sett_idx
    cur_giorno = giorno_idx

    inseriti = 0
    while data_cur <= data_fine:
        turno_raw = settimane[cur_sett][cur_giorno] if cur_sett < len(settimane) else ""
        turno = turno_raw.strip().split()[0] if turno_raw.strip() else ""

        # Normalizza turno
        TURNI_VALIDI = set(TURNI_CONFIG.keys())
        if turno not in TURNI_VALIDI:
            turno = None

        if turno:
            data_str = data_cur.isoformat()
            ore = calcola_ore(turno, None, None, data_str)
            tipo_rep = ""
            # Salva gli orari standard del turno per mostrare Inizio/Fine nel calendario
            std = TURNO_ORARI.get(turno)
            if std:
                def mins_to_hhmm(m):
                    m = m % 1440
                    return f"{m//60:02d}:{m%60:02d}"
                std_ini_str = mins_to_hhmm(std[0])
                std_fin_str = mins_to_hhmm(std[1])
            else:
                std_ini_str = None
                std_fin_str = None

            if USE_PG:
                ex(conn, """INSERT INTO turni
                      (user_id,data,turno,ora_inizio,ora_fine,ore_diurne,ore_notturne,
                       strao_diurno,strao_notturno,strao_fest_diurno,strao_fest_notturno,reperibilita,note)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT(user_id,data) DO UPDATE SET
                      turno=EXCLUDED.turno, ora_inizio=EXCLUDED.ora_inizio, ora_fine=EXCLUDED.ora_fine,
                      ore_diurne=EXCLUDED.ore_diurne, ore_notturne=EXCLUDED.ore_notturne,
                      strao_diurno=EXCLUDED.strao_diurno, strao_notturno=EXCLUDED.strao_notturno,
                      strao_fest_diurno=EXCLUDED.strao_fest_diurno,
                      strao_fest_notturno=EXCLUDED.strao_fest_notturno,
                      reperibilita=EXCLUDED.reperibilita""",
                   (user["id"],data_str,turno,std_ini_str,std_fin_str,
                    ore["ore_diurne"],ore["ore_notturne"],ore["strao_diurno"],ore["strao_notturno"],
                    ore["strao_fest_diurno"],ore["strao_fest_notturno"],tipo_rep or None,None))
            else:
                conn.execute("""INSERT INTO turni
                      (user_id,data,turno,ora_inizio,ora_fine,ore_diurne,ore_notturne,
                       strao_diurno,strao_notturno,strao_fest_diurno,strao_fest_notturno,reperibilita,note)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(user_id,data) DO UPDATE SET
                      turno=excluded.turno, ora_inizio=excluded.ora_inizio, ora_fine=excluded.ora_fine,
                      ore_diurne=excluded.ore_diurne, ore_notturne=excluded.ore_notturne,
                      strao_diurno=excluded.strao_diurno, strao_notturno=excluded.strao_notturno,
                      strao_fest_diurno=excluded.strao_fest_diurno,
                      strao_fest_notturno=excluded.strao_fest_notturno,
                      reperibilita=excluded.reperibilita""",
                   (user["id"],data_str,turno,std_ini_str,std_fin_str,
                    ore["ore_diurne"],ore["ore_notturne"],ore["strao_diurno"],ore["strao_notturno"],
                    ore["strao_fest_diurno"],ore["strao_fest_notturno"],tipo_rep or None,None))
            inseriti += 1

        # Avanza al giorno successivo
        cur_giorno += 1
        if cur_giorno >= 7:
            cur_giorno = 0
            cur_sett = (cur_sett + 1) % num_sett
        data_cur += timedelta(days=1)

    conn.commit(); conn.close()
    return {"ok": True, "inseriti": inseriti}

# ─── API ──────────────────────────────────────────────────────────────────────
@app.get("/api/config")
def get_config():
    out = {}
    for k, v in TURNI_CONFIG.items():
        orari = TURNO_ORARI.get(k)
        out[k] = {**v, "std_ini": orari[0] if orari else None, "std_fin": orari[1] if orari else None}
    return out

@app.get("/api/festivita")
def get_festivita():
    return list(FESTIVITA)

@app.get("/api/turni/{anno}/{mese}")
def get_turni_mese(anno: int, mese: int, user=Depends(get_current_user)):
    conn = get_db()
    rows = fetchall(conn, "SELECT * FROM turni WHERE user_id=? AND data LIKE ?",
                    (user["id"], f"{anno:04d}-{mese:02d}-%"))
    conn.close()
    result = {}
    for r in rows:
        d = r["data"]
        result[d if isinstance(d, str) else d.isoformat()] = r
    return result

@app.post("/api/turni/{data}")
def set_turno(data: str, payload: TurnoInput, user=Depends(get_current_user)):
    ore     = calcola_ore(payload.turno or "", payload.ora_inizio, payload.ora_fine, data)
    tipo_rep = calcola_tipo_rep(payload.turno or "", data) if payload.reperibilita else ""
    conn = get_db()
    if USE_PG:
        ex(conn, """INSERT INTO turni
              (user_id,data,turno,ora_inizio,ora_fine,ore_diurne,ore_notturne,
               strao_diurno,strao_notturno,strao_fest_diurno,strao_fest_notturno,reperibilita,note)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT(user_id,data) DO UPDATE SET
              turno=EXCLUDED.turno, ora_inizio=EXCLUDED.ora_inizio, ora_fine=EXCLUDED.ora_fine,
              ore_diurne=EXCLUDED.ore_diurne, ore_notturne=EXCLUDED.ore_notturne,
              strao_diurno=EXCLUDED.strao_diurno, strao_notturno=EXCLUDED.strao_notturno,
              strao_fest_diurno=EXCLUDED.strao_fest_diurno, strao_fest_notturno=EXCLUDED.strao_fest_notturno,
              reperibilita=EXCLUDED.reperibilita, note=EXCLUDED.note""",
           (user["id"],data,payload.turno,payload.ora_inizio,payload.ora_fine,
            ore["ore_diurne"],ore["ore_notturne"],ore["strao_diurno"],ore["strao_notturno"],
            ore["strao_fest_diurno"],ore["strao_fest_notturno"],tipo_rep or None,payload.note))
    else:
        conn.execute("""INSERT INTO turni
              (user_id,data,turno,ora_inizio,ora_fine,ore_diurne,ore_notturne,
               strao_diurno,strao_notturno,strao_fest_diurno,strao_fest_notturno,reperibilita,note)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(user_id,data) DO UPDATE SET
              turno=excluded.turno, ora_inizio=excluded.ora_inizio, ora_fine=excluded.ora_fine,
              ore_diurne=excluded.ore_diurne, ore_notturne=excluded.ore_notturne,
              strao_diurno=excluded.strao_diurno, strao_notturno=excluded.strao_notturno,
              strao_fest_diurno=excluded.strao_fest_diurno, strao_fest_notturno=excluded.strao_fest_notturno,
              reperibilita=excluded.reperibilita, note=excluded.note""",
           (user["id"],data,payload.turno,payload.ora_inizio,payload.ora_fine,
            ore["ore_diurne"],ore["ore_notturne"],ore["strao_diurno"],ore["strao_notturno"],
            ore["strao_fest_diurno"],ore["strao_fest_notturno"],tipo_rep or None,payload.note))
    conn.commit(); conn.close()
    return {"ok": True, **ore, "tipo_reperibilita": tipo_rep}

@app.delete("/api/turni/{data}")
def delete_turno(data: str, user=Depends(get_current_user)):
    conn = get_db()
    ex(conn, "DELETE FROM turni WHERE user_id=? AND data=?", (user["id"], data))
    conn.commit(); conn.close()
    return {"ok": True}

@app.delete("/api/turni-mese/{anno}/{mese}")
def delete_mese(anno: int, mese: int, user=Depends(get_current_user)):
    conn = get_db()
    if USE_PG:
        ex(conn, "DELETE FROM turni WHERE user_id=%s AND EXTRACT(YEAR FROM data::date)=%s AND EXTRACT(MONTH FROM data::date)=%s",
           (user["id"], anno, mese))
    else:
        ex(conn, "DELETE FROM turni WHERE user_id=? AND data LIKE ?",
           (user["id"], f"{anno:04d}-{mese:02d}-%"))
    conn.commit(); conn.close()
    return {"ok": True}

@app.get("/api/riepilogo/{anno}")
def get_riepilogo(anno: int, user=Depends(get_current_user)):
    conn = get_db()
    if USE_PG:
        rows = fetchall(conn, "SELECT * FROM turni WHERE user_id=%s AND EXTRACT(YEAR FROM data::date)=%s",
                        (user["id"], anno))
    else:
        rows = fetchall(conn, "SELECT * FROM turni WHERE user_id=? AND data LIKE ?",
                        (user["id"], f"{anno:04d}-%"))
    conn.close()
    mesi = {m: {"ore_diurne":0.0,"ore_notturne":0.0,"strao_diurno":0.0,"strao_notturno":0.0,
                "strao_fest_diurno":0.0,"strao_fest_notturno":0.0,
                "reperibilita_feriale":0,"reperibilita_semifestiva":0,"reperibilita_festiva":0,
                "mal":0,"ferie":0,"rc":0,"r":0,"rot":0,"rf":0,
                "fest_riposo":0} for m in range(1,13)}
    for r in rows:
        d = r["data"]; d_str = d if isinstance(d,str) else d.isoformat()
        mes = int(d_str.split("-")[1]); m = mesi[mes]; t = r.get("turno") or ""
        for c in ["ore_diurne","ore_notturne","strao_diurno","strao_notturno","strao_fest_diurno","strao_fest_notturno"]:
            m[c] += r.get(c) or 0
        if t=="MAL": m["mal"]+=1
        if t in("F","F-P","F-N"): m["ferie"]+=1
        if t=="RC": m["rc"]+=1
        if t=="R": m["r"]+=1
        if t=="ROT": m["rot"]+=1
        if t=="RF": m["rf"]+=1
        rep = r.get("reperibilita") or ""
        if rep=="feriale": m["reperibilita_feriale"]+=1
        elif rep=="semifestiva": m["reperibilita_semifestiva"]+=1
        elif rep=="festiva": m["reperibilita_festiva"]+=1
        # Festività in giorno di riposo (R o RC in giorno festivo da calendario)
        if t in ("R", "RC") and d_str in FESTIVITA:
            m["fest_riposo"] += 1
    return mesi

@app.get("/api/impostazioni")
def get_impostazioni(user=Depends(get_current_user)):
    conn = get_db()
    cfg = get_user_settings(user["id"], conn)
    conn.close()
    return cfg

@app.post("/api/impostazioni")
def set_impostazioni(payload: ImpostazioniInput, user=Depends(get_current_user)):
    conn = get_db()
    for k, v in payload.valori.items():
        if USE_PG:
            ex(conn, """INSERT INTO impostazioni (user_id,chiave,valore) VALUES (%s,%s,%s)
               ON CONFLICT (user_id,chiave) DO UPDATE SET valore=EXCLUDED.valore""",
               (user["id"], k, str(v)))
        else:
            conn.execute("INSERT OR REPLACE INTO impostazioni (user_id,chiave,valore) VALUES (?,?,?)",
                         (user["id"], k, str(v)))
    conn.commit(); conn.close()
    return {"ok": True}

@app.get("/api/bustapaga/{anno}/{mese}")
def get_busta_paga(anno: int, mese: int, user=Depends(get_current_user)):
    mp = mese-1 if mese>1 else 12
    ap = anno   if mese>1 else anno-1
    conn = get_db()
    if USE_PG:
        rows = fetchall(conn, "SELECT * FROM turni WHERE user_id=%s AND EXTRACT(YEAR FROM data::date)=%s AND EXTRACT(MONTH FROM data::date)=%s",
                        (user["id"], ap, mp))
    else:
        rows = fetchall(conn, "SELECT * FROM turni WHERE user_id=? AND data LIKE ?",
                        (user["id"], f"{ap:04d}-{mp:02d}-%"))
    cfg = get_user_settings(user["id"], conn)
    conn.close()

    tot = {"ore_diurne":0.0,"ore_notturne":0.0,"strao_diurno":0.0,"strao_notturno":0.0,
           "strao_fest_diurno":0.0,"strao_fest_notturno":0.0,
           "rep_feriale":0,"rep_semifestiva":0,"rep_festiva":0,
           "domeniche":0,"giorni_lavoro":0,"notte_assenza":0.0,"fest_riposo":0}

    for r in rows:
        d = r["data"]; d_str = d if isinstance(d,str) else d.isoformat()
        t = r.get("turno") or ""
        if TURNI_CONFIG.get(t,{}).get("lavorativo"):
            tot["giorni_lavoro"] += 1
            if date.fromisoformat(d_str).weekday() == 6: tot["domeniche"] += 1
        for c in ["ore_diurne","ore_notturne","strao_diurno","strao_notturno","strao_fest_diurno","strao_fest_notturno"]:
            tot[c] += r.get(c) or 0
        rep = r.get("reperibilita") or ""
        if rep=="feriale": tot["rep_feriale"]+=1
        elif rep=="semifestiva": tot["rep_semifestiva"]+=1
        elif rep=="festiva": tot["rep_festiva"]+=1
        if t in NOTTE_ASSENZA: tot["notte_assenza"] += NOTTE_ASSENZA[t]
        # Festività in giorno di riposo
        if t in ("R", "RC") and d_str in FESTIVITA:
            tot["fest_riposo"] += 1

    mi = ["","Gen","Feb","Mar","Apr","Mag","Giu","Lug","Ago","Set","Ott","Nov","Dic"]
    rp = f"{mi[mp]}/{str(ap)[-2:]}"; rc = f"{mi[mese]}/{str(anno)[-2:]}"

    vc = [
        {"voce":"Retribuzione totale mensile",   "ref":rc,"qty":None,"tariffa":None,                          "importo":cfg["retribuzione_totale"]},
        {"voce":"Indennità turno X",             "ref":rc,"qty":None,"tariffa":None,                          "importo":cfg["indennita_turno"]},
        {"voce":"Ore notturne in turno 50%",     "ref":rp,"qty":tot["ore_notturne"],       "tariffa":cfg["tariffa_nott_50"],        "importo":round(tot["ore_notturne"]       *cfg["tariffa_nott_50"],       2)},
        {"voce":"Indennità lavoro domenicale",   "ref":rp,"qty":tot["domeniche"]*8,        "tariffa":cfg["tariffa_dom"],            "importo":round(tot["domeniche"]*8        *cfg["tariffa_dom"],           2)},
        {"voce":"Lavoro ordinario notte",        "ref":rp,"qty":tot["notte_assenza"],      "tariffa":cfg["tariffa_nott_ord"],       "importo":round(tot["notte_assenza"]      *cfg["tariffa_nott_ord"],      2)},
        {"voce":"Str. Feriale Diurno 150%",      "ref":rp,"qty":tot["strao_diurno"],       "tariffa":cfg["tariffa_strao_fer_d"],   "importo":round(tot["strao_diurno"]       *cfg["tariffa_strao_fer_d"],   2)},
        {"voce":"Str. Feriale Notturno 160%",    "ref":rp,"qty":tot["strao_notturno"],     "tariffa":cfg["tariffa_strao_fer_n"],   "importo":round(tot["strao_notturno"]     *cfg["tariffa_strao_fer_n"],   2)},
        {"voce":"Str. Festivo Diurno 160%",      "ref":rp,"qty":tot["strao_fest_diurno"],  "tariffa":cfg["tariffa_strao_fest_d"],  "importo":round(tot["strao_fest_diurno"]  *cfg["tariffa_strao_fest_d"],  2)},
        {"voce":"Str. Festivo Notturno 175%",    "ref":rp,"qty":tot["strao_fest_notturno"],"tariffa":cfg["tariffa_strao_fest_n"],  "importo":round(tot["strao_fest_notturno"]*cfg["tariffa_strao_fest_n"],  2)},
        {"voce":"Ind. Reperibilità Feriale",     "ref":rp,"qty":tot["rep_feriale"],        "tariffa":cfg["tariffa_rep_feriale"],   "importo":round(tot["rep_feriale"]        *cfg["tariffa_rep_feriale"],   2)},
        {"voce":"Ind. Reperibilità Semifestiva", "ref":rp,"qty":tot["rep_semifestiva"],    "tariffa":cfg["tariffa_rep_semifestiva"],"importo":round(tot["rep_semifestiva"]   *cfg["tariffa_rep_semifestiva"],2)},
        {"voce":"Ind. Reperibilità Festiva",     "ref":rp,"qty":tot["rep_festiva"],        "tariffa":cfg["tariffa_rep_festiva"],   "importo":round(tot["rep_festiva"]        *cfg["tariffa_rep_festiva"],   2)},
        {"voce":"Festività in giorno di riposo", "ref":rp,"qty":tot["fest_riposo"],       "tariffa":cfg["tariffa_fest_riposo"],   "importo":round(tot["fest_riposo"]        *cfg["tariffa_fest_riposo"]*2, 2)},
    ]
    tc = round(sum(v["importo"] for v in vc), 2)
    inps = round(tc * cfg.get("aliquota_inps",9.19)/100, 2)
    imp_ann = round((tc-inps)*12, 2)

    def irpef(r):
        if r<=0: return 0.0
        imp,res=0.0,r
        for soglia,aliq in [(28000,.23),(22000,.35),(float("inf"),.43)]:
            p=min(res,soglia); imp+=p*aliq; res-=p
            if res<=0: break
        return round(imp,2)

    il = irpef(imp_ann); detr=cfg.get("detrazioni_annue",1955.0)
    if   imp_ann<=15000: det=max(detr,690.0)
    elif imp_ann<=28000: det=round(detr*(28000-imp_ann)/13000,2)
    elif imp_ann<=50000: det=round(658*(50000-imp_ann)/22000,2)
    else: det=0.0
    in_ = max(0.0,round(il-det,2)); im = round(in_/12,2)

    vt = [
        {"voce":f"Contributi INPS ({cfg.get('aliquota_inps',9.19):.2f}%)","importo":inps,"calcolato":True},
        {"voce":"IRPEF stimata mensile (2024)","importo":im,"calcolato":True},
        {"voce":"Trattenuta sindacato (CISL)","importo":cfg["trattenuta_sindacato"]},
        {"voce":"Add. regionale trattenuta",  "importo":cfg["trattenuta_regionale"]},
        {"voce":"Contr. Prev. Compl. (Pegaso)","importo":cfg["trattenuta_pegaso"]},
    ]
    tt = round(sum(v["importo"] for v in vt),2)
    return {"anno":anno,"mese":mese,"mese_prec":mp,"anno_prec":ap,
            "ore_totali":tot,"voci_competenze":vc,"voci_trattenute":vt,
            "tot_competenze":tc,"tot_trattenute":tt,"netto":round(tc-tt,2),
            "dettaglio_fiscale":{"imponibile_annuo_stimato":imp_ann,"irpef_lorda_annua":il,
                                 "detrazione_applicata":det,"irpef_netta_annua":in_,
                                 "inps_mensile":inps,"irpef_mensile":im}}

app.mount("/", StaticFiles(directory="static", html=True), name="static")
