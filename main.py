from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from typing import Optional
import sqlite3
from datetime import date, datetime, timedelta
from jose import JWTError, jwt
from passlib.context import CryptContext
import secrets

app = FastAPI(title="Gestione Turni")
DB_PATH = "turni.db"

# ─── Sicurezza ───────────────────────────────────────────────────────────────
SECRET_KEY   = secrets.token_hex(32)   # rigenerato ad ogni riavvio (va bene per uso locale)
ALGORITHM    = "HS256"
TOKEN_EXPIRE = 60 * 24  # minuti → 24h

pwd_context = CryptContext(schemes=["sha256_crypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

# ─── Turni config ─────────────────────────────────────────────────────────────
TURNO_ORARI = {
    "M":  (7*60,  15*60),
    "M1": (8*60,  16*60),
    "M2": (9*60,  17*60),
    "M3": (11*60, 19*60),
    "P":  (15*60, 23*60),
    "N":  (23*60, 7*60),
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

# ─── Calcolo ore ──────────────────────────────────────────────────────────────
def split_diurno_notturno(start: int, end: int) -> tuple:
    if end <= start: end += 1440
    notturni = [(0, 360), (1200, 1440), (1440, 1800)]
    nott = 0
    for ns, ne in notturni:
        ov_s = max(start, ns); ov_e = min(end, ne)
        if ov_e > ov_s: nott += ov_e - ov_s
    total = end - start
    return round((total - nott) / 60, 2), round(nott / 60, 2)

def to_min(s: str) -> Optional[int]:
    try:
        h, m = s.strip().split(":")
        return int(h) * 60 + int(m)
    except: return None

def calcola_ore(turno: str, ora_inizio: Optional[str], ora_fine: Optional[str], data_str: str) -> dict:
    result = {"ore_diurne": 0.0, "ore_notturne": 0.0,
              "strao_diurno": 0.0, "strao_notturno": 0.0,
              "strao_fest_diurno": 0.0, "strao_fest_notturno": 0.0}
    eff_ini = to_min(ora_inizio) if ora_inizio else None
    eff_fin = to_min(ora_fine)   if ora_fine   else None
    std     = TURNO_ORARI.get(turno)

    if turno == "R":
        if eff_ini is not None and eff_fin is not None:
            d, n = split_diurno_notturno(eff_ini, eff_fin)
            result["strao_fest_diurno"] = d; result["strao_fest_notturno"] = n
        return result
    if turno == "RC":
        if eff_ini is not None and eff_fin is not None:
            d, n = split_diurno_notturno(eff_ini, eff_fin)
            result["strao_diurno"] = d; result["strao_notturno"] = n
        return result
    if not std:
        if eff_ini is not None and eff_fin is not None:
            d, n = split_diurno_notturno(eff_ini, eff_fin)
            result["strao_diurno"] = d; result["strao_notturno"] = n
        return result

    std_ini, std_fin = std
    if eff_ini is None and eff_fin is None:
        d, n = split_diurno_notturno(std_ini, std_fin)
        result["ore_diurne"] = d; result["ore_notturne"] = n
        return result

    ini = eff_ini if eff_ini is not None else std_ini
    fin = eff_fin if eff_fin is not None else std_fin
    std_fin_norm = std_fin if std_fin > std_ini else std_fin + 1440
    fin_norm     = fin     if fin     > ini     else fin + 1440

    ord_ini = max(ini, std_ini); ord_fin = min(fin_norm, std_fin_norm)
    if ord_fin > ord_ini:
        d, n = split_diurno_notturno(ord_ini, ord_fin)
        result["ore_diurne"] += d; result["ore_notturne"] += n
    if ini < std_ini:
        d, n = split_diurno_notturno(ini, std_ini)
        result["strao_diurno"] += d; result["strao_notturno"] += n
    if fin_norm > std_fin_norm:
        d, n = split_diurno_notturno(std_fin_norm, fin_norm)
        result["strao_diurno"] += d; result["strao_notturno"] += n
    return result

def calcola_tipo_reperibilita(turno: str, data_str: str) -> str:
    if turno == "RC": return "semifestiva"
    if turno == "R" or data_str in FESTIVITA: return "festiva"
    if TURNI_CONFIG.get(turno, {}).get("lavorativo"): return "feriale"
    return ""

# ─── DB ───────────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

IMPOSTAZIONI_DEFAULTS = {
    "retribuzione_totale":     "2573.39",
    "tariffa_nott_50":         "7.53974",
    "tariffa_dom":             "8.39811",
    "tariffa_nott_ord":        "5.27782",
    "tariffa_strao_fer_d":     "22.61922",
    "tariffa_strao_fer_n":     "24.12716",
    "tariffa_strao_fest_d":    "24.12716",
    "tariffa_strao_fest_n":    "24.36517",
    "tariffa_rep_feriale":     "15.26",
    "tariffa_rep_semifestiva": "32.99",
    "tariffa_rep_festiva":     "53.13",
    "indennita_turno":         "279.66",
    "trattenuta_sindacato":    "18.86",
    "trattenuta_regionale":    "50.00",
    "trattenuta_pegaso":       "33.90",
    "aliquota_inps":           "9.19",
    "detrazioni_annue":        "1955.00",
}

def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS utenti (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            username      TEXT UNIQUE NOT NULL,
            nome          TEXT,
            password_hash TEXT NOT NULL,
            created_at    TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS turni (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id             INTEGER NOT NULL,
            data                TEXT NOT NULL,
            turno               TEXT,
            ora_inizio          TEXT,
            ora_fine            TEXT,
            ore_diurne          REAL DEFAULT 0,
            ore_notturne        REAL DEFAULT 0,
            strao_diurno        REAL DEFAULT 0,
            strao_notturno      REAL DEFAULT 0,
            strao_fest_diurno   REAL DEFAULT 0,
            strao_fest_notturno REAL DEFAULT 0,
            reperibilita        TEXT,
            note                TEXT,
            UNIQUE(user_id, data)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS impostazioni (
            user_id INTEGER NOT NULL,
            chiave  TEXT NOT NULL,
            valore  TEXT,
            PRIMARY KEY (user_id, chiave)
        )
    """)
    # Migrazione da vecchio schema (senza user_id)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(turni)").fetchall()]
    if "user_id" not in cols:
        conn.execute("ALTER TABLE turni ADD COLUMN user_id INTEGER NOT NULL DEFAULT 1")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_turni_user_data ON turni(user_id, data)")
    for col, typ in [("ora_inizio","TEXT"),("ora_fine","TEXT"),("ore_diurne","REAL"),
                     ("ore_notturne","REAL"),("strao_fest_diurno","REAL"),("strao_fest_notturno","REAL")]:
        if col not in cols:
            conn.execute(f"ALTER TABLE turni ADD COLUMN {col} {typ} DEFAULT 0")
    # Migrazione impostazioni: aggiungi user_id se manca
    imp_cols = [r[1] for r in conn.execute("PRAGMA table_info(impostazioni)").fetchall()]
    if "user_id" not in imp_cols:
        # Ricrea la tabella impostazioni con user_id
        conn.execute("ALTER TABLE impostazioni RENAME TO impostazioni_old")
        conn.execute("""
            CREATE TABLE impostazioni (
                user_id INTEGER NOT NULL,
                chiave  TEXT NOT NULL,
                valore  TEXT,
                PRIMARY KEY (user_id, chiave)
            )
        """)
        # Migra i dati vecchi assegnandoli all'utente 1
        conn.execute("INSERT INTO impostazioni (user_id, chiave, valore) SELECT 1, chiave, valore FROM impostazioni_old")
        conn.execute("DROP TABLE impostazioni_old")
    conn.commit()
    conn.close()

init_db()

# ─── Auth helpers ─────────────────────────────────────────────────────────────
def hash_password(pwd: str) -> str:
    return pwd_context.hash(pwd)

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)

def create_token(user_id: int, username: str) -> str:
    expire = datetime.utcnow() + timedelta(minutes=TOKEN_EXPIRE)
    return jwt.encode({"sub": str(user_id), "username": username, "exp": expire}, SECRET_KEY, algorithm=ALGORITHM)

def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = int(payload.get("sub"))
        username = payload.get("username")
        if not user_id: raise HTTPException(status_code=401, detail="Token non valido")
        return {"id": user_id, "username": username}
    except JWTError:
        raise HTTPException(status_code=401, detail="Token non valido o scaduto")

def get_user_settings(user_id: int, conn) -> dict:
    rows = conn.execute("SELECT chiave, valore FROM impostazioni WHERE user_id=?", (user_id,)).fetchall()
    result = {k: float(v) for k, v in IMPOSTAZIONI_DEFAULTS.items()}
    for r in rows:
        try: result[r["chiave"]] = float(r["valore"])
        except: pass
    return result

# ─── Modelli ──────────────────────────────────────────────────────────────────
class RegisterInput(BaseModel):
    username: str
    password: str
    nome:     Optional[str] = None

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
    if len(payload.username) < 3:
        raise HTTPException(400, "Username troppo corto (min 3 caratteri)")
    if len(payload.password) < 6:
        raise HTTPException(400, "Password troppo corta (min 6 caratteri)")
    conn = get_db()
    try:
        conn.execute("INSERT INTO utenti (username, nome, password_hash) VALUES (?,?,?)",
                     (payload.username.strip().lower(), payload.nome or payload.username,
                      hash_password(payload.password)))
        conn.commit()
        user = conn.execute("SELECT id FROM utenti WHERE username=?",
                            (payload.username.strip().lower(),)).fetchone()
        token = create_token(user["id"], payload.username.strip().lower())
        conn.close()
        return {"access_token": token, "token_type": "bearer", "username": payload.username}
    except sqlite3.IntegrityError:
        conn.close()
        raise HTTPException(400, "Username già esistente")

@app.post("/api/auth/login")
def login(form: OAuth2PasswordRequestForm = Depends()):
    conn = get_db()
    user = conn.execute("SELECT * FROM utenti WHERE username=?",
                        (form.username.strip().lower(),)).fetchone()
    conn.close()
    if not user or not verify_password(form.password, user["password_hash"]):
        raise HTTPException(401, "Credenziali non corrette")
    token = create_token(user["id"], user["username"])
    return {"access_token": token, "token_type": "bearer",
            "username": user["username"], "nome": user["nome"]}

@app.get("/api/auth/me")
def me(current_user = Depends(get_current_user)):
    return current_user

# ─── API (protette) ───────────────────────────────────────────────────────────
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
def get_turni_mese(anno: int, mese: int, user = Depends(get_current_user)):
    conn = get_db()
    rows = conn.execute("SELECT * FROM turni WHERE user_id=? AND data LIKE ?",
                        (user["id"], f"{anno:04d}-{mese:02d}-%")).fetchall()
    conn.close()
    return {dict(r)["data"]: dict(r) for r in rows}

@app.post("/api/turni/{data}")
def set_turno(data: str, payload: TurnoInput, user = Depends(get_current_user)):
    ore      = calcola_ore(payload.turno or "", payload.ora_inizio, payload.ora_fine, data)
    tipo_rep = calcola_tipo_reperibilita(payload.turno or "", data) if payload.reperibilita else ""
    conn = get_db()
    conn.execute("""
        INSERT INTO turni
          (user_id, data, turno, ora_inizio, ora_fine,
           ore_diurne, ore_notturne, strao_diurno, strao_notturno,
           strao_fest_diurno, strao_fest_notturno, reperibilita, note)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(user_id, data) DO UPDATE SET
          turno=excluded.turno, ora_inizio=excluded.ora_inizio, ora_fine=excluded.ora_fine,
          ore_diurne=excluded.ore_diurne, ore_notturne=excluded.ore_notturne,
          strao_diurno=excluded.strao_diurno, strao_notturno=excluded.strao_notturno,
          strao_fest_diurno=excluded.strao_fest_diurno, strao_fest_notturno=excluded.strao_fest_notturno,
          reperibilita=excluded.reperibilita, note=excluded.note
    """, (user["id"], data, payload.turno, payload.ora_inizio, payload.ora_fine,
          ore["ore_diurne"], ore["ore_notturne"], ore["strao_diurno"], ore["strao_notturno"],
          ore["strao_fest_diurno"], ore["strao_fest_notturno"], tipo_rep or None, payload.note))
    conn.commit(); conn.close()
    return {"ok": True, **ore, "tipo_reperibilita": tipo_rep}

@app.delete("/api/turni/{data}")
def delete_turno(data: str, user = Depends(get_current_user)):
    conn = get_db()
    conn.execute("DELETE FROM turni WHERE user_id=? AND data=?", (user["id"], data))
    conn.commit(); conn.close()
    return {"ok": True}

@app.get("/api/riepilogo/{anno}")
def get_riepilogo(anno: int, user = Depends(get_current_user)):
    conn = get_db()
    rows = conn.execute("SELECT * FROM turni WHERE user_id=? AND data LIKE ?",
                        (user["id"], f"{anno:04d}-%")).fetchall()
    conn.close()
    mesi = {m: {"ore_diurne":0.0,"ore_notturne":0.0,"strao_diurno":0.0,"strao_notturno":0.0,
                "strao_fest_diurno":0.0,"strao_fest_notturno":0.0,
                "reperibilita_feriale":0,"reperibilita_semifestiva":0,"reperibilita_festiva":0,
                "mal":0,"ferie":0,"rc":0,"r":0,"rot":0,"rf":0} for m in range(1,13)}
    for r in rows:
        d = dict(r); mes = int(d["data"].split("-")[1]); m = mesi[mes]; t = d.get("turno") or ""
        for c in ["ore_diurne","ore_notturne","strao_diurno","strao_notturno","strao_fest_diurno","strao_fest_notturno"]:
            m[c] += d.get(c) or 0
        if t=="MAL": m["mal"]+=1
        if t in("F","F-P","F-N"): m["ferie"]+=1
        if t=="RC": m["rc"]+=1
        if t=="R": m["r"]+=1
        if t=="ROT": m["rot"]+=1
        if t=="RF": m["rf"]+=1
        rep = d.get("reperibilita") or ""
        if rep=="feriale": m["reperibilita_feriale"]+=1
        elif rep=="semifestiva": m["reperibilita_semifestiva"]+=1
        elif rep=="festiva": m["reperibilita_festiva"]+=1
    return mesi

@app.get("/api/impostazioni")
def get_impostazioni(user = Depends(get_current_user)):
    conn = get_db()
    cfg = get_user_settings(user["id"], conn)
    conn.close()
    return cfg

@app.post("/api/impostazioni")
def set_impostazioni(payload: ImpostazioniInput, user = Depends(get_current_user)):
    conn = get_db()
    for k, v in payload.valori.items():
        conn.execute("INSERT OR REPLACE INTO impostazioni (user_id, chiave, valore) VALUES (?,?,?)",
                     (user["id"], k, str(v)))
    conn.commit(); conn.close()
    return {"ok": True}

@app.get("/api/bustapaga/{anno}/{mese}")
def get_busta_paga(anno: int, mese: int, user = Depends(get_current_user)):
    mese_prec = mese - 1 if mese > 1 else 12
    anno_prec = anno if mese > 1 else anno - 1
    conn = get_db()
    rows_prec = conn.execute("SELECT * FROM turni WHERE user_id=? AND data LIKE ?",
                             (user["id"], f"{anno_prec:04d}-{mese_prec:02d}-%")).fetchall()
    cfg = get_user_settings(user["id"], conn)
    conn.close()

    tot = {"ore_diurne":0.0,"ore_notturne":0.0,"strao_diurno":0.0,"strao_notturno":0.0,
           "strao_fest_diurno":0.0,"strao_fest_notturno":0.0,
           "rep_feriale":0,"rep_semifestiva":0,"rep_festiva":0,
           "domeniche":0,"giorni_lavoro":0,
           "notte_assenza":0.0}  # ore notturne teoriche nei giorni di assenza

    # Ore notturne teoriche per turno di assenza
    NOTTE_ASSENZA = {"F": 0.0, "F-P": 3.0, "F-N": 7.0}
    for r in rows_prec:
        d = dict(r); t = d.get("turno") or ""
        if TURNI_CONFIG.get(t,{}).get("lavorativo"):
            tot["giorni_lavoro"] += 1
            if date.fromisoformat(d["data"]).weekday() == 6: tot["domeniche"] += 1
        for c in ["ore_diurne","ore_notturne","strao_diurno","strao_notturno","strao_fest_diurno","strao_fest_notturno"]:
            tot[c] += d.get(c) or 0
        rep = d.get("reperibilita") or ""
        if rep=="feriale": tot["rep_feriale"]+=1
        elif rep=="semifestiva": tot["rep_semifestiva"]+=1
        elif rep=="festiva": tot["rep_festiva"]+=1

        # Ore notturne teoriche nei giorni di assenza (per Lavoro Ordinario Notte)
        if t in NOTTE_ASSENZA:
            tot["notte_assenza"] += NOTTE_ASSENZA[t]

    mesi_it = ["","Gen","Feb","Mar","Apr","Mag","Giu","Lug","Ago","Set","Ott","Nov","Dic"]
    ref_prec = f"{mesi_it[mese_prec]}/{str(anno_prec)[-2:]}"
    ref_corr = f"{mesi_it[mese]}/{str(anno)[-2:]}"

    voci_competenze = [
        {"voce":"Retribuzione totale mensile",   "ref":ref_corr,"qty":None,"tariffa":None,                          "importo":cfg["retribuzione_totale"]},
        {"voce":"Indennità turno X",             "ref":ref_corr,"qty":None,"tariffa":None,                          "importo":cfg["indennita_turno"]},
        {"voce":"Ore notturne in turno 50%",     "ref":ref_prec,"qty":tot["ore_notturne"],       "tariffa":cfg["tariffa_nott_50"],        "importo":round(tot["ore_notturne"]       *cfg["tariffa_nott_50"],       2)},
        {"voce":"Indennità lavoro domenicale",   "ref":ref_prec,"qty":tot["domeniche"]*8,        "tariffa":cfg["tariffa_dom"],            "importo":round(tot["domeniche"]*8        *cfg["tariffa_dom"],           2)},
        {"voce":"Lavoro ordinario notte",        "ref":ref_prec,"qty":tot["notte_assenza"],       "tariffa":cfg["tariffa_nott_ord"],       "importo":round(tot["notte_assenza"]       *cfg["tariffa_nott_ord"],      2)},
        {"voce":"Str. Feriale Diurno 150%",      "ref":ref_prec,"qty":tot["strao_diurno"],       "tariffa":cfg["tariffa_strao_fer_d"],   "importo":round(tot["strao_diurno"]       *cfg["tariffa_strao_fer_d"],   2)},
        {"voce":"Str. Feriale Notturno 160%",    "ref":ref_prec,"qty":tot["strao_notturno"],     "tariffa":cfg["tariffa_strao_fer_n"],   "importo":round(tot["strao_notturno"]     *cfg["tariffa_strao_fer_n"],   2)},
        {"voce":"Str. Festivo Diurno 160%",      "ref":ref_prec,"qty":tot["strao_fest_diurno"],  "tariffa":cfg["tariffa_strao_fest_d"],  "importo":round(tot["strao_fest_diurno"]  *cfg["tariffa_strao_fest_d"],  2)},
        {"voce":"Str. Festivo Notturno 175%",    "ref":ref_prec,"qty":tot["strao_fest_notturno"],"tariffa":cfg["tariffa_strao_fest_n"],  "importo":round(tot["strao_fest_notturno"]*cfg["tariffa_strao_fest_n"],  2)},
        {"voce":"Ind. Reperibilità Feriale",     "ref":ref_prec,"qty":tot["rep_feriale"],        "tariffa":cfg["tariffa_rep_feriale"],   "importo":round(tot["rep_feriale"]        *cfg["tariffa_rep_feriale"],   2)},
        {"voce":"Ind. Reperibilità Semifestiva", "ref":ref_prec,"qty":tot["rep_semifestiva"],    "tariffa":cfg["tariffa_rep_semifestiva"],"importo":round(tot["rep_semifestiva"]   *cfg["tariffa_rep_semifestiva"],2)},
        {"voce":"Ind. Reperibilità Festiva",     "ref":ref_prec,"qty":tot["rep_festiva"],        "tariffa":cfg["tariffa_rep_festiva"],   "importo":round(tot["rep_festiva"]        *cfg["tariffa_rep_festiva"],   2)},
    ]
    tot_competenze = round(sum(v["importo"] for v in voci_competenze), 2)
    aliq_inps = cfg.get("aliquota_inps", 9.19) / 100
    inps_mensile = round(tot_competenze * aliq_inps, 2)
    imponibile_annuo = round((tot_competenze - inps_mensile) * 12, 2)

    def irpef_annua(r):
        if r <= 0: return 0.0
        imp, res = 0.0, r
        for soglia, aliq in [(28000,.23),(22000,.35),(float('inf'),.43)]:
            p = min(res, soglia); imp += p*aliq; res -= p
            if res <= 0: break
        return round(imp, 2)

    irpef_lorda = irpef_annua(imponibile_annuo)
    detr = cfg.get("detrazioni_annue", 1955.0)
    if   imponibile_annuo <= 15000: detrazione = max(detr, 690.0)
    elif imponibile_annuo <= 28000: detrazione = round(detr*(28000-imponibile_annuo)/13000, 2)
    elif imponibile_annuo <= 50000: detrazione = round(658*(50000-imponibile_annuo)/22000, 2)
    else: detrazione = 0.0
    irpef_netta = max(0.0, round(irpef_lorda - detrazione, 2))
    irpef_mensile = round(irpef_netta / 12, 2)

    voci_trattenute = [
        {"voce":f"Contributi INPS ({cfg.get('aliquota_inps',9.19):.2f}%)","importo":inps_mensile,"calcolato":True},
        {"voce":"IRPEF stimata mensile (scaglioni 2024)",                  "importo":irpef_mensile,"calcolato":True},
        {"voce":"Trattenuta sindacato (CISL)", "importo":cfg["trattenuta_sindacato"]},
        {"voce":"Add. regionale trattenuta",   "importo":cfg["trattenuta_regionale"]},
        {"voce":"Contr. Prev. Compl. (Pegaso)","importo":cfg["trattenuta_pegaso"]},
    ]
    tot_trattenute = round(sum(v["importo"] for v in voci_trattenute), 2)
    return {
        "anno":anno,"mese":mese,"mese_prec":mese_prec,"anno_prec":anno_prec,
        "ore_totali":tot,"voci_competenze":voci_competenze,"voci_trattenute":voci_trattenute,
        "tot_competenze":tot_competenze,"tot_trattenute":tot_trattenute,
        "netto":round(tot_competenze-tot_trattenute,2),
        "dettaglio_fiscale":{"imponibile_annuo_stimato":imponibile_annuo,"irpef_lorda_annua":irpef_lorda,
                             "detrazione_applicata":detrazione,"irpef_netta_annua":irpef_netta,
                             "inps_mensile":inps_mensile,"irpef_mensile":irpef_mensile},
    }

app.mount("/", StaticFiles(directory="static", html=True), name="static")
