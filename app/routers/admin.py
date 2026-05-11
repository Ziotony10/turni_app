import time
from datetime import datetime
import json
from fastapi import APIRouter, Depends, HTTPException, Request

import app.database as db
from app.config import USE_PG, SQLITE_LOG_BUSY_TIMEOUT_MS
from app.schemas import ResetPasswordInput, DbCleanupPayload, TabellaTurniInput, FeedbackInput
from app.security import require_admin, get_current_user, hash_password
from app.services import _format_date_it, _local_now_iso

router = APIRouter(tags=["admin"])

@router.post("/api/feedback")
def create_feedback(payload: FeedbackInput, user=Depends(get_current_user)):
    msg = (payload.messaggio or "").strip()
    pagina = (payload.pagina or "").strip()[:240]
    if len(msg) < 5:
        raise HTTPException(400, "Scrivi almeno qualche parola per descrivere la segnalazione")
    if len(msg) > 2000:
        raise HTTPException(400, "Feedback troppo lungo: massimo 2000 caratteri")

    conn = db.get_db()
    try:
        db.ex(conn, """INSERT INTO feedback_utenti (user_id, username, nome, messaggio, pagina, created_at)
                       VALUES (?,?,?,?,?,?)""",
              (user["id"], user["username"], user.get("nome"), msg, pagina, _local_now_iso()))
        admins = db.fetchall(conn, "SELECT id FROM utenti WHERE is_admin=1")
        for adm in admins:
            db.ex(conn, """INSERT INTO team_notifications (user_id, title, messaggio, link, letto, created_at)
                           VALUES (?,?,?,?,0,?)""",
                  (adm["id"], "Nuovo feedback", f"{user.get('nome') or user['username']} ha inviato una segnalazione", "/admin.html#sec-feedback", _local_now_iso()))
        conn.commit()
        return {"ok": True}
    finally:
        db.release_db(conn)

# ─── Log Page Visits (Non ha bisogno di permessi, invocata dal JS client) ───
@router.post("/api/log-page-visit")
def log_page_visit(request: Request):
    fwd = request.headers.get("X-Forwarded-For")
    ip = fwd.split(",")[0].strip() if fwd else (request.client.host if request.client else "—")
    ua = request.headers.get("User-Agent", "")
    ref = request.headers.get("Referer", "")
    is_bot = any(kw in ua.lower() for kw in ["bot", "crawler", "spider", "ping", "monitor", "uptime"])
    conn = None
    try:
        if USE_PG:
            conn = db.get_db()
        else:
            conn = db._open_sqlite_connection(SQLITE_LOG_BUSY_TIMEOUT_MS)
        db.ex(conn, "INSERT INTO login_page_visits (ip_address, user_agent, referrer, is_bot, timestamp) VALUES (?,?,?,?,?)",
           (ip, ua, ref, is_bot, _local_now_iso()))
        conn.commit()
    except Exception as e:
        print(f"Errore log visita: {e}")
    finally:
        if conn:
            db.release_db(conn)
    return {"status": "ok"}

# ─── Admin: utenti ─────────────────────────────────────────────────────────────
@router.get("/api/admin/utenti")
def get_utenti(admin=Depends(require_admin)):
    conn = db.get_db()
    rows = db.fetchall(conn, """
        SELECT u.id, u.username, u.nome, u.is_admin, u.is_editor, u.is_team_editor, u.created_at,
               o.id AS linked_operatore_id, o.nome AS linked_operatore_nome, o.posizione AS linked_operatore_posizione
        FROM utenti u
        LEFT JOIN team_operatori o ON o.linked_user_id = u.id AND o.attivo=1
        ORDER BY u.created_at
    """)
    db.release_db(conn)
    return rows

@router.post("/api/admin/utenti/{user_id}/admin")
def toggle_admin(user_id: int, admin=Depends(require_admin)):
    conn = db.get_db()
    user = db.fetchone(conn, "SELECT is_admin, username FROM utenti WHERE id=?", (user_id,))
    if not user: raise HTTPException(404, "Utente non trovato")
    new_val = 0 if user["is_admin"] else 1
    db.ex(conn, "UPDATE utenti SET is_admin=? WHERE id=?", (new_val, user_id))
    conn.commit(); db.release_db(conn)
    return {"ok": True, "is_admin": bool(new_val)}

@router.post("/api/admin/utenti/{user_id}/editor")
def toggle_editor(user_id: int, admin=Depends(require_admin)):
    conn = db.get_db()
    u = db.fetchone(conn, "SELECT is_editor, username FROM utenti WHERE id=?", (user_id,))
    if not u: raise HTTPException(404, "Utente non trovato")
    new_val = 0 if u.get("is_editor") else 1
    db.ex(conn, "UPDATE utenti SET is_editor=? WHERE id=?", (new_val, user_id))
    conn.commit(); db.release_db(conn)
    return {"ok": True, "is_editor": bool(new_val)}

@router.post("/api/admin/utenti/{user_id}/team-editor")
def toggle_team_editor(user_id: int, admin=Depends(require_admin)):
    conn = db.get_db()
    u = db.fetchone(conn, "SELECT is_team_editor, username FROM utenti WHERE id=?", (user_id,))
    if not u: raise HTTPException(404, "Utente non trovato")
    new_val = 0 if u.get("is_team_editor") else 1
    db.ex(conn, "UPDATE utenti SET is_team_editor=? WHERE id=?", (new_val, user_id))
    conn.commit(); db.release_db(conn)
    return {"ok": True, "is_team_editor": bool(new_val)}

@router.delete("/api/admin/utenti/{user_id}")
def delete_user(user_id: int, admin=Depends(require_admin)):
    conn = db.get_db()
    user = db.fetchone(conn, "SELECT username FROM utenti WHERE id=?", (user_id,))
    if not user: raise HTTPException(404, "Utente non trovato")
    db.ex(conn, "DELETE FROM turni WHERE user_id=?", (user_id,))
    db.ex(conn, "DELETE FROM impostazioni WHERE user_id=?", (user_id,))
    db.ex(conn, "UPDATE team_operatori SET linked_user_id=NULL WHERE linked_user_id=?", (user_id,))
    db.ex(conn, "DELETE FROM team_ferie_requests WHERE user_id=?", (user_id,))
    db.ex(conn, "DELETE FROM utenti WHERE id=?", (user_id,))
    conn.commit(); db.release_db(conn)
    return {"ok": True}

@router.post("/api/admin/utenti/{user_id}/reset-password")
def reset_password(user_id: int, payload: ResetPasswordInput, admin=Depends(require_admin)):
    if len(payload.nuova_password) < 6:
        raise HTTPException(400, "Password troppo corta (min 6 caratteri)")
    conn = db.get_db()
    user = db.fetchone(conn, "SELECT username FROM utenti WHERE id=?", (user_id,))
    if not user: raise HTTPException(404, "Utente non trovato")
    db.ex(conn, "UPDATE utenti SET password_hash=? WHERE id=?", (hash_password(payload.nuova_password), user_id))
    conn.commit(); db.release_db(conn)
    return {"ok": True}

# ─── Admin: health / stats / logs ──────────────────────────────────────────────
@router.get("/api/health")
def health_check():
    t0 = time.time()
    try:
        conn = db.get_db()
        db.fetchone(conn, "SELECT 1 as ok")
        db.release_db(conn)
        db_ms = round((time.time() - t0) * 1000, 1)
        db_ok = True
    except:
        db_ms = -1
        db_ok = False
    return {"status": "ok", "db_ok": db_ok, "db_latency_ms": db_ms,
            "db_type": "PostgreSQL" if USE_PG else "SQLite",
            "timestamp": _local_now_iso()}

_status_cache = {"ts": 0, "data": None}
_STATUS_CACHE_TTL = 5

@router.get("/api/admin/status")
def get_status(admin=Depends(require_admin)):
    now = time.time()
    if now - _status_cache["ts"] < _STATUS_CACHE_TTL and _status_cache["data"]:
        return _status_cache["data"]
    t0 = time.time()
    try:
        conn = db.get_db()
        db.fetchone(conn, "SELECT 1 as x")
        db.release_db(conn)
        db_ms = round((time.time() - t0) * 1000, 1)
        db_ok = True
    except:
        db_ms = -1; db_ok = False
    result = {"site": "ok", "db": "ok" if db_ok else "error", "db_ms": db_ms,
              "db_type": "PostgreSQL (Supabase)" if USE_PG else "SQLite"}
    _status_cache["ts"] = now
    _status_cache["data"] = result
    return result

@router.get("/api/admin/stats")
def get_stats(admin=Depends(require_admin)):
    conn = db.get_db()
    stats = {}
    for k, q_str in [
        ("utenti",            "SELECT COUNT(*) as n FROM utenti"),
        ("turni",             "SELECT COUNT(*) as n FROM turni"),
        ("tabelle",           "SELECT COUNT(*) as n FROM tabelle_turni"),
        ("team_operatori",    "SELECT COUNT(*) as n FROM team_operatori WHERE attivo=1"),
    ]:
        try:
            stats[k] = (db.fetchone(conn, q_str) or {}).get("n", 0)
        except:
            stats[k] = 0
    try:
        if USE_PG:
            stats["log_accessi_oggi"] = (db.fetchone(conn,
                "SELECT COUNT(*) as n FROM log_accessi WHERE timestamp::date = CURRENT_DATE") or {}).get("n", 0)
            stats["login_falliti_oggi"] = (db.fetchone(conn,
                "SELECT COUNT(*) as n FROM log_accessi WHERE esito='fallito' AND timestamp::date = CURRENT_DATE") or {}).get("n", 0)
        else:
            stats["log_accessi_oggi"] = (db.fetchone(conn,
                "SELECT COUNT(*) as n FROM log_accessi WHERE date(timestamp) = date('now')") or {}).get("n", 0)
            stats["login_falliti_oggi"] = (db.fetchone(conn,
                "SELECT COUNT(*) as n FROM log_accessi WHERE esito='fallito' AND date(timestamp) = date('now')") or {}).get("n", 0)
    except:
        stats["log_accessi_oggi"] = 0; stats["login_falliti_oggi"] = 0
    db.release_db(conn)
    return stats



@router.get("/api/admin/log-accessi")
def get_log_accessi(limit: int = 200, admin=Depends(require_admin)):
    conn = db.get_db()
    try:
        logs = db.fetchall(conn, f"SELECT * FROM log_accessi ORDER BY id DESC LIMIT {db.get_limit_placeholder()}", (limit,))
    except:
        logs = []
    db.release_db(conn)
    for r in logs:
        if r.get("timestamp") and not isinstance(r["timestamp"], str):
            r["timestamp"] = r["timestamp"].isoformat()
    return logs

@router.get("/api/admin/page-visits")
def get_page_visits(admin=Depends(require_admin)):
    try:
        conn = db.get_db()
        rows = db.fetchall(conn, "SELECT * FROM login_page_visits ORDER BY timestamp DESC LIMIT 200")
        db.release_db(conn)
        return rows
    except Exception as e:
        raise HTTPException(500, str(e))

@router.get("/api/admin/bootstrap")
def get_admin_bootstrap(admin=Depends(require_admin)):
    conn = db.get_db()
    try:
        utenti = db.fetchall(conn, """
            SELECT u.id, u.username, u.nome, u.is_admin, u.is_editor, u.is_team_editor, u.created_at,
                   o.id AS linked_operatore_id, o.nome AS linked_operatore_nome, o.posizione AS linked_operatore_posizione
            FROM utenti u
            LEFT JOIN team_operatori o ON o.linked_user_id = u.id AND o.attivo=1
            ORDER BY u.created_at
        """)
        tabelle = db.fetchall(conn, "SELECT id, nome, tipo, num_settimane, created_at FROM tabelle_turni ORDER BY tipo, nome")
        operatori_links = db.fetchall(conn, """
            SELECT o.id, o.nome, o.posizione, o.linked_user_id, u.username AS linked_username, u.nome AS linked_nome
            FROM team_operatori o
            LEFT JOIN utenti u ON u.id = o.linked_user_id
            WHERE o.attivo=1
            ORDER BY o.posizione
        """)
        ferie_pending = db.fetchall(conn, """
            SELECT r.operatore_id, o.nome AS operatore_nome, u.username, u.nome,
                   COUNT(*) AS giorni,
                   MIN(r.data) AS first_day,
                   MAX(r.data) AS last_day
            FROM team_ferie_requests r
            LEFT JOIN team_operatori o ON o.id = r.operatore_id
            LEFT JOIN utenti u ON u.id = r.user_id
            WHERE r.stato='pending'
            GROUP BY r.operatore_id, o.nome, u.username, u.nome
            ORDER BY first_day, operatore_nome
        """)
        for group in ferie_pending:
            rows = db.fetchall(conn, "SELECT data FROM team_ferie_requests WHERE stato='pending' AND operatore_id=? ORDER BY data",
                            (group["operatore_id"],))
            group["dates"] = [r["data"] for r in rows]
            group["first_day"] = _format_date_it(group.get("first_day"))
            group["last_day"] = _format_date_it(group.get("last_day"))
        ferie_log = db.fetchall(conn, f"SELECT * FROM team_ferie_log ORDER BY id DESC LIMIT {db.get_limit_placeholder()}", (120,))
        for log in ferie_log:
            if "data_turno" in log: log["data_turno"] = _format_date_it(log["data_turno"])
            if "data_modifica" in log: log["data_modifica"] = _format_date_it(log["data_modifica"])
            if "created_at" in log: log["created_at"] = _format_date_it(log["created_at"])
        log_accessi = db.fetchall(conn, f"SELECT * FROM log_accessi ORDER BY id DESC LIMIT {db.get_limit_placeholder()}", (100,))
        for row in log_accessi:
            if row.get("timestamp") and not isinstance(row["timestamp"], str):
                row["timestamp"] = row["timestamp"].isoformat()

        feedback = db.fetchall(conn, f"""
            SELECT id, user_id, username, nome, messaggio, pagina, letto, letto_da, letto_il, created_at
            FROM feedback_utenti
            ORDER BY id DESC
            LIMIT {db.get_limit_placeholder()}
        """, (80,))

        def count(table, where=""):
            sql = f"SELECT COUNT(*) AS cnt FROM {table}" + (f" WHERE {where}" if where else "")
            row = db.fetchone(conn, sql)
            return int((row or {}).get("cnt", 0) or 0)

        return {
            "utenti": utenti,
            "tabelle": tabelle,
            "operatori_links": operatori_links,
            "ferie_pending": ferie_pending,
            "ferie_log": ferie_log,
            "feedback": feedback,
            "log_accessi": log_accessi,
            "db_stats": {
                "ferie_requests_pending":  count("team_ferie_requests", "stato='pending'"),
                "ferie_requests_processed": count("team_ferie_requests", "stato!='pending'"),
                "ferie_requests_total":    count("team_ferie_requests"),
                "ferie_log_total":         count("team_ferie_log"),
                "login_visits_total":      count("login_page_visits"),
                "feedback_unread":         count("feedback_utenti", "letto=0"),
            },
            "status": {
                "site": "ok",
                "db": "ok",
                "db_ms": 0,
                "db_type": "PostgreSQL (Supabase)" if USE_PG else "SQLite",
            },
        }
    finally:
        db.release_db(conn)

@router.get("/api/admin/db-stats")
def get_db_stats(admin=Depends(require_admin)):
    """Restituisce il conteggio delle righe nelle tabelle pulizia."""
    conn = db.get_db()
    try:
        def count(table, where=""):
            sql = f"SELECT COUNT(*) AS cnt FROM {table}" + (f" WHERE {where}" if where else "")
            row = db.fetchone(conn, sql)
            return int((row or {}).get("cnt", 0) or 0)
        return {
            "ferie_requests_pending":  count("team_ferie_requests", "stato='pending'"),
            "ferie_requests_processed": count("team_ferie_requests", "stato!='pending'"),
            "ferie_requests_total":    count("team_ferie_requests"),
            "ferie_log_total":         count("team_ferie_log"),
            "login_visits_total":      count("login_page_visits"),
            "feedback_unread":         count("feedback_utenti", "letto=0"),
        }
    finally:
        db.release_db(conn)

@router.get("/api/admin/feedback")
def get_feedback(limit: int = 100, admin=Depends(require_admin)):
    conn = db.get_db()
    try:
        return db.fetchall(conn, f"""
            SELECT id, user_id, username, nome, messaggio, pagina, letto, letto_da, letto_il, created_at
            FROM feedback_utenti
            ORDER BY id DESC
            LIMIT {db.get_limit_placeholder()}
        """, (limit,))
    finally:
        db.release_db(conn)

@router.post("/api/admin/feedback/{feedback_id}/read")
def mark_feedback_read(feedback_id: int, admin=Depends(require_admin)):
    conn = db.get_db()
    try:
        row = db.fetchone(conn, "SELECT id FROM feedback_utenti WHERE id=?", (feedback_id,))
        if not row:
            raise HTTPException(404, "Feedback non trovato")
        db.ex(conn, "UPDATE feedback_utenti SET letto=1, letto_da=?, letto_il=? WHERE id=?",
              (admin["username"], _local_now_iso(), feedback_id))
        conn.commit()
        return {"ok": True}
    finally:
        db.release_db(conn)

@router.post("/api/admin/db-cleanup")
def db_cleanup(payload: DbCleanupPayload, admin=Depends(require_admin)):
    """Cancella le righe dal target specificato."""
    conn = db.get_db()
    try:
        def count_rows(table, where=""):
            sql = f"SELECT COUNT(*) AS cnt FROM {table}" + (f" WHERE {where}" if where else "")
            row = db.fetchone(conn, sql)
            return int((row or {}).get("cnt", 0) or 0)

        if payload.target == "ferie_closed_history":
            processed_deleted = count_rows("team_ferie_requests", "stato != 'pending'")
            log_deleted = count_rows("team_ferie_log")
            db.ex(conn, "DELETE FROM team_ferie_requests WHERE stato != 'pending'")
            db.ex(conn, "DELETE FROM team_ferie_log")
            deleted = processed_deleted + log_deleted
            msg = "Storico ferie concluse ripulito"
        elif payload.target == "ferie_requests_processed":
            deleted = count_rows("team_ferie_requests", "stato != 'pending'")
            db.ex(conn, "DELETE FROM team_ferie_requests WHERE stato != 'pending'")
            msg = "Richieste ferie processate cancellate"
        elif payload.target == "ferie_requests_all":
            deleted = count_rows("team_ferie_requests")
            db.ex(conn, "DELETE FROM team_ferie_requests")
            msg = "Tutte le richieste ferie cancellate"
        elif payload.target == "ferie_log":
            deleted = count_rows("team_ferie_log")
            db.ex(conn, "DELETE FROM team_ferie_log")
            msg = "Log ferie cancellato"
        elif payload.target == "login_visits":
            deleted = count_rows("login_page_visits")
            db.ex(conn, "DELETE FROM login_page_visits")
            msg = "Log visite pagina login cancellato"
        else:
            raise HTTPException(400, "Target non valido")
        conn.commit()
        if not USE_PG:
            try:
                db.ex(conn, "PRAGMA optimize")
            except Exception:
                pass
        if deleted <= 0:
            return {"ok": True, "msg": "Nessun record da cancellare", "deleted": 0}
        return {"ok": True, "msg": f"{msg}: {deleted} record", "deleted": deleted}
    finally:
        db.release_db(conn)

# ─── Admin: tabelle turni ──────────────────────────────────────────────────────
@router.get("/api/admin/tabelle")
def get_tabelle(user=Depends(get_current_user)):
    conn = db.get_db()
    rows = db.fetchall(conn, "SELECT id, nome, tipo, num_settimane, created_at FROM tabelle_turni ORDER BY tipo, nome")
    db.release_db(conn)
    return rows

@router.get("/api/admin/tabelle/{tab_id}")
def get_tabella(tab_id: int, user=Depends(get_current_user)):
    conn = db.get_db()
    row = db.fetchone(conn, "SELECT * FROM tabelle_turni WHERE id=?", (tab_id,))
    db.release_db(conn)
    if not row: raise HTTPException(404, "Tabella non trovata")
    row["turni_json"] = json.loads(row["turni_json"])
    return row

@router.post("/api/admin/tabelle")
def create_tabella(payload: TabellaTurniInput, admin=Depends(require_admin)):
    conn = db.get_db()
    db.ex(conn, "INSERT INTO tabelle_turni (nome, tipo, num_settimane, turni_json) VALUES (?,?,?,?)",
       (payload.nome, payload.tipo, payload.num_settimane, json.dumps(payload.turni)))
    conn.commit(); db.release_db(conn)
    return {"ok": True}

@router.put("/api/admin/tabelle/{tab_id}")
def update_tabella(tab_id: int, payload: TabellaTurniInput, admin=Depends(require_admin)):
    conn = db.get_db()
    db.ex(conn, "UPDATE tabelle_turni SET nome=?, tipo=?, num_settimane=?, turni_json=? WHERE id=?",
       (payload.nome, payload.tipo, payload.num_settimane, json.dumps(payload.turni), tab_id))
    conn.commit(); db.release_db(conn)
    return {"ok": True}

@router.delete("/api/admin/tabelle/{tab_id}")
def delete_tabella(tab_id: int, admin=Depends(require_admin)):
    conn = db.get_db()
    db.ex(conn, "DELETE FROM tabelle_turni WHERE id=?", (tab_id,))
    conn.commit(); db.release_db(conn)
    return {"ok": True}
