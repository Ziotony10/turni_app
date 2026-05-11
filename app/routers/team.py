import io
from datetime import datetime, date, timedelta
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

try:
    from weasyprint import HTML as WeasyHTML
    WEASYPRINT_AVAILABLE = True
except Exception:
    WeasyHTML = None
    WEASYPRINT_AVAILABLE = False

import app.database as db
from app.config import USE_PG
from app.schemas import (
    TeamOperatoriInput, TeamOperatoreUpdateInput, TeamOperatoreLinkInput,
    TeamCellaInput, TeamColonneDestraInput, TeamFerieBatchInput,
    TeamFerieReviewInput, TeamTemplateWeekInput, TeamFerieRangeApplyInput,
    TeamSwapRequestInput, TeamSwapActionInput, TeamNotificationUpdateInput
)
from app.security import get_current_user, require_team_editor, require_admin
from app.services import (
    get_team_operator_for_user, _parse_iso_date, _log_team_ferie,
    _build_team_turni_payload, _preserve_team_schedule_outside_range, _clear_team_schedule_in_range,
    _apply_team_ferie_to_var, _format_date_it, _create_notification,
    _load_team_template_context, _compute_team_template_slot, _local_now_iso
)

router = APIRouter(tags=["team"])

def _notify_team_editors(conn, title: str, message: str, exclude_user_id: int = None):
    editors = db.fetchall(conn, "SELECT id FROM utenti WHERE is_admin=1 OR is_editor=1 OR is_team_editor=1")
    for editor in editors:
        if exclude_user_id and editor["id"] == exclude_user_id:
            continue
        _create_notification(conn, editor["id"], message, title=title, link="/turni-team.html")

def _effective_swap_col(turno_row):
    """VAR prevale su TAB: il click seleziona giorno+operatore, non la colonna fisica."""
    if turno_row and (turno_row.get("turno_var") or "").strip():
        return "var"
    return "base"

def _swap_value(turno_row, col):
    key = "turno_var" if col == "var" else "turno_base"
    return ((turno_row or {}).get(key) or "").strip()

def _swap_flags(turno_row, col):
    key = "flags_var" if col == "var" else "flags_base"
    return ((turno_row or {}).get(key) or "").strip()

def _valid_swap_col(col):
    return col if col in {"base", "var"} else None

def _effective_team_turno_row(conn, data_str: str, operatore_id: int):
    existing = db.fetchone(conn, "SELECT * FROM team_turni WHERE data=? AND operatore_id=?", (data_str, operatore_id))
    if existing:
        if not (existing.get("flags_base") or existing.get("flags_var")) and existing.get("flags"):
            if existing.get("turno_var"):
                existing["flags_var"] = existing.get("flags") or ""
            else:
                existing["flags_base"] = existing.get("flags") or ""
        return existing

    op = db.fetchone(conn, "SELECT id, posizione FROM team_operatori WHERE id=? AND attivo=1", (operatore_id,))
    d_obj = _parse_iso_date(data_str)
    if not op or not d_obj:
        return {"turno_base": "", "turno_var": "", "flags_base": "", "flags_var": ""}

    template, op_count, start_obj, end_obj, start_week_monday = _load_team_template_context(conn)
    in_template_range = True
    if start_obj and d_obj < start_obj:
        in_template_range = False
    if end_obj and d_obj > end_obj:
        in_template_range = False
    if not in_template_range:
        return {"turno_base": "", "turno_var": "", "flags_base": "", "flags_var": ""}

    tpl = _compute_team_template_slot(template, d_obj, op["posizione"], op_count, start_week_monday)
    return {
        "turno_base": tpl.get("turno_base", "") or "",
        "turno_var": tpl.get("turno_var", "") or "",
        "flags_base": tpl.get("flags", "") or "",
        "flags_var": "",
    }

# ─── Team: operatori ───────────────────────────────────────────────────────────
@router.get("/api/team/me")
def team_me(user=Depends(get_current_user)):
    conn = db.get_db()
    op = get_team_operator_for_user(conn, user["id"])
    pending_count_row = db.fetchone(conn, "SELECT COUNT(*) AS cnt FROM team_ferie_requests WHERE stato='pending'")
    pending_rows = db.fetchall(conn, """
        SELECT operatore_id, operatore_nome, username, COUNT(*) AS giorni
        FROM (
            SELECT r.operatore_id, o.nome AS operatore_nome, u.username, r.data
            FROM team_ferie_requests r
            LEFT JOIN team_operatori o ON o.id = r.operatore_id
            LEFT JOIN utenti u ON u.id = r.user_id
            WHERE r.stato='pending'
        ) x
        GROUP BY operatore_id, operatore_nome, username
        ORDER BY operatore_nome
    """)
    db.release_db(conn)
    return {
        "is_editor": bool(user.get("is_editor")) or bool(user.get("is_admin")),
        "is_admin": bool(user.get("is_admin")),
        "linked_operatore_id": op["id"] if op else None,
        "linked_operatore_nome": op["nome"] if op else None,
        "linked_operatore_posizione": op["posizione"] if op else None,
        "can_request_ferie": bool(op),
        "ferie_pending_count": int((pending_count_row or {}).get("cnt", 0) or 0),
        "ferie_pending_summary": pending_rows,
    }

@router.get("/api/team/operatori")
def get_team_operatori(user=Depends(get_current_user)):
    conn = db.get_db()
    ops = db.fetchall(conn, """
        SELECT o.*, u.username AS linked_username, u.nome AS linked_nome
        FROM team_operatori o
        LEFT JOIN utenti u ON u.id = o.linked_user_id
        WHERE o.attivo=1
        ORDER BY o.posizione
    """)
    db.release_db(conn)
    return ops

@router.post("/api/team/operatori")
def save_operatori(payload: TeamOperatoriInput, user=Depends(require_team_editor)):
    conn = db.get_db()
    existing = db.fetchall(conn, "SELECT id, posizione, linked_user_id FROM team_operatori")
    by_position = {row["posizione"]: row for row in existing}
    active_positions = set()
    for op in payload.operatori:
        nome = op.nome.strip()
        if not nome:
            continue
        active_positions.add(op.posizione)
        row = by_position.get(op.posizione)
        if row:
            db.ex(conn, "UPDATE team_operatori SET nome=?, attivo=1 WHERE id=?", (nome, row["id"]))
        else:
            db.ex(conn, "INSERT INTO team_operatori (nome, posizione, attivo) VALUES (?,?,1)",
               (nome, op.posizione))
    for posizione, row in by_position.items():
        if posizione not in active_positions:
            db.ex(conn, "UPDATE team_operatori SET attivo=0 WHERE id=?", (row["id"],))
    max_active_position = max(active_positions) if active_positions else 0
    db.ex(conn, "DELETE FROM team_template_weekly WHERE posizione > ?", (max_active_position,))
    for campo in ("rep1_pos", "rep2_pos", "rep3_pos", "fest_m1_pos", "fest_m2_pos", "fest_p1_pos", "fest_p2_pos"):
        db.ex(conn,
           f"UPDATE team_template_reperibili_weekly SET {campo}=NULL WHERE COALESCE({campo}, 0) > ?",
           (max_active_position,))
    conn.commit(); db.release_db(conn)
    return {"ok": True}

@router.get("/api/admin/team/operatori-links")
def get_team_operatori_links(admin=Depends(require_admin)):
    conn = db.get_db()
    rows = db.fetchall(conn, """
        SELECT o.id, o.nome, o.posizione, o.linked_user_id, u.username AS linked_username, u.nome AS linked_nome
        FROM team_operatori o
        LEFT JOIN utenti u ON u.id = o.linked_user_id
        WHERE o.attivo=1
        ORDER BY o.posizione
    """)
    db.release_db(conn)
    return rows

@router.post("/api/admin/team/operatori/{op_id}/link")
def set_team_operatore_link(op_id: int, payload: TeamOperatoreLinkInput, admin=Depends(require_admin)):
    conn = db.get_db()
    op = db.fetchone(conn, "SELECT id, nome, linked_user_id FROM team_operatori WHERE id=? AND attivo=1", (op_id,))
    if not op:
        db.release_db(conn)
        raise HTTPException(404, "Operatore non trovato")
    user_id = payload.user_id
    if user_id is not None:
        user = db.fetchone(conn, "SELECT id FROM utenti WHERE id=?", (user_id,))
        if not user:
            db.release_db(conn)
            raise HTTPException(404, "Utente non trovato")
        db.ex(conn, "UPDATE team_operatori SET linked_user_id=NULL WHERE linked_user_id=? AND id<>?", (user_id, op_id))
    db.ex(conn, "UPDATE team_operatori SET linked_user_id=? WHERE id=?", (user_id, op_id))
    conn.commit()
    db.release_db(conn)
    return {"ok": True}

@router.put("/api/team/operatori/{op_id}")
def update_team_operatore(op_id: int, payload: TeamOperatoreUpdateInput, user=Depends(require_team_editor)):
    conn = db.get_db()
    db.ex(conn, "UPDATE team_operatori SET nome=?, posizione=? WHERE id=?",
       (payload.nome.strip(), payload.posizione, op_id))
    conn.commit(); db.release_db(conn)
    return {"ok": True}

@router.delete("/api/team/operatori/{op_id}")
def delete_team_operatore(op_id: int, user=Depends(require_team_editor)):
    conn = db.get_db()
    db.ex(conn, "UPDATE team_operatori SET attivo=0 WHERE id=?", (op_id,))
    conn.commit(); db.release_db(conn)
    return {"ok": True}

# ─── Team: turni ───────────────────────────────────────────────────────────────
@router.get("/api/team/turni/{anno}/{mese}")
def get_team_turni(anno: int, mese: int,
                   start_date: str = None, end_date: str = None,
                   user=Depends(get_current_user)):
    try:
        return _build_team_turni_payload(anno, mese, user, start_date, end_date)
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(500, f"Errore interno: {str(e)}")

@router.post("/api/team/turni/pdf-from-html")
async def pdf_from_html(request: Request, user=Depends(get_current_user)):
    """Riceve HTML dal frontend e lo converte in PDF con WeasyPrint."""
    if not WEASYPRINT_AVAILABLE:
        raise HTTPException(501, "WeasyPrint non disponibile sul server")
    try:
        body = await request.json()
        html_content = body.get("html", "")
        if not html_content:
            raise HTTPException(400, "HTML mancante")
        pdf_bytes = WeasyHTML(string=html_content).write_pdf()
        anno = body.get("anno", "")
        mese = body.get("mese", "")
        filename = f"turni-team-{anno}-{str(mese).zfill(2)}.pdf"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(io.BytesIO(pdf_bytes), media_type="application/pdf", headers=headers)
    except HTTPException:
        raise
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(500, f"Errore generazione PDF: {str(e)}")


@router.post("/api/team/turni")
def set_team_turni(payload: TeamCellaInput, user=Depends(require_team_editor)):
    """
    Salva un turno team. Se col='var', aggiorna solo turno_var preservando turno_base esistente.
    Se col='base', aggiorna solo turno_base preservando turno_var esistente.
    """
    data    = payload.data
    op_id   = payload.operatore_id
    col     = payload.col or "base"
    flags_base = payload.flags_base
    flags_var  = payload.flags_var
    now     = _local_now_iso()

    conn = db.get_db()
    existing = db.fetchone(conn, "SELECT turno_base, turno_var, flags, flags_base, flags_var FROM team_turni WHERE data=? AND operatore_id=?",
                        (data, op_id))
    existing_flags_base = (existing.get("flags_base") if existing else None) or ""
    existing_flags_var  = (existing.get("flags_var") if existing else None) or ""
    if existing and not existing_flags_base and not existing_flags_var and existing.get("flags"):
        if existing.get("turno_var"):
            existing_flags_var = existing.get("flags") or ""
        else:
            existing_flags_base = existing.get("flags") or ""

    if flags_base is None and flags_var is None:
        incoming_flags = payload.flags or ""
        if col == "var":
            flags_base = existing_flags_base
            flags_var = incoming_flags
        else:
            flags_base = incoming_flags
            flags_var = existing_flags_var
    else:
        flags_base = existing_flags_base if flags_base is None else flags_base
        flags_var = existing_flags_var if flags_var is None else flags_var

    # Preserva la colonna non modificata
    if col == "var":
        turno_base = existing["turno_base"] if existing else (payload.turno_base or "")
        turno_var  = payload.turno_var if payload.turno_var is not None else ""
    else:
        turno_base = payload.turno_base if payload.turno_base is not None else ""
        turno_var  = existing["turno_var"] if existing else (payload.turno_var or "")

    shared_flags = flags_var if turno_var else flags_base

    db.ex(conn, """INSERT INTO team_turni (data, operatore_id, turno_base, turno_var, flags, flags_base, flags_var, modificato_da, modificato_il)
       VALUES (?,?,?,?,?,?,?,?,?)
       ON CONFLICT(data, operatore_id) DO UPDATE SET
         turno_base=excluded.turno_base, turno_var=excluded.turno_var,
         flags=excluded.flags, flags_base=excluded.flags_base, flags_var=excluded.flags_var,
         modificato_da=excluded.modificato_da, modificato_il=excluded.modificato_il""",
       (data, op_id, turno_base, turno_var, shared_flags, flags_base, flags_var, user["username"], now))

    op = db.fetchone(conn, "SELECT nome FROM team_operatori WHERE id=?", (op_id,))
    campo_log = "turno_var" if col == "var" else "turno_base"
    vecchio = (existing.get(campo_log) or "") if existing else ""
    nuovo   = turno_var if col == "var" else turno_base
    log_flags = flags_var if col == "var" else flags_base
    db.ex(conn, """INSERT INTO team_log (data_modifica, utente, data_turno, operatore_nome, campo, vecchio_valore, nuovo_valore, flags)
       VALUES (?,?,?,?,?,?,?,?)""",
       (now, user["username"], data, op["nome"] if op else str(op_id), campo_log, vecchio, nuovo, log_flags))

    conn.commit(); db.release_db(conn)
    return {"ok": True, "propagati": 0}

@router.delete("/api/team/turni/{data}/{op_id}")
def delete_team_turno(data: str, op_id: int, user=Depends(require_team_editor)):
    conn = db.get_db()
    db.ex(conn, "DELETE FROM team_turni WHERE data=? AND operatore_id=?", (data, op_id))
    conn.commit(); db.release_db(conn)
    return {"ok": True}

# ─── Team: colonne destra ──────────────────────────────────────────────────────
@router.post("/api/team/colonne-destra")
def set_colonne_destra(payload: TeamColonneDestraInput, user=Depends(require_team_editor)):
    data = payload.data
    conn = db.get_db()
    # rep1, rep2, rep3 identificano i reperibili del giorno (primo, secondo, terzo da chiamare)
    vals = (data,
            payload.rep1 or "", payload.rep2 or "", payload.rep3 or "",
            payload.fest_m1 or "", payload.fest_m2 or "",
            payload.fest_p1 or "", payload.fest_p2 or "")
    db.ex(conn, """INSERT INTO team_colonne_destra (data,rep1,rep2,rep3,fest_m1,fest_m2,fest_p1,fest_p2)
       VALUES (?,?,?,?,?,?,?,?)
       ON CONFLICT(data) DO UPDATE SET
         rep1=excluded.rep1, rep2=excluded.rep2, rep3=excluded.rep3,
         fest_m1=excluded.fest_m1, fest_m2=excluded.fest_m2,
         fest_p1=excluded.fest_p1, fest_p2=excluded.fest_p2""", vals)
    conn.commit(); db.release_db(conn)
    return {"ok": True}

@router.post("/api/team/ferie/request-batch")
def save_team_ferie_request(payload: TeamFerieBatchInput, user=Depends(get_current_user)):
    conn = db.get_db()
    op = get_team_operator_for_user(conn, user["id"])
    if not op:
        db.release_db(conn)
        raise HTTPException(403, "Account non associato a un operatore team")

    add_pairs = [p for p in payload.add_dates if _parse_iso_date(p.data)]
    add_pairs_sorted = sorted(add_pairs, key=lambda p: p.data)
    seen_add = {}
    for pair in add_pairs_sorted:
        seen_add[pair.data] = pair.tipo or "ferie"
    add_dates = list(seen_add.keys())
    remove_dates = sorted({d for d in payload.remove_dates if _parse_iso_date(d)})
    for data_turno in add_dates:
        tipo_giorno = seen_add[data_turno]
        row = db.fetchone(conn, "SELECT id, stato, user_id FROM team_ferie_requests WHERE operatore_id=? AND data=?",
                       (op["id"], data_turno))
        if row and row["user_id"] != user["id"] and not (user.get("is_editor") or user.get("is_admin")):
            db.release_db(conn)
            raise HTTPException(403, "Richiesta ferie già presente per questo giorno")
        if row:
            old_status = row.get("stato")
            db.ex(conn, """UPDATE team_ferie_requests
                        SET user_id=?, tipo=?, note=?, stato='pending', requested_by=?, reviewed_by=NULL, updated_at=?
                        WHERE id=?""",
               (user["id"], tipo_giorno, payload.note, user["username"], _local_now_iso(), row["id"]))
            _log_team_ferie(conn, user["username"], user["id"], user["username"], op["id"], op["nome"], data_turno,
                            "requested", old_status, "pending")
        else:
            now = _local_now_iso()
            db.ex(conn, """INSERT INTO team_ferie_requests
                        (user_id, operatore_id, data, tipo, note, stato, requested_by, created_at, updated_at)
                        VALUES (?,?,?,?,?,?,?,?,?)""",
               (user["id"], op["id"], data_turno, tipo_giorno, payload.note, "pending", user["username"], now, now))
            _log_team_ferie(conn, user["username"], user["id"], user["username"], op["id"], op["nome"], data_turno,
                            "requested", None, "pending")

    for data_turno in remove_dates:
        row = db.fetchone(conn, "SELECT id, stato, user_id FROM team_ferie_requests WHERE operatore_id=? AND data=?",
                       (op["id"], data_turno))
        if not row:
            continue
        if row["user_id"] != user["id"] and not (user.get("is_editor") or user.get("is_admin")):
            db.release_db(conn)
            raise HTTPException(403, "Non puoi rimuovere questa richiesta")
        db.ex(conn, "DELETE FROM team_ferie_requests WHERE id=?", (row["id"],))
        _log_team_ferie(conn, user["username"], user["id"], user["username"], op["id"], op["nome"], data_turno,
                        "removed", row.get("stato"), None)

    if add_dates:
        period = _format_date_it(add_dates[0]) if len(add_dates) == 1 else f"{_format_date_it(add_dates[0])} - {_format_date_it(add_dates[-1])}"
        _notify_team_editors(
            conn,
            "Richiesta ferie da approvare",
            f"{op['nome']} ha inviato {len(add_dates)} richiesta/e ferie per {period}.",
            exclude_user_id=user["id"],
        )

    conn.commit()
    db.release_db(conn)
    return {"ok": True, "linked_operatore_id": op["id"]}

@router.post("/api/team/ferie/review")
def review_team_ferie(payload: TeamFerieReviewInput, user=Depends(require_team_editor)):
    new_status = (payload.status or "").strip().lower()
    if new_status not in {"approved", "rejected"}:
        raise HTTPException(400, "Stato non valido")
    dates = sorted({d for d in payload.dates if _parse_iso_date(d)})
    if not dates:
        raise HTTPException(400, "Nessuna data valida")

    conn = db.get_db()
    op = db.fetchone(conn, "SELECT id, nome FROM team_operatori WHERE id=?", (payload.operatore_id,))
    if not op:
        db.release_db(conn)
        raise HTTPException(404, "Operatore non trovato")
    rows = db.fetchall(conn, f"""
        SELECT r.*, u.username
        FROM team_ferie_requests r
        LEFT JOIN utenti u ON u.id = r.user_id
        WHERE r.operatore_id=? AND r.data IN ({','.join([db.get_limit_placeholder() for _ in dates])})
    """, tuple([payload.operatore_id] + dates))
    row_by_date = {r["data"]: r for r in rows}
    now = _local_now_iso()
    notifications_by_request = {}
    for data_turno in dates:
        row = row_by_date.get(data_turno)
        if not row:
            continue
        db.ex(conn, "UPDATE team_ferie_requests SET stato=?, reviewed_by=?, updated_at=? WHERE id=?",
           (new_status, user["username"], now, row["id"]))
        _log_team_ferie(conn, user["username"], row["user_id"], row.get("username"), op["id"], op["nome"], data_turno,
                        "reviewed", row.get("stato"), new_status)

        tipo = row.get("tipo", "ferie") or "ferie"
        key = (row["user_id"], tipo)
        if key not in notifications_by_request:
            notifications_by_request[key] = []
        notifications_by_request[key].append(data_turno)

    outcome = "approvata" if new_status == "approved" else "rifiutata"
    for (target_user_id, tipo), request_dates in notifications_by_request.items():
        ordered_dates = sorted(request_dates)
        if len(ordered_dates) == 1:
            period = f"il {_format_date_it(ordered_dates[0])}"
            days_text = "1 giorno"
        else:
            period = f"dal {_format_date_it(ordered_dates[0])} al {_format_date_it(ordered_dates[-1])}"
            days_text = f"{len(ordered_dates)} giorni"
        msg = f"La tua richiesta {tipo} per {days_text} ({period}) è stata {outcome}."
        _create_notification(conn, target_user_id, msg, title=f"Richiesta {outcome}", link="/turni-team.html")

    applied = {"applied": 0, "skipped": 0}
    if new_status == "approved":
        # Applica ogni riga individualmente per gestire il tipo
        for data_turno in dates:
            row = row_by_date.get(data_turno)
            if row:
                res = _apply_team_ferie_to_var(conn, payload.operatore_id, [data_turno], user["username"], row.get("tipo", "ferie"))
                applied["applied"] += res["applied"]
                applied["skipped"] += res["skipped"]
    conn.commit()
    db.release_db(conn)
    return {"ok": True, **applied}

@router.post("/api/team/ferie/apply-range")
def apply_team_ferie_range(payload: TeamFerieRangeApplyInput, user=Depends(require_team_editor)):
    start = _parse_iso_date(payload.start_date)
    giorni = int(payload.giorni or 0)
    if not start:
        raise HTTPException(400, "Data inizio non valida")
    if giorni < 1 or giorni > 31:
        raise HTTPException(400, "Numero giorni non valido")
    dates = [(start + timedelta(days=i)).isoformat() for i in range(giorni)]
    conn = db.get_db()
    result = _apply_team_ferie_to_var(conn, payload.operatore_id, dates, user["username"])
    conn.commit()
    db.release_db(conn)
    return {"ok": True, **result}

@router.get("/api/team/ferie/dashboard")
def get_team_ferie_dashboard(user=Depends(get_current_user)):
    conn = db.get_db()
    is_editor = bool(user.get("is_editor")) or bool(user.get("is_admin"))
    linked_op = get_team_operator_for_user(conn, user["id"])
    pending_rows = db.fetchall(conn, """
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
    """) if is_editor else []
    if is_editor:
        for row in pending_rows:
            dates = db.fetchall(conn, "SELECT data FROM team_ferie_requests WHERE stato='pending' AND operatore_id=? ORDER BY data",
                             (row["operatore_id"],))
            row["dates"] = [d["data"] for d in dates]
            row["first_day"] = _format_date_it(row.get("first_day"))
            row["last_day"] = _format_date_it(row.get("last_day"))
    if is_editor:
        recent_log = db.fetchall(conn, f"""
            SELECT * FROM team_ferie_log
            ORDER BY id DESC
            LIMIT {db.get_limit_placeholder()}
        """, (120,))
    elif linked_op:
        recent_log = db.fetchall(conn, f"""
            SELECT * FROM team_ferie_log
            WHERE operatore_id=?
            ORDER BY id DESC
            LIMIT {db.get_limit_placeholder()}
        """, (linked_op["id"], 60))
    else:
        recent_log = []
    
    for log in recent_log:
        if "data_turno" in log: log["data_turno"] = _format_date_it(log["data_turno"])
        if "data_modifica" in log: log["data_modifica"] = _format_date_it(log["data_modifica"])
        if "created_at" in log: log["created_at"] = _format_date_it(log["created_at"])
        
    db.release_db(conn)
    return {
        "pending": pending_rows,
        "recent_log": recent_log,
        "linked_operatore_id": linked_op["id"] if linked_op else None,
    }

@router.get("/api/admin/team/ferie/pending")
def get_admin_team_ferie_pending(admin=Depends(require_admin)):
    conn = db.get_db()
    groups = db.fetchall(conn, """
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
    for group in groups:
        rows = db.fetchall(conn, "SELECT data FROM team_ferie_requests WHERE stato='pending' AND operatore_id=? ORDER BY data",
                        (group["operatore_id"],))
        group["dates"] = [r["data"] for r in rows]
        group["first_day"] = _format_date_it(group.get("first_day"))
        group["last_day"] = _format_date_it(group.get("last_day"))
    db.release_db(conn)
    return groups

@router.get("/api/admin/team/ferie/log")
def get_admin_team_ferie_log(limit: int = 150, admin=Depends(require_admin)):
    conn = db.get_db()
    rows = db.fetchall(conn, f"SELECT * FROM team_ferie_log ORDER BY id DESC LIMIT {db.get_limit_placeholder()}", (limit,))
    db.release_db(conn)
    for row in rows:
        if "data_turno" in row: row["data_turno"] = _format_date_it(row["data_turno"])
        if "data_modifica" in row: row["data_modifica"] = _format_date_it(row["data_modifica"])
        if "created_at" in row: row["created_at"] = _format_date_it(row["created_at"])
    return rows

# ─── Team: template settimanale ────────────────────────────────────────────────
@router.get("/api/team/template-week")
def get_team_template_week(user=Depends(get_current_user)):
    conn = db.get_db()
    rows = db.fetchall(conn, "SELECT * FROM team_template_weekly ORDER BY giorno_settimana, posizione")
    rep_rows = db.fetchall(conn, "SELECT * FROM team_template_reperibili_weekly ORDER BY giorno_settimana")
    cfg = db.fetchone(conn, "SELECT start_date, end_date FROM team_template_config WHERE id=1")
    db.release_db(conn)
    template = {}
    for r in rows:
        g = r["giorno_settimana"]
        if g not in template: template[g] = []
        template[g].append({"posizione": r["posizione"], "turno_base": r["turno_base"] or "",
                             "turno_var": r["turno_var"] or "", "flags": r["flags"] or ""})
    reperibili = {
        str(r["giorno_settimana"]): {
            "rep1_pos": r.get("rep1_pos"),
            "rep2_pos": r.get("rep2_pos"),
            "rep3_pos": r.get("rep3_pos"),
            "fest_m1_pos": r.get("fest_m1_pos"),
            "fest_m2_pos": r.get("fest_m2_pos"),
            "fest_p1_pos": r.get("fest_p1_pos"),
            "fest_p2_pos": r.get("fest_p2_pos"),
        }
        for r in rep_rows
    }
    return {
        "posizioni": template,
        "reperibili": reperibili,
        "start_date": (cfg or {}).get("start_date", "") or "",
        "end_date": (cfg or {}).get("end_date", "") or "",
    }

@router.post("/api/team/template-week")
def save_team_template_week(payload: TeamTemplateWeekInput, user=Depends(require_team_editor)):
    conn = db.get_db()
    old_rows = db.fetchall(conn, "SELECT * FROM team_template_weekly ORDER BY giorno_settimana, posizione")
    old_rep_rows = db.fetchall(conn, "SELECT * FROM team_template_reperibili_weekly ORDER BY giorno_settimana")
    old_cfg = db.fetchone(conn, "SELECT start_date, end_date FROM team_template_config WHERE id=1")
    ops = db.fetchall(conn, "SELECT * FROM team_operatori WHERE attivo=1 ORDER BY posizione")

    old_template = {}
    for r in old_rows:
        g = r["giorno_settimana"]
        if g not in old_template:
            old_template[g] = {}
        old_template[g][r["posizione"]] = {
            "turno_base": r["turno_base"] or "",
            "turno_var": r["turno_var"] or "",
            "flags": r["flags"] or "",
        }
    old_reperibili = {
        r["giorno_settimana"]: {
            "rep1": r.get("rep1_pos"),
            "rep2": r.get("rep2_pos"),
            "rep3": r.get("rep3_pos"),
            "fest_m1": r.get("fest_m1_pos"),
            "fest_m2": r.get("fest_m2_pos"),
            "fest_p1": r.get("fest_p1_pos"),
            "fest_p2": r.get("fest_p2_pos"),
        }
        for r in old_rep_rows
    }
    _preserve_team_schedule_outside_range(
        conn, ops, old_template, old_reperibili,
        (old_cfg or {}).get("start_date"), (old_cfg or {}).get("end_date"),
        payload.start_date, payload.end_date
    )
    _clear_team_schedule_in_range(conn, payload.start_date, payload.end_date)

    db.ex(conn, "DELETE FROM team_template_weekly")
    db.ex(conn, "DELETE FROM team_template_reperibili_weekly")
    for giorno_str, posizioni in payload.posizioni.items():
        giorno = int(giorno_str)
        for pos in posizioni:
            db.ex(conn, "INSERT INTO team_template_weekly (giorno_settimana, posizione, turno_base, turno_var, flags) VALUES (?,?,?,?,?)",
               (giorno, pos.posizione, pos.turno_base or "", pos.turno_var or "", pos.flags or ""))
    for giorno_str, rep in payload.reperibili.items():
        giorno = int(giorno_str)
        db.ex(conn, """INSERT INTO team_template_reperibili_weekly
           (giorno_settimana, rep1_pos, rep2_pos, rep3_pos, fest_m1_pos, fest_m2_pos, fest_p1_pos, fest_p2_pos)
           VALUES (?,?,?,?,?,?,?,?)""",
           (giorno, rep.rep1_pos, rep.rep2_pos, rep.rep3_pos,
            rep.fest_m1_pos, rep.fest_m2_pos, rep.fest_p1_pos, rep.fest_p2_pos))
    db.ex(conn,
       """INSERT INTO team_template_config (id, start_date, end_date)
          VALUES (1, ?, ?)
          ON CONFLICT(id) DO UPDATE SET start_date=excluded.start_date, end_date=excluded.end_date""",
       (payload.start_date or None, payload.end_date or None))
    conn.commit(); db.release_db(conn)
    return {"ok": True}

# ─── Team: notifiche ───────────────────────────────────────────────────────────
@router.get("/api/team/notifications")
def get_team_notifications(user=Depends(get_current_user)):
    conn = db.get_db()
    rows = db.fetchall(conn, "SELECT * FROM team_notifications WHERE user_id=? ORDER BY created_at DESC LIMIT 50", (user["id"],))
    db.release_db(conn)
    result = []
    for r in rows:
        created_at = str(r.get("created_at") or "")
        if " " in created_at and "T" not in created_at:
            created_at = created_at.replace(" ", "T", 1)
        result.append({
            "id": r["id"],
            "title": r.get("title") or "Notifica",
            "message": r.get("messaggio") or "",
            "link": r.get("link") or "",
            "is_read": bool(r.get("letto")),
            "created_at": created_at,
        })
    return result

@router.post("/api/team/notifications/{notif_id}/read")
def mark_notification_read(notif_id: int, payload: TeamNotificationUpdateInput = None, user=Depends(get_current_user)):
    conn = db.get_db()
    letto = True if payload is None else bool(payload.letto)
    db.ex(conn, "UPDATE team_notifications SET letto=? WHERE id=? AND user_id=?", (1 if letto else 0, notif_id, user["id"]))
    conn.commit(); db.release_db(conn)
    return {"ok": True}

@router.post("/api/team/notifications/read-all")
def mark_all_notifications_read(user=Depends(get_current_user)):
    conn = db.get_db()
    db.ex(conn, "UPDATE team_notifications SET letto=1 WHERE user_id=?", (user["id"],))
    conn.commit(); db.release_db(conn)
    return {"ok": True}

@router.get("/api/team/pending-counts")
def get_team_pending_counts(user=Depends(get_current_user)):
    conn = db.get_db()
    is_editor = bool(user.get("is_editor")) or bool(user.get("is_admin")) or bool(user.get("is_team_editor"))
    if not is_editor:
        db.release_db(conn)
        return {"ferie": 0, "scambi": 0}
    ferie_row = db.fetchone(conn, "SELECT COUNT(*) AS cnt FROM team_ferie_requests WHERE stato='pending'")
    scambi_row = db.fetchone(conn, "SELECT COUNT(*) AS cnt FROM team_swap_requests WHERE stato IN ('pending_target', 'pending_editor')")
    db.release_db(conn)
    return {
        "ferie": int((ferie_row or {}).get("cnt", 0) or 0),
        "scambi": int((scambi_row or {}).get("cnt", 0) or 0),
    }

# ─── Team: scambi ──────────────────────────────────────────────────────────────
@router.post("/api/team/swaps")
def request_swap(payload: TeamSwapRequestInput, user=Depends(get_current_user)):
    conn = db.get_db()
    richiedente = get_team_operator_for_user(conn, user["id"])
    if not richiedente:
        db.release_db(conn)
        raise HTTPException(403, "Account non associato a un operatore")
    
    collega = db.fetchone(conn, "SELECT * FROM team_operatori WHERE id=? AND attivo=1", (payload.collega_id,))
    if not collega:
        db.release_db(conn)
        raise HTTPException(404, "Collega non trovato")
    
    if collega["linked_user_id"] is None:
        db.release_db(conn)
        raise HTTPException(400, "Il collega non ha un account associato")

    t1 = _effective_team_turno_row(conn, payload.data, richiedente["id"])
    t2 = _effective_team_turno_row(conn, payload.data, collega["id"])
    from_col = _effective_swap_col(t1)
    to_col = _effective_swap_col(t2)
    from_turno = _swap_value(t1, from_col)
    to_turno = _swap_value(t2, to_col)
    if not from_turno or not to_turno:
        db.release_db(conn)
        raise HTTPException(400, "Per richiedere lo scambio entrambi gli operatori devono avere un turno in quel giorno")

    db.ex(conn, """INSERT INTO team_swap_requests (richiedente_id, collega_id, data, from_turno, to_turno, from_col, to_col, stato, created_at)
                VALUES (?,?,?,?,?,?,?,?,?)""", 
                (richiedente["id"], collega["id"], payload.data, from_turno, to_turno, from_col, to_col, "pending_target", _local_now_iso()))
    
    _create_notification(
        conn,
        collega["linked_user_id"],
        f"{richiedente['nome']} ti ha chiesto uno scambio per il giorno {_format_date_it(payload.data)}: {from_turno} con {to_turno}.",
        title="Richiesta cambio turno",
        link="/turni-team.html",
    )
    
    conn.commit(); db.release_db(conn)
    return {"ok": True}

@router.get("/api/team/swaps")
def get_team_swaps(user=Depends(get_current_user)):
    conn = db.get_db()
    op = get_team_operator_for_user(conn, user["id"])
    is_editor = bool(user.get("is_editor")) or bool(user.get("is_admin")) or bool(user.get("is_team_editor"))
    if not op and not is_editor:
        db.release_db(conn)
        return []

    if is_editor:
        rows = db.fetchall(conn, """
            SELECT s.*, r.nome AS richiedente_nome, c.nome AS collega_nome
            FROM team_swap_requests s
            JOIN team_operatori r ON r.id = s.richiedente_id
            JOIN team_operatori c ON c.id = s.collega_id
            ORDER BY s.created_at DESC
        """)
    else:
        rows = db.fetchall(conn, """
            SELECT s.*, r.nome AS richiedente_nome, c.nome AS collega_nome
            FROM team_swap_requests s
            JOIN team_operatori r ON r.id = s.richiedente_id
            JOIN team_operatori c ON c.id = s.collega_id
            WHERE s.richiedente_id=? OR s.collega_id=?
            ORDER BY s.created_at DESC
        """, (op["id"], op["id"]))
    db.release_db(conn)
    return rows

@router.post("/api/team/swaps/{swap_id}/respond")
def respond_swap(swap_id: int, payload: TeamSwapActionInput, user=Depends(get_current_user)):
    action = (payload.action or "").strip().lower()
    if action not in {"accept", "reject", "cancel"}:
        raise HTTPException(400, "Azione non valida")
    conn = db.get_db()
    op = get_team_operator_for_user(conn, user["id"])
    swap = db.fetchone(conn, "SELECT * FROM team_swap_requests WHERE id=?", (swap_id,))
    if not op or not swap:
        db.release_db(conn)
        raise HTTPException(403, "Azione non permessa")
    if action == "cancel" and swap["richiedente_id"] != op["id"]:
        db.release_db(conn)
        raise HTTPException(403, "Azione non permessa")
    if action != "cancel" and swap["collega_id"] != op["id"]:
        db.release_db(conn)
        raise HTTPException(403, "Azione non permessa")
    
    if action == "accept":
        db.ex(conn, "UPDATE team_swap_requests SET stato='pending_editor' WHERE id=?", (swap_id,))
        _notify_team_editors(
            conn,
            "Cambio turno da approvare",
            f"{op['nome']} ha accettato lo scambio del {_format_date_it(swap['data'])}. Serve approvazione editor.",
            exclude_user_id=user["id"],
        )
    elif action == "cancel":
        db.ex(conn, "UPDATE team_swap_requests SET stato='cancelled' WHERE id=?", (swap_id,))
    else:
        db.ex(conn, "UPDATE team_swap_requests SET stato='rejected' WHERE id=?", (swap_id,))
    
    notify_op_id = swap["collega_id"] if action == "cancel" else swap["richiedente_id"]
    notify_op = db.fetchone(conn, "SELECT linked_user_id FROM team_operatori WHERE id=?", (notify_op_id,))
    if notify_op and notify_op["linked_user_id"]:
        status_text = "accettato" if action == "accept" else ("annullato" if action == "cancel" else "rifiutato")
        suffix = " Ora deve approvarla un editor." if action == "accept" else ""
        _create_notification(
            conn,
            notify_op["linked_user_id"],
            f"{op['nome']} ha {status_text} la richiesta di scambio per il {_format_date_it(swap['data'])}.{suffix}",
            title="Aggiornamento cambio turno",
            link="/turni-team.html",
        )

    conn.commit(); db.release_db(conn)
    return {"ok": True}

@router.post("/api/team/swaps/{swap_id}/review")
def review_swap(swap_id: int, payload: TeamSwapActionInput, user=Depends(require_team_editor)):
    action = (payload.action or "").strip().lower()
    if action not in {"approve", "reject"}:
        raise HTTPException(400, "Azione non valida")
    conn = db.get_db()
    swap = db.fetchone(conn, "SELECT * FROM team_swap_requests WHERE id=?", (swap_id,))
    if not swap:
        db.release_db(conn)
        raise HTTPException(404, "Scambio non trovato")
    
    if action == "approve":
        # Esegui lo scambio effettivo dei turni
        data = swap["data"]
        op1_id = swap["richiedente_id"]
        op2_id = swap["collega_id"]
        
        t1 = _effective_team_turno_row(conn, data, op1_id)
        t2 = _effective_team_turno_row(conn, data, op2_id)
        
        from_col = _valid_swap_col(swap.get("from_col")) or _effective_swap_col(t1)
        to_col = _valid_swap_col(swap.get("to_col")) or _effective_swap_col(t2)
        if not _swap_value(t1, from_col) or not _swap_value(t2, to_col):
            db.release_db(conn)
            raise HTTPException(400, "Impossibile approvare: uno dei turni da scambiare non e' piu' presente")

        now = _local_now_iso()
        
        # Gli scambi sono variazioni: il TAB resta storico/template, si aggiorna sempre VAR.
        def update_turno(oid, turno_base, turno_var, flags_base, flags_var):
            shared_flags = flags_var if turno_var else flags_base
            db.ex(conn, """INSERT INTO team_turni (data, operatore_id, turno_base, turno_var, flags, flags_base, flags_var, modificato_da, modificato_il)
                        VALUES (?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(data, operatore_id) DO UPDATE SET
                        turno_base=excluded.turno_base, turno_var=excluded.turno_var,
                        flags=excluded.flags, flags_base=excluded.flags_base, flags_var=excluded.flags_var,
                        modificato_da=excluded.modificato_da, modificato_il=excluded.modificato_il""",
                   (data, oid, turno_base, turno_var, shared_flags, flags_base, flags_var, f"swap-{swap_id}", now))

        v1 = t1 or {"turno_base":"", "turno_var":"", "flags_base":"", "flags_var":""}
        v2 = t2 or {"turno_base":"", "turno_var":"", "flags_base":"", "flags_var":""}

        op1_base, op1_var = v1.get("turno_base") or "", v1.get("turno_var") or ""
        op2_base, op2_var = v2.get("turno_base") or "", v2.get("turno_var") or ""
        op1_flags_base, op1_flags_var = v1.get("flags_base") or "", v1.get("flags_var") or ""
        op2_flags_base, op2_flags_var = v2.get("flags_base") or "", v2.get("flags_var") or ""

        from_turno, from_flags = _swap_value(v1, from_col), _swap_flags(v1, from_col)
        to_turno, to_flags = _swap_value(v2, to_col), _swap_flags(v2, to_col)

        op1_var, op1_flags_var = to_turno, to_flags
        op2_var, op2_flags_var = from_turno, from_flags

        update_turno(op1_id, op1_base, op1_var, op1_flags_base, op1_flags_var)
        update_turno(op2_id, op2_base, op2_var, op2_flags_base, op2_flags_var)
        
        db.ex(conn, "UPDATE team_swap_requests SET stato='approved' WHERE id=?", (swap_id,))
    else:
        db.ex(conn, "UPDATE team_swap_requests SET stato='rejected' WHERE id=?", (swap_id,))

    # Notifiche
    for op_id in [swap["richiedente_id"], swap["collega_id"]]:
        u = db.fetchone(conn, "SELECT linked_user_id FROM team_operatori WHERE id=?", (op_id,))
        if u and u["linked_user_id"]:
            outcome = "approvato" if action == "approve" else "rifiutato"
            _create_notification(
                conn,
                u["linked_user_id"],
                f"Lo scambio per il {_format_date_it(swap['data'])} è stato {outcome} dall'editor.",
                title=f"Cambio turno {outcome}",
                link="/turni-team.html",
            )

    conn.commit(); db.release_db(conn)
    return {"ok": True}

# ─── Team: log ─────────────────────────────────────────────────────────────────
@router.get("/api/team/log")
def get_team_log(limit: int = 100, user=Depends(get_current_user)):
    conn = db.get_db()
    logs = db.fetchall(conn, f"SELECT * FROM team_log ORDER BY id DESC LIMIT {db.get_limit_placeholder()}", (limit,))
    db.release_db(conn)
    for log in logs:
        if "data_modifica" in log: log["data_modifica"] = _format_date_it(log["data_modifica"])
        if "data_turno" in log: log["data_turno"] = _format_date_it(log["data_turno"])
    return logs
