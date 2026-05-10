import time
from datetime import date, datetime, timedelta
from typing import Optional, List
from calendar import monthrange
from fastapi import Request, HTTPException

from app.config import TURNO_ORARI, TURNI_CONFIG, FESTIVITA, IMPOSTAZIONI_DEFAULTS, USE_PG, SQLITE_LOG_BUSY_TIMEOUT_MS
import app.database as db

def get_user_record(conn, user_id: int):
    return db.fetchone(conn, "SELECT id, username, nome, is_admin, is_editor, is_team_editor FROM utenti WHERE id=?", (user_id,))

def get_team_operator_for_user(conn, user_id: int):
    return db.fetchone(conn, """SELECT o.id, o.nome, o.posizione, o.linked_user_id
                             FROM team_operatori o
                             WHERE o.attivo=1 AND o.linked_user_id=?""", (user_id,))

def _log_team_ferie(conn, actor_username: str, user_id: int, username: str,
                    operatore_id: int, operatore_nome: str, data_turno: str,
                    action: str, status_from: Optional[str], status_to: Optional[str]):
    db.ex(conn, """INSERT INTO team_ferie_log
       (actor_username, user_id, username, operatore_id, operatore_nome, data_turno, action, status_from, status_to)
       VALUES (?,?,?,?,?,?,?,?,?)""",
       (actor_username, user_id, username, operatore_id, operatore_nome, data_turno, action, status_from, status_to))

def _create_notification(conn, user_id: int, messaggio: str, title: str = "Notifica", link: str = "/turni-team.html"):
    if not user_id:
        return
    db.ex(conn, "INSERT INTO team_notifications (user_id, title, messaggio, link) VALUES (?,?,?,?)",
          (user_id, title or "Notifica", messaggio, link))

def get_user_settings(user_id, conn):
    rows = db.fetchall(conn, "SELECT chiave, valore FROM impostazioni WHERE user_id=?", (user_id,))
    result = {k: float(v) for k, v in IMPOSTAZIONI_DEFAULTS.items()}
    for r in rows:
        try:
            result[r["chiave"]] = float(r["valore"])
        except:
            pass
    return result

def _log_accesso(username: str, esito: str, request: Request = None):
    conn2 = None
    try:
        ip = request.client.host if request and request.client else "—"
        ua = (request.headers.get("user-agent", "—")[:200]) if request else "—"
        if USE_PG:
            conn2 = db.get_db()
        else:
            conn2 = db._open_sqlite_connection(SQLITE_LOG_BUSY_TIMEOUT_MS)
        db.ex(conn2, "INSERT INTO log_accessi (username, esito, ip, user_agent) VALUES (?,?,?,?)",
           (username, esito, ip, ua))
        conn2.commit()
    except:
        pass
    finally:
        if conn2:
            db.release_db(conn2)

# ─── Calcolo ore ───────────────────────────────────────────────────────────────
def split_dn(start, end):
    if end <= start: end += 1440
    notturni = [(0, 360), (1200, 1440), (1440, 1800)]
    nott = 0
    for ns, ne in notturni:
        s, e = max(start, ns), min(end, ne)
        if e > s: nott += e - s
    tot = end - start
    return round((tot - nott) / 60, 2), round(nott / 60, 2)

def to_min(s):
    try:
        h, m = s.strip().split(":")
        return int(h) * 60 + int(m)
    except:
        return None

def calcola_ore(turno, ora_inizio, ora_fine, data_str):
    r = {"ore_diurne": 0.0, "ore_notturne": 0.0, "strao_diurno": 0.0,
         "strao_notturno": 0.0, "strao_fest_diurno": 0.0, "strao_fest_notturno": 0.0}
    ei = to_min(ora_inizio) if ora_inizio else None
    ef = to_min(ora_fine) if ora_fine else None
    std = TURNO_ORARI.get(turno)
    try:
        festivo = data_str in FESTIVITA
    except:
        festivo = False

    if turno == "R":
        if ei is not None and ef is not None:
            d, n = split_dn(ei, ef); r["strao_fest_diurno"] = d; r["strao_fest_notturno"] = n
        return r
    if turno == "RC":
        if ei is not None and ef is not None:
            d, n = split_dn(ei, ef); r["strao_diurno"] = d; r["strao_notturno"] = n
        return r
    if not std:
        if ei is not None and ef is not None:
            d, n = split_dn(ei, ef); r["strao_diurno"] = d; r["strao_notturno"] = n
        return r

    si, sf = std
    if festivo:
        ini = ei if ei is not None else si
        fin = ef if ef is not None else sf
        d, n = split_dn(ini, fin)
        r["strao_fest_diurno"] = d; r["strao_fest_notturno"] = n
        return r

    if ei is None and ef is None:
        d, n = split_dn(si, sf); r["ore_diurne"] = d; r["ore_notturne"] = n
        return r

    ini = ei if ei is not None else si
    fin = ef if ef is not None else sf
    sfn = sf if sf > si else sf + 1440
    fn = fin if fin > ini else fin + 1440
    oi, of = max(ini, si), min(fn, sfn)
    if of > oi:
        d, n = split_dn(oi, of); r["ore_diurne"] += d; r["ore_notturne"] += n
    if ini < si:
        d, n = split_dn(ini, si); r["strao_diurno"] += d; r["strao_notturno"] += n
    if fn > sfn:
        d, n = split_dn(sfn, fn); r["strao_diurno"] += d; r["strao_notturno"] += n
    return r

def calcola_tipo_rep(turno, data_str):
    if turno == "RC": return "semifestiva"
    if turno == "R": return "festiva"
    if data_str in FESTIVITA: return "festiva"
    if TURNI_CONFIG.get(turno, {}).get("lavorativo"): return "feriale"
    return ""

def _parse_iso_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except Exception:
        return None

def _date_in_range(d: date, start_obj: Optional[date], end_obj: Optional[date]) -> bool:
    if start_obj and end_obj:
        return start_obj <= d <= end_obj
    if start_obj:
        return d >= start_obj
    if end_obj:
        return d <= end_obj
    return True

def _format_date_it(val):
    if not val: return val
    if isinstance(val, date) or isinstance(val, datetime):
        val = val.isoformat()
    if not isinstance(val, str): return val
    parts = val.split("T") if "T" in val else val.split(" ")
    date_part = parts[0]
    time_part = parts[1][:5] if len(parts) > 1 else "" # keep only HH:MM
    try:
        y, m, d = date_part.split("-")
        formatted = f"{d}-{m}-{y}"
        return f"{formatted} {time_part}".strip()
    except Exception:
        return val

def _clear_team_schedule_in_range(conn, start_date_str: Optional[str], end_date_str: Optional[str]):
    start_obj = _parse_iso_date(start_date_str)
    end_obj = _parse_iso_date(end_date_str)
    if start_obj and end_obj and end_obj < start_obj:
        start_obj, end_obj = end_obj, start_obj

    if start_obj and end_obj:
        bounds = (start_obj.isoformat(), end_obj.isoformat())
        db.ex(conn, "DELETE FROM team_turni WHERE data >= ? AND data <= ?", bounds)
        db.ex(conn, "DELETE FROM team_colonne_destra WHERE data >= ? AND data <= ?", bounds)
        return
    if start_obj:
        db.ex(conn, "DELETE FROM team_turni WHERE data >= ?", (start_obj.isoformat(),))
        db.ex(conn, "DELETE FROM team_colonne_destra WHERE data >= ?", (start_obj.isoformat(),))
        return
    if end_obj:
        db.ex(conn, "DELETE FROM team_turni WHERE data <= ?", (end_obj.isoformat(),))
        db.ex(conn, "DELETE FROM team_colonne_destra WHERE data <= ?", (end_obj.isoformat(),))
        return

    db.ex(conn, "DELETE FROM team_turni")
    db.ex(conn, "DELETE FROM team_colonne_destra")

def _compute_team_template_slot(template_map: dict, d: date, posizione: int, operator_count: int,
                                start_week_monday: Optional[date]) -> dict:
    if not start_week_monday or operator_count <= 0:
        return {"turno_base": "", "turno_var": "", "flags": ""}
    dow = d.weekday()
    cur_mon = d - timedelta(days=d.weekday())
    sett_idx = (cur_mon - start_week_monday).days // 7
    sett_ciclo = (sett_idx % operator_count) + 1
    pos_orig = ((posizione + sett_ciclo - 2) % operator_count) + 1
    tpl_row = template_map.get(dow, {}).get(pos_orig, {})
    return {
        "turno_base": tpl_row.get("turno_base", "") or "",
        "turno_var": tpl_row.get("turno_var", "") or "",
        "flags": tpl_row.get("flags", "") or "",
    }

def _compute_team_rep_defaults(rep_template: dict, d: date, operator_count: int,
                               start_week_monday: Optional[date]) -> dict:
    defaults = {
        "rep1": "", "rep2": "", "rep3": "",
        "fest_m1": "", "fest_m2": "", "fest_p1": "", "fest_p2": "",
    }
    if not start_week_monday or operator_count <= 0:
        return defaults
    dow = d.weekday()
    if dow not in rep_template:
        return defaults
    cur_mon = d - timedelta(days=d.weekday())
    sett_idx = (cur_mon - start_week_monday).days // 7
    field_map = {
        "rep1": "rep1", "rep2": "rep2", "rep3": "rep3",
        "fest_m1": "fest_m1", "fest_m2": "fest_m2",
        "fest_p1": "fest_p1", "fest_p2": "fest_p2",
    }
    for key, base_pos in rep_template.get(dow, {}).items():
        if base_pos:
            mapped = field_map.get(key)
            if mapped:
                defaults[mapped] = str(((int(base_pos) - sett_idx - 1) % operator_count) + 1)
    return defaults

def _preserve_team_schedule_outside_range(conn, ops: List[dict], template_map: dict, rep_template: dict,
                                          old_start: Optional[str], old_end: Optional[str],
                                          new_start: Optional[str], new_end: Optional[str]):
    old_start_obj = _parse_iso_date(old_start)
    old_end_obj = _parse_iso_date(old_end)
    new_start_obj = _parse_iso_date(new_start)
    new_end_obj = _parse_iso_date(new_end)
    if old_start_obj and old_end_obj and old_end_obj < old_start_obj:
        old_start_obj, old_end_obj = old_end_obj, old_start_obj
    if new_start_obj and new_end_obj and new_end_obj < new_start_obj:
        new_start_obj, new_end_obj = new_end_obj, new_start_obj
    if not old_start_obj:
        return

    operator_count = len(ops)
    if operator_count <= 0:
        return

    start_week_monday = old_start_obj - timedelta(days=old_start_obj.weekday())
    horizon_start = date(2024, 1, 1)
    horizon_end = date(2030, 12, 31)
    existing_turni = {
        (r["data"], r["operatore_id"])
        for r in db.fetchall(conn, "SELECT data, operatore_id FROM team_turni WHERE data >= ? AND data <= ?",
                          (horizon_start.isoformat(), horizon_end.isoformat()))
    }
    existing_cols = {
        r["data"]
        for r in db.fetchall(conn, "SELECT data FROM team_colonne_destra WHERE data >= ? AND data <= ?",
                          (horizon_start.isoformat(), horizon_end.isoformat()))
    }
    now = datetime.now().isoformat()[:19]
    d = horizon_start
    while d <= horizon_end:
        old_in_range = _date_in_range(d, old_start_obj, old_end_obj)
        new_in_range = _date_in_range(d, new_start_obj, new_end_obj)
        if old_in_range and not new_in_range:
            data_str = d.isoformat()
            for op in ops:
                key = (data_str, op["id"])
                if key in existing_turni:
                    continue
                tpl = _compute_team_template_slot(template_map, d, op["posizione"], operator_count, start_week_monday)
                if not (tpl["turno_base"] or tpl["turno_var"] or tpl["flags"]):
                    continue
                db.ex(conn, """INSERT INTO team_turni
                       (data, operatore_id, turno_base, turno_var, flags, flags_base, flags_var, modificato_da, modificato_il)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                   (data_str, op["id"], tpl["turno_base"], tpl["turno_var"], tpl["flags"], tpl["flags"], "", "template-preserve", now))
                existing_turni.add(key)

            if data_str not in existing_cols:
                rep_defaults = _compute_team_rep_defaults(rep_template, d, operator_count, start_week_monday)
                if rep_defaults["rep1"] or rep_defaults["rep2"] or rep_defaults["rep3"]:
                    db.ex(conn, """INSERT INTO team_colonne_destra
                           (data, rep1, rep2, rep3, fest_m1, fest_m2, fest_p1, fest_p2)
                           VALUES (?,?,?,?,?,?,?,?)""",
                       (data_str, rep_defaults["rep1"], rep_defaults["rep2"], rep_defaults["rep3"],
                        rep_defaults["fest_m1"], rep_defaults["fest_m2"], rep_defaults["fest_p1"], rep_defaults["fest_p2"]))
                    existing_cols.add(data_str)
                elif rep_defaults["fest_m1"] or rep_defaults["fest_m2"] or rep_defaults["fest_p1"] or rep_defaults["fest_p2"]:
                    db.ex(conn, """INSERT INTO team_colonne_destra
                           (data, rep1, rep2, rep3, fest_m1, fest_m2, fest_p1, fest_p2)
                           VALUES (?,?,?,?,?,?,?,?)""",
                       (data_str, "", "", "", rep_defaults["fest_m1"], rep_defaults["fest_m2"], rep_defaults["fest_p1"], rep_defaults["fest_p2"]))
                    existing_cols.add(data_str)
        d += timedelta(days=1)

def _build_team_turni_payload(anno: int, mese: int, user: dict,
                              start_date: str = None, end_date: str = None):
    _, days = monthrange(anno, mese)
    conn = db.get_db()
    try:
        template_rows = db.fetchall(conn, "SELECT * FROM team_template_weekly ORDER BY giorno_settimana, posizione")
        template = {}
        for r in template_rows:
            g = r["giorno_settimana"]
            if g not in template:
                template[g] = {}
            template[g][r["posizione"]] = {
                "turno_base": r["turno_base"] or "",
                "turno_var": r["turno_var"] or "",
                "flags": r["flags"] or "",
            }

        rep_template_rows = db.fetchall(conn, "SELECT * FROM team_template_reperibili_weekly ORDER BY giorno_settimana")
        rep_template = {
            r["giorno_settimana"]: {
                "rep1": r.get("rep1_pos"),
                "rep2": r.get("rep2_pos"),
                "rep3": r.get("rep3_pos"),
                "fest_m1": r.get("fest_m1_pos"),
                "fest_m2": r.get("fest_m2_pos"),
                "fest_p1": r.get("fest_p1_pos"),
                "fest_p2": r.get("fest_p2_pos"),
            }
            for r in rep_template_rows
        }

        template_cfg = db.fetchone(conn, "SELECT start_date, end_date FROM team_template_config WHERE id=1")
        if template_cfg:
            if start_date is None:
                start_date = template_cfg.get("start_date") or None
            if end_date is None:
                end_date = template_cfg.get("end_date") or None

        ops = db.fetchall(conn, """
            SELECT o.*, u.username AS linked_username, u.nome AS linked_nome
            FROM team_operatori o
            LEFT JOIN utenti u ON u.id = o.linked_user_id
            WHERE o.attivo=1
            ORDER BY o.posizione
        """)
        operator_count = max(len(ops), 1)
        d_from = f"{anno:04d}-{mese:02d}-01"
        d_to = f"{anno:04d}-{mese:02d}-{days:02d}"
        turni_esistenti = db.fetchall(
            conn,
            "SELECT * FROM team_turni WHERE data >= ? AND data <= ? ORDER BY data, operatore_id",
            (d_from, d_to),
        )
        turni_idx = {(t["data"], t["operatore_id"]): t for t in turni_esistenti}
        colonne = db.fetchall(
            conn,
            "SELECT * FROM team_colonne_destra WHERE data >= ? AND data <= ? ORDER BY data",
            (d_from, d_to),
        )
        col_idx = {c["data"]: c for c in colonne}
        ferie_params = [d_from, d_to]
        ferie_sql = """
            SELECT r.*, u.username, o.nome AS operatore_nome
            FROM team_ferie_requests r
            LEFT JOIN utenti u ON u.id = r.user_id
            LEFT JOIN team_operatori o ON o.id = r.operatore_id
            WHERE r.data >= ? AND r.data <= ? AND r.stato IN ('pending','approved')
        """
        linked_op = get_team_operator_for_user(conn, user["id"]) if not (user.get("is_editor") or user.get("is_admin")) else None
        if linked_op:
            ferie_sql += " AND r.operatore_id = ?"
            ferie_params.append(linked_op["id"])
        ferie_rows = db.fetchall(conn, ferie_sql, tuple(ferie_params))
        ferie_idx = {(r["data"], r["operatore_id"]): r for r in ferie_rows}

        start_date_obj = None
        end_date_obj = None
        start_week_monday = None
        if start_date:
            try:
                start_date_obj = date.fromisoformat(start_date)
                start_week_monday = start_date_obj - timedelta(days=start_date_obj.weekday())
            except Exception:
                start_date_obj = None
                start_week_monday = None
        if end_date:
            try:
                end_date_obj = date.fromisoformat(end_date)
            except Exception:
                end_date_obj = None
        if start_date_obj and end_date_obj and end_date_obj < start_date_obj:
            start_date_obj, end_date_obj = end_date_obj, start_date_obj

        giorni = []
        for g in range(1, days + 1):
            data = f"{anno:04d}-{mese:02d}-{g:02d}"
            d = date.fromisoformat(data)
            dow = d.weekday()
            is_fest = data in FESTIVITA

            if start_date_obj and end_date_obj:
                in_range = start_date_obj <= d <= end_date_obj
            elif start_date_obj:
                in_range = d >= start_date_obj
            elif end_date_obj:
                in_range = d <= end_date_obj
            else:
                in_range = True

            if in_range and start_week_monday:
                cur_mon = d - timedelta(days=d.weekday())
                sett_idx = (cur_mon - start_week_monday).days // 7
                sett_ciclo = (sett_idx % operator_count) + 1
            else:
                sett_ciclo = None

            row_turni = []
            for op in ops:
                pos = op["posizione"]
                tpl = {"turno_base": "", "turno_var": "", "flags_base": "", "flags_var": ""}
                if in_range and sett_ciclo:
                    pos_orig = ((pos + sett_ciclo - 2) % operator_count) + 1
                    tpl_row = template.get(dow, {}).get(pos_orig, {})
                    tpl = {
                        "turno_base": tpl_row.get("turno_base", ""),
                        "turno_var": tpl_row.get("turno_var", ""),
                        "flags_base": tpl_row.get("flags", ""),
                        "flags_var": "",
                    }

                key = (data, op["id"])
                if key in turni_idx:
                    row = turni_idx[key]
                    shared_flags = row.get("flags", "") or ""
                    tpl = {
                        "turno_base": row.get("turno_base", "") or "",
                        "turno_var": row.get("turno_var", "") or "",
                        "flags_base": row.get("flags_base", "") or (shared_flags if not (row.get("turno_var") or "") else ""),
                        "flags_var": row.get("flags_var", "") or (shared_flags if (row.get("turno_var") or "") else ""),
                    }

                row_turni.append({
                    "operatore_id": op["id"],
                    "turno_base": tpl.get("turno_base", ""),
                    "turno_var": tpl.get("turno_var", ""),
                    "flags_base": tpl.get("flags_base", ""),
                    "flags_var": tpl.get("flags_var", ""),
                    "ferie_request": ferie_idx.get((data, op["id"])),
                })

            col = col_idx.get(data, {})
            rep_defaults = {
                "rep1": "", "rep2": "", "rep3": "",
                "fest_m1": "", "fest_m2": "", "fest_p1": "", "fest_p2": "",
            }
            if in_range and start_week_monday and dow in rep_template and operator_count > 0:
                rep_defaults = _compute_team_rep_defaults(rep_template, d, operator_count, start_week_monday)
            giorni.append({
                "data": data,
                "giorno": g,
                "dow": dow,
                "is_domenica": dow == 6,
                "is_sabato": dow == 5,
                "is_festivo": is_fest,
                "turni": row_turni,
                "colonne_destra": {
                    "rep1": col.get("rep1", "") or rep_defaults["rep1"],
                    "rep2": col.get("rep2", "") or rep_defaults["rep2"],
                    "rep3": col.get("rep3", "") or rep_defaults["rep3"],
                    "fest_m1": col.get("fest_m1", "") or rep_defaults["fest_m1"],
                    "fest_m2": col.get("fest_m2", "") or rep_defaults["fest_m2"],
                    "fest_p1": col.get("fest_p1", "") or rep_defaults["fest_p1"],
                    "fest_p2": col.get("fest_p2", "") or rep_defaults["fest_p2"],
                }
            })

        return {
            "anno": anno,
            "mese": mese,
            "start_date": start_date or "",
            "end_date": end_date or "",
            "operatori": ops,
            "giorni": giorni,
        }
    finally:
        db.release_db(conn)

def _load_team_template_context(conn):
    template_rows = db.fetchall(conn, "SELECT * FROM team_template_weekly ORDER BY giorno_settimana, posizione")
    template = {}
    for r in template_rows:
        g = r["giorno_settimana"]
        if g not in template:
            template[g] = {}
        template[g][r["posizione"]] = {
            "turno_base": r.get("turno_base", "") or "",
            "turno_var": r.get("turno_var", "") or "",
            "flags": r.get("flags", "") or "",
        }
    op_count = int((db.fetchone(conn, "SELECT COUNT(*) AS cnt FROM team_operatori WHERE attivo=1") or {}).get("cnt", 0) or 0)
    cfg = db.fetchone(conn, "SELECT start_date, end_date FROM team_template_config WHERE id=1") or {}
    start_obj = _parse_iso_date(cfg.get("start_date"))
    end_obj = _parse_iso_date(cfg.get("end_date"))
    if start_obj and end_obj and end_obj < start_obj:
        start_obj, end_obj = end_obj, start_obj
    start_week_monday = start_obj - timedelta(days=start_obj.weekday()) if start_obj else None
    return template, op_count, start_obj, end_obj, start_week_monday

def _apply_team_ferie_to_var(conn, operatore_id: int, dates: List[str], username: str, tipo: str = "ferie") -> dict:
    op = db.fetchone(conn, "SELECT id, nome, posizione FROM team_operatori WHERE id=? AND attivo=1", (operatore_id,))
    if not op:
        raise HTTPException(404, "Operatore non trovato")

    template, op_count, start_obj, end_obj, start_week_monday = _load_team_template_context(conn)
    now = datetime.now().isoformat()[:19]
    applied = 0
    skipped = 0

    for data_str in sorted({d for d in dates if _parse_iso_date(d)}):
        d_obj = date.fromisoformat(data_str)
        existing = db.fetchone(conn, "SELECT turno_base, turno_var, flags, flags_base, flags_var FROM team_turni WHERE data=? AND operatore_id=?",
                            (data_str, operatore_id))
        if existing and (existing.get("turno_var") or "").strip():
            skipped += 1
            continue

        tpl = {"turno_base": "", "turno_var": "", "flags": ""}
        in_template_range = True
        if start_obj and d_obj < start_obj:
            in_template_range = False
        if end_obj and d_obj > end_obj:
            in_template_range = False
        if in_template_range:
            tpl = _compute_team_template_slot(template, d_obj, op["posizione"], op_count, start_week_monday)
        if not existing and (tpl.get("turno_var") or "").strip():
            skipped += 1
            continue

        shared_existing_flags = (existing.get("flags") if existing else "") or ""
        turno_base = (existing.get("turno_base") if existing else tpl.get("turno_base")) or ""
        flags_base = (existing.get("flags_base") if existing else tpl.get("flags")) or ""
        flags_var = (existing.get("flags_var") if existing else "") or ""
        if existing and not flags_base and not flags_var and shared_existing_flags:
            flags_base = shared_existing_flags
        shared_flags = flags_var

        # Mappa il tipo al simbolo in tabella
        simbolo = "F"
        if tipo == "EX FEST": simbolo = "EX FEST"
        elif tipo == "ROT": simbolo = "ROT"

        db.ex(conn, """INSERT INTO team_turni (data, operatore_id, turno_base, turno_var, flags, flags_base, flags_var, modificato_da, modificato_il)
           VALUES (?,?,?,?,?,?,?,?,?)
           ON CONFLICT(data, operatore_id) DO UPDATE SET
             turno_base=excluded.turno_base, turno_var=excluded.turno_var,
             flags=excluded.flags, flags_base=excluded.flags_base, flags_var=excluded.flags_var,
             modificato_da=excluded.modificato_da, modificato_il=excluded.modificato_il""",
           (data_str, operatore_id, turno_base, simbolo, shared_flags, flags_base, flags_var, username, now))
        db.ex(conn, """INSERT INTO team_log (data_modifica, utente, data_turno, operatore_nome, campo, vecchio_valore, nuovo_valore, flags)
           VALUES (?,?,?,?,?,?,?,?)""",
           (now, username, data_str, op["nome"], "turno_var", "", simbolo, flags_var))
        applied += 1

    return {"applied": applied, "skipped": skipped}
