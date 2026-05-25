import json
from datetime import date, timedelta
from fastapi import APIRouter, Depends, HTTPException

import app.database as db
from app.config import TURNI_CONFIG, TURNO_ORARI, FESTIVITA, NOTTE_ASSENZA, USE_PG
from app.schemas import TurnoInput, ImpostazioniInput, ApplicaTabella
from app.security import get_current_user
from app.services import calcola_ore, calcola_tipo_rep, get_user_settings

router = APIRouter(tags=["turni"])

@router.get("/api/config")
def get_config():
    out = {}
    for k, v in TURNI_CONFIG.items():
        orari = TURNO_ORARI.get(k)
        out[k] = {**v, "std_ini": orari[0] if orari else None, "std_fin": orari[1] if orari else None}
    return out

@router.get("/api/festivita")
def get_festivita():
    return list(FESTIVITA)

@router.get("/api/turni/{anno}/{mese}")
def get_turni_mese(anno: int, mese: int, user=Depends(get_current_user)):
    conn = db.get_db()
    rows = db.fetchall(conn, "SELECT * FROM turni WHERE user_id=? AND data LIKE ?",
                    (user["id"], f"{anno:04d}-{mese:02d}-%"))
    db.release_db(conn)
    result = {}
    for r in rows:
        d = r["data"]
        result[d if isinstance(d, str) else d.isoformat()] = r
    return result

@router.post("/api/turni/{data}")
def set_turno(data: str, payload: TurnoInput, user=Depends(get_current_user)):
    ore = calcola_ore(payload.turno or "", payload.ora_inizio, payload.ora_fine, data)
    tipo_rep = calcola_tipo_rep(payload.turno or "", data) if payload.reperibilita else ""
    conn = db.get_db()
    if USE_PG:
        db.ex(conn, """INSERT INTO turni
              (user_id,data,turno,ora_inizio,ora_fine,ore_diurne,ore_notturne,
               strao_diurno,strao_notturno,strao_fest_diurno,strao_fest_notturno,reperibilita,note)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT(user_id,data) DO UPDATE SET
              turno=EXCLUDED.turno,ora_inizio=EXCLUDED.ora_inizio,ora_fine=EXCLUDED.ora_fine,
              ore_diurne=EXCLUDED.ore_diurne,ore_notturne=EXCLUDED.ore_notturne,
              strao_diurno=EXCLUDED.strao_diurno,strao_notturno=EXCLUDED.strao_notturno,
              strao_fest_diurno=EXCLUDED.strao_fest_diurno,strao_fest_notturno=EXCLUDED.strao_fest_notturno,
              reperibilita=EXCLUDED.reperibilita,note=EXCLUDED.note""",
           (user["id"],data,payload.turno,payload.ora_inizio,payload.ora_fine,
            ore["ore_diurne"],ore["ore_notturne"],ore["strao_diurno"],ore["strao_notturno"],
            ore["strao_fest_diurno"],ore["strao_fest_notturno"],tipo_rep or None,payload.note))
    else:
        conn.execute("""INSERT INTO turni
              (user_id,data,turno,ora_inizio,ora_fine,ore_diurne,ore_notturne,
               strao_diurno,strao_notturno,strao_fest_diurno,strao_fest_notturno,reperibilita,note)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(user_id,data) DO UPDATE SET
              turno=excluded.turno,ora_inizio=excluded.ora_inizio,ora_fine=excluded.ora_fine,
              ore_diurne=excluded.ore_diurne,ore_notturne=excluded.ore_notturne,
              strao_diurno=excluded.strao_diurno,strao_notturno=excluded.strao_notturno,
              strao_fest_diurno=excluded.strao_fest_diurno,strao_fest_notturno=excluded.strao_fest_notturno,
              reperibilita=excluded.reperibilita,note=excluded.note""",
           (user["id"],data,payload.turno,payload.ora_inizio,payload.ora_fine,
            ore["ore_diurne"],ore["ore_notturne"],ore["strao_diurno"],ore["strao_notturno"],
            ore["strao_fest_diurno"],ore["strao_fest_notturno"],tipo_rep or None,payload.note))
    conn.commit(); db.release_db(conn)
    return {"ok": True, **ore, "tipo_reperibilita": tipo_rep}

@router.delete("/api/turni/{data}")
def delete_turno(data: str, user=Depends(get_current_user)):
    conn = db.get_db()
    db.ex(conn, "DELETE FROM turni WHERE user_id=? AND data=?", (user["id"], data))
    conn.commit(); db.release_db(conn)
    return {"ok": True}

@router.delete("/api/turni-mese/{anno}/{mese}")
def delete_mese(anno: int, mese: int, user=Depends(get_current_user)):
    conn = db.get_db()
    if USE_PG:
        db.ex(conn, "DELETE FROM turni WHERE user_id=%s AND EXTRACT(YEAR FROM data::date)=%s AND EXTRACT(MONTH FROM data::date)=%s",
           (user["id"], anno, mese))
    else:
        db.ex(conn, "DELETE FROM turni WHERE user_id=? AND data LIKE ?",
           (user["id"], f"{anno:04d}-{mese:02d}-%"))
    conn.commit(); db.release_db(conn)
    return {"ok": True}

@router.get("/api/riepilogo/{anno}")
def get_riepilogo(anno: int, user=Depends(get_current_user)):
    conn = db.get_db()
    if USE_PG:
        rows = db.fetchall(conn, "SELECT * FROM turni WHERE user_id=%s AND EXTRACT(YEAR FROM data::date)=%s",
                        (user["id"], anno))
    else:
        rows = db.fetchall(conn, "SELECT * FROM turni WHERE user_id=? AND data LIKE ?",
                        (user["id"], f"{anno:04d}-%"))
    db.release_db(conn)
    mesi = {m: {
        "ore_diurne": 0.0, "ore_notturne": 0.0, "strao_diurno": 0.0, "strao_notturno": 0.0,
        "strao_fest_diurno": 0.0, "strao_fest_notturno": 0.0,
        "reperibilita_feriale": 0, "reperibilita_semifestiva": 0, "reperibilita_festiva": 0,
        "mal": 0, "ferie": 0, "rc": 0, "r": 0, "rot": 0, "rf": 0, "fest_riposo": 0
    } for m in range(1, 13)}
    for r in rows:
        d = r["data"]; d_str = d if isinstance(d, str) else d.isoformat()
        mes = int(d_str.split("-")[1]); m = mesi[mes]; t = r.get("turno") or ""
        for c in ["ore_diurne","ore_notturne","strao_diurno","strao_notturno","strao_fest_diurno","strao_fest_notturno"]:
            m[c] += r.get(c) or 0
        if t == "MAL": m["mal"] += 1
        if t in ("F","F-P","F-N"): m["ferie"] += 1
        if t == "RC": m["rc"] += 1
        if t == "R": m["r"] += 1
        if t == "ROT": m["rot"] += 1
        if t == "RF": m["rf"] += 1
        rep = r.get("reperibilita") or ""
        if rep == "feriale": m["reperibilita_feriale"] += 1
        elif rep == "semifestiva": m["reperibilita_semifestiva"] += 1
        elif rep == "festiva": m["reperibilita_festiva"] += 1
        if t in ("R", "RC") and d_str in FESTIVITA:
            m["fest_riposo"] += 1
    return mesi

@router.get("/api/impostazioni")
def get_impostazioni(user=Depends(get_current_user)):
    conn = db.get_db()
    s = get_user_settings(user["id"], conn)
    db.release_db(conn)
    return s

@router.post("/api/impostazioni")
def set_impostazioni(payload: ImpostazioniInput, user=Depends(get_current_user)):
    conn = db.get_db()
    for k, v in payload.valori.items():
        if USE_PG:
            db.ex(conn, """INSERT INTO impostazioni (user_id,chiave,valore) VALUES (%s,%s,%s)
               ON CONFLICT(user_id,chiave) DO UPDATE SET valore=EXCLUDED.valore""",
               (user["id"], k, str(v)))
        else:
            conn.execute("INSERT OR REPLACE INTO impostazioni (user_id,chiave,valore) VALUES (?,?,?)",
                         (user["id"], k, str(v)))
    conn.commit(); db.release_db(conn)
    return {"ok": True}

def _vuoto_totali_busta():
    return {"ore_diurne":0.0,"ore_notturne":0.0,"strao_diurno":0.0,"strao_notturno":0.0,
            "strao_fest_diurno":0.0,"strao_fest_notturno":0.0,
            "rep_feriale":0,"rep_semifestiva":0,"rep_festiva":0,
            "domeniche":0,"giorni_lavoro":0,"notte_assenza":0.0,"fest_riposo":0}

def _somma_turni_busta(rows):
    tot = _vuoto_totali_busta()
    for r in rows:
        d = r["data"]; d_str = d if isinstance(d, str) else d.isoformat()
        t = r.get("turno") or ""
        if TURNI_CONFIG.get(t, {}).get("lavorativo"):
            tot["giorni_lavoro"] += 1
            if date.fromisoformat(d_str).weekday() == 6: tot["domeniche"] += 1
        for c in ["ore_diurne","ore_notturne","strao_diurno","strao_notturno","strao_fest_diurno","strao_fest_notturno"]:
            tot[c] += r.get(c) or 0
        rep = r.get("reperibilita") or ""
        if rep == "feriale": tot["rep_feriale"] += 1
        elif rep == "semifestiva": tot["rep_semifestiva"] += 1
        elif rep == "festiva": tot["rep_festiva"] += 1
        if t in NOTTE_ASSENZA: tot["notte_assenza"] += NOTTE_ASSENZA[t]
        if t in ("R", "RC") and d_str in FESTIVITA: tot["fest_riposo"] += 1
    return tot

def _voci_competenze_busta(cfg, tot, ref_corrente, ref_variabili):
    return [
        {"voce":"Retribuzione totale mensile",   "ref":ref_corrente,"qty":None,"tariffa":None,"importo":cfg["retribuzione_totale"]},
        {"voce":"Indennita turno X",             "ref":ref_corrente,"qty":None,"tariffa":None,"importo":cfg["indennita_turno"]},
        {"voce":"Ore notturne in turno 50%",     "ref":ref_variabili,"qty":tot["ore_notturne"],       "tariffa":cfg["tariffa_nott_50"],        "importo":round(tot["ore_notturne"]*cfg["tariffa_nott_50"],2)},
        {"voce":"Indennita lavoro domenicale",   "ref":ref_variabili,"qty":tot["domeniche"]*8,        "tariffa":cfg["tariffa_dom"],            "importo":round(tot["domeniche"]*8*cfg["tariffa_dom"],2)},
        {"voce":"Lavoro ordinario notte",        "ref":ref_variabili,"qty":tot["notte_assenza"],      "tariffa":cfg["tariffa_nott_ord"],       "importo":round(tot["notte_assenza"]*cfg["tariffa_nott_ord"],2)},
        {"voce":"Str. Feriale Diurno 150%",      "ref":ref_variabili,"qty":tot["strao_diurno"],       "tariffa":cfg["tariffa_strao_fer_d"],   "importo":round(tot["strao_diurno"]*cfg["tariffa_strao_fer_d"],2)},
        {"voce":"Str. Feriale Notturno 160%",    "ref":ref_variabili,"qty":tot["strao_notturno"],     "tariffa":cfg["tariffa_strao_fer_n"],   "importo":round(tot["strao_notturno"]*cfg["tariffa_strao_fer_n"],2)},
        {"voce":"Str. Festivo Diurno 160%",      "ref":ref_variabili,"qty":tot["strao_fest_diurno"],  "tariffa":cfg["tariffa_strao_fest_d"],  "importo":round(tot["strao_fest_diurno"]*cfg["tariffa_strao_fest_d"],2)},
        {"voce":"Str. Festivo Notturno 175%",    "ref":ref_variabili,"qty":tot["strao_fest_notturno"],"tariffa":cfg["tariffa_strao_fest_n"],  "importo":round(tot["strao_fest_notturno"]*cfg["tariffa_strao_fest_n"],2)},
        {"voce":"Ind. Reperibilita Feriale",     "ref":ref_variabili,"qty":tot["rep_feriale"],        "tariffa":cfg["tariffa_rep_feriale"],   "importo":round(tot["rep_feriale"]*cfg["tariffa_rep_feriale"],2)},
        {"voce":"Ind. Reperibilita Semifestiva", "ref":ref_variabili,"qty":tot["rep_semifestiva"],    "tariffa":cfg["tariffa_rep_semifestiva"],"importo":round(tot["rep_semifestiva"]*cfg["tariffa_rep_semifestiva"],2)},
        {"voce":"Ind. Reperibilita Festiva",     "ref":ref_variabili,"qty":tot["rep_festiva"],        "tariffa":cfg["tariffa_rep_festiva"],   "importo":round(tot["rep_festiva"]*cfg["tariffa_rep_festiva"],2)},
        {"voce":"Festivita in giorno di riposo", "ref":ref_variabili,"qty":tot["fest_riposo"],        "tariffa":cfg["tariffa_fest_riposo"],   "importo":round(tot["fest_riposo"]*cfg["tariffa_fest_riposo"]*2,2)},
    ]

def _irpef_lorda(r, anno):
    if r <= 0: return 0.0
    imp, res = 0.0, r
    aliquota_secondo_scaglione = .33 if anno >= 2026 else .35
    for soglia, aliq in [(28000, .23), (22000, aliquota_secondo_scaglione), (float("inf"), .43)]:
        p = min(res, soglia); imp += p * aliq; res -= p
        if res <= 0: break
    return round(imp, 2)

def _detrazione_lavoro_dipendente(imp_ann, cfg):
    detrazione_base = cfg.get("detrazioni_annue", 1955.0)
    if imp_ann <= 0:
        return 0.0
    if imp_ann <= 15000:
        return round(max(detrazione_base, 690.0), 2)
    if imp_ann <= 28000:
        coeff = int(((28000 - imp_ann) / 13000) * 10000) / 10000
        det = 1910 + (1190 * coeff)
        if imp_ann > 25000:
            det += 65
        return round(det, 2)
    if imp_ann <= 50000:
        coeff = int(((50000 - imp_ann) / 22000) * 10000) / 10000
        det = 1910 * coeff
        if imp_ann <= 35000:
            det += 65
        return round(det, 2)
    return 0.0

def _ulteriore_detrazione_l207(imp_ann):
    if imp_ann <= 20000 or imp_ann > 40000:
        return 0.0
    if imp_ann <= 32000:
        return 1000.0
    return round(1000 * (40000 - imp_ann) / 8000, 2)

def _bonus_l207(imp_ann):
    if imp_ann <= 0 or imp_ann > 20000:
        return 0.0
    if imp_ann <= 8500:
        aliquota = .071
    elif imp_ann <= 15000:
        aliquota = .053
    else:
        aliquota = .048
    return round(imp_ann * aliquota, 2)

def _mese_precedente(anno, mese):
    return (anno, mese - 1) if mese > 1 else (anno - 1, 12)

def _stima_imponibile_annuo(anno, mese, cfg, rows_by_month, imponibile_mese_corrente):
    imponibili_presenti = []
    for mese_busta in range(1, mese + 1):
        anno_var, mese_var = _mese_precedente(anno, mese_busta)
        rows = rows_by_month.get((anno_var, mese_var), [])
        if not rows:
            continue
        tot = _somma_turni_busta(rows)
        voci = _voci_competenze_busta(cfg, tot, "", "")
        competenze = round(sum(v["importo"] for v in voci), 2)
        inps = round(competenze * cfg.get("aliquota_inps", 9.19) / 100, 2)
        imponibili_presenti.append(round(competenze - inps, 2))

    if not imponibili_presenti:
        media = imponibile_mese_corrente
        mesi_presenti = 1
    else:
        media = round(sum(imponibili_presenti) / len(imponibili_presenti), 2)
        mesi_presenti = len(imponibili_presenti)

    return round(sum(imponibili_presenti) + (12 - mesi_presenti) * media, 2), mesi_presenti, media

@router.get("/api/bustapaga/{anno}/{mese}")
def get_busta_paga(anno: int, mese: int, user=Depends(get_current_user)):
    mp = mese - 1 if mese > 1 else 12
    ap = anno if mese > 1 else anno - 1
    conn = db.get_db()
    if USE_PG:
        rows = db.fetchall(conn, "SELECT * FROM turni WHERE user_id=%s AND EXTRACT(YEAR FROM data::date)=%s AND EXTRACT(MONTH FROM data::date)=%s",
                        (user["id"], ap, mp))
    else:
        rows = db.fetchall(conn, "SELECT * FROM turni WHERE user_id=? AND data LIKE ?",
                        (user["id"], f"{ap:04d}-{mp:02d}-%"))
    cfg = get_user_settings(user["id"], conn)
    start_ann = date(anno - 1, 12, 1)
    end_ann = date(ap, mp, 31) if mp == 12 else date(ap, mp + 1, 1) - timedelta(days=1)
    if USE_PG:
        rows_ann = db.fetchall(conn, "SELECT * FROM turni WHERE user_id=%s AND data::date >= %s AND data::date <= %s",
                            (user["id"], start_ann.isoformat(), end_ann.isoformat()))
    else:
        rows_ann = db.fetchall(conn, "SELECT * FROM turni WHERE user_id=? AND data >= ? AND data <= ?",
                            (user["id"], start_ann.isoformat(), end_ann.isoformat()))
    db.release_db(conn)

    rows_by_month = {}
    for r in rows_ann:
        d = r["data"]; d_str = d if isinstance(d, str) else d.isoformat()
        key = tuple(int(x) for x in d_str[:7].split("-"))
        rows_by_month.setdefault(key, []).append(r)

    tot = _somma_turni_busta(rows)

    mi = ["","Gen","Feb","Mar","Apr","Mag","Giu","Lug","Ago","Set","Ott","Nov","Dic"]
    rp = f"{mi[mp]}/{str(ap)[-2:]}"; rc = f"{mi[mese]}/{str(anno)[-2:]}"

    vc = [
        {"voce":"Retribuzione totale mensile",   "ref":rc,"qty":None,"tariffa":None,"importo":cfg["retribuzione_totale"]},
        {"voce":"Indennità turno X",             "ref":rc,"qty":None,"tariffa":None,"importo":cfg["indennita_turno"]},
        {"voce":"Ore notturne in turno 50%",     "ref":rp,"qty":tot["ore_notturne"],       "tariffa":cfg["tariffa_nott_50"],        "importo":round(tot["ore_notturne"]*cfg["tariffa_nott_50"],2)},
        {"voce":"Indennità lavoro domenicale",   "ref":rp,"qty":tot["domeniche"]*8,        "tariffa":cfg["tariffa_dom"],            "importo":round(tot["domeniche"]*8*cfg["tariffa_dom"],2)},
        {"voce":"Lavoro ordinario notte",        "ref":rp,"qty":tot["notte_assenza"],      "tariffa":cfg["tariffa_nott_ord"],       "importo":round(tot["notte_assenza"]*cfg["tariffa_nott_ord"],2)},
        {"voce":"Str. Feriale Diurno 150%",      "ref":rp,"qty":tot["strao_diurno"],       "tariffa":cfg["tariffa_strao_fer_d"],   "importo":round(tot["strao_diurno"]*cfg["tariffa_strao_fer_d"],2)},
        {"voce":"Str. Feriale Notturno 160%",    "ref":rp,"qty":tot["strao_notturno"],     "tariffa":cfg["tariffa_strao_fer_n"],   "importo":round(tot["strao_notturno"]*cfg["tariffa_strao_fer_n"],2)},
        {"voce":"Str. Festivo Diurno 160%",      "ref":rp,"qty":tot["strao_fest_diurno"],  "tariffa":cfg["tariffa_strao_fest_d"],  "importo":round(tot["strao_fest_diurno"]*cfg["tariffa_strao_fest_d"],2)},
        {"voce":"Str. Festivo Notturno 175%",    "ref":rp,"qty":tot["strao_fest_notturno"],"tariffa":cfg["tariffa_strao_fest_n"],  "importo":round(tot["strao_fest_notturno"]*cfg["tariffa_strao_fest_n"],2)},
        {"voce":"Ind. Reperibilità Feriale",     "ref":rp,"qty":tot["rep_feriale"],        "tariffa":cfg["tariffa_rep_feriale"],   "importo":round(tot["rep_feriale"]*cfg["tariffa_rep_feriale"],2)},
        {"voce":"Ind. Reperibilità Semifestiva", "ref":rp,"qty":tot["rep_semifestiva"],    "tariffa":cfg["tariffa_rep_semifestiva"],"importo":round(tot["rep_semifestiva"]*cfg["tariffa_rep_semifestiva"],2)},
        {"voce":"Ind. Reperibilità Festiva",     "ref":rp,"qty":tot["rep_festiva"],        "tariffa":cfg["tariffa_rep_festiva"],   "importo":round(tot["rep_festiva"]*cfg["tariffa_rep_festiva"],2)},
        {"voce":"Festività in giorno di riposo", "ref":rp,"qty":tot["fest_riposo"],        "tariffa":cfg["tariffa_fest_riposo"],   "importo":round(tot["fest_riposo"]*cfg["tariffa_fest_riposo"]*2,2)},
    ]
    tc = round(sum(v["importo"] for v in vc), 2)
    inps = round(tc * cfg.get("aliquota_inps", 9.19) / 100, 2)
    imponibile_mese = round(tc - inps, 2)
    imp_ann, mesi_stima, media_imp = _stima_imponibile_annuo(anno, mese, cfg, rows_by_month, imponibile_mese)

    il = _irpef_lorda(imp_ann, anno)
    det_base = _detrazione_lavoro_dipendente(imp_ann, cfg)
    det_extra = _ulteriore_detrazione_l207(imp_ann) if anno >= 2025 else 0.0
    det = round(det_base + det_extra, 2)
    bonus_l207 = _bonus_l207(imp_ann) if anno >= 2025 else 0.0
    in_ = max(0.0, round(il - det, 2)); im = round(in_ / 12, 2)
    bonus_l207_mensile = round(bonus_l207 / 12, 2)

    vt = [
        {"voce": f"Contributi INPS ({cfg.get('aliquota_inps',9.19):.2f}%)", "importo": inps, "calcolato": True},
        {"voce": "IRPEF stimata mensile", "importo": im, "calcolato": True},
        {"voce": "Bonus fiscale L.207/2024 stimato", "importo": -bonus_l207_mensile if bonus_l207_mensile else 0.0, "calcolato": True},
        {"voce": "Trattenuta sindacato (CISL)", "importo": cfg["trattenuta_sindacato"]},
        {"voce": "Add. reg. da tratt. A.P.", "importo": cfg["trattenuta_regionale"]},
        {"voce": "Add. com. da tratt. A.P.", "importo": cfg.get("trattenuta_comunale", 0.0)},
        {"voce": "Contr. Prev. Compl. (Pegaso)", "importo": cfg["trattenuta_pegaso"]},
    ]
    tt = round(sum(v["importo"] for v in vt), 2)
    return {"anno": anno, "mese": mese, "mese_prec": mp, "anno_prec": ap,
            "ore_totali": tot, "voci_competenze": vc, "voci_trattenute": vt,
            "tot_competenze": tc, "tot_trattenute": tt, "netto": round(tc - tt, 2),
            "dettaglio_fiscale": {"imponibile_annuo_stimato": imp_ann, "irpef_lorda_annua": il,
                                  "detrazione_applicata": det, "irpef_netta_annua": in_,
                                  "inps_mensile": inps, "irpef_mensile": im,
                                  "detrazione_lavoro_dipendente": det_base,
                                  "ulteriore_detrazione_l207": det_extra,
                                  "bonus_l207_annuo": bonus_l207,
                                  "bonus_l207_mensile": bonus_l207_mensile,
                                  "mesi_usati_stima": mesi_stima,
                                  "imponibile_medio_mensile_stima": media_imp}}

@router.post("/api/tabella/applica")
def applica_tabella(payload: ApplicaTabella, user=Depends(get_current_user)):
    conn = db.get_db()
    tab = db.fetchone(conn, "SELECT * FROM tabelle_turni WHERE id=?", (payload.tab_id,))
    if not tab: db.release_db(conn); raise HTTPException(404, "Tabella non trovata")
    settimane = json.loads(tab["turni_json"])
    num_sett = len(settimane)
    data_inizio = date.fromisoformat(payload.data_inizio)
    data_fine = date.fromisoformat(payload.data_fine) if payload.data_fine else date(payload.anno_fine, 12, 31)
    sett_idx = (payload.settimana_inizio - 1) % num_sett
    giorno_idx = payload.giorno_inizio
    data_cur = data_inizio
    cur_sett = sett_idx
    cur_giorno = giorno_idx
    inseriti = 0

    def mins_to_hhmm(m):
        m = m % 1440
        return f"{m//60:02d}:{m%60:02d}"

    while data_cur <= data_fine:
        turno_raw = settimane[cur_sett][cur_giorno] if cur_sett < len(settimane) else ""
        turno = turno_raw.strip().split()[0] if turno_raw.strip() else ""
        if turno not in set(TURNI_CONFIG.keys()):
            turno = None
        if turno:
            data_str = data_cur.isoformat()
            ore = calcola_ore(turno, None, None, data_str)
            si_str = None
            sf_str = None
            if USE_PG:
                db.ex(conn, """INSERT INTO turni
                      (user_id,data,turno,ora_inizio,ora_fine,ore_diurne,ore_notturne,
                       strao_diurno,strao_notturno,strao_fest_diurno,strao_fest_notturno,reperibilita,note)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT(user_id,data) DO UPDATE SET
                      turno=EXCLUDED.turno,ora_inizio=EXCLUDED.ora_inizio,ora_fine=EXCLUDED.ora_fine,
                      ore_diurne=EXCLUDED.ore_diurne,ore_notturne=EXCLUDED.ore_notturne,
                      strao_diurno=EXCLUDED.strao_diurno,strao_notturno=EXCLUDED.strao_notturno,
                      strao_fest_diurno=EXCLUDED.strao_fest_diurno,strao_fest_notturno=EXCLUDED.strao_fest_notturno,
                      reperibilita=EXCLUDED.reperibilita""",
                   (user["id"],data_str,turno,si_str,sf_str,ore["ore_diurne"],ore["ore_notturne"],
                    ore["strao_diurno"],ore["strao_notturno"],ore["strao_fest_diurno"],ore["strao_fest_notturno"],None,None))
            else:
                conn.execute("""INSERT INTO turni
                      (user_id,data,turno,ora_inizio,ora_fine,ore_diurne,ore_notturne,
                       strao_diurno,strao_notturno,strao_fest_diurno,strao_fest_notturno,reperibilita,note)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(user_id,data) DO UPDATE SET
                      turno=excluded.turno,ora_inizio=excluded.ora_inizio,ora_fine=excluded.ora_fine,
                      ore_diurne=excluded.ore_diurne,ore_notturne=excluded.ore_notturne,
                      strao_diurno=excluded.strao_diurno,strao_notturno=excluded.strao_notturno,
                      strao_fest_diurno=excluded.strao_fest_diurno,strao_fest_notturno=excluded.strao_fest_notturno,
                      reperibilita=excluded.reperibilita""",
                   (user["id"],data_str,turno,si_str,sf_str,ore["ore_diurne"],ore["ore_notturne"],
                    ore["strao_diurno"],ore["strao_notturno"],ore["strao_fest_diurno"],ore["strao_fest_notturno"],None,None))
            inseriti += 1
        cur_giorno += 1
        if cur_giorno >= 7:
            cur_giorno = 0
            cur_sett = (cur_sett + 1) % num_sett
        data_cur += timedelta(days=1)
    conn.commit(); db.release_db(conn)
    return {"ok": True, "inseriti": inseriti}
