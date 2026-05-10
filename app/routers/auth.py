from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security import OAuth2PasswordRequestForm

import app.database as db
from app.schemas import RegisterInput, ChangePasswordInput
from app.security import hash_password, verify_password, create_token, get_current_user
from app.services import _log_accesso

router = APIRouter(prefix="/api/auth", tags=["auth"])

@router.post("/register")
def register(payload: RegisterInput):
    if len(payload.username) < 3: raise HTTPException(400, "Username troppo corto (min 3)")
    if len(payload.password) < 6: raise HTTPException(400, "Password troppo corta (min 6)")
    conn = db.get_db()
    try:
        db.ex(conn, "INSERT INTO utenti (username, nome, password_hash) VALUES (?,?,?)",
           (payload.username.strip().lower(), payload.nome or payload.username, hash_password(payload.password)))
        conn.commit()
        user = db.fetchone(conn, "SELECT id, is_admin FROM utenti WHERE username=?", (payload.username.strip().lower(),))
        token = create_token(user["id"], payload.username.strip().lower(), bool(user.get("is_admin")))
        return {"access_token": token, "token_type": "bearer", "username": payload.username,
                "is_admin": bool(user.get("is_admin"))}
    except Exception as e:
        conn.rollback()
        if "UNIQUE" in str(e) or "unique" in str(e):
            raise HTTPException(400, "Username già esistente")
        raise HTTPException(500, str(e))
    finally:
        db.release_db(conn)

@router.post("/login")
def login(form: OAuth2PasswordRequestForm = Depends(), request: Request = None):
    conn = db.get_db()
    user = db.fetchone(conn, "SELECT * FROM utenti WHERE username=?", (form.username.strip().lower(),))
    success = bool(user and verify_password(form.password, user["password_hash"]))
    esito = "ok" if success else "fallito"
    _log_accesso(form.username.strip().lower(), esito, request)
    if not success:
        db.release_db(conn)
        raise HTTPException(401, "Credenziali non corrette")
    token = create_token(user["id"], user["username"], bool(user.get("is_admin")))
    db.release_db(conn)
    return {"access_token": token, "token_type": "bearer",
            "username": user["username"], "nome": user["nome"],
            "is_admin": bool(user.get("is_admin"))}

@router.get("/me")
def me(current_user=Depends(get_current_user)):
    return {
        "id": current_user["id"],
        "username": current_user["username"],
        "nome": current_user.get("nome"),
        "is_admin": bool(current_user.get("is_admin")),
        "is_editor": bool(current_user.get("is_editor")),
        "is_team_editor": bool(current_user.get("is_team_editor")),
    }

@router.post("/change-password")
def change_password(payload: ChangePasswordInput, user=Depends(get_current_user)):
    if len(payload.nuova_password) < 6:
        raise HTTPException(400, "Password troppo corta (min 6 caratteri)")
    conn = db.get_db()
    u = db.fetchone(conn, "SELECT password_hash FROM utenti WHERE id=?", (user["id"],))
    if not u or not verify_password(payload.password_attuale, u["password_hash"]):
        db.release_db(conn); raise HTTPException(400, "Password attuale non corretta")
    db.ex(conn, "UPDATE utenti SET password_hash=? WHERE id=?", (hash_password(payload.nuova_password), user["id"]))
    conn.commit(); db.release_db(conn)
    return {"ok": True}
