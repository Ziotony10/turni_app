from datetime import datetime, timedelta
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi.security import OAuth2PasswordBearer
from fastapi import Depends, HTTPException

from app.config import SECRET_KEY, ALGORITHM, TOKEN_EXPIRE
import app.database as db

pwd_context   = CryptContext(schemes=["sha256_crypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

def hash_password(pwd):
    return pwd_context.hash(pwd)

def verify_password(p, h):
    return pwd_context.verify(p, h)

def create_token(user_id, username, is_admin=False):
    exp = datetime.utcnow() + timedelta(minutes=TOKEN_EXPIRE)
    return jwt.encode(
        {"sub": str(user_id), "username": username, "is_admin": is_admin, "exp": exp},
        SECRET_KEY, algorithm=ALGORITHM)

def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        uid = int(payload.get("sub"))
        if not uid: raise HTTPException(401, "Token non valido")
        conn = db.get_db()
        try:
            user = db.fetchone(conn, "SELECT id, username, nome, is_admin, is_editor, is_team_editor FROM utenti WHERE id=?", (uid,))
        finally:
            db.release_db(conn)
        if not user:
            raise HTTPException(401, "Utente non trovato")
        return {
            "id": user["id"],
            "username": user["username"],
            "nome": user.get("nome"),
            "is_admin": bool(user.get("is_admin")),
            "is_editor": bool(user.get("is_editor")),
            "is_team_editor": bool(user.get("is_team_editor")),
            "token_is_admin": bool(payload.get("is_admin", False)),
        }
    except JWTError:
        raise HTTPException(401, "Token non valido o scaduto")

def require_admin(user=Depends(get_current_user)):
    if not user.get("is_admin"):
        raise HTTPException(403, "Accesso riservato agli amministratori")
    return user

def require_editor(user=Depends(get_current_user)):
    if not user.get("is_editor") and not user.get("is_admin"):
        raise HTTPException(403, "Accesso riservato agli editor")
    return user

def require_team_editor(user=Depends(get_current_user)):
    if not user.get("is_editor") and not user.get("is_admin"):
        raise HTTPException(403, "Accesso riservato agli editor")
    return user
