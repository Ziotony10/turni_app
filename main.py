from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.database import init_db
from app.routers import auth, admin, turni, team

app = FastAPI(title="Gestione Turni")

# Inizializza il database all'avvio
init_db()

# Includi i vari router dell'applicazione
app.include_router(auth.router)
app.include_router(admin.router)
app.include_router(turni.router)
app.include_router(team.router)

# Monta la cartella static per servire il frontend HTML/CSS/JS
app.mount("/", StaticFiles(directory="static", html=True), name="static")
