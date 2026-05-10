from pydantic import BaseModel
from typing import Optional, List

class RegisterInput(BaseModel):
    username: str
    password: str
    nome: Optional[str] = None

class TurnoInput(BaseModel):
    turno: Optional[str] = None
    ora_inizio: Optional[str] = None
    ora_fine: Optional[str] = None
    reperibilita: Optional[bool] = False
    note: Optional[str] = None

class ImpostazioniInput(BaseModel):
    valori: dict

class ResetPasswordInput(BaseModel):
    nuova_password: str

class ChangePasswordInput(BaseModel):
    password_attuale: str
    nuova_password: str

class TabellaTurniInput(BaseModel):
    nome: str
    tipo: str
    num_settimane: int
    turni: list

class ApplicaTabella(BaseModel):
    tab_id: int
    data_inizio: str
    data_fine: Optional[str] = None
    settimana_inizio: int
    giorno_inizio: int
    anno_fine: int

class TeamCellaInput(BaseModel):
    data: str
    operatore_id: int
    turno_base: Optional[str] = None   # None = non aggiornare
    turno_var: Optional[str] = None    # None = non aggiornare
    flags: Optional[str] = ""
    flags_base: Optional[str] = None
    flags_var: Optional[str] = None
    col: Optional[str] = "base"        # 'base' o 'var'

class TeamBulkInput(BaseModel):
    data_inizio: str
    settimana: list

class TeamOperatoreItem(BaseModel):
    nome: str
    posizione: int

class TeamOperatoriInput(BaseModel):
    operatori: List[TeamOperatoreItem]

class TeamOperatoreUpdateInput(BaseModel):
    nome: str
    posizione: int

class TeamOperatoreLinkInput(BaseModel):
    user_id: Optional[int] = None

class TeamColonneDestraInput(BaseModel):
    data: str
    rep1: Optional[str] = ""
    rep2: Optional[str] = ""
    rep3: Optional[str] = ""
    fest_m1: Optional[str] = ""
    fest_m2: Optional[str] = ""
    fest_p1: Optional[str] = ""
    fest_p2: Optional[str] = ""

class TeamTemplatePosizioneInput(BaseModel):
    posizione: int
    turno_base: Optional[str] = ""
    turno_var: Optional[str] = ""
    flags: Optional[str] = ""

class TeamTemplateReperibiliInput(BaseModel):
    rep1_pos: Optional[int] = None
    rep2_pos: Optional[int] = None
    rep3_pos: Optional[int] = None
    fest_m1_pos: Optional[int] = None
    fest_m2_pos: Optional[int] = None
    fest_p1_pos: Optional[int] = None
    fest_p2_pos: Optional[int] = None

class TeamTemplateWeekInput(BaseModel):
    posizioni: dict[str, List[TeamTemplatePosizioneInput]]
    reperibili: dict[str, TeamTemplateReperibiliInput] = {}
    start_date: Optional[str] = None
    end_date: Optional[str] = None

class TeamFerieBatchInput(BaseModel):
    add_dates: List[str] = []
    remove_dates: List[str] = []
    tipo: Optional[str] = "ferie"
    note: Optional[str] = None

class TeamFerieReviewInput(BaseModel):
    operatore_id: int
    dates: List[str]
    status: str

class TeamFerieRangeApplyInput(BaseModel):
    operatore_id: int
    start_date: str
    giorni: int

class TeamSwapRequestInput(BaseModel):
    collega_id: int
    data: str
    from_turno: str = ""
    to_turno: str = ""
    from_col: Optional[str] = None
    to_col: Optional[str] = None

class TeamSwapActionInput(BaseModel):
    action: str

class TeamNotificationUpdateInput(BaseModel):
    letto: bool

class DbCleanupPayload(BaseModel):
    target: str          # "ferie_closed_history" | "ferie_requests_processed" | "ferie_requests_all" | "ferie_log" | "login_visits"
