"""
AgroPrix - Routes d'authentification.

POST /api/auth/register  - Inscription
POST /api/auth/login     - Connexion
GET  /api/auth/me        - Profil utilisateur (auth requise)
PUT  /api/auth/me        - Mise a jour du profil (auth requise)
"""

from datetime import datetime, timezone
from typing import Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select, update

from app.auth import (
    create_token,
    get_current_user,
    hash_password,
    verify_password,
)
from app.config import RATE_LIMIT_EXPERT, RATE_LIMIT_FREE, RATE_LIMIT_PRO
from app.database import get_engine, users

router = APIRouter(tags=["auth"])


# ---------------------------------------------------------------------------
# Schemas Pydantic
# ---------------------------------------------------------------------------

class RegisterRequest(BaseModel):
    email: str
    password: str = Field(..., min_length=6, description="Minimum 6 caracteres")
    name: str
    phone: Optional[str] = None
    country: Optional[str] = "benin"


class LoginRequest(BaseModel):
    email: str
    password: str


class UpdateProfileRequest(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    country: Optional[str] = None
    cultures: Optional[str] = None           # JSON array string: '["mais","cajou"]'
    superficie: Optional[float] = None       # hectares
    genre: Optional[str] = None              # homme/femme
    age: Optional[int] = None
    experience: Optional[int] = None         # years
    type_exploitation: Optional[str] = None  # individuel/cooperative/entreprise
    membre_cooperative: Optional[str] = None # yes/no + name
    profil_type: Optional[str] = None        # producteur/negociant/exportateur/proprietaire


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_RATE_LIMIT_BY_ROLE = {
    "free": RATE_LIMIT_FREE,
    "pro": RATE_LIMIT_PRO,
    "expert": RATE_LIMIT_EXPERT,
    "admin": "illimite",
}


def _user_response(row: Dict) -> Dict:
    """Formate les donnees utilisateur pour la reponse (sans mot de passe)."""
    role = row["role"] or "free"
    return {
        "id": row["id"],
        "email": row["email"],
        "nom": row["name"],
        "role": role,
        "telephone": row.get("phone"),
        "pays": row.get("country", "benin"),
        "cree_le": str(row.get("created_at", "")),
        "derniere_connexion": str(row.get("last_login", "")),
        "rate_limit": _RATE_LIMIT_BY_ROLE.get(role, RATE_LIMIT_FREE),
        "cultures": row.get("cultures"),
        "superficie": row.get("superficie"),
        "genre": row.get("genre"),
        "age": row.get("age"),
        "experience": row.get("experience"),
        "type_exploitation": row.get("type_exploitation"),
        "membre_cooperative": row.get("membre_cooperative"),
        "profil_type": row.get("profil_type"),
    }


# ---------------------------------------------------------------------------
# POST /register
# ---------------------------------------------------------------------------

@router.post("/register", status_code=status.HTTP_201_CREATED)
async def register(body: RegisterRequest):
    """Inscription d'un nouvel utilisateur."""
    engine = get_engine()

    # Verifier si l'email existe deja
    with engine.connect() as conn:
        existing = conn.execute(
            select(users).where(users.c.email == body.email)
        ).fetchone()
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Un compte avec cet email existe deja",
            )

    # Creer l'utilisateur
    hashed = hash_password(body.password)
    with engine.begin() as conn:
        result = conn.execute(
            users.insert().values(
                email=body.email,
                password_hash=hashed,
                name=body.name,
                role="free",
                phone=body.phone,
                country=body.country or "benin",
            )
        )
        user_id = result.inserted_primary_key[0]

    # Recuperer l'utilisateur cree
    with engine.connect() as conn:
        row = conn.execute(
            select(users).where(users.c.id == user_id)
        ).fetchone()
        user_dict = dict(row._mapping)

    token = create_token(
        user_id=user_dict["id"],
        email=user_dict["email"],
        role=user_dict["role"],
        name=user_dict["name"],
    )

    return {
        "token": token,
        "utilisateur": _user_response(user_dict),
    }


# ---------------------------------------------------------------------------
# POST /login
# ---------------------------------------------------------------------------

@router.post("/login")
async def login(body: LoginRequest):
    """Connexion d'un utilisateur existant."""
    engine = get_engine()

    with engine.connect() as conn:
        row = conn.execute(
            select(users).where(users.c.email == body.email)
        ).fetchone()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Email ou mot de passe incorrect",
        )

    user_dict = dict(row._mapping)

    if not verify_password(body.password, user_dict["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Email ou mot de passe incorrect",
        )

    # Mettre a jour la derniere connexion
    with engine.begin() as conn:
        conn.execute(
            update(users)
            .where(users.c.id == user_dict["id"])
            .values(last_login=datetime.now(timezone.utc))
        )

    token = create_token(
        user_id=user_dict["id"],
        email=user_dict["email"],
        role=user_dict["role"],
        name=user_dict["name"],
    )

    user_dict["last_login"] = datetime.now(timezone.utc)

    return {
        "token": token,
        "utilisateur": _user_response(user_dict),
    }


# ---------------------------------------------------------------------------
# GET /me
# ---------------------------------------------------------------------------

@router.get("/me")
async def me(current_user: Dict = Depends(get_current_user)):
    """Retourne le profil de l'utilisateur connecte."""
    engine = get_engine()

    with engine.connect() as conn:
        row = conn.execute(
            select(users).where(users.c.id == current_user["id"])
        ).fetchone()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Utilisateur introuvable",
        )

    return {"utilisateur": _user_response(dict(row._mapping))}


# ---------------------------------------------------------------------------
# PUT /me
# ---------------------------------------------------------------------------

@router.put("/me")
async def update_me(
    body: UpdateProfileRequest,
    current_user: Dict = Depends(get_current_user),
):
    """Met a jour le profil de l'utilisateur connecte (nom, telephone, pays)."""
    engine = get_engine()

    # Construire les champs a mettre a jour
    update_values: Dict = {}
    if body.name is not None:
        update_values["name"] = body.name
    if body.phone is not None:
        update_values["phone"] = body.phone
    if body.country is not None:
        update_values["country"] = body.country
    if body.cultures is not None:
        update_values["cultures"] = body.cultures
    if body.superficie is not None:
        update_values["superficie"] = body.superficie
    if body.genre is not None:
        update_values["genre"] = body.genre
    if body.age is not None:
        update_values["age"] = body.age
    if body.experience is not None:
        update_values["experience"] = body.experience
    if body.type_exploitation is not None:
        update_values["type_exploitation"] = body.type_exploitation
    if body.membre_cooperative is not None:
        update_values["membre_cooperative"] = body.membre_cooperative
    if body.profil_type is not None:
        update_values["profil_type"] = body.profil_type

    if not update_values:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Aucun champ a mettre a jour",
        )

    with engine.begin() as conn:
        conn.execute(
            update(users)
            .where(users.c.id == current_user["id"])
            .values(**update_values)
        )

    # Retourner le profil mis a jour
    with engine.connect() as conn:
        row = conn.execute(
            select(users).where(users.c.id == current_user["id"])
        ).fetchone()

    return {"utilisateur": _user_response(dict(row._mapping))}
