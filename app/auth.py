"""
AgroPrix - Module d'authentification JWT et controle d'acces par role (RBAC).
"""

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from fastapi import HTTPException, Request, status
from jose import JWTError, jwt
import bcrypt

from app.config import JWT_ALGORITHM, JWT_EXPIRE_HOURS, JWT_SECRET


# ---------------------------------------------------------------------------
# Hachage de mot de passe (bcrypt — resistant au brute-force)
# ---------------------------------------------------------------------------

def hash_password(password: str) -> str:
    """Hache un mot de passe avec bcrypt."""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(password: str, hashed: str) -> bool:
    """Verifie un mot de passe contre son hash bcrypt.
    Supporte aussi les anciens hash sha256_crypt ($5$) pour migration progressive.
    """
    if hashed.startswith("$5$"):
        # Legacy sha256_crypt — passlib fallback
        from passlib.hash import sha256_crypt
        return sha256_crypt.verify(password, hashed)
    return bcrypt.checkpw(password.encode(), hashed.encode())


# ---------------------------------------------------------------------------
# Creation / decodage de tokens JWT (python-jose)
# ---------------------------------------------------------------------------

def create_token(user_id: int, email: str, role: str, name: str = "") -> str:
    """Cree un token JWT avec expiration."""
    expire = datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRE_HOURS)
    payload = {
        "sub": str(user_id),
        "email": email,
        "role": role,
        "name": name,
        "exp": expire,
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_token(token: str) -> Dict:
    """Decode et valide un token JWT. Leve une exception si invalide/expire."""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token invalide ou expire",
            headers={"WWW-Authenticate": "Bearer"},
        )


# ---------------------------------------------------------------------------
# Dependances FastAPI
# ---------------------------------------------------------------------------

def _extract_token(request: Request) -> str:
    """Extrait le token Bearer du header Authorization."""
    auth_header: Optional[str] = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Header Authorization manquant ou invalide",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return auth_header[7:]  # retire "Bearer "


async def get_current_user(request: Request) -> Dict:
    """
    Dependance FastAPI : extrait le Bearer token, le decode,
    et retourne les infos utilisateur.

    Usage dans un endpoint :
        user = Depends(get_current_user)
    """
    # Demo mode fallback: no header or token == "demo" → return demo user
    auth_header: Optional[str] = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return {"id": 0, "email": "demo@agroprix.com", "name": "Utilisateur Demo", "role": "free", "country": "benin"}
    raw_token = auth_header[7:]
    if raw_token == "demo":
        return {"id": 0, "email": "demo@agroprix.com", "name": "Utilisateur Demo", "role": "free", "country": "benin"}

    token = raw_token
    payload = decode_token(token)

    user_id = payload.get("sub")
    email = payload.get("email")
    role = payload.get("role")
    name = payload.get("name", "")

    if not user_id or not email:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token invalide : donnees utilisateur manquantes",
        )

    return {
        "id": int(user_id),
        "email": email,
        "role": role or "free",
        "name": name,
    }


def require_role(*roles: str):
    """
    Fabrique de dependance qui verifie le role de l'utilisateur.

    Usage :
        @router.get("/admin-only", dependencies=[Depends(require_role("admin"))])
        async def admin_endpoint(): ...

    Ou directement :
        user = Depends(require_role("pro", "expert", "admin"))
    """
    async def _check_role(request: Request) -> Dict:
        user = await get_current_user(request)
        if user["role"] not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Acces refuse : role insuffisant. Roles requis : {}".format(
                    ", ".join(roles)
                ),
            )
        return user

    return _check_role
