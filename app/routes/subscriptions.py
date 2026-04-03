"""
AgroPrix — Gestion des abonnements.

POST /api/subscriptions : active un plan après paiement KKiaPay réussi.
Met à jour users.role en base pour persister le plan entre les sessions.
"""

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import text

from app.auth import get_current_user
from app.database import get_engine

router = APIRouter(prefix="", tags=["subscriptions"])

# Map des noms de plans frontend → rôle en base
_PLAN_TO_ROLE = {
    "Starter": "starter",
    "starter": "starter",
    "Pro": "pro",
    "pro": "pro",
    "Expert": "expert",
    "expert": "expert",
    "Plantain Pro": "pro",
    "Hevea Pro": "pro",
    "Institution": "expert",
}


class SubscriptionRequest(BaseModel):
    plan: str
    montant: int
    transactionId: Optional[str] = None


@router.post("/subscriptions")
async def activate_subscription(
    body: SubscriptionRequest,
    current_user: dict = Depends(get_current_user),
):
    """Active un plan pour l'utilisateur après paiement KKiaPay confirmé."""
    role = _PLAN_TO_ROLE.get(body.plan)
    if not role:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Plan inconnu : {body.plan}. Plans valides : Starter, Pro, Expert",
        )

    user_id = current_user["id"]
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(
            text("UPDATE users SET role = :role WHERE id = :uid"),
            {"role": role, "uid": user_id},
        )

    return {
        "success": True,
        "user_id": user_id,
        "plan": body.plan,
        "role": role,
        "transactionId": body.transactionId,
        "activated_at": datetime.now(timezone.utc).isoformat(),
        "message": f"Plan {body.plan} activé avec succès. Reconnectez-vous pour rafraîchir vos droits.",
    }
