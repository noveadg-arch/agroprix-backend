"""
AgroPrix — Gestion des abonnements & webhook FedaPay.

Deux endpoints :
  - POST /api/subscriptions           : appel frontend après paiement (optimiste,
                                         nécessite JWT ; l'activation n'est
                                         PROVISOIRE que jusqu'à la confirmation
                                         webhook).
  - POST /api/subscriptions/fedapay/webhook : webhook serveur-à-serveur signé
                                              par FedaPay (source de vérité).

La vérification webhook utilise HMAC-SHA256 sur le corps brut avec
FEDAPAY_WEBHOOK_SECRET (à définir dans Railway). L'événement
`transaction.approved` déclenche l'activation définitive du plan.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from pydantic import BaseModel
from sqlalchemy import text

from app.auth import get_current_user
from app.database import get_engine

logger = logging.getLogger("agroprix.subscriptions")

router = APIRouter(prefix="", tags=["subscriptions"])

# ---------------------------------------------------------------------------
# Env
# ---------------------------------------------------------------------------
FEDAPAY_SECRET_KEY = os.getenv("FEDAPAY_SECRET_KEY", "")          # sk_live_...
FEDAPAY_WEBHOOK_SECRET = os.getenv("FEDAPAY_WEBHOOK_SECRET", "")  # défini côté FedaPay dashboard
FEDAPAY_API_BASE = os.getenv("FEDAPAY_API_BASE", "https://api.fedapay.com/v1")

# ---------------------------------------------------------------------------
# Plans → rôles
# ---------------------------------------------------------------------------
_PLAN_TO_ROLE = {
    "Starter": "starter", "starter": "starter",
    "Pro": "pro", "pro": "pro",
    "Expert": "expert", "expert": "expert",
    "Plantain Pro": "pro",
    "Hevea Pro": "pro",
    "Institution": "expert",
}

# Montants attendus (FCFA) — toute incohérence montant/plan = rejet
_PLAN_AMOUNTS = {
    "Starter": 990, "Pro": 2900, "Expert": 14900,
    "Plantain Pro": 1490, "Hevea Pro": 1490,
}

# ---------------------------------------------------------------------------
# Table paiements (création idempotente)
# ---------------------------------------------------------------------------
def _ensure_payments_table() -> None:
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS payments (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id         INTEGER,
                plan            TEXT,
                amount          INTEGER,
                currency        TEXT DEFAULT 'XOF',
                transaction_id  TEXT UNIQUE,
                status          TEXT,
                provider        TEXT DEFAULT 'fedapay',
                raw_event       TEXT,
                created_at      TEXT,
                confirmed_at    TEXT
            )
        """))


# ---------------------------------------------------------------------------
# POST /subscriptions — appel frontend (optimiste, à confirmer par webhook)
# ---------------------------------------------------------------------------
class SubscriptionRequest(BaseModel):
    plan: str
    montant: int
    transactionId: Optional[str] = None


@router.post("/subscriptions")
async def activate_subscription(
    body: SubscriptionRequest,
    current_user: dict = Depends(get_current_user),
):
    """Enregistre l'intention de paiement d'un plan.

    IMPORTANT : cet endpoint ne modifie PAS le role de l'utilisateur. Il se
    contente d'inserer une ligne 'pending_webhook' dans la table payments. Le
    role n'est mis a jour que par le webhook FedaPay apres re-verification
    cote serveur (voir /subscriptions/fedapay/webhook).

    Eviter d'upgrader immediatement empeche un utilisateur d'obtenir les
    features premium sans paiement effectif.
    """
    role = _PLAN_TO_ROLE.get(body.plan)
    if not role:
        raise HTTPException(400, detail=f"Plan inconnu : {body.plan}")

    # Verification montant attendu (anti-manipulation frontend)
    expected = _PLAN_AMOUNTS.get(body.plan)
    if expected is None:
        raise HTTPException(400, detail=f"Plan sans grille tarifaire : {body.plan}")
    if body.montant != expected:
        raise HTTPException(400, detail=f"Montant incoherent pour {body.plan} : {body.montant} vs {expected}")

    _ensure_payments_table()
    user_id = current_user["id"]
    engine = get_engine()
    now = datetime.now(timezone.utc).isoformat()

    if body.transactionId:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT OR IGNORE INTO payments
                    (user_id, plan, amount, transaction_id, status, created_at)
                VALUES (:uid, :plan, :amt, :tx, 'pending_webhook', :ts)
            """), {
                "uid": user_id, "plan": body.plan, "amt": body.montant,
                "tx": str(body.transactionId), "ts": now,
            })

    return {
        "success": True,
        "user_id": user_id,
        "plan": body.plan,
        "role_pending": role,
        "transactionId": body.transactionId,
        "registered_at": now,
        "status": "pending_webhook",
        "confirmation": (
            "Intention enregistree. Le role sera active apres confirmation "
            "du webhook FedaPay (generalement < 1 min)."
        ),
    }


# ---------------------------------------------------------------------------
# POST /subscriptions/fedapay/webhook — source de vérité
# ---------------------------------------------------------------------------
def _verify_signature(raw_body: bytes, signature_header: str) -> bool:
    """Vérifie la signature FedaPay (format: 't=...,s=...' ou hex brut).

    FedaPay envoie `x-fedapay-signature` contenant un HMAC-SHA256 du corps brut
    de la requête, signé avec le secret webhook. On accepte les deux formats
    connus (hex simple ou 'v1=hex').
    """
    if not FEDAPAY_WEBHOOK_SECRET or not signature_header:
        return False

    computed = hmac.new(
        FEDAPAY_WEBHOOK_SECRET.encode("utf-8"),
        raw_body,
        hashlib.sha256,
    ).hexdigest()

    # Comparaison constante (anti timing-attack)
    # On tolère un préfixe style 'v1=' ou une virgule pour 't=...,v1=...'
    candidates = [signature_header.strip()]
    if "," in signature_header:
        for part in signature_header.split(","):
            if "=" in part:
                candidates.append(part.split("=", 1)[1].strip())
    if "=" in signature_header:
        candidates.append(signature_header.split("=", 1)[1].strip())

    return any(hmac.compare_digest(computed, c) for c in candidates)


async def _fetch_transaction(transaction_id: str) -> dict:
    """Appelle l'API FedaPay avec la clé secrète pour confirmer l'état.

    Défense en profondeur : même si la signature est valide, on re-vérifie
    l'état auprès de FedaPay avant d'activer le plan.
    """
    if not FEDAPAY_SECRET_KEY:
        raise HTTPException(500, detail="FEDAPAY_SECRET_KEY non configuree")

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(
            f"{FEDAPAY_API_BASE}/transactions/{transaction_id}",
            headers={"Authorization": f"Bearer {FEDAPAY_SECRET_KEY}"},
        )
    if resp.status_code != 200:
        logger.warning("FedaPay fetch %s → HTTP %s : %s",
                       transaction_id, resp.status_code, resp.text[:200])
        raise HTTPException(502, detail="Impossible de verifier la transaction aupres de FedaPay")
    return resp.json()


@router.post("/subscriptions/fedapay/webhook")
async def fedapay_webhook(
    request: Request,
    x_fedapay_signature: Optional[str] = Header(None),
):
    """Webhook FedaPay — source de vérité pour l'activation des plans.

    Événements traités :
      - transaction.approved → activation définitive du plan
      - transaction.declined / canceled → marquage échec (pas de downgrade auto)

    Configuration FedaPay dashboard :
      URL    : https://<backend>.railway.app/api/subscriptions/fedapay/webhook
      Événements : transaction.approved, transaction.declined, transaction.canceled
      Secret : copier la valeur dans FEDAPAY_WEBHOOK_SECRET côté Railway
    """
    raw_body = await request.body()

    # 1. Vérification signature
    if not _verify_signature(raw_body, x_fedapay_signature or ""):
        logger.warning("Webhook FedaPay : signature invalide (header=%s)", x_fedapay_signature)
        raise HTTPException(401, detail="Signature invalide")

    # 2. Parse payload
    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except Exception as e:
        raise HTTPException(400, detail=f"JSON invalide : {e}")

    event_name = payload.get("name") or payload.get("event") or ""
    entity = payload.get("entity") or payload.get("data") or {}
    transaction_id = str(entity.get("id") or "")
    status_str = (entity.get("status") or "").lower()
    amount = int(entity.get("amount") or 0)
    metadata = entity.get("custom_metadata") or entity.get("metadata") or {}

    logger.info("FedaPay webhook: event=%s tx=%s status=%s amount=%s",
                event_name, transaction_id, status_str, amount)

    if not transaction_id:
        raise HTTPException(400, detail="transaction.id manquant")

    _ensure_payments_table()
    engine = get_engine()
    now = datetime.now(timezone.utc).isoformat()

    # 3. Défense en profondeur — re-vérifier auprès de l'API FedaPay
    try:
        verified = await _fetch_transaction(transaction_id)
        vtx = verified.get("v1/transaction") or verified.get("transaction") or verified
        verified_status = (vtx.get("status") or "").lower()
        verified_amount = int(vtx.get("amount") or 0)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Re-verification FedaPay echouee")
        raise HTTPException(502, detail=f"Verification FedaPay impossible : {e}")

    # 4. Actions selon statut vérifié
    if verified_status == "approved" and event_name.endswith("approved"):
        # Récupérer user_id + plan depuis metadata OU retomber sur payments.pending
        user_id = metadata.get("user_id")
        plan = metadata.get("plan")

        if not user_id or not plan:
            with engine.begin() as conn:
                row = conn.execute(
                    text("SELECT user_id, plan FROM payments WHERE transaction_id = :tx"),
                    {"tx": transaction_id},
                ).fetchone()
            if row:
                user_id = row[0]
                plan = row[1]

        if not user_id or not plan:
            logger.error("Webhook approved sans metadata ni payment pending (tx=%s)", transaction_id)
            raise HTTPException(422, detail="Impossible d'identifier user_id/plan")

        role = _PLAN_TO_ROLE.get(plan, "starter")
        expected = _PLAN_AMOUNTS.get(plan)
        if expected and verified_amount != expected:
            logger.warning("Montant incoherent tx=%s : %s vs attendu %s",
                           transaction_id, verified_amount, expected)
            # On n'active pas : potentielle fraude
            raise HTTPException(400, detail="Montant verifie incoherent")

        with engine.begin() as conn:
            conn.execute(
                text("UPDATE users SET role = :role WHERE id = :uid"),
                {"role": role, "uid": int(user_id)},
            )
            conn.execute(text("""
                INSERT INTO payments
                    (user_id, plan, amount, transaction_id, status, raw_event, created_at, confirmed_at)
                VALUES (:uid, :plan, :amt, :tx, 'approved', :raw, :ts, :ts)
                ON CONFLICT(transaction_id) DO UPDATE SET
                    status = 'approved',
                    confirmed_at = :ts,
                    raw_event = :raw
            """), {
                "uid": int(user_id), "plan": plan, "amt": verified_amount,
                "tx": transaction_id, "raw": json.dumps(payload)[:4000], "ts": now,
            })

        return {"ok": True, "action": "plan_activated", "user_id": user_id, "plan": plan}

    if event_name.endswith("declined") or event_name.endswith("canceled"):
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO payments (user_id, plan, amount, transaction_id, status, raw_event, created_at)
                VALUES (:uid, :plan, :amt, :tx, :st, :raw, :ts)
                ON CONFLICT(transaction_id) DO UPDATE SET
                    status = :st,
                    raw_event = :raw
            """), {
                "uid": metadata.get("user_id"),
                "plan": metadata.get("plan"),
                "amt": amount,
                "tx": transaction_id,
                "st": verified_status or "failed",
                "raw": json.dumps(payload)[:4000],
                "ts": now,
            })
        return {"ok": True, "action": "payment_failed", "status": verified_status}

    # Événement non géré → on ACK quand même (sinon FedaPay retry)
    return {"ok": True, "action": "ignored", "event": event_name}
