"""
AgroPrix - Health Check endpoint.
Vérifie : base de données, données de prix, connectivité APIs externes.
"""

import time
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import APIRouter
from sqlalchemy import text

from app.config import WFP_API_BASE, NASA_POWER_BASE
from app.database import get_engine

router = APIRouter()


@router.get("/health")
async def health_check():
    """
    Endpoint de santé complet — vérifie DB, données et sources externes.
    Retourne HTTP 200 si tout est OK, HTTP 503 si dégradé.
    """
    results = {}
    degraded = False
    start = time.time()

    # -----------------------------------------------------------------------
    # 1. Base de données — connexion + comptage des prix
    # -----------------------------------------------------------------------
    try:
        engine = get_engine()
        with engine.connect() as conn:
            count_row = conn.execute(text("SELECT COUNT(*) FROM prices")).fetchone()
            price_count = count_row[0] if count_row else 0

            # Derniere observation (colonne `date` = date du releve WFP,
            # plus pertinente pour l'utilisateur que `created_at`).
            latest_row = conn.execute(
                text("SELECT MAX(date) FROM prices")
            ).fetchone()
            latest = str(latest_row[0]) if latest_row and latest_row[0] else "unknown"

            # Repartition par source pour diagnostiquer seed vs reel
            src_rows = conn.execute(
                text("SELECT source, COUNT(*) AS n FROM prices GROUP BY source ORDER BY n DESC")
            ).fetchall()
            sources = {r[0] or "null": r[1] for r in src_rows}

        results["database"] = {
            "status": "ok",
            "price_records": price_count,
            "latest_observation": latest,
            "by_source": sources,
            "warning": "Aucun prix en base - lancer POST /api/sync/seed" if price_count == 0 else None,
        }
        if price_count == 0:
            degraded = True
    except Exception as e:
        results["database"] = {"status": "error", "message": str(e)}
        degraded = True

    # -----------------------------------------------------------------------
    # 2. WFP DataBridges API — accessibilité (sans consommer de quota)
    # -----------------------------------------------------------------------
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(WFP_API_BASE + "/Commodities/List?CountryCode=BJ", timeout=5.0)
        wfp_ok = resp.status_code in (200, 401, 403)  # 401/403 = API en ligne mais clé requise
        results["wfp_api"] = {
            "status": "reachable" if wfp_ok else "unreachable",
            "http_code": resp.status_code,
            "note": "Cle API WFP non configuree - donnees seedees utilisees" if resp.status_code in (401, 403) else None,
        }
        if not wfp_ok:
            degraded = True
    except Exception as e:
        results["wfp_api"] = {"status": "unreachable", "message": str(e)}
        # WFP non critique si données seedées présentes
        if results.get("database", {}).get("price_records", 0) == 0:
            degraded = True

    # -----------------------------------------------------------------------
    # 3. NASA POWER API — accessibilité météo
    # -----------------------------------------------------------------------
    try:
        # NASA_POWER_BASE pointe sur l'endpoint /monthly/point (utilise par le
        # connecteur reel pour alimenter la meteo pays). Cet endpoint attend
        # des dates au format YYYY (annee seule), pas YYYYMM ni YYYYMMDD.
        # On sonde l'annee derniere pour garantir la disponibilite des donnees.
        nasa_year = str(datetime.now(timezone.utc).year - 1)
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                NASA_POWER_BASE,
                params={"parameters": "T2M", "community": "AG", "longitude": "2.3", "latitude": "6.4",
                        "start": nasa_year, "end": nasa_year, "format": "JSON"},
                timeout=5.0,
            )
        nasa_ok = resp.status_code == 200
        results["nasa_power"] = {
            "status": "reachable" if nasa_ok else "degraded",
            "http_code": resp.status_code,
        }
    except Exception as e:
        results["nasa_power"] = {"status": "unreachable", "message": str(e)}

    # -----------------------------------------------------------------------
    # 4. Récapitulatif
    # -----------------------------------------------------------------------
    elapsed_ms = round((time.time() - start) * 1000)
    overall = "degraded" if degraded else "ok"

    payload = {
        "status": overall,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "response_time_ms": elapsed_ms,
        "checks": results,
    }

    if degraded:
        from fastapi import Response
        import json
        return Response(
            content=json.dumps(payload),
            status_code=503,
            media_type="application/json",
        )

    return payload
