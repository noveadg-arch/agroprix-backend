"""
AgroPrix — Connecteur NDVI via Planet Insights Platform.

Source : Analysis-Ready PlanetScope (ARPS) — imagerie 3m, corrigée atmosphériquement.
Authentification : PLANET_API_KEY (Bearer via HTTP Basic : key:"" en Base64).

Endpoints :
    GET  /api/ndvi/health                       → test clé + quota
    POST /api/ndvi/scenes                       → scènes PlanetScope sur AOI + période
    GET  /api/ndvi/parcelle/{parcelle_id}       → séries NDVI pour une parcelle
    POST /api/ndvi/order                        → (phase 2) soumet ordre bandmath NDVI
    GET  /api/ndvi/order/{planet_order_id}      → (phase 2) poll état + récupère GeoTIFFs

Phase 1 : listing des acquisitions Planet réelles disponibles (date, cloud_cover,
item_id, preview thumbnail).

Phase 2 (livrée) : Orders API async — POST /compute/ops/orders/v2 avec tools
[{"clip":{"aoi":...}}, {"bandmath":{"pl:bands":[{"expression":"(b8-b6)/(b8+b6)"}]}}]
sur PSScene 8-band (b6=red, b8=nir). Le client poll jusqu'à state=success puis
télécharge le GeoTIFF NDVI généré.

Phase 2b (à venir) : worker hors image Render (GDAL/rasterio) pour calculer les
stats zonales (mean/min/max NDVI) à partir des GeoTIFF téléchargés.
"""

from __future__ import annotations

import base64
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text

from app.auth import get_current_user
from app.database import get_engine

logger = logging.getLogger("agroprix.ndvi")

router = APIRouter(prefix="", tags=["ndvi"])

# ---------------------------------------------------------------------------
# Env & constants
# ---------------------------------------------------------------------------
PLANET_API_KEY = os.getenv("PLANET_API_KEY", "")
PLANET_API_BASE = "https://api.planet.com"
PLANET_DATA_BASE = f"{PLANET_API_BASE}/data/v1"
PLANET_ORDERS_BASE = f"{PLANET_API_BASE}/compute/ops/orders/v2"
PLANET_ITEM_TYPE = "PSScene"                 # PlanetScope 8-band (compat ARPS)
PLANET_MAX_CLOUD_COVER = 0.3                 # 30% max (tolérant pour UEMOA nuageux)
# Bandes PSScene 8-band : b6=red, b8=nir → NDVI = (nir-red)/(nir+red)
PLANET_NDVI_EXPRESSION = "(b8 - b6) / (b8 + b6)"


def _auth_header() -> dict:
    """Planet accepte l'API key en HTTP Basic (username=key, password=vide)."""
    if not PLANET_API_KEY:
        raise HTTPException(500, detail="PLANET_API_KEY non configuree (Railway env)")
    token = base64.b64encode(f"{PLANET_API_KEY}:".encode()).decode()
    return {"Authorization": f"Basic {token}", "Content-Type": "application/json"}


# ---------------------------------------------------------------------------
# Schémas
# ---------------------------------------------------------------------------
class GeoJSONPolygon(BaseModel):
    type: str = "Polygon"
    coordinates: list  # [[[lng,lat],...]]


class ScenesRequest(BaseModel):
    aoi: GeoJSONPolygon
    date_start: Optional[str] = Field(None, description="YYYY-MM-DD — défaut: J-90")
    date_end: Optional[str] = Field(None, description="YYYY-MM-DD — défaut: aujourd'hui")
    max_cloud_cover: float = Field(PLANET_MAX_CLOUD_COVER, ge=0.0, le=1.0)


# ---------------------------------------------------------------------------
# Client Planet
# ---------------------------------------------------------------------------
async def _planet_search(aoi: dict, date_start: str, date_end: str,
                          max_cloud: float = PLANET_MAX_CLOUD_COVER) -> list[dict]:
    """Appelle Planet Data API quick-search pour PSScene sur AOI + période."""
    filter_body = {
        "item_types": [PLANET_ITEM_TYPE],
        "filter": {
            "type": "AndFilter",
            "config": [
                {"type": "GeometryFilter", "field_name": "geometry", "config": aoi},
                {"type": "DateRangeFilter", "field_name": "acquired", "config": {
                    "gte": f"{date_start}T00:00:00Z",
                    "lte": f"{date_end}T23:59:59Z",
                }},
                {"type": "RangeFilter", "field_name": "cloud_cover",
                 "config": {"lte": max_cloud}},
            ],
        },
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{PLANET_DATA_BASE}/quick-search",
            headers=_auth_header(),
            json=filter_body,
        )

    if resp.status_code == 401:
        raise HTTPException(401, detail="Cle Planet invalide ou expiree")
    if resp.status_code == 429:
        raise HTTPException(429, detail="Quota Planet atteint (trial 30j ou rate limit)")
    if resp.status_code >= 400:
        logger.warning("Planet quick-search %s : %s", resp.status_code, resp.text[:300])
        raise HTTPException(502, detail=f"Planet API erreur {resp.status_code}")

    features = resp.json().get("features", [])
    scenes = []
    for f in features:
        p = f.get("properties") or {}
        scenes.append({
            "item_id": f.get("id"),
            "acquired": p.get("acquired"),
            "cloud_cover": p.get("cloud_cover"),
            "sun_elevation": p.get("sun_elevation"),
            "satellite_id": p.get("satellite_id"),
            "pixel_resolution": p.get("pixel_resolution"),
            "usable_data": p.get("usable_data"),
            "links": {
                "thumbnail": (f.get("_links") or {}).get("thumbnail"),
                "assets": (f.get("_links") or {}).get("assets"),
            },
        })
    scenes.sort(key=lambda s: s.get("acquired") or "", reverse=True)
    return scenes


# ---------------------------------------------------------------------------
# GET /ndvi/health — validation clé + quota
# ---------------------------------------------------------------------------
@router.get("/ndvi/health")
async def ndvi_health():
    """Vérifie la connectivité Planet + validité de la clé (aucun quota consommé)."""
    if not PLANET_API_KEY:
        return {"status": "misconfigured", "detail": "PLANET_API_KEY absente"}

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(f"{PLANET_DATA_BASE}/item-types",
                                headers=_auth_header())

    if resp.status_code == 200:
        types = [t.get("id") for t in resp.json().get("item_types", [])]
        return {
            "status": "ok",
            "provider": "Planet Insights Platform",
            "data_source": "Analysis-Ready PlanetScope (ARPS)",
            "key_prefix": PLANET_API_KEY[:8] + "...",
            "item_types_available": types[:10],
        }
    return {"status": "error", "http_status": resp.status_code,
            "detail": resp.text[:300]}


# ---------------------------------------------------------------------------
# POST /ndvi/scenes — recherche ad hoc sur un polygone
# ---------------------------------------------------------------------------
@router.post("/ndvi/scenes")
async def search_scenes(
    body: ScenesRequest,
    current_user: dict = Depends(get_current_user),
):
    """Retourne les scènes PlanetScope réelles disponibles sur AOI + période."""
    today = datetime.now(timezone.utc).date()
    start = body.date_start or (today - timedelta(days=90)).isoformat()
    end = body.date_end or today.isoformat()

    aoi = body.aoi.model_dump() if hasattr(body.aoi, "model_dump") else body.aoi.dict()
    scenes = await _planet_search(aoi, start, end, body.max_cloud_cover)

    return {
        "source": "Planet Insights Platform — PSScene",
        "aoi": aoi,
        "period": {"start": start, "end": end},
        "max_cloud_cover": body.max_cloud_cover,
        "count": len(scenes),
        "scenes": scenes,
        "ndvi_status": "acquisition_only — pixel extraction en phase 2 (Orders API)",
    }


# ---------------------------------------------------------------------------
# GET /ndvi/parcelle/{id} — séries sur une parcelle de l'utilisateur
# ---------------------------------------------------------------------------
def _parcelle_to_aoi(row: dict) -> dict:
    """Construit un AOI GeoJSON à partir d'une parcelle.

    Si la parcelle a un GeoJSON enregistré → on le prend.
    Sinon fallback : buffer carré 200m autour de (lat, lng).
    """
    # Cas 1 : géométrie polygonale déjà stockée (champ 'geojson' si présent)
    geom = row.get("geojson") or row.get("geometry")
    if geom and isinstance(geom, dict) and geom.get("type") == "Polygon":
        return geom

    lat = row.get("lat")
    lng = row.get("lng")
    if lat is None or lng is None:
        raise HTTPException(400, detail="Parcelle sans coordonnees — impossible de calculer AOI")

    # Buffer ~200m (0.0018° en latitude à l'équateur)
    d = 0.0018
    return {
        "type": "Polygon",
        "coordinates": [[
            [lng - d, lat - d],
            [lng + d, lat - d],
            [lng + d, lat + d],
            [lng - d, lat + d],
            [lng - d, lat - d],
        ]],
    }


@router.get("/ndvi/parcelle/{parcelle_id}")
async def ndvi_parcelle(
    parcelle_id: int,
    days: int = 90,
    max_cloud_cover: float = PLANET_MAX_CLOUD_COVER,
    current_user: dict = Depends(get_current_user),
):
    """Retourne les scènes Planet disponibles pour une parcelle de l'utilisateur."""
    engine = get_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT * FROM parcelles WHERE id = :pid AND user_id = :uid"),
            {"pid": parcelle_id, "uid": current_user["id"]},
        ).mappings().fetchone()

    if not row:
        raise HTTPException(404, detail="Parcelle introuvable")

    aoi = _parcelle_to_aoi(dict(row))
    today = datetime.now(timezone.utc).date()
    start = (today - timedelta(days=days)).isoformat()
    end = today.isoformat()

    scenes = await _planet_search(aoi, start, end, max_cloud_cover)

    return {
        "parcelle_id": parcelle_id,
        "parcelle_nom": row.get("nom"),
        "culture": row.get("culture"),
        "aoi": aoi,
        "period": {"start": start, "end": end, "days": days},
        "source": "Planet Insights Platform — PSScene (ARPS)",
        "count": len(scenes),
        "scenes": scenes,
        "ndvi_status": "acquisition_only — pixel extraction en phase 2 (Orders API)",
        "recommendation": (
            f"{len(scenes)} scene(s) exploitable(s) sur {days} jours "
            f"(< {int(max_cloud_cover*100)}% nuages). "
            "Phase 2 : calcul NDVI zonal via Orders API."
        ),
    }


# ---------------------------------------------------------------------------
# Phase 2 — Orders API : calcul NDVI pixel via bandmath
# ---------------------------------------------------------------------------
def _ensure_ndvi_orders_table():
    """Table de suivi des ordres NDVI soumis à Planet."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ndvi_orders (
                id SERIAL PRIMARY KEY,
                planet_order_id VARCHAR(100) UNIQUE,
                user_id INTEGER,
                parcelle_id INTEGER,
                item_ids TEXT,
                aoi TEXT,
                state VARCHAR(30) DEFAULT 'queued',
                assets TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                last_polled_at TIMESTAMP
            )
        """))


class NDVIOrderRequest(BaseModel):
    parcelle_id: int
    item_ids: list[str] = Field(..., min_length=1, max_length=20,
                                 description="IDs PSScene (retournés par /ndvi/parcelle/{id})")


@router.post("/ndvi/order")
async def submit_ndvi_order(
    body: NDVIOrderRequest,
    current_user: dict = Depends(get_current_user),
):
    """Soumet un ordre bandmath NDVI à Planet sur N scènes + AOI parcelle.

    L'Orders API est asynchrone : on récupère un `planet_order_id` immédiatement,
    puis on poll `/ndvi/order/{id}` jusqu'à `state=success`. Typiquement 2-10 min.
    Chaque scène consomme du quota Planet (compter ~2 km² par scène pour un buffer
    200m autour d'une parcelle).
    """
    import json as _json
    engine = get_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT * FROM parcelles WHERE id = :pid AND user_id = :uid"),
            {"pid": body.parcelle_id, "uid": current_user["id"]},
        ).mappings().fetchone()

    if not row:
        raise HTTPException(404, detail="Parcelle introuvable")

    aoi = _parcelle_to_aoi(dict(row))

    # Structure Orders API : produits + tools (clip → bandmath)
    order_body = {
        "name": f"AgroPrix_NDVI_parcelle_{body.parcelle_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "products": [{
            "item_ids": body.item_ids,
            "item_type": PLANET_ITEM_TYPE,
            "product_bundle": "analytic_8b_sr_udm2",  # 8-band Surface Reflectance
        }],
        "tools": [
            {"clip": {"aoi": aoi}},
            {"bandmath": {
                "pl:bands": [{"expression": PLANET_NDVI_EXPRESSION}],
            }},
        ],
        "delivery": {"single_archive": False},
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            PLANET_ORDERS_BASE,
            headers=_auth_header(),
            json=order_body,
        )

    if resp.status_code == 401:
        raise HTTPException(401, detail="Cle Planet invalide")
    if resp.status_code == 402:
        raise HTTPException(402, detail="Quota Planet depasse — credits insuffisants")
    if resp.status_code >= 400:
        logger.warning("Planet order %s : %s", resp.status_code, resp.text[:400])
        raise HTTPException(502, detail=f"Planet Orders erreur {resp.status_code}: {resp.text[:200]}")

    data = resp.json()
    planet_order_id = data.get("id")
    state = data.get("state", "queued")

    _ensure_ndvi_orders_table()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO ndvi_orders
              (planet_order_id, user_id, parcelle_id, item_ids, aoi, state)
            VALUES (:oid, :uid, :pid, :items, :aoi, :state)
            ON CONFLICT (planet_order_id) DO NOTHING
        """), {
            "oid": planet_order_id,
            "uid": current_user["id"],
            "pid": body.parcelle_id,
            "items": ",".join(body.item_ids),
            "aoi": _json.dumps(aoi),
            "state": state,
        })

    return {
        "planet_order_id": planet_order_id,
        "state": state,
        "parcelle_id": body.parcelle_id,
        "scene_count": len(body.item_ids),
        "ndvi_expression": PLANET_NDVI_EXPRESSION,
        "poll_url": f"/api/ndvi/order/{planet_order_id}",
        "message": "Ordre soumis — pollez le poll_url toutes les 30s jusqu'a state=success (typ. 2-10 min).",
    }


@router.get("/ndvi/order/{planet_order_id}")
async def poll_ndvi_order(
    planet_order_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Poll l'état d'un ordre NDVI. Renvoie les URLs GeoTIFF quand state=success."""
    import json as _json

    # Vérifier propriété
    engine = get_engine()
    with engine.begin() as conn:
        own = conn.execute(
            text("SELECT id FROM ndvi_orders WHERE planet_order_id = :oid AND user_id = :uid"),
            {"oid": planet_order_id, "uid": current_user["id"]},
        ).fetchone()
    if not own:
        raise HTTPException(404, detail="Ordre introuvable ou non autorise")

    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(
            f"{PLANET_ORDERS_BASE}/{planet_order_id}",
            headers=_auth_header(),
        )

    if resp.status_code == 404:
        raise HTTPException(404, detail="Ordre inconnu cote Planet")
    if resp.status_code >= 400:
        raise HTTPException(502, detail=f"Planet Orders poll erreur {resp.status_code}")

    data = resp.json()
    state = data.get("state", "unknown")
    # Quand success : _links._results[] contient les fichiers (incl. NDVI.tif)
    results = (data.get("_links") or {}).get("results") or []
    assets = [
        {"name": r.get("name"), "url": r.get("location"),
         "expires_at": r.get("expires_at")}
        for r in results
    ]
    ndvi_tifs = [a for a in assets if a.get("name", "").endswith(".tif")
                 or "bandmath" in (a.get("name") or "").lower()]

    # Mise à jour DB
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE ndvi_orders
               SET state = :state, assets = :assets, last_polled_at = NOW()
             WHERE planet_order_id = :oid
        """), {
            "state": state,
            "assets": _json.dumps(assets) if assets else None,
            "oid": planet_order_id,
        })

    return {
        "planet_order_id": planet_order_id,
        "state": state,
        "is_terminal": state in ("success", "failed", "partial", "cancelled"),
        "assets_count": len(assets),
        "ndvi_geotiffs": ndvi_tifs,
        "all_assets": assets,
        "note": (
            "GeoTIFF NDVI pret pour telechargement (URLs valides ~1h). "
            "Stats zonales (mean/min/max NDVI par parcelle) : phase 2b via worker "
            "rasterio hors image Render."
        ) if state == "success" else f"Ordre en cours (state={state})",
    }
