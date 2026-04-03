"""
AgroPrix — Gestion des parcelles agricoles.

Stocke en base de données (plus en localStorage) les parcelles géolocalisées.
Export GeoJSON RFC 7946 pour compatibilité EUDR, ECOAGRIS, SIG institutionnels.
EUDR score calculé selon GPS + date plantation + culture + surface.
"""

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy import text

from app.auth import get_current_user
from app.database import get_engine, parcelles

router = APIRouter(prefix="", tags=["parcelles"])


# ---------------------------------------------------------------------------
# Schémas Pydantic
# ---------------------------------------------------------------------------

class ParcelleCreate(BaseModel):
    nom: str
    culture: str                          # cacao, cajou, cafe, mais, riz, soja, hevea, autre
    surface_ha: Optional[float] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    pays: Optional[str] = "benin"
    region: Optional[str] = None
    date_plantation: Optional[str] = None  # YYYY-MM-DD
    notes: Optional[str] = None


class ParcelleUpdate(BaseModel):
    nom: Optional[str] = None
    culture: Optional[str] = None
    surface_ha: Optional[float] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    region: Optional[str] = None
    date_plantation: Optional[str] = None
    notes: Optional[str] = None


# ---------------------------------------------------------------------------
# EUDR Score (0-100)
# ---------------------------------------------------------------------------

EUDR_CULTURES = {"cacao", "cajou", "cafe", "soja", "hevea", "huile_palme",
                 "cocoa", "cashew", "coffee", "soybean", "rubber", "palm"}

def _compute_eudr_score(row: dict) -> dict:
    """Calculate EUDR compliance score based on available parcel data."""
    score = 0
    details = []

    if row.get("lat") and row.get("lng"):
        score += 35
        details.append({"criterion": "GPS geolocation", "points": 35, "status": "ok"})
    else:
        details.append({"criterion": "GPS geolocation", "points": 0, "max": 35, "status": "missing"})

    if row.get("date_plantation"):
        score += 25
        details.append({"criterion": "Plantation date", "points": 25, "status": "ok"})
    else:
        details.append({"criterion": "Plantation date", "points": 0, "max": 25, "status": "missing"})

    if row.get("surface_ha"):
        score += 15
        details.append({"criterion": "Surface area (ha)", "points": 15, "status": "ok"})
    else:
        details.append({"criterion": "Surface area", "points": 0, "max": 15, "status": "missing"})

    culture = (row.get("culture") or "").lower()
    if culture in EUDR_CULTURES:
        score += 15
        details.append({"criterion": "EUDR-relevant crop", "points": 15, "status": "ok"})
    else:
        details.append({"criterion": "EUDR-relevant crop", "points": 0, "max": 15, "status": "not_applicable"})

    if row.get("region"):
        score += 10
        details.append({"criterion": "Administrative region", "points": 10, "status": "ok"})
    else:
        details.append({"criterion": "Administrative region", "points": 0, "max": 10, "status": "missing"})

    if score >= 70:
        compliance = "compliant"
        label = "EUDR Compliant"
        color = "green"
    elif score >= 40:
        compliance = "partial"
        label = "EUDR Partial"
        color = "orange"
    else:
        compliance = "non_compliant"
        label = "EUDR Non-Compliant"
        color = "red"

    return {
        "score": score,
        "max_score": 100,
        "compliance": compliance,
        "label": label,
        "color": color,
        "criteria": details,
        "note": "Self-reported score. For regulatory EUDR compliance, submit DDS via EU TRACES platform.",
    }


# ---------------------------------------------------------------------------
# GET /parcelles
# ---------------------------------------------------------------------------

@router.get("/", summary="List user parcelles")
async def list_parcelles(
    pays: Optional[str] = Query(None, description="Filter by country"),
    culture: Optional[str] = Query(None, description="Filter by crop"),
    current_user: dict = Depends(get_current_user),
):
    engine = get_engine()
    conditions = ["user_id = :uid"]
    params = {"uid": current_user["id"]}
    if pays:
        conditions.append("pays = :pays")
        params["pays"] = pays
    if culture:
        conditions.append("culture LIKE :culture")
        params["culture"] = f"%{culture}%"

    where = " AND ".join(conditions)
    q = text(f"""
        SELECT id, nom, culture, surface_ha, lat, lng, pays, region,
               date_plantation, notes, created_at, updated_at
        FROM parcelles
        WHERE {where}
        ORDER BY created_at DESC
    """)
    with engine.connect() as conn:
        rows = [dict(r._mapping) for r in conn.execute(q, params)]

    for r in rows:
        r["eudr"] = _compute_eudr_score(r)

    return {"count": len(rows), "data": rows}


# ---------------------------------------------------------------------------
# POST /parcelles
# ---------------------------------------------------------------------------

@router.post("/", status_code=201, summary="Create a parcelle")
async def create_parcelle(
    body: ParcelleCreate,
    current_user: dict = Depends(get_current_user),
):
    engine = get_engine()
    now = datetime.now(timezone.utc).isoformat()
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                INSERT INTO parcelles
                    (user_id, nom, culture, surface_ha, lat, lng, pays, region,
                     date_plantation, notes, created_at, updated_at)
                VALUES
                    (:uid, :nom, :culture, :surface_ha, :lat, :lng, :pays, :region,
                     :date_plantation, :notes, :ts, :ts)
            """),
            {
                "uid": current_user["id"],
                "nom": body.nom,
                "culture": body.culture,
                "surface_ha": body.surface_ha,
                "lat": body.lat,
                "lng": body.lng,
                "pays": body.pays or "benin",
                "region": body.region,
                "date_plantation": body.date_plantation,
                "notes": body.notes,
                "ts": now,
            },
        )
        pid = result.lastrowid

    row = {"id": pid, **body.model_dump()}
    return {"id": pid, "eudr": _compute_eudr_score(row), "message": "Parcelle créée avec succès"}


# ---------------------------------------------------------------------------
# PUT /parcelles/{id}
# ---------------------------------------------------------------------------

@router.put("/{parcelle_id}", summary="Update a parcelle")
async def update_parcelle(
    parcelle_id: int,
    body: ParcelleUpdate,
    current_user: dict = Depends(get_current_user),
):
    engine = get_engine()
    # Ownership check
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id, user_id FROM parcelles WHERE id = :pid"),
            {"pid": parcelle_id},
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Parcelle introuvable")
    if row.user_id != current_user["id"] and current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Accès refusé")

    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    if not updates:
        raise HTTPException(status_code=400, detail="Aucun champ à mettre à jour")

    updates["updated_at"] = datetime.now(timezone.utc).isoformat()
    set_clause = ", ".join(f"{k} = :{k}" for k in updates)
    updates["pid"] = parcelle_id

    with engine.begin() as conn:
        conn.execute(text(f"UPDATE parcelles SET {set_clause} WHERE id = :pid"), updates)

    return {"message": "Parcelle mise à jour", "id": parcelle_id}


# ---------------------------------------------------------------------------
# DELETE /parcelles/{id}
# ---------------------------------------------------------------------------

@router.delete("/{parcelle_id}", summary="Delete a parcelle")
async def delete_parcelle(
    parcelle_id: int,
    current_user: dict = Depends(get_current_user),
):
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT user_id FROM parcelles WHERE id = :pid"),
            {"pid": parcelle_id},
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Parcelle introuvable")
    if row.user_id != current_user["id"] and current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Accès refusé")

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM parcelles WHERE id = :pid"), {"pid": parcelle_id})

    return {"message": "Parcelle supprimée", "id": parcelle_id}


# ---------------------------------------------------------------------------
# GET /parcelles/geojson — EUDR-ready export
# ---------------------------------------------------------------------------

@router.get(
    "/geojson",
    summary="Export parcelles as GeoJSON (EUDR-ready)",
    description=(
        "Exports all user parcelles as a GeoJSON FeatureCollection (RFC 7946). "
        "Each feature includes EUDR compliance score, crop type, surface area, and plantation date. "
        "Compatible with QGIS, ArcGIS, Google Earth, and EU TRACES platform."
    ),
)
async def export_geojson(
    pays: Optional[str] = Query(None),
    culture: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user),
):
    engine = get_engine()
    conditions = ["user_id = :uid", "lat IS NOT NULL", "lng IS NOT NULL"]
    params = {"uid": current_user["id"]}
    if pays:
        conditions.append("pays = :pays")
        params["pays"] = pays
    if culture:
        conditions.append("culture LIKE :culture")
        params["culture"] = f"%{culture}%"

    where = " AND ".join(conditions)
    q = text(f"""
        SELECT id, nom, culture, surface_ha, lat, lng, pays, region,
               date_plantation, notes, created_at
        FROM parcelles WHERE {where}
    """)
    with engine.connect() as conn:
        rows = [dict(r._mapping) for r in conn.execute(q, params)]

    features = []
    for r in rows:
        eudr = _compute_eudr_score(r)
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [r["lng"], r["lat"]],
            },
            "properties": {
                "id": r["id"],
                "nom": r["nom"],
                "culture": r["culture"],
                "surface_ha": r["surface_ha"],
                "pays": r["pays"],
                "region": r["region"],
                "date_plantation": r["date_plantation"],
                "notes": r["notes"],
                "eudr_score": eudr["score"],
                "eudr_compliance": eudr["compliance"],
                "eudr_label": eudr["label"],
                "created_at": r["created_at"],
            },
        })

    return {
        "type": "FeatureCollection",
        "name": f"AgroPrix Parcelles — {current_user.get('name', 'User')}",
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}},
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generator": "AgroPrix by 33Lab — https://agroprix.app",
        "eudr_ready": True,
        "features": features,
    }
