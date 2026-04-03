"""
AgroPrix Public API v1 — Open data for institutional partners.

Authentication: X-API-Key header (free registration via POST /api/v1/keys/register)
Compatible: ECOAGRIS/ECOWAS, World Bank AgriConnect, USAID Digital Frontiers, Enabel
Standards: OpenAPI 3.0, GeoJSON RFC 7946, AGROVOC terminology
Rate limits: 200 req/hour (free), 2000 req/hour (institutional)
"""

import secrets
import hashlib
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, status
from pydantic import BaseModel, EmailStr
from sqlalchemy import text

from app.database import get_engine, api_keys, sql_year_month
from app.config import UEMOA_COUNTRIES

router = APIRouter(prefix="", tags=["Public API v1"])


# ---------------------------------------------------------------------------
# API Key authentication
# ---------------------------------------------------------------------------

def _hash_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode()).hexdigest()


async def get_api_key(x_api_key: Optional[str] = Header(None, alias="X-API-Key")) -> dict:
    """Validate API key from X-API-Key header. Required for all /api/v1/ endpoints."""
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required. Register for free at POST /api/v1/keys/register",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    engine = get_engine()
    key_hash = _hash_key(x_api_key)
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id, name, org, plan, is_active FROM api_keys WHERE key_hash = :h"),
            {"h": key_hash},
        ).fetchone()
        if not row or not row.is_active:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Invalid or inactive API key.",
            )
        # Update last_used
        conn.execute(
            text("UPDATE api_keys SET last_used_at = :ts WHERE key_hash = :h"),
            {"ts": datetime.now(timezone.utc).isoformat(), "h": key_hash},
        )
        conn.commit()
    return {"name": row.name, "org": row.org, "plan": row.plan}


# ---------------------------------------------------------------------------
# Key registration
# ---------------------------------------------------------------------------

class KeyRegisterRequest(BaseModel):
    name: str
    org: str
    email: str
    plan: Optional[str] = "free"   # free | institutional
    use_case: Optional[str] = None


@router.post(
    "/keys/register",
    summary="Register for a free API key",
    description=(
        "Get a free API key for AgroPrix Open Data. "
        "Institutional partners (Enabel, USAID, GIZ, AFD, IFAD, AfDB, World Bank) "
        "may request an institutional key with higher rate limits."
    ),
    tags=["API Keys"],
)
async def register_api_key(body: KeyRegisterRequest):
    raw_key = "agroprix_" + secrets.token_urlsafe(32)
    key_hash = _hash_key(raw_key)
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO api_keys (key_hash, name, org, email, plan, use_case, is_active, created_at) "
                "VALUES (:kh, :name, :org, :email, :plan, :use_case, 1, :ts)"
            ),
            {
                "kh": key_hash,
                "name": body.name,
                "org": body.org,
                "email": body.email,
                "plan": body.plan or "free",
                "use_case": body.use_case,
                "ts": datetime.now(timezone.utc).isoformat(),
            },
        )
    return {
        "api_key": raw_key,
        "plan": body.plan or "free",
        "rate_limit": "2000/hour" if body.plan == "institutional" else "200/hour",
        "instructions": {
            "header": "X-API-Key: " + raw_key,
            "example": "curl https://api.agroprix.app/api/v1/prices?country=benin&commodity=maize -H 'X-API-Key: " + raw_key + "'",
        },
        "documentation": "https://agroprix.app/api-docs",
        "ecoagris_compatible": True,
        "message": (
            "Your API key has been created. "
            "Keep it secret — it identifies your organization in our logs. "
            "For institutional access (higher limits, SLA, support), "
            "contact: api@agroprix.app"
        ),
    }


# ---------------------------------------------------------------------------
# GET /api/v1/prices
# ---------------------------------------------------------------------------

@router.get(
    "/prices",
    summary="Market prices — UEMOA zone",
    description=(
        "Returns agricultural market prices for a country in the UEMOA zone. "
        "Data sourced from WFP DataBridges and crowdsourced field observations. "
        "Compatible with ECOAGRIS/RESIMAO data interchange format. "
        "EUDR-relevant commodities: cacao (cocoa), cajou (cashew), cafe (coffee), soja (soybean)."
    ),
)
async def public_prices(
    country: str = Query(..., description="Country key: benin | cote_divoire | senegal | mali | burkina_faso | niger | togo | guinee_bissau"),
    commodity: Optional[str] = Query(None, description="Commodity name (partial match, case-insensitive). E.g. 'maize', 'cacao', 'cajou'"),
    market: Optional[str] = Query(None, description="Market name (partial match)"),
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    limit: int = Query(200, le=2000, description="Max records (default 200, max 2000)"),
    api_key_info: dict = Depends(get_api_key),
):
    engine = get_engine()
    conditions = ["country = :country"]
    params: dict = {"country": country, "limit": limit}

    if commodity:
        conditions.append("commodity LIKE :commodity")
        params["commodity"] = f"%{commodity}%"
    if market:
        conditions.append("market LIKE :market")
        params["market"] = f"%{market}%"
    if start_date:
        conditions.append("date >= :start_date")
        params["start_date"] = start_date
    if end_date:
        conditions.append("date <= :end_date")
        params["end_date"] = end_date

    where = " AND ".join(conditions)
    q = text(f"""
        SELECT country, market, commodity, price, currency, unit, date, source,
               latitude, longitude
        FROM prices
        WHERE {where}
        ORDER BY date DESC
        LIMIT :limit
    """)

    with engine.connect() as conn:
        rows = [dict(r._mapping) for r in conn.execute(q, params)]

    country_meta = UEMOA_COUNTRIES.get(country, {})
    return {
        "meta": {
            "api_version": "v1",
            "source": "AgroPrix by 33Lab",
            "country": country,
            "iso3": country_meta.get("iso3"),
            "iso2": country_meta.get("iso2"),
            "ecoagris_compatible": True,
            "eudr_relevant_commodities": ["cacao", "cajou", "cafe", "soja", "hevea"],
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "record_count": len(rows),
            "requester": api_key_info.get("org"),
        },
        "data": rows,
    }


# ---------------------------------------------------------------------------
# GET /api/v1/markets
# ---------------------------------------------------------------------------

@router.get(
    "/markets",
    summary="Markets list with geolocation",
    description=(
        "Returns all monitored markets for a country with GPS coordinates. "
        "Suitable for GIS integration, mapping, and ECOAGRIS node registry."
    ),
)
async def public_markets(
    country: str = Query(..., description="Country key"),
    api_key_info: dict = Depends(get_api_key),
):
    engine = get_engine()
    q = text(f"""
        SELECT
            market as name,
            MAX(latitude)  as latitude,
            MAX(longitude) as longitude,
            MAX({sql_year_month('date')}) as last_update,
            COUNT(DISTINCT commodity) as commodities_tracked,
            COUNT(*) as total_price_records
        FROM prices
        WHERE country = :country AND latitude IS NOT NULL
        GROUP BY market
        ORDER BY market
    """)
    with engine.connect() as conn:
        rows = [dict(r._mapping) for r in conn.execute(q, {"country": country})]

    # GeoJSON FeatureCollection (RFC 7946)
    features = []
    for r in rows:
        if r.get("latitude") and r.get("longitude"):
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [r["longitude"], r["latitude"]],
                },
                "properties": {
                    "name": r["name"],
                    "country": country,
                    "last_update": r["last_update"],
                    "commodities_tracked": r["commodities_tracked"],
                    "total_price_records": r["total_price_records"],
                },
            })

    return {
        "meta": {
            "api_version": "v1",
            "country": country,
            "iso3": UEMOA_COUNTRIES.get(country, {}).get("iso3"),
            "ecoagris_compatible": True,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "market_count": len(rows),
        },
        "geojson": {
            "type": "FeatureCollection",
            "features": features,
        },
        "data": rows,
    }


# ---------------------------------------------------------------------------
# GET /api/v1/commodities
# ---------------------------------------------------------------------------

@router.get(
    "/commodities",
    summary="Tracked commodities by country",
    description="Returns all commodities monitored in a country with data coverage statistics.",
)
async def public_commodities(
    country: str = Query(..., description="Country key"),
    api_key_info: dict = Depends(get_api_key),
):
    engine = get_engine()
    q = text(f"""
        SELECT
            commodity,
            COUNT(DISTINCT market) as num_markets,
            MIN(date) as first_date,
            MAX(date) as last_date,
            COUNT(*) as total_records,
            ROUND(AVG(price), 1) as avg_price_xof,
            MIN(price) as min_price_xof,
            MAX(price) as max_price_xof
        FROM prices
        WHERE country = :country
        GROUP BY commodity
        ORDER BY total_records DESC
    """)
    with engine.connect() as conn:
        rows = [dict(r._mapping) for r in conn.execute(q, {"country": country})]

    eudr_commodities = {"cacao", "cajou", "cafe", "soja", "hevea", "cocoa", "cashew", "coffee", "soybean"}
    for r in rows:
        r["eudr_relevant"] = any(kw in r["commodity"].lower() for kw in eudr_commodities)

    return {
        "meta": {
            "api_version": "v1",
            "country": country,
            "iso3": UEMOA_COUNTRIES.get(country, {}).get("iso3"),
            "ecoagris_compatible": True,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "commodity_count": len(rows),
        },
        "data": rows,
    }


# ---------------------------------------------------------------------------
# GET /api/v1/compare
# ---------------------------------------------------------------------------

@router.get(
    "/compare",
    summary="Regional price comparison — all UEMOA countries",
    description=(
        "Compare prices for a commodity across all 8 UEMOA countries. "
        "Detects arbitrage opportunities (price gaps >10%). "
        "Useful for regional food security monitoring and trade facilitation."
    ),
)
async def public_compare(
    commodity: str = Query(..., description="Commodity name (partial match). E.g. 'maize', 'rice', 'cacao'"),
    date: Optional[str] = Query(None, description="Month YYYY-MM (defaults to latest available)"),
    api_key_info: dict = Depends(get_api_key),
):
    engine = get_engine()
    conditions = ["commodity LIKE :commodity"]
    params: dict = {"commodity": f"%{commodity}%"}
    if date:
        conditions.append(f"{sql_year_month('date')} = :month")
        params["month"] = date

    where = " AND ".join(conditions)
    q = text(f"""
        SELECT
            country,
            ROUND(AVG(price), 1) as avg_price_xof,
            MIN(price) as min_price_xof,
            MAX(price) as max_price_xof,
            COUNT(DISTINCT market) as num_markets,
            MAX(date) as latest_date
        FROM prices
        WHERE {where}
        GROUP BY country
        ORDER BY avg_price_xof DESC
    """)
    with engine.connect() as conn:
        rows = [dict(r._mapping) for r in conn.execute(q, params)]

    # Arbitrage detection
    prices_vals = [r["avg_price_xof"] for r in rows if r["avg_price_xof"]]
    arbitrage = []
    if len(prices_vals) >= 2:
        max_p = max(prices_vals)
        min_p = min(prices_vals)
        if min_p and (max_p - min_p) / min_p > 0.10:
            max_country = next(r["country"] for r in rows if r["avg_price_xof"] == max_p)
            min_country = next(r["country"] for r in rows if r["avg_price_xof"] == min_p)
            arbitrage.append({
                "gap_pct": round((max_p - min_p) / min_p * 100, 1),
                "buy_in": min_country,
                "sell_in": max_country,
                "potential_margin_xof": round(max_p - min_p, 1),
            })

    for r in rows:
        r["iso3"] = UEMOA_COUNTRIES.get(r["country"], {}).get("iso3")

    return {
        "meta": {
            "api_version": "v1",
            "commodity": commodity,
            "period": date or "latest",
            "ecoagris_compatible": True,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "countries_with_data": len(rows),
        },
        "arbitrage_alerts": arbitrage,
        "data": rows,
    }


# ---------------------------------------------------------------------------
# GET /api/v1/status
# ---------------------------------------------------------------------------

@router.get(
    "/status",
    summary="API status and data coverage",
    description="Returns current data coverage, freshness, and API capabilities.",
    tags=["API Keys"],
)
async def public_status(api_key_info: dict = Depends(get_api_key)):
    engine = get_engine()
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM prices")).scalar()
        latest = conn.execute(text("SELECT MAX(date) FROM prices")).scalar()
        countries = conn.execute(text("SELECT COUNT(DISTINCT country) FROM prices")).scalar()
        commodities = conn.execute(text("SELECT COUNT(DISTINCT commodity) FROM prices")).scalar()
        markets = conn.execute(text("SELECT COUNT(DISTINCT market) FROM prices")).scalar()

    return {
        "status": "operational",
        "api_version": "v1",
        "provider": "AgroPrix by 33Lab",
        "contact": "api@agroprix.app",
        "documentation": "https://agroprix.app/api-docs",
        "ecoagris_compatible": True,
        "eudr_ready": True,
        "standards": ["OpenAPI 3.0", "GeoJSON RFC 7946", "AGROVOC"],
        "coverage": {
            "total_price_records": total,
            "latest_data_date": latest,
            "countries": countries,
            "commodities": commodities,
            "markets": markets,
            "zone": "UEMOA (8 countries)",
        },
        "rate_limits": {
            "free": "200 requests/hour",
            "institutional": "2000 requests/hour",
        },
        "requester": api_key_info,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
