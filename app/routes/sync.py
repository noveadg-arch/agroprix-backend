"""
Sync endpoints — trigger data collection from all APIs.
"""

from fastapi import APIRouter, Query
from typing import Optional
from sqlalchemy import select, text

from app.database import sync_log, get_engine
from app.connectors.wfp import wfp_connector
from app.connectors.nasa_power import nasa_connector
from app.connectors.exchange_rate import exchange_connector

router = APIRouter(prefix="/api/sync", tags=["Data Sync"])


@router.post("/wfp")
async def sync_wfp_prices(
    country: Optional[str] = Query(None, description="Specific country key, or all if omitted"),
):
    """Sync market prices from WFP DataBridges."""
    if country:
        result = await wfp_connector.sync_country(country)
        return {"results": [result]}
    else:
        results = await wfp_connector.sync_all_countries()
        return {"results": results}


@router.post("/weather")
async def sync_nasa_weather(
    country: Optional[str] = Query(None),
):
    """Sync historical weather from NASA POWER."""
    if country:
        result = await nasa_connector.sync_country(country)
        return {"results": [result]}
    else:
        results = await nasa_connector.sync_all_countries()
        return {"results": results}


@router.post("/exchange-rates")
async def sync_exchange_rates():
    """Sync current exchange rates."""
    result = await exchange_connector.sync_rates()
    return result


@router.post("/all")
async def sync_everything():
    """Sync ALL data sources. Run this first to populate the database."""
    results = {
        "wfp_prices": [],
        "nasa_weather": [],
        "exchange_rates": {},
    }

    # 1. Exchange rates (fastest)
    results["exchange_rates"] = await exchange_connector.sync_rates()

    # 2. NASA weather for all countries
    results["nasa_weather"] = await nasa_connector.sync_all_countries()

    # 3. WFP prices for all countries (slowest, most data)
    results["wfp_prices"] = await wfp_connector.sync_all_countries()

    return results


@router.get("/status")
async def sync_status():
    """Get last sync status for each source."""
    engine = get_engine()
    try:
        q = text("""
            SELECT source, country, records_fetched, records_inserted,
                   status, error_message, synced_at
            FROM sync_log
            ORDER BY synced_at DESC
            LIMIT 50
        """)
        with engine.connect() as conn:
            result = conn.execute(q)
            rows = [dict(r._mapping) for r in result]
    finally:
        engine.dispose()

    return {"logs": rows}
