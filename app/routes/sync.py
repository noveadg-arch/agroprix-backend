"""
Sync endpoints — trigger data collection from all external APIs.
Order for /all: exchange rates -> weather -> prices.
"""

from fastapi import APIRouter, Query
from typing import Optional
from sqlalchemy import text

from app.database import get_engine
from app.connectors.wfp import wfp_connector
from app.connectors.nasa_power import nasa_connector
from app.connectors.exchange_rate import exchange_connector

router = APIRouter(prefix="", tags=["sync"])


@router.post("/wfp")
async def sync_wfp_prices(
    country: Optional[str] = Query(None, description="Country key, or all if omitted"),
):
    """Sync market prices from WFP DataBridges."""
    if country:
        result = wfp_connector.sync_country(country)
        return {"results": [result]}
    else:
        results = wfp_connector.sync_all_countries()
        return {"results": results}


@router.post("/weather")
async def sync_nasa_weather(
    country: Optional[str] = Query(None, description="Country key, or all if omitted"),
):
    """Sync historical weather data from NASA POWER."""
    if country:
        result = nasa_connector.sync_country(country)
        return {"results": [result]}
    else:
        results = nasa_connector.sync_all_countries()
        return {"results": results}


@router.post("/exchange-rates")
async def sync_exchange_rates():
    """Sync current exchange rates (XOF base)."""
    result = exchange_connector.sync_rates()
    return result


@router.post("/all")
async def sync_everything():
    """
    Sync ALL data sources in order:
    1. Exchange rates (fastest)
    2. NASA POWER weather (all countries)
    3. WFP prices (all countries, slowest)
    """
    results = {
        "exchange_rates": {},
        "nasa_weather": [],
        "wfp_prices": [],
    }

    # 1. Exchange rates first
    results["exchange_rates"] = exchange_connector.sync_rates()

    # 2. NASA weather for all countries
    results["nasa_weather"] = nasa_connector.sync_all_countries()

    # 3. WFP prices for all countries
    results["wfp_prices"] = wfp_connector.sync_all_countries()

    return results


@router.get("/status")
async def sync_status():
    """Get last 50 sync log entries."""
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
