"""
Price endpoints — the core of AgroPrix.
Query market prices, monthly aggregations, markets, commodities, and regional comparisons.
"""

from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from sqlalchemy import text

from app.database import get_engine, sql_year_month
from app.config import UEMOA_COUNTRIES

router = APIRouter(prefix="", tags=["prices"])


@router.get("/")
async def list_prices(
    country: str = Query(..., description="Country key (e.g., 'benin')"),
    commodity: Optional[str] = Query(None, description="Commodity name (case-insensitive LIKE)"),
    market: Optional[str] = Query(None, description="Market name (case-insensitive LIKE)"),
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    limit: int = Query(500, le=5000, description="Max records (default 500, max 5000)"),
):
    """Get price records with filters."""
    engine = get_engine()
    try:
        # Build dynamic WHERE clauses
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

        where_clause = " AND ".join(conditions)

        q = text(f"""
            SELECT id, country, market, commodity, price, currency, unit,
                   date, source, latitude, longitude, created_at
            FROM prices
            WHERE {where_clause}
            ORDER BY date DESC
            LIMIT :limit
        """)

        with engine.connect() as conn:
            result = conn.execute(q, params)
            rows = [dict(r._mapping) for r in result]
    finally:
        engine.dispose()

    return {"count": len(rows), "data": rows}


@router.get("/monthly")
async def get_monthly_averages(
    country: str = Query(..., description="Country key"),
    commodity: str = Query(..., description="Commodity name"),
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
):
    """Get monthly aggregated prices (avg, min, max) for charting."""
    engine = get_engine()
    try:
        conditions = ["country = :country", "commodity LIKE :commodity"]
        params: dict = {"country": country, "commodity": f"%{commodity}%"}

        if start_date:
            conditions.append("date >= :start_date")
            params["start_date"] = start_date
        if end_date:
            conditions.append("date <= :end_date")
            params["end_date"] = end_date

        where_clause = " AND ".join(conditions)

        q = text(f"""
            SELECT
                {sql_year_month('date')} as month,
                ROUND(AVG(price), 1) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price,
                COUNT(DISTINCT market) as num_markets
            FROM prices
            WHERE {where_clause}
            GROUP BY {sql_year_month('date')}
            ORDER BY month
        """)

        with engine.connect() as conn:
            result = conn.execute(q, params)
            rows = [dict(r._mapping) for r in result]
    finally:
        engine.dispose()

    return {"count": len(rows), "data": rows}


@router.get("/markets")
async def list_markets(
    country: str = Query(..., description="Country key"),
):
    """List all markets for a country with metadata."""
    engine = get_engine()
    try:
        q = text(f"""
            SELECT DISTINCT
                market,
                latitude,
                longitude,
                MAX({sql_year_month('date')}) as last_update,
                COUNT(DISTINCT commodity) as commodities_tracked
            FROM prices
            WHERE country = :country
            GROUP BY market
            ORDER BY market
        """)
        with engine.connect() as conn:
            result = conn.execute(q, {"country": country})
            rows = [dict(r._mapping) for r in result]
    finally:
        engine.dispose()

    return {"count": len(rows), "data": rows}


@router.get("/commodities")
async def list_commodities(
    country: str = Query(..., description="Country key"),
):
    """List all commodities tracked for a country."""
    engine = get_engine()
    try:
        q = text(f"""
            SELECT DISTINCT
                commodity,
                COUNT(DISTINCT market) as num_markets,
                MIN({sql_year_month('date')}) as first_date,
                MAX({sql_year_month('date')}) as last_date,
                COUNT(*) as total_records
            FROM prices
            WHERE country = :country
            GROUP BY commodity
            ORDER BY commodity
        """)
        with engine.connect() as conn:
            result = conn.execute(q, {"country": country})
            rows = [dict(r._mapping) for r in result]
    finally:
        engine.dispose()

    return {"count": len(rows), "data": rows}


@router.get("/compare")
async def compare_regional(
    commodity: str = Query(..., description="Commodity name"),
    date: Optional[str] = Query(None, description="Month YYYY-MM (optional)"),
):
    """Compare prices across all UEMOA countries for a commodity."""
    engine = get_engine()
    try:
        conditions = ["commodity LIKE :commodity"]
        params: dict = {"commodity": f"%{commodity}%"}

        if date:
            conditions.append(f"{sql_year_month('date')} = :month")
            params["month"] = date

        where_clause = " AND ".join(conditions)

        q = text(f"""
            SELECT
                country,
                ROUND(AVG(price), 1) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price,
                COUNT(DISTINCT market) as num_markets
            FROM prices
            WHERE {where_clause}
            GROUP BY country
            ORDER BY avg_price DESC
        """)

        with engine.connect() as conn:
            result = conn.execute(q, params)
            rows = [dict(r._mapping) for r in result]
    finally:
        engine.dispose()

    return {"count": len(rows), "data": rows}
