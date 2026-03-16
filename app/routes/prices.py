"""
Price endpoints — the core of AgroPrix.
"""

from fastapi import APIRouter, Query
from typing import Optional
from sqlalchemy import select, func, text

from app.database import prices, get_engine
from app.config import settings

router = APIRouter(prefix="/api/prices", tags=["Prices"])


@router.get("/")
async def get_prices(
    country: str = Query(..., description="Country key (e.g., 'benin')"),
    commodity: Optional[str] = Query(None, description="Commodity name"),
    market: Optional[str] = Query(None, description="Market name"),
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    limit: int = Query(500, le=5000),
):
    """Get price records with filters."""
    engine = get_engine()
    try:
        q = select(prices).where(prices.c.country == country)
        if commodity:
            q = q.where(prices.c.commodity.ilike(f"%{commodity}%"))
        if market:
            q = q.where(prices.c.market.ilike(f"%{market}%"))
        if start_date:
            q = q.where(prices.c.date >= start_date)
        if end_date:
            q = q.where(prices.c.date <= end_date)
        q = q.order_by(prices.c.date.desc()).limit(limit)

        with engine.connect() as conn:
            result = conn.execute(q)
            rows = [dict(r._mapping) for r in result]
    finally:
        engine.dispose()

    return {"count": len(rows), "data": rows}


@router.get("/monthly")
async def get_monthly_averages(
    country: str = Query(...),
    commodity: str = Query(...),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Get monthly average prices for charting."""
    engine = get_engine()
    try:
        # SQLite date functions
        q = text("""
            SELECT
                strftime('%Y-%m', date) as month,
                AVG(price) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price,
                COUNT(*) as num_markets
            FROM prices
            WHERE country = :country
              AND commodity LIKE :commodity
              {date_filter}
            GROUP BY strftime('%Y-%m', date)
            ORDER BY month
        """.format(
            date_filter=(
                f"AND date >= :start_date AND date <= :end_date"
                if start_date and end_date
                else ""
            )
        ))

        params = {"country": country, "commodity": f"%{commodity}%"}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date

        with engine.connect() as conn:
            result = conn.execute(q, params)
            rows = [dict(r._mapping) for r in result]
    finally:
        engine.dispose()

    return {"count": len(rows), "data": rows}


@router.get("/markets")
async def get_markets(country: str = Query(...)):
    """Get all markets for a country with latest prices."""
    engine = get_engine()
    try:
        q = text("""
            SELECT DISTINCT market, latitude, longitude,
                   MAX(date) as last_update,
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
async def get_commodities(country: str = Query(...)):
    """Get all commodities tracked for a country."""
    engine = get_engine()
    try:
        q = text("""
            SELECT DISTINCT commodity,
                   COUNT(DISTINCT market) as num_markets,
                   MIN(date) as first_date,
                   MAX(date) as last_date,
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
    commodity: str = Query(...),
    date: Optional[str] = Query(None, description="Month YYYY-MM"),
):
    """Compare prices across all UEMOA countries for a commodity."""
    engine = get_engine()
    try:
        month_filter = f"AND strftime('%Y-%m', date) = :month" if date else ""
        q = text(f"""
            SELECT country, AVG(price) as avg_price,
                   MIN(price) as min_price, MAX(price) as max_price,
                   COUNT(DISTINCT market) as num_markets
            FROM prices
            WHERE commodity LIKE :commodity
            {month_filter}
            GROUP BY country
            ORDER BY avg_price DESC
        """)
        params = {"commodity": f"%{commodity}%"}
        if date:
            params["month"] = date

        with engine.connect() as conn:
            result = conn.execute(q, params)
            rows = [dict(r._mapping) for r in result]
    finally:
        engine.dispose()

    return {"count": len(rows), "data": rows}
