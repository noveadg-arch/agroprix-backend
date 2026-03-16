"""
Weather endpoints — historical + forecast.
"""

from fastapi import APIRouter, Query
from typing import Optional
from sqlalchemy import select, text

from app.database import weather, get_engine
from app.connectors.open_meteo import meteo_connector

router = APIRouter(prefix="/api/weather", tags=["Weather"])


@router.get("/historical")
async def get_historical_weather(
    country: str = Query(...),
    start_year: Optional[int] = Query(2020),
    end_year: Optional[int] = Query(2026),
):
    """Get stored historical weather data from NASA POWER."""
    engine = get_engine()
    try:
        q = select(weather).where(
            weather.c.country == country,
            weather.c.year >= start_year,
            weather.c.year <= end_year,
        ).order_by(weather.c.year, weather.c.month)

        with engine.connect() as conn:
            result = conn.execute(q)
            rows = [dict(r._mapping) for r in result]
    finally:
        engine.dispose()

    return {"count": len(rows), "data": rows}


@router.get("/forecast")
async def get_weather_forecast(country: str = Query(...)):
    """Get live 16-day weather forecast from Open-Meteo."""
    result = await meteo_connector.get_forecast(country)
    return result


@router.get("/correlation")
async def get_price_weather_correlation(
    country: str = Query(...),
    commodity: str = Query(...),
):
    """
    Get price + weather data aligned by month for correlation analysis.
    Frontend can use this to show price vs rainfall charts.
    """
    engine = get_engine()
    try:
        q = text("""
            SELECT
                p.month,
                p.avg_price,
                w.temperature,
                w.precipitation,
                w.humidity
            FROM (
                SELECT strftime('%Y-%m', date) as month,
                       AVG(price) as avg_price
                FROM prices
                WHERE country = :country AND commodity LIKE :commodity
                GROUP BY strftime('%Y-%m', date)
            ) p
            LEFT JOIN (
                SELECT country,
                       printf('%04d-%02d', year, month) as month,
                       temperature, precipitation, humidity
                FROM weather
                WHERE country = :country
            ) w ON p.month = w.month
            ORDER BY p.month
        """)

        with engine.connect() as conn:
            result = conn.execute(q, {
                "country": country,
                "commodity": f"%{commodity}%"
            })
            rows = [dict(r._mapping) for r in result]
    finally:
        engine.dispose()

    return {"count": len(rows), "data": rows}
