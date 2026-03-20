"""
Weather endpoints — historical data from NASA POWER + live forecasts from Open-Meteo.
"""

from fastapi import APIRouter, Query
from typing import Optional
from sqlalchemy import text

from app.database import get_engine, sql_year_month, sql_year_month_from_ym
from app.connectors.open_meteo import meteo_connector

router = APIRouter(prefix="", tags=["weather"])


@router.get("/historical")
async def get_historical_weather(
    country: str = Query(..., description="Country key (e.g., 'benin')"),
    start_year: int = Query(2020, description="Start year"),
    end_year: int = Query(2026, description="End year"),
):
    """Get stored historical weather data from NASA POWER."""
    engine = get_engine()
    try:
        q = text("""
            SELECT id, country, latitude, longitude, year, month,
                   temperature, precipitation, humidity, solar_radiation,
                   source, created_at
            FROM weather
            WHERE country = :country
              AND year >= :start_year
              AND year <= :end_year
            ORDER BY year, month
        """)

        with engine.connect() as conn:
            result = conn.execute(q, {
                "country": country,
                "start_year": start_year,
                "end_year": end_year,
            })
            rows = [dict(r._mapping) for r in result]
    finally:
        engine.dispose()

    return {"count": len(rows), "data": rows}


@router.get("/forecast")
async def get_weather_forecast(
    country: str = Query(..., description="Country key (e.g., 'benin')"),
):
    """
    Get live 16-day weather forecast from Open-Meteo.

    Returns daily forecast with temp_max, temp_min, precipitation, rain, wind_max.
    """
    result = meteo_connector.get_forecast(country)
    if "error" in result:
        return result
    # Response: {country, location, latitude, longitude, forecast: [{date, temp_max, temp_min, precipitation, rain, wind_max}, ...]}
    return result


@router.get("/correlation")
async def get_price_weather_correlation(
    country: str = Query(..., description="Country key"),
    commodity: str = Query(..., description="Commodity name"),
):
    """
    Join price and weather data by month for correlation analysis.
    Useful for price vs rainfall / temperature charts.
    """
    engine = get_engine()
    try:
        q = text(f"""
            SELECT
                p.month,
                p.avg_price,
                w.temperature,
                w.precipitation,
                w.humidity
            FROM (
                SELECT {sql_year_month('date')} as month,
                       ROUND(AVG(price), 1) as avg_price
                FROM prices
                WHERE country = :country AND commodity LIKE :commodity
                GROUP BY {sql_year_month('date')}
            ) p
            LEFT JOIN (
                SELECT country,
                       {sql_year_month_from_ym('year', 'month')} as month,
                       temperature, precipitation, humidity
                FROM weather
                WHERE country = :country
            ) w ON p.month = w.month
            ORDER BY p.month
        """)

        with engine.connect() as conn:
            result = conn.execute(q, {
                "country": country,
                "commodity": f"%{commodity}%",
            })
            rows = [dict(r._mapping) for r in result]
    finally:
        engine.dispose()

    return {"count": len(rows), "data": rows}
