"""
Open-Meteo API connector.
Fetches weather forecasts (7-16 days) for agricultural planning.

No registration needed. 10,000 requests/day free.
Docs: https://open-meteo.com/en/docs
"""

import httpx
from app.config import settings
from app.connectors.nasa_power import COUNTRY_COORDS


class OpenMeteoConnector:

    async def get_forecast(self, country_key: str) -> dict:
        """Get 7-day weather forecast for a country's capital."""
        coords = COUNTRY_COORDS.get(country_key)
        if not coords:
            return {"error": f"No coordinates for {country_key}"}

        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                settings.OPEN_METEO_URL,
                params={
                    "latitude": coords["lat"],
                    "longitude": coords["lon"],
                    "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,rain_sum,windspeed_10m_max",
                    "timezone": "Africa/Porto-Novo",
                    "forecast_days": 16,
                },
            )
            resp.raise_for_status()
            data = resp.json()

        daily = data.get("daily", {})
        days = []
        dates = daily.get("time", [])
        for i, d in enumerate(dates):
            days.append({
                "date": d,
                "temp_max": daily.get("temperature_2m_max", [None])[i],
                "temp_min": daily.get("temperature_2m_min", [None])[i],
                "precipitation": daily.get("precipitation_sum", [None])[i],
                "rain": daily.get("rain_sum", [None])[i],
                "wind_max": daily.get("windspeed_10m_max", [None])[i],
            })

        return {
            "country": country_key,
            "location": coords["name"],
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "forecast": days,
        }

    async def get_all_forecasts(self) -> list:
        results = []
        for country_key in COUNTRY_COORDS:
            result = await self.get_forecast(country_key)
            results.append(result)
        return results


meteo_connector = OpenMeteoConnector()
