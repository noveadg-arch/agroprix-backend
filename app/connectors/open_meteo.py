"""
AgroPrix - Open-Meteo API connector.
Fetches 16-day weather forecasts for UEMOA capital cities.
"""

import logging

import httpx

from app.config import OPEN_METEO_BASE
from app.connectors.nasa_power import COUNTRY_COORDS

logger = logging.getLogger(__name__)


class OpenMeteoConnector:
    """Connector for the Open-Meteo forecast API (free, no API key)."""

    def get_forecast(self, country_key: str) -> dict:
        """Fetch a 16-day daily forecast for *country_key*.

        Parameters
        ----------
        country_key : str
            Key from ``COUNTRY_COORDS`` (e.g. "benin").

        Returns
        -------
        dict
            Structured forecast payload::

                {
                    "country": str,
                    "location": str,
                    "latitude": float,
                    "longitude": float,
                    "forecast": [
                        {
                            "date": "YYYY-MM-DD",
                            "temp_max": float | None,
                            "temp_min": float | None,
                            "precipitation": float | None,
                            "rain": float | None,
                            "wind_max": float | None,
                        },
                        ...
                    ],
                }
        """
        coords = COUNTRY_COORDS[country_key]
        lat, lon = coords["lat"], coords["lon"]

        params = {
            "latitude": lat,
            "longitude": lon,
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,rain_sum,windspeed_10m_max",
            "timezone": "Africa/Porto-Novo",
            "forecast_days": 16,
        }

        with httpx.Client(timeout=30) as client:
            resp = client.get(OPEN_METEO_BASE, params=params)
            resp.raise_for_status()
            data = resp.json()

        daily = data.get("daily", {})
        dates = daily.get("time", [])
        temp_max = daily.get("temperature_2m_max", [])
        temp_min = daily.get("temperature_2m_min", [])
        precipitation = daily.get("precipitation_sum", [])
        rain = daily.get("rain_sum", [])
        wind_max = daily.get("windspeed_10m_max", [])

        forecast = []
        for i, date in enumerate(dates):
            forecast.append(
                {
                    "date": date,
                    "temp_max": temp_max[i] if i < len(temp_max) else None,
                    "temp_min": temp_min[i] if i < len(temp_min) else None,
                    "precipitation": precipitation[i] if i < len(precipitation) else None,
                    "rain": rain[i] if i < len(rain) else None,
                    "wind_max": wind_max[i] if i < len(wind_max) else None,
                }
            )

        return {
            "country": country_key,
            "location": coords["name"],
            "latitude": lat,
            "longitude": lon,
            "forecast": forecast,
        }

    def get_all_forecasts(self):
        """Fetch forecasts for all eight UEMOA countries."""
        results = {}
        for country_key in COUNTRY_COORDS:
            try:
                results[country_key] = self.get_forecast(country_key)
            except Exception as exc:
                logger.error("Open-Meteo error for %s: %s", country_key, exc)
                results[country_key] = {"country": country_key, "error": str(exc)}
        return results

# Module-level singleton
meteo_connector = OpenMeteoConnector()
