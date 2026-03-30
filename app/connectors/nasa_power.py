"""
AgroPrix - NASA POWER API connector.
Fetches monthly climate data (temperature, precipitation, humidity, solar radiation)
for UEMOA capital cities.
"""

import logging
from datetime import datetime, timezone

import httpx
from sqlalchemy import text

from app.config import NASA_POWER_BASE, UEMOA_COUNTRIES
from app.database import get_engine

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Capital-city coordinates for each UEMOA country
# ---------------------------------------------------------------------------

COUNTRY_COORDS: dict[str, dict] = {
    "benin":         {"lat": 6.36,  "lon": 2.42,   "name": "Cotonou"},
    "burkina_faso":  {"lat": 12.37, "lon": -1.52,  "name": "Ouagadougou"},
    "cote_divoire":  {"lat": 5.35,  "lon": -4.02,  "name": "Abidjan"},
    "guinee_bissau": {"lat": 11.86, "lon": -15.60, "name": "Bissau"},
    "mali":          {"lat": 12.64, "lon": -8.00,  "name": "Bamako"},
    "niger":         {"lat": 13.51, "lon": 2.13,   "name": "Niamey"},
    "senegal":       {"lat": 14.69, "lon": -17.44, "name": "Dakar"},
    "togo":          {"lat": 6.17,  "lon": 1.23,   "name": "Lomé"},
}

# NASA POWER parameters of interest
PARAMETERS = "T2M,PRECTOTCORR,RH2M,ALLSKY_SFC_SW_DWN"


class NASAPowerConnector:
    """Connector for the NASA POWER monthly point API."""

    # ------------------------------------------------------------------
    # Data fetching
    # ------------------------------------------------------------------

    def fetch_weather(
        self,
        lat: float,
        lon: float,
        start_year: int = 2015,
        end_year: int = 2024,
    ) -> dict:
        """Fetch monthly weather data from NASA POWER.

        Parameters
        ----------
        lat, lon : float
            Geographic coordinates.
        start_year, end_year : int
            Year range (inclusive).

        Returns
        -------
        dict
            Raw JSON response from the API.
        """
        params = {
            "parameters": PARAMETERS,
            "community": "AG",
            "longitude": lon,
            "latitude": lat,
            "start": f"{start_year}01",
            "end": f"{end_year}12",
            "format": "JSON",
        }

        with httpx.Client(timeout=120) as client:
            resp = client.get(NASA_POWER_BASE, params=params)
            resp.raise_for_status()
            return resp.json()

    # ------------------------------------------------------------------
    # Sync helpers
    # ------------------------------------------------------------------

    def sync_country(self, country_key: str) -> dict:
        """Fetch and insert weather data for *country_key*.

        Parameters
        ----------
        country_key : str
            Key from ``COUNTRY_COORDS`` (e.g. "benin").

        Returns
        -------
        dict
            ``{"fetched": int, "inserted": int}``
        """
        coords = COUNTRY_COORDS[country_key]
        lat, lon = coords["lat"], coords["lon"]
        logger.info(
            "Syncing NASA POWER for %s (%s, lat=%.2f, lon=%.2f)",
            country_key, coords["name"], lat, lon,
        )

        engine = get_engine()
        fetched = 0
        inserted = 0

        try:
            data = self.fetch_weather(lat, lon)
            parameters = data.get("properties", {}).get("parameter", {})

            t2m = parameters.get("T2M", {})
            precip = parameters.get("PRECTOTCORR", {})
            humidity = parameters.get("RH2M", {})
            solar = parameters.get("ALLSKY_SFC_SW_DWN", {})

            rows = []
            for date_key in t2m:
                # date_key format: "YYYYMM" — skip annual summaries (13)
                if len(date_key) != 6:
                    continue
                year = int(date_key[:4])
                month = int(date_key[4:6])
                if month < 1 or month > 12:
                    continue

                fetched += 1

                def _clean(val):
                    """Convert NASA -999 sentinel to None."""
                    if val is None or val == -999 or val == -999.0:
                        return None
                    return float(val)

                rows.append(
                    {
                        "country": country_key,
                        "lat": lat,
                        "lon": lon,
                        "year": year,
                        "month": month,
                        "temperature": _clean(t2m.get(date_key)),
                        "precipitation": _clean(precip.get(date_key)),
                        "humidity": _clean(humidity.get(date_key)),
                        "solar_radiation": _clean(solar.get(date_key)),
                        "source": "NASA_POWER",
                    }
                )

            if rows:
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            """
                            INSERT INTO weather
                                (country, lat, lon, year, month,
                                 temperature, precipitation, humidity,
                                 solar_radiation, source)
                            VALUES
                                (:country, :lat, :lon, :year, :month,
                                 :temperature, :precipitation, :humidity,
                                 :solar_radiation, :source)
                            """
                        ),
                        rows,
                    )
                inserted = len(rows)

            # Log the sync
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO sync_log
                            (source, country, records_fetched, records_inserted, status)
                        VALUES
                            (:source, :country, :fetched, :inserted, :status)
                        """
                    ),
                    {
                        "source": "NASA_POWER",
                        "country": country_key,
                        "fetched": fetched,
                        "inserted": inserted,
                        "status": "success",
                    },
                )

            logger.info(
                "NASA POWER sync %s: fetched=%d inserted=%d",
                country_key, fetched, inserted,
            )

        except Exception as exc:
            logger.error("NASA POWER sync error for %s: %s", country_key, exc)
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO sync_log
                            (source, country, records_fetched, records_inserted,
                             status, error_message)
                        VALUES
                            (:source, :country, :fetched, :inserted,
                             :status, :error)
                        """
                    ),
                    {
                        "source": "NASA_POWER",
                        "country": country_key,
                        "fetched": fetched,
                        "inserted": inserted,
                        "status": "error",
                        "error": str(exc),
                    },
                )
            raise

        return {"fetched": fetched, "inserted": inserted}

    def sync_all_countries(self) -> dict[str, dict]:
        """Sync weather data for all eight UEMOA countries.

        Returns
        -------
        dict
            Mapping ``country_key -> {"fetched": int, "inserted": int}``.
        """
        results: dict[str, dict] = {}
        for country_key in COUNTRY_COORDS:
            try:
                results[country_key] = self.sync_country(country_key)
            except Exception as exc:
                logger.error("Skipping %s due to error: %s", country_key, exc)
                results[country_key] = {"fetched": 0, "inserted": 0, "error": str(exc)}
        return results

# Module-level singleton
nasa_connector = NASAPowerConnector()
