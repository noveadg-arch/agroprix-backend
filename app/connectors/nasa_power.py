"""
NASA POWER API connector.
Fetches historical weather data for agricultural analysis.

No registration needed. Free unlimited access.
Docs: https://power.larc.nasa.gov/docs/
"""

import httpx
from datetime import date
from sqlalchemy import insert

from app.config import settings
from app.database import weather, sync_log, get_engine

# Capital coordinates for each UEMOA country (used as reference points)
COUNTRY_COORDS = {
    "benin":         {"lat": 6.36,  "lon": 2.42,   "name": "Cotonou"},
    "burkina_faso":  {"lat": 12.37, "lon": -1.52,  "name": "Ouagadougou"},
    "cote_divoire":  {"lat": 5.35,  "lon": -4.02,  "name": "Abidjan"},
    "guinee_bissau": {"lat": 11.86, "lon": -15.60, "name": "Bissau"},
    "mali":          {"lat": 12.64, "lon": -8.00,  "name": "Bamako"},
    "niger":         {"lat": 13.51, "lon": 2.13,   "name": "Niamey"},
    "senegal":       {"lat": 14.69, "lon": -17.44, "name": "Dakar"},
    "togo":          {"lat": 6.17,  "lon": 1.23,   "name": "Lomé"},
}

# Parameters to fetch
PARAMETERS = "T2M,PRECTOTCORR,RH2M,ALLSKY_SFC_SW_DWN"


class NASAPowerConnector:

    async def fetch_weather(
        self,
        latitude: float,
        longitude: float,
        start_year: int = 2015,
        end_year: int = 2025,
    ) -> dict:
        """
        Fetch monthly weather data from NASA POWER.

        Returns dict with parameter arrays indexed by YYYYMM.
        """
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(
                settings.NASA_POWER_URL,
                params={
                    "parameters": PARAMETERS,
                    "community": "AG",
                    "longitude": longitude,
                    "latitude": latitude,
                    "start": start_year,
                    "end": end_year,
                    "format": "JSON",
                },
            )
            resp.raise_for_status()
            return resp.json()

    async def sync_country(self, country_key: str) -> dict:
        """Fetch and store weather data for a country."""
        coords = COUNTRY_COORDS.get(country_key)
        if not coords:
            return {"error": f"No coordinates for {country_key}"}

        engine = get_engine()
        total_inserted = 0

        try:
            data = await self.fetch_weather(coords["lat"], coords["lon"])
            params = data.get("properties", {}).get("parameter", {})

            t2m = params.get("T2M", {})
            prec = params.get("PRECTOTCORR", {})
            rh2m = params.get("RH2M", {})
            solar = params.get("ALLSKY_SFC_SW_DWN", {})

            rows = []
            for key in t2m:
                if len(key) != 6:  # Skip annual averages (keys like "ANN")
                    continue
                year = int(key[:4])
                month = int(key[4:])

                temp_val = t2m.get(key)
                prec_val = prec.get(key)
                rh_val = rh2m.get(key)
                solar_val = solar.get(key)

                # NASA uses -999 for missing data
                if temp_val == -999:
                    temp_val = None
                if prec_val == -999:
                    prec_val = None
                if rh_val == -999:
                    rh_val = None
                if solar_val == -999:
                    solar_val = None

                rows.append({
                    "country": country_key,
                    "latitude": coords["lat"],
                    "longitude": coords["lon"],
                    "year": year,
                    "month": month,
                    "temperature": temp_val,
                    "precipitation": prec_val,
                    "humidity": rh_val,
                    "solar_radiation": solar_val,
                    "source": "NASA_POWER",
                })

            if rows:
                with engine.connect() as conn:
                    conn.execute(insert(weather), rows)
                    conn.commit()
                total_inserted = len(rows)

            with engine.connect() as conn:
                conn.execute(insert(sync_log), {
                    "source": "NASA_POWER",
                    "country": country_key,
                    "records_fetched": len(rows),
                    "records_inserted": total_inserted,
                    "status": "success",
                })
                conn.commit()

        except Exception as e:
            with engine.connect() as conn:
                conn.execute(insert(sync_log), {
                    "source": "NASA_POWER",
                    "country": country_key,
                    "records_fetched": 0,
                    "records_inserted": 0,
                    "status": "error",
                    "error_message": str(e),
                })
                conn.commit()
            return {"error": str(e)}
        finally:
            engine.dispose()

        return {
            "country": country_key,
            "location": coords["name"],
            "inserted": total_inserted,
            "status": "success",
        }

    async def sync_all_countries(self) -> list:
        results = []
        for country_key in COUNTRY_COORDS:
            result = await self.sync_country(country_key)
            results.append(result)
        return results


nasa_connector = NASAPowerConnector()
