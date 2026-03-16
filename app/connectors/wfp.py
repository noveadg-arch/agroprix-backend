"""
WFP DataBridges API connector.
Fetches real market prices for UEMOA countries.

Registration (free): https://dataviz.vam.wfp.org/
API docs: https://dataviz.vam.wfp.org/api
"""

import httpx
from datetime import datetime, date
from typing import Optional
from sqlalchemy import insert, select

from app.config import settings
from app.database import prices, sync_log, get_engine


class WFPConnector:
    def __init__(self):
        self.token: Optional[str] = None
        self.token_expiry: Optional[datetime] = None

    async def _get_token(self, client: httpx.AsyncClient) -> str:
        """Get OAuth2 token from WFP API."""
        if self.token and self.token_expiry and datetime.now() < self.token_expiry:
            return self.token

        resp = await client.post(
            settings.WFP_TOKEN_URL,
            data={"grant_type": "client_credentials"},
            auth=(settings.WFP_API_KEY, settings.WFP_API_SECRET),
        )
        resp.raise_for_status()
        data = resp.json()
        self.token = data["access_token"]
        self.token_expiry = datetime.now()
        return self.token

    async def fetch_prices(
        self,
        country_code: str,
        commodity_id: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
    ) -> dict:
        """
        Fetch monthly market prices from WFP DataBridges.

        Args:
            country_code: ISO3 country code (e.g., "BEN" for Benin)
            commodity_id: WFP commodity ID (e.g., 51 for Maize)
            start_date: Start date "YYYY-MM-DD"
            end_date: End date "YYYY-MM-DD"
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            token = await self._get_token(client)

            params = {
                "CountryCode": country_code,
                "page": page,
                "format": "json",
            }
            if commodity_id:
                params["CommodityID"] = commodity_id
            if start_date:
                params["startDate"] = start_date
            if end_date:
                params["endDate"] = end_date

            resp = await client.get(
                f"{settings.WFP_BASE_URL}/MarketPrices/PriceMonthly",
                params=params,
                headers={"Authorization": f"Bearer {token}"},
            )
            resp.raise_for_status()
            return resp.json()

    async def sync_country(self, country_key: str) -> dict:
        """
        Sync all available price data for a country into the database.
        Returns stats about what was synced.
        """
        country_info = settings.COUNTRIES.get(country_key)
        if not country_info:
            return {"error": f"Unknown country: {country_key}"}

        iso3 = country_info["iso3"]
        engine = get_engine()
        total_inserted = 0
        total_fetched = 0

        try:
            data = await self.fetch_prices(iso3)
            items = data.get("items", [])
            total_fetched = len(items)

            rows = []
            for item in items:
                try:
                    price_date = date.fromisoformat(item.get("date", "")[:10])
                except (ValueError, TypeError):
                    continue

                rows.append({
                    "country": country_key,
                    "market": item.get("marketName", "Unknown"),
                    "commodity": item.get("commodityName", "Unknown"),
                    "price": float(item.get("commodityPrice", 0)),
                    "currency": item.get("currencyName", "XOF"),
                    "unit": item.get("commodityPriceUnit", "KG"),
                    "date": price_date,
                    "source": "WFP",
                    "latitude": item.get("marketLatitude"),
                    "longitude": item.get("marketLongitude"),
                })

            if rows:
                with engine.connect() as conn:
                    conn.execute(insert(prices), rows)
                    conn.commit()
                total_inserted = len(rows)

            # Log sync
            with engine.connect() as conn:
                conn.execute(insert(sync_log), {
                    "source": "WFP",
                    "country": country_key,
                    "records_fetched": total_fetched,
                    "records_inserted": total_inserted,
                    "status": "success",
                })
                conn.commit()

        except Exception as e:
            with engine.connect() as conn:
                conn.execute(insert(sync_log), {
                    "source": "WFP",
                    "country": country_key,
                    "records_fetched": total_fetched,
                    "records_inserted": 0,
                    "status": "error",
                    "error_message": str(e),
                })
                conn.commit()
            return {"error": str(e), "country": country_key}

        finally:
            engine.dispose()

        return {
            "country": country_key,
            "fetched": total_fetched,
            "inserted": total_inserted,
            "status": "success",
        }

    async def sync_all_countries(self) -> list:
        """Sync prices for all UEMOA countries."""
        results = []
        for country_key in settings.COUNTRIES:
            result = await self.sync_country(country_key)
            results.append(result)
        return results


wfp_connector = WFPConnector()
