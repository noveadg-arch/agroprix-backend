"""
AgroPrix - WFP DataBridges API connector.
Fetches monthly market prices for UEMOA countries via OAuth2 client credentials.
"""

import logging
import time
from datetime import datetime, timezone

import httpx
from sqlalchemy import text

from app.config import (
    UEMOA_COUNTRIES,
    WFP_API_BASE,
    WFP_API_KEY,
    WFP_API_SECRET,
    WFP_COMMODITIES,
    WFP_TOKEN_URL,
)
from app.database import get_engine

logger = logging.getLogger(__name__)


class WFPConnector:
    """Connector for the WFP VAM DataBridges API (OAuth2 client credentials)."""

    def __init__(self) -> None:
        self._token = None
        self._token_expiry: float = 0.0

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def _get_token(self) -> str:
        """Obtain or return a cached OAuth2 access token."""
        if self._token and time.time() < self._token_expiry:
            return self._token

        if not WFP_API_KEY or not WFP_API_SECRET:
            raise RuntimeError("WFP_API_KEY and WFP_API_SECRET must be set")

        with httpx.Client(timeout=30) as client:
            resp = client.post(
                WFP_TOKEN_URL,
                data={"grant_type": "client_credentials"},
                auth=(WFP_API_KEY, WFP_API_SECRET),
            )
            resp.raise_for_status()
            data = resp.json()

        self._token = data["access_token"]
        # Cache with a small margin (60 s) before real expiry
        expires_in = data.get("expires_in", 3600)
        self._token_expiry = time.time() + expires_in - 60
        logger.info("WFP token acquired (expires in %s s)", expires_in)
        return self._token

    # ------------------------------------------------------------------
    # Data fetching
    # ------------------------------------------------------------------

    def fetch_prices(
        self,
        country_code: str,
        commodity_id=None,
        start_date=None,
        end_date=None,
        page: int = 1,
        page_size: int = 1000,
    ) -> dict:
        """Fetch monthly market prices from WFP DataBridges.

        Parameters
        ----------
        country_code : str
            ISO3 country code (e.g. "BEN").
        commodity_id : int, optional
            WFP commodity ID to filter on.
        start_date, end_date : str, optional
            Date filters in YYYY-MM-DD format.
        page, page_size : int
            Pagination parameters.

        Returns
        -------
        dict
            Raw JSON response from the API.
        """
        token = self._get_token()
        params: dict = {
            "CountryCode": country_code,
            "page": page,
            "format": "json",
        }
        if commodity_id is not None:
            params["CommodityID"] = commodity_id
        if start_date:
            params["startDate"] = start_date
        if end_date:
            params["endDate"] = end_date
        if page_size:
            params["pageSize"] = page_size

        url = f"{WFP_API_BASE}/MarketPrices/PriceMonthly"

        with httpx.Client(timeout=60) as client:
            resp = client.get(
                url,
                params=params,
                headers={"Authorization": f"Bearer {token}"},
            )
            resp.raise_for_status()
            return resp.json()

    # ------------------------------------------------------------------
    # Sync helpers
    # ------------------------------------------------------------------

    def sync_country(self, country_key: str) -> dict:
        """Fetch all prices for *country_key* and insert them into the DB.

        Parameters
        ----------
        country_key : str
            Key from ``UEMOA_COUNTRIES`` (e.g. "benin").

        Returns
        -------
        dict
            ``{"fetched": int, "inserted": int}``
        """
        country_info = UEMOA_COUNTRIES[country_key]
        iso3 = country_info["iso3"]
        logger.info("Syncing WFP prices for %s (%s)", country_key, iso3)

        engine = get_engine()
        fetched = 0
        inserted = 0

        try:
            data = self.fetch_prices(country_code=iso3)
            items = data.get("items", [])
            fetched = len(items)

            rows = []
            for item in items:
                price_val = item.get("price") or item.get("Price")
                if price_val is None or float(price_val) <= 0:
                    continue

                # Parse date from various WFP formats
                raw_date = item.get("date") or item.get("Date") or item.get("mp_year", "")
                if raw_date:
                    try:
                        parsed = datetime.fromisoformat(str(raw_date).replace("Z", "+00:00"))
                        date_str = parsed.strftime("%Y-%m-%d")
                    except (ValueError, TypeError):
                        date_str = str(raw_date)
                else:
                    continue

                rows.append(
                    {
                        "country": country_key,
                        "market": item.get("market") or item.get("Market") or "Unknown",
                        "commodity": item.get("commodity") or item.get("Commodity") or "Unknown",
                        "price": float(price_val),
                        "currency": item.get("currency") or item.get("Currency") or "XOF",
                        "unit": item.get("unit") or item.get("Unit") or "KG",
                        "date": date_str,
                        "source": "WFP",
                        "latitude": item.get("latitude") or item.get("Latitude"),
                        "longitude": item.get("longitude") or item.get("Longitude"),
                    }
                )

            if rows:
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            """
                            INSERT INTO prices
                                (country, market, commodity, price, currency,
                                 unit, date, source, latitude, longitude)
                            VALUES
                                (:country, :market, :commodity, :price, :currency,
                                 :unit, :date, :source, :latitude, :longitude)
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
                        "source": "WFP",
                        "country": country_key,
                        "fetched": fetched,
                        "inserted": inserted,
                        "status": "success",
                    },
                )

            logger.info(
                "WFP sync %s: fetched=%d inserted=%d", country_key, fetched, inserted
            )

        except Exception as exc:
            logger.error("WFP sync error for %s: %s", country_key, exc)
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
                        "source": "WFP",
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
        """Sync prices for all eight UEMOA countries.

        Returns
        -------
        dict
            Mapping ``country_key -> {"fetched": int, "inserted": int}``.
        """
        results: dict[str, dict] = {}
        for country_key in UEMOA_COUNTRIES:
            try:
                results[country_key] = self.sync_country(country_key)
            except Exception as exc:
                logger.error("Skipping %s due to error: %s", country_key, exc)
                results[country_key] = {"fetched": 0, "inserted": 0, "error": str(exc)}
        return results

# Module-level singleton
wfp_connector = WFPConnector()
