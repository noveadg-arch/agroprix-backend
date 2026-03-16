"""
ExchangeRate API connector.
Fetches real-time currency rates relevant to UEMOA trade.

Free tier: 1500 requests/month.
Registration: https://www.exchangerate-api.com/
"""

import httpx
from datetime import date
from sqlalchemy import insert

from app.config import settings
from app.database import exchange_rates, sync_log, get_engine

# Currencies relevant to UEMOA agricultural trade
TARGET_CURRENCIES = ["USD", "EUR", "NGN", "GHS", "GBP", "CNY", "INR"]


class ExchangeRateConnector:

    async def fetch_rates(self) -> dict:
        """Fetch current exchange rates for XOF."""
        if not settings.EXCHANGERATE_API_KEY or settings.EXCHANGERATE_API_KEY.startswith("your_"):
            return await self._fetch_free_rates()

        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                f"{settings.EXCHANGERATE_URL}/{settings.EXCHANGERATE_API_KEY}/latest/XOF"
            )
            resp.raise_for_status()
            return resp.json()

    async def _fetch_free_rates(self) -> dict:
        """Fallback: use open.er-api.com (no key needed, limited)."""
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get("https://open.er-api.com/v6/latest/XOF")
            resp.raise_for_status()
            return resp.json()

    async def sync_rates(self) -> dict:
        """Fetch and store current exchange rates."""
        engine = get_engine()
        total_inserted = 0

        try:
            data = await self.fetch_rates()
            rates = data.get("conversion_rates", data.get("rates", {}))
            today = date.today()

            rows = []
            for currency in TARGET_CURRENCIES:
                rate = rates.get(currency)
                if rate:
                    rows.append({
                        "base_currency": "XOF",
                        "target_currency": currency,
                        "rate": float(rate),
                        "date": today,
                    })

            if rows:
                with engine.connect() as conn:
                    conn.execute(insert(exchange_rates), rows)
                    conn.commit()
                total_inserted = len(rows)

            with engine.connect() as conn:
                conn.execute(insert(sync_log), {
                    "source": "EXCHANGE_RATE",
                    "records_fetched": len(rates),
                    "records_inserted": total_inserted,
                    "status": "success",
                })
                conn.commit()

        except Exception as e:
            with engine.connect() as conn:
                conn.execute(insert(sync_log), {
                    "source": "EXCHANGE_RATE",
                    "records_fetched": 0,
                    "records_inserted": 0,
                    "status": "error",
                    "error_message": str(e),
                })
                conn.commit()
            return {"error": str(e)}
        finally:
            engine.dispose()

        return {"inserted": total_inserted, "status": "success"}


exchange_connector = ExchangeRateConnector()
