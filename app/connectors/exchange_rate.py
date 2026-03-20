"""
AgroPrix - Exchange rate API connector.
Fetches XOF exchange rates from exchangerate-api.com (with API key)
or falls back to the free open.er-api.com endpoint.
"""

import logging
from datetime import datetime, timezone

import httpx
from sqlalchemy import text

from app.config import EXCHANGERATE_API_KEY, EXCHANGERATE_BASE
from app.database import get_engine

logger = logging.getLogger(__name__)

# Currencies to track against XOF (CFA Franc BCEAO)
TARGET_CURRENCIES = ["USD", "EUR", "NGN", "GHS", "GBP", "CNY", "INR"]


class ExchangeRateConnector:
    """Connector for exchange-rate APIs (XOF base)."""

    # ------------------------------------------------------------------
    # Data fetching
    # ------------------------------------------------------------------

    def fetch_rates(self) -> dict:
        """Fetch current XOF exchange rates.

        Uses exchangerate-api.com when ``EXCHANGERATE_API_KEY`` is set,
        otherwise falls back to the free open.er-api.com endpoint.

        Returns
        -------
        dict
            Mapping of currency code to rate (e.g. ``{"USD": 0.0016, ...}``).
        """
        with httpx.Client(timeout=30) as client:
            if EXCHANGERATE_API_KEY:
                url = f"{EXCHANGERATE_BASE}/{EXCHANGERATE_API_KEY}/latest/XOF"
                logger.info("Fetching exchange rates from exchangerate-api.com")
                resp = client.get(url)
            else:
                url = "https://open.er-api.com/v6/latest/XOF"
                logger.info("Fetching exchange rates from open.er-api.com (fallback)")
                resp = client.get(url)

            resp.raise_for_status()
            data = resp.json()

        all_rates = data.get("conversion_rates") or data.get("rates", {})

        # Filter to target currencies only
        rates: dict[str, float] = {}
        for currency in TARGET_CURRENCIES:
            if currency in all_rates:
                rates[currency] = float(all_rates[currency])
            else:
                logger.warning("Currency %s not found in API response", currency)

        return rates

    # ------------------------------------------------------------------
    # Sync helpers
    # ------------------------------------------------------------------

    def sync_rates(self) -> dict:
        """Fetch rates and insert into the ``exchange_rates`` table.

        Returns
        -------
        dict
            ``{"fetched": int, "inserted": int}``
        """
        engine = get_engine()
        fetched = 0
        inserted = 0

        try:
            rates = self.fetch_rates()
            fetched = len(rates)
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            rows = [
                {
                    "base_currency": "XOF",
                    "target_currency": currency,
                    "rate": rate,
                    "date": today,
                }
                for currency, rate in rates.items()
            ]

            if rows:
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            """
                            INSERT INTO exchange_rates
                                (base_currency, target_currency, rate, date)
                            VALUES
                                (:base_currency, :target_currency, :rate, :date)
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
                        "source": "EXCHANGE_RATE",
                        "country": None,
                        "fetched": fetched,
                        "inserted": inserted,
                        "status": "success",
                    },
                )

            logger.info(
                "Exchange rate sync: fetched=%d inserted=%d", fetched, inserted
            )

        except Exception as exc:
            logger.error("Exchange rate sync error: %s", exc)
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
                        "source": "EXCHANGE_RATE",
                        "country": None,
                        "fetched": fetched,
                        "inserted": inserted,
                        "status": "error",
                        "error": str(exc),
                    },
                )
            raise

        return {"fetched": fetched, "inserted": inserted}

# Module-level singleton
exchange_connector = ExchangeRateConnector()
