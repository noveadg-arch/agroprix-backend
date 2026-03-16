"""
Import all WFP CSV files into the SQLite database.
Run once after downloading CSVs from HDX.

Usage: python3 import_csv.py
"""

import csv
import os
import sys
from datetime import date
from sqlalchemy import insert, text

# Add parent to path
sys.path.insert(0, os.path.dirname(__file__))

from app.database import init_db, prices, sync_log, get_engine

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

# Map CSV filenames to our country keys
CSV_FILES = {
    "wfp_benin.csv":          "benin",
    "wfp_burkina_faso.csv":   "burkina_faso",
    "wfp_cote_d_ivoire.csv":  "cote_divoire",
    "wfp_guinea_bissau.csv":  "guinee_bissau",
    "wfp_mali.csv":           "mali",
    "wfp_niger.csv":          "niger",
    "wfp_senegal.csv":        "senegal",
    "wfp_togo.csv":           "togo",
}


def import_csv(filepath: str, country_key: str, engine) -> dict:
    """Import a single WFP CSV file into the prices table."""
    rows = []
    skipped = 0

    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                price_date = date.fromisoformat(row["date"])
                price_val = float(row["price"])
                if price_val <= 0:
                    skipped += 1
                    continue

                lat = float(row["latitude"]) if row.get("latitude") else None
                lon = float(row["longitude"]) if row.get("longitude") else None

                rows.append({
                    "country": country_key,
                    "market": row.get("market", "Unknown"),
                    "commodity": row.get("commodity", "Unknown"),
                    "price": price_val,
                    "currency": row.get("currency", "XOF"),
                    "unit": row.get("unit", "KG"),
                    "date": price_date,
                    "source": "WFP_HDX",
                    "latitude": lat,
                    "longitude": lon,
                })
            except (ValueError, KeyError) as e:
                skipped += 1
                continue

    # Insert in batches of 5000
    inserted = 0
    with engine.connect() as conn:
        for i in range(0, len(rows), 5000):
            batch = rows[i:i + 5000]
            conn.execute(insert(prices), batch)
            inserted += len(batch)
        conn.commit()

    return {"country": country_key, "inserted": inserted, "skipped": skipped}


def main():
    print("=" * 50)
    print("  AgroPrix — Import WFP Price Data")
    print("=" * 50)

    # Initialize DB
    init_db()
    engine = get_engine()

    # Clear existing WFP data to avoid duplicates
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM prices WHERE source IN ('WFP', 'WFP_HDX')"))
        conn.commit()
    print("[OK] Cleared existing WFP data\n")

    total_inserted = 0
    for filename, country_key in CSV_FILES.items():
        filepath = os.path.join(DATA_DIR, filename)
        if not os.path.exists(filepath):
            print(f"[SKIP] {filename} — file not found")
            continue

        print(f"[IMPORT] {filename}...", end=" ", flush=True)
        result = import_csv(filepath, country_key, engine)
        total_inserted += result["inserted"]
        print(f"{result['inserted']:,} records (skipped {result['skipped']})")

        # Log
        with engine.connect() as conn:
            conn.execute(insert(sync_log), {
                "source": "WFP_HDX_IMPORT",
                "country": country_key,
                "records_fetched": result["inserted"] + result["skipped"],
                "records_inserted": result["inserted"],
                "status": "success",
            })
            conn.commit()

    engine.dispose()

    print(f"\n{'=' * 50}")
    print(f"  TOTAL: {total_inserted:,} price records imported")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
