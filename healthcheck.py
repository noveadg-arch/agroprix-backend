#!/usr/bin/env python3
"""
AgroPrix — Script de Health Check autonome (Python)
Usage : python healthcheck.py [--url https://web-production-46fb2.up.railway.app]
Retourne exit code 0 si OK, 1 si dégradé.
"""

import sys
import argparse
import json
import urllib.request
import urllib.error

DEFAULT_URL = "https://web-production-46fb2.up.railway.app"


def check(base_url: str) -> int:
    """Appelle /api/health et affiche le résultat. Retourne 0 ou 1."""
    url = base_url.rstrip("/") + "/api/health"
    print(f"\n--- AgroPrix Health Check ---")
    print(f"URL : {url}\n")

    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            http_code = resp.status
            body = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        http_code = e.code
        try:
            body = json.loads(e.read().decode())
        except Exception:
            body = {"status": "error", "message": str(e)}
    except Exception as e:
        print(f"❌ Impossible de joindre le backend : {e}")
        return 1

    # --- Affichage ---
    status = body.get("status", "unknown")
    icon = "✅" if status == "ok" else "⚠️"
    print(f"{icon}  Statut global : {status.upper()}  (HTTP {http_code})")
    print(f"   Timestamp    : {body.get('timestamp', 'N/A')}")
    print(f"   Temps réponse: {body.get('response_time_ms', 'N/A')} ms\n")

    checks = body.get("checks", {})

    # Base de données
    db = checks.get("database", {})
    db_status = db.get("status", "?")
    db_icon = "✅" if db_status == "ok" else "❌"
    print(f"{db_icon}  Base de données : {db_status}")
    if db_status == "ok":
        print(f"   Prix en base    : {db.get('price_records', 0)}")
        print(f"   Dernière entrée : {db.get('latest_record', 'N/A')}")
    if db.get("warning"):
        print(f"   ⚠️  {db['warning']}")
    if db.get("message"):
        print(f"   Erreur : {db['message']}")
    print()

    # WFP API
    wfp = checks.get("wfp_api", {})
    wfp_status = wfp.get("status", "?")
    wfp_icon = "✅" if wfp_status == "reachable" else "❌"
    print(f"{wfp_icon}  WFP DataBridges : {wfp_status} (HTTP {wfp.get('http_code', 'N/A')})")
    if wfp.get("note"):
        print(f"   ℹ️  {wfp['note']}")
    if wfp.get("message"):
        print(f"   Erreur : {wfp['message']}")
    print()

    # NASA POWER
    nasa = checks.get("nasa_power", {})
    nasa_status = nasa.get("status", "?")
    nasa_icon = "✅" if nasa_status == "reachable" else "⚠️"
    print(f"{nasa_icon}  NASA POWER (météo) : {nasa_status} (HTTP {nasa.get('http_code', 'N/A')})")
    if nasa.get("message"):
        print(f"   Erreur : {nasa['message']}")
    print()

    # Résultat final
    if status != "ok":
        print("⚠️  ALERTE : Système dégradé — vérifier les logs Railway.")
        return 1
    else:
        print("🚀 Tout est opérationnel.")
        return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AgroPrix Health Check")
    parser.add_argument("--url", default=DEFAULT_URL, help="URL du backend Railway")
    args = parser.parse_args()
    sys.exit(check(args.url))
