"""
Matching engine: connects sellers (UEMOA producers) with international buyers.
Uses commodity compatibility, volume fit, certification match, and geographic proximity scoring.
"""
from typing import List, Dict, Optional
import math


def calculate_match_score(seller: Dict, buyer: Dict) -> Dict:
    """
    Calculate a compatibility score between a seller and a buyer.
    Returns dict with overall score (0-100) and breakdown.

    seller: {commodity, volume_tonnes, country, certifications: [], price_per_kg}
    buyer: from SIMULATED_BUYERS
    """
    scores = {}

    # 1. Commodity match (0 or 40 points)
    seller_commodity = seller.get("commodity", "").lower()
    buyer_commodities = [c.lower() for c in buyer.get("commodities", [])]
    scores["commodity"] = 40 if seller_commodity in buyer_commodities else 0

    # 2. Volume fit (0-25 points)
    seller_vol = seller.get("volume_tonnes", 0)
    buyer_min = buyer.get("volume_min_tonnes", 0)
    if seller_vol >= buyer_min:
        scores["volume"] = 25
    elif seller_vol >= buyer_min * 0.5:
        scores["volume"] = 15
    elif seller_vol > 0:
        scores["volume"] = 5
    else:
        scores["volume"] = 0

    # 3. Certification match (0-20 points)
    seller_certs = set(c.lower() for c in seller.get("certifications", []))
    buyer_certs = set(c.lower() for c in buyer.get("certifications", []))
    if not buyer_certs:
        scores["certification"] = 20  # Buyer has no requirements
    elif seller_certs & buyer_certs:
        overlap = len(seller_certs & buyer_certs) / len(buyer_certs)
        scores["certification"] = int(20 * overlap)
    else:
        scores["certification"] = 0

    # 4. Geographic proximity (0-15 points)
    UEMOA_COUNTRIES = {"benin", "burkina faso", "côte d'ivoire", "cote d'ivoire", "guinee-bissau", "mali", "niger", "senegal", "togo"}
    buyer_country = buyer.get("country", "").lower()
    if buyer_country in UEMOA_COUNTRIES:
        scores["proximity"] = 15  # Same economic zone
    elif buyer_country in {"ghana", "nigeria", "guinea", "gambia", "sierra leone", "liberia"}:
        scores["proximity"] = 12  # West Africa
    elif buyer_country in {"france", "spain", "portugal", "belgium", "netherlands", "germany", "uk", "switzerland"}:
        scores["proximity"] = 8  # Europe (traditional trade partners)
    elif buyer_country in {"china", "india", "singapore", "vietnam"}:
        scores["proximity"] = 5  # Asia
    else:
        scores["proximity"] = 3  # Other

    total = sum(scores.values())

    return {
        "buyer_id": buyer["id"],
        "buyer_name": buyer["name"],
        "buyer_country": buyer["country"],
        "score": total,
        "grade": "A" if total >= 80 else "B" if total >= 60 else "C" if total >= 40 else "D",
        "breakdown": scores,
        "buyer_contact": buyer.get("contact", ""),
        "buyer_rating": buyer.get("rating", 0)
    }


def find_matches(seller: Dict, buyers: List[Dict], min_score: int = 20, limit: int = 10) -> List[Dict]:
    """Find and rank best buyer matches for a seller."""
    matches = []
    for buyer in buyers:
        result = calculate_match_score(seller, buyer)
        if result["score"] >= min_score:
            matches.append(result)

    matches.sort(key=lambda x: x["score"], reverse=True)
    return matches[:limit]
