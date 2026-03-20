"""
Scraper for international agricultural commodity buyers.
Sources: Trade portals, commodity exchanges, export directories.
"""
import httpx
from typing import List, Dict, Optional
from datetime import datetime

# Target sources (public trade portals)
SOURCES = [
    {"name": "Trade Map (ITC)", "url": "https://www.trademap.org", "type": "directory"},
    {"name": "Alibaba Agriculture", "url": "https://www.alibaba.com/Agriculture", "type": "marketplace"},
    {"name": "Africa Business Pages", "url": "https://www.africa-business.com", "type": "directory"},
]

# Simulated buyer database (real scraping would populate this from the sources above)
SIMULATED_BUYERS = [
    {"id": "B001", "name": "Olam International", "country": "Singapore", "commodities": ["cajou", "cacao", "cafe"], "volume_min_tonnes": 100, "certifications": ["Fair Trade", "Organic"], "contact": "procurement@olam.com", "rating": 4.8},
    {"id": "B002", "name": "Barry Callebaut", "country": "Switzerland", "commodities": ["cacao", "karite"], "volume_min_tonnes": 50, "certifications": ["UTZ", "Rainforest Alliance"], "contact": "sourcing@barry-callebaut.com", "rating": 4.9},
    {"id": "B003", "name": "Cargill West Africa", "country": "USA", "commodities": ["cajou", "coton", "soja", "mais"], "volume_min_tonnes": 200, "certifications": ["GlobalGAP"], "contact": "wa-procurement@cargill.com", "rating": 4.7},
    {"id": "B004", "name": "Ets. Kagnassy", "country": "Guinea", "commodities": ["oignon", "pomme_de_terre", "tomate"], "volume_min_tonnes": 10, "certifications": [], "contact": "import@kagnassy.gn", "rating": 4.2},
    {"id": "B005", "name": "Groupe Mimran", "country": "Senegal", "commodities": ["riz", "mais", "mil", "sorgho"], "volume_min_tonnes": 500, "certifications": [], "contact": "achat@mimran.sn", "rating": 4.5},
    {"id": "B006", "name": "Touton SA", "country": "France", "commodities": ["cacao", "cafe", "cajou"], "volume_min_tonnes": 100, "certifications": ["Fair Trade", "UTZ"], "contact": "sourcing@touton.com", "rating": 4.6},
    {"id": "B007", "name": "Neptune Foods Nigeria", "country": "Nigeria", "commodities": ["niebe", "arachide", "soja"], "volume_min_tonnes": 25, "certifications": [], "contact": "buy@neptunefoods.ng", "rating": 4.0},
    {"id": "B008", "name": "Sucrivoire", "country": "Côte d'Ivoire", "commodities": ["mais", "sorgho", "mil"], "volume_min_tonnes": 50, "certifications": ["ISO 9001"], "contact": "approvisionnement@sucrivoire.ci", "rating": 4.3},
    {"id": "B009", "name": "Gebana International", "country": "Switzerland", "commodities": ["cajou", "mangue", "ananas"], "volume_min_tonnes": 20, "certifications": ["Organic", "Fair Trade"], "contact": "purchase@gebana.com", "rating": 4.7},
    {"id": "B010", "name": "AMS Trading Ghana", "country": "Ghana", "commodities": ["karite", "cajou", "cacao"], "volume_min_tonnes": 30, "certifications": ["Organic"], "contact": "trade@amstrading.gh", "rating": 4.4},
]


async def search_buyers(commodity: Optional[str] = None, country: Optional[str] = None, min_rating: float = 0) -> List[Dict]:
    """Search simulated buyer database with filters."""
    results = SIMULATED_BUYERS.copy()
    if commodity:
        commodity_lower = commodity.lower()
        results = [b for b in results if commodity_lower in [c.lower() for c in b["commodities"]]]
    if country:
        country_lower = country.lower()
        results = [b for b in results if country_lower in b["country"].lower()]
    if min_rating > 0:
        results = [b for b in results if b["rating"] >= min_rating]
    return results


async def get_buyer_detail(buyer_id: str) -> Optional[Dict]:
    """Get details for a specific buyer."""
    for b in SIMULATED_BUYERS:
        if b["id"] == buyer_id:
            return b
    return None
