from fastapi import APIRouter, Query, Response
from typing import Optional
from app.connectors.scraper import search_buyers, get_buyer_detail, SIMULATED_BUYERS
from app.services.matching import find_matches
from app.services.pdf_report import generate_price_report

router = APIRouter(tags=["market"])


@router.get("/buyers")
async def list_buyers(
    commodity: Optional[str] = None,
    country: Optional[str] = None,
    min_rating: float = Query(0, ge=0, le=5)
):
    """Search international buyers."""
    results = await search_buyers(commodity=commodity, country=country, min_rating=min_rating)
    return {"count": len(results), "buyers": results}


@router.get("/buyers/{buyer_id}")
async def buyer_detail(buyer_id: str):
    """Get buyer details."""
    buyer = await get_buyer_detail(buyer_id)
    if not buyer:
        return {"error": "Acheteur non trouvé"}
    return buyer


@router.post("/match")
async def match_seller(
    commodity: str,
    volume_tonnes: float = Query(gt=0),
    country: str = "benin",
    certifications: str = ""  # comma-separated
):
    """Find best buyer matches for a seller."""
    seller = {
        "commodity": commodity,
        "volume_tonnes": volume_tonnes,
        "country": country,
        "certifications": [c.strip() for c in certifications.split(",") if c.strip()]
    }
    matches = find_matches(seller, SIMULATED_BUYERS)
    return {
        "seller": seller,
        "matches_count": len(matches),
        "matches": matches
    }


@router.get("/report/pdf")
async def generate_report(
    country: str = "benin",
    commodity: str = "mais",
    user_name: str = "Utilisateur"
):
    """Generate a weekly PDF price report."""
    # Simulated price data for the report
    from datetime import datetime, timedelta
    import random
    random.seed(42)
    base_price = {"mais": 250, "riz": 450, "tomate": 800, "oignon": 350, "mil": 200, "sorgho": 180, "niebe": 500, "manioc": 150, "igname": 400, "arachide": 600, "cajou": 1200, "piment": 900}
    bp = base_price.get(commodity, 300)

    prices = []
    markets = ["Dantokpa", "Glazoue", "Bohicon", "Parakou", "Malanville"]
    for i in range(10):
        d = datetime.now() - timedelta(days=10-i)
        prices.append({
            "date": d.strftime("%d/%m/%Y"),
            "market": markets[i % len(markets)],
            "price": bp + random.randint(-50, 80)
        })

    recommendation = {
        "action": "STOCKER",
        "confidence": "élevée",
        "strategy": f"Les prix du {commodity} sont en hausse saisonnière. Stocker 2-3 semaines pour maximiser les marges."
    }

    pdf_bytes = generate_price_report(
        country=country,
        commodity=commodity,
        prices=prices,
        recommendation=recommendation,
        weather={"temp": 32, "precipitation": 15, "humidity": 72},
        user_name=user_name
    )

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=AgroPrix_Rapport_{commodity}_{country}.pdf"}
    )
