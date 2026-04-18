from datetime import datetime, timezone
from fastapi import APIRouter, Depends, Query, Response
from typing import Dict, List, Optional
from pydantic import BaseModel
from sqlalchemy import text

from app.connectors.scraper import search_buyers, get_buyer_detail, SIMULATED_BUYERS
from app.services.matching import find_matches
from app.services.pdf_report import generate_price_report
from app.auth import get_current_user
from app.database import get_engine

router = APIRouter(tags=["market"])

# ---------------------------------------------------------------------------
# Marketplace Offers (P2P)
# ---------------------------------------------------------------------------

class MarketOffer(BaseModel):
    type: str  # "sell" or "buy"
    crop: str
    crop_name: str
    quantity: float
    unit: Optional[str] = "tonnes"
    price: float
    price_unit: Optional[str] = "FCFA/kg"
    market: str
    description: Optional[str] = ""
    phone: Optional[str] = ""
    country: Optional[str] = "benin"


@router.get("/offers")
async def list_offers(
    country: Optional[str] = None,
    crop: Optional[str] = None,
    offer_type: Optional[str] = None,
):
    """List marketplace offers (sell/buy)."""
    engine = get_engine()
    try:
        conditions = ["1=1"]
        params: dict = {}
        if country:
            conditions.append("country = :country")
            params["country"] = country
        if crop:
            conditions.append("crop LIKE :crop")
            params["crop"] = f"%{crop}%"
        if offer_type:
            conditions.append("type = :type")
            params["type"] = offer_type
        where = " AND ".join(conditions)
        q = text(f"""
            SELECT * FROM marketplace_offers
            WHERE {where}
            ORDER BY created_at DESC
            LIMIT 100
        """)
        with engine.connect() as conn:
            result = conn.execute(q, params)
            rows = [dict(r._mapping) for r in result]
    except Exception:
        rows = []
    finally:
        engine.dispose()
    return {"count": len(rows), "offers": rows}


@router.post("/offers", status_code=201)
async def create_offer(
    body: MarketOffer,
    current_user: Dict = Depends(get_current_user),
):
    """Create a marketplace offer (sell or buy)."""
    engine = get_engine()
    now = datetime.now(timezone.utc).isoformat()
    user_name = current_user.get("name", "Anonyme")

    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS marketplace_offers (
                    id SERIAL PRIMARY KEY,
                    type VARCHAR(10),
                    crop VARCHAR(100),
                    crop_name VARCHAR(100),
                    quantity FLOAT,
                    unit VARCHAR(20),
                    price FLOAT,
                    price_unit VARCHAR(20),
                    market VARCHAR(100),
                    description TEXT,
                    seller VARCHAR(100),
                    phone VARCHAR(50),
                    country VARCHAR(50),
                    status VARCHAR(20) DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """))
            conn.execute(text("""
                INSERT INTO marketplace_offers (type, crop, crop_name, quantity, unit, price, price_unit, market, description, seller, phone, country)
                VALUES (:type, :crop, :crop_name, :quantity, :unit, :price, :price_unit, :market, :description, :seller, :phone, :country)
            """), {
                "type": body.type,
                "crop": body.crop,
                "crop_name": body.crop_name,
                "quantity": body.quantity,
                "unit": body.unit or "tonnes",
                "price": body.price,
                "price_unit": body.price_unit or "FCFA/kg",
                "market": body.market,
                "description": body.description or "",
                "seller": user_name,
                "phone": body.phone or "",
                "country": body.country or "benin",
            })
    finally:
        engine.dispose()

    return {"message": "Offre publiee avec succes", "seller": user_name}


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
    user_name: str = "Utilisateur",
    days: int = 30,
):
    """Generate a weekly PDF price report.

    Source principale : table `prices` (alimentée par WFP VAM DataBridges via
    sync.py). Fallback transparent sur un échantillon démo si la BD est vide,
    avec disclaimer affiché dans le PDF.
    """
    from datetime import datetime, timedelta

    prices: list[dict] = []
    data_source_note = ""
    engine = get_engine()

    # 1. Tenter une vraie requête sur la table prices (données WFP)
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT date, market, price, currency, unit, source
                FROM prices
                WHERE country = :country
                  AND commodity = :commodity
                ORDER BY date DESC
                LIMIT :limit
            """), {
                "country": country,
                "commodity": commodity,
                "limit": max(10, days),
            }).mappings().all()

        for r in rows:
            d = r["date"]
            date_str = d.strftime("%d/%m/%Y") if hasattr(d, "strftime") else str(d)[:10]
            prices.append({
                "date": date_str,
                "market": r["market"] or "N/A",
                "price": round(float(r["price"] or 0)),
            })
        if prices:
            # Remettre dans l'ordre chronologique pour le PDF
            prices.reverse()
            sources = {r["source"] for r in rows if r["source"]}
            data_source_note = (
                f"Source : {', '.join(sorted(sources)) or 'WFP VAM DataBridges'} "
                f"— {len(prices)} observation(s)."
            )
    except Exception as e:
        # Table absente / erreur DB → on bascule sur le fallback démo
        data_source_note = f"⚠️ Connexion base de données indisponible ({type(e).__name__})."
    finally:
        try:
            engine.dispose()
        except Exception:
            pass

    # 2. Fallback transparent si aucune donnée réelle
    if not prices:
        base_price = {
            "mais": 250, "riz": 450, "tomate": 800, "oignon": 350, "mil": 200,
            "sorgho": 180, "niebe": 500, "manioc": 150, "igname": 400,
            "arachide": 600, "cajou": 1200, "piment": 900,
        }
        bp = base_price.get(commodity, 300)
        sample_markets = ["Dantokpa", "Glazoue", "Bohicon", "Parakou", "Malanville"]
        # Variation déterministe (pas de random) basée sur l'index, visiblement démo
        for i in range(10):
            d = datetime.now() - timedelta(days=10 - i)
            prices.append({
                "date": d.strftime("%d/%m/%Y"),
                "market": sample_markets[i % len(sample_markets)],
                "price": bp + ((i * 17) % 60) - 25,  # ±25-35 déterministe
            })
        data_source_note = (
            "⚠️ DONNEES DEMONSTRATION — pas de releve WFP disponible pour "
            f"{commodity}/{country} dans la base. Relancez la synchronisation "
            "via POST /api/sync/prices pour charger les vraies donnees."
        )

    # 3. Recommandation basique basée sur la tendance réelle (si dispo)
    if len(prices) >= 5:
        first_avg = sum(p["price"] for p in prices[: len(prices) // 2]) / (len(prices) // 2)
        last_avg = sum(p["price"] for p in prices[len(prices) // 2 :]) / (len(prices) - len(prices) // 2)
        trend_pct = ((last_avg - first_avg) / first_avg * 100) if first_avg else 0

        if trend_pct > 5:
            recommendation = {
                "action": "STOCKER",
                "confidence": "elevee" if trend_pct > 10 else "moyenne",
                "strategy": (
                    f"Les prix du {commodity} ont progresse de {trend_pct:+.1f}% "
                    f"sur la periode. Tendance haussiere — stockage 2-3 semaines "
                    f"envisageable si capacite de conservation disponible."
                ),
            }
        elif trend_pct < -5:
            recommendation = {
                "action": "VENDRE",
                "confidence": "elevee" if trend_pct < -10 else "moyenne",
                "strategy": (
                    f"Les prix du {commodity} ont recule de {trend_pct:+.1f}%. "
                    f"Tendance baissiere — ecouler les stocks avant aggravation."
                ),
            }
        else:
            recommendation = {
                "action": "OBSERVER",
                "confidence": "moyenne",
                "strategy": (
                    f"Prix du {commodity} stables ({trend_pct:+.1f}%). "
                    f"Ni urgence de vente ni opportunite de stockage nette."
                ),
            }
    else:
        recommendation = {
            "action": "OBSERVER",
            "confidence": "faible",
            "strategy": (
                f"Donnees insuffisantes ({len(prices)} obs.) pour une recommandation "
                "robuste. Patientez ou synchronisez plus de donnees WFP."
            ),
        }

    pdf_bytes = generate_price_report(
        country=country,
        commodity=commodity,
        prices=prices,
        recommendation=recommendation,
        weather={"temp": 32, "precipitation": 15, "humidity": 72},
        user_name=user_name,
        data_source_note=data_source_note,
    )

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=AgroPrix_Rapport_{commodity}_{country}.pdf"}
    )
