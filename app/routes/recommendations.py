"""
Recommendations engine — the intelligence layer of AgroPrix.

Generates JUSTIFIED recommendations (STOCKER/VENDRE/ATTENDRE) based on:
- Price trends (historical WFP data)
- Seasonality patterns (multi-year averages)
- Weather conditions (Open-Meteo 16-day forecast)
- Regional price differences (cross-country arbitrage)

Each recommendation includes factual justification with traceable data points.
Score system: -3 to +3 maps to VENDRE / ATTENDRE / STOCKER.
"""

from fastapi import APIRouter, Query
from typing import Optional
from datetime import date as dt_date
from sqlalchemy import text

from app.database import get_engine, sql_year_month, sql_month_num, sql_date_months_ago
from app.connectors.open_meteo import meteo_connector

router = APIRouter(prefix="", tags=["recommendations"])


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def compute_trend(prices_last_6: list) -> dict:
    """Compute price trend from last 6 months of data with 3-month moving average."""
    if len(prices_last_6) < 2:
        return {
            "direction": "stable",
            "change_pct": 0,
            "mom_change_pct": 0,
            "momentum": "neutre",
            "current_price": round(prices_last_6[-1]) if prices_last_6 else 0,
            "prev_price": 0,
            "moving_avg_3m": round(prices_last_6[-1], 1) if prices_last_6 else 0,
        }

    first_half = sum(prices_last_6[:3]) / max(len(prices_last_6[:3]), 1)
    second_half = sum(prices_last_6[3:]) / max(len(prices_last_6[3:]), 1)
    current = prices_last_6[-1]
    prev = prices_last_6[-2]

    mom_change = ((current - prev) / prev * 100) if prev else 0
    trend_change = ((second_half - first_half) / first_half * 100) if first_half else 0

    # 3-month moving average
    if len(prices_last_6) >= 3:
        ma3 = sum(prices_last_6[-3:]) / 3
    else:
        ma3 = current

    if trend_change > 5:
        direction = "hausse"
        momentum = "haussier"
    elif trend_change < -5:
        direction = "baisse"
        momentum = "baissier"
    else:
        direction = "stable"
        momentum = "neutre"

    return {
        "direction": direction,
        "change_pct": round(trend_change, 1),
        "mom_change_pct": round(mom_change, 1),
        "momentum": momentum,
        "current_price": round(current),
        "prev_price": round(prev),
        "moving_avg_3m": round(ma3, 1),
    }


def compute_seasonality(monthly_avgs_by_month: dict, current_month: int) -> dict:
    """Analyze seasonal patterns from multi-year data."""
    if not monthly_avgs_by_month:
        return {"pattern": "inconnu", "expected_change_pct": 0}

    current_avg = monthly_avgs_by_month.get(current_month, 0)
    next_1 = monthly_avgs_by_month.get((current_month % 12) + 1, current_avg)
    next_2 = monthly_avgs_by_month.get(((current_month + 1) % 12) + 1, current_avg)
    next_3 = monthly_avgs_by_month.get(((current_month + 2) % 12) + 1, current_avg)

    avg_next_3 = (next_1 + next_2 + next_3) / 3 if (next_1 + next_2 + next_3) else current_avg
    expected_change = ((avg_next_3 - current_avg) / current_avg * 100) if current_avg else 0

    if monthly_avgs_by_month:
        peak_month = max(monthly_avgs_by_month, key=monthly_avgs_by_month.get)
        trough_month = min(monthly_avgs_by_month, key=monthly_avgs_by_month.get)
    else:
        peak_month = trough_month = current_month

    month_names = {
        1: "janvier", 2: "fevrier", 3: "mars", 4: "avril",
        5: "mai", 6: "juin", 7: "juillet", 8: "aout",
        9: "septembre", 10: "octobre", 11: "novembre", 12: "decembre",
    }

    if expected_change > 8:
        pattern = "hausse_saisonniere"
    elif expected_change < -8:
        pattern = "baisse_saisonniere"
    else:
        pattern = "stable_saisonnier"

    return {
        "pattern": pattern,
        "expected_change_pct": round(expected_change, 1),
        "peak_month": month_names.get(peak_month, "?"),
        "trough_month": month_names.get(trough_month, "?"),
        "peak_price": round(monthly_avgs_by_month.get(peak_month, 0)),
        "trough_price": round(monthly_avgs_by_month.get(trough_month, 0)),
    }


def compute_arbitrage(country: str, compare_data: list) -> dict:
    """Find arbitrage opportunities across UEMOA countries (>10% gap)."""
    if not compare_data:
        return {"opportunity": False}

    country_price = None
    best_market = None
    best_price = 0

    for row in compare_data:
        if row["country"] == country:
            country_price = row["avg_price"]
        if row["avg_price"] > best_price:
            best_price = row["avg_price"]
            best_market = row["country"]

    if not country_price or not best_market:
        return {"opportunity": False}

    diff_pct = ((best_price - country_price) / country_price * 100) if country_price else 0

    country_names = {
        "benin": "Benin", "burkina_faso": "Burkina Faso",
        "cote_divoire": "Cote d'Ivoire", "guinee_bissau": "Guinee-Bissau",
        "mali": "Mali", "niger": "Niger", "senegal": "Senegal", "togo": "Togo",
    }

    return {
        "opportunity": diff_pct > 10,
        "best_country": country_names.get(best_market, best_market),
        "best_price": round(best_price),
        "local_price": round(country_price),
        "diff_pct": round(diff_pct, 1),
    }


def generate_recommendation(
    trend: dict, season: dict, arbitrage: dict, weather_outlook: dict
) -> dict:
    """
    Generate a justified recommendation combining all signals.
    Score from -3 to +3:
      >= 1  -> STOCKER
      <= -1 -> VENDRE
         0  -> ATTENDRE
    Confidence: elevee (|score|>=3), moderee (1-2), faible (<1).
    """
    signals = []
    score = 0

    # Signal 1: Price trend
    if trend["direction"] == "hausse":
        score += 2
        signals.append({
            "signal": "Tendance haussiere",
            "impact": "positif",
            "detail": (
                f"Les prix ont augmente de {trend['change_pct']}% sur les 6 derniers mois. "
                f"Prix actuel: {trend['current_price']} FCFA/kg vs {trend['prev_price']} FCFA/kg le mois dernier "
                f"({'+' if trend['mom_change_pct'] >= 0 else ''}{trend['mom_change_pct']}%). "
                f"Moyenne mobile 3 mois: {trend['moving_avg_3m']} FCFA/kg."
            ),
        })
    elif trend["direction"] == "baisse":
        score -= 2
        signals.append({
            "signal": "Tendance baissiere",
            "impact": "negatif",
            "detail": (
                f"Les prix ont baisse de {abs(trend['change_pct'])}% sur les 6 derniers mois. "
                f"Prix actuel: {trend['current_price']} FCFA/kg vs {trend['prev_price']} FCFA/kg le mois dernier "
                f"({'+' if trend['mom_change_pct'] >= 0 else ''}{trend['mom_change_pct']}%). "
                f"Moyenne mobile 3 mois: {trend['moving_avg_3m']} FCFA/kg."
            ),
        })
    else:
        signals.append({
            "signal": "Prix stables",
            "impact": "neutre",
            "detail": (
                f"Les prix sont restes stables ({trend['change_pct']}% sur 6 mois). "
                f"Prix actuel: {trend['current_price']} FCFA/kg. "
                f"Moyenne mobile 3 mois: {trend['moving_avg_3m']} FCFA/kg."
            ),
        })

    # Signal 2: Seasonality
    if season["pattern"] == "hausse_saisonniere":
        score += 2
        signals.append({
            "signal": "Hausse saisonniere attendue",
            "impact": "positif",
            "detail": (
                f"Historiquement, les prix augmentent de {season['expected_change_pct']}% "
                f"dans les 3 prochains mois. Pic habituel en {season['peak_month']} "
                f"({season['peak_price']} FCFA/kg en moyenne)."
            ),
        })
    elif season["pattern"] == "baisse_saisonniere":
        score -= 2
        signals.append({
            "signal": "Baisse saisonniere attendue",
            "impact": "negatif",
            "detail": (
                f"Historiquement, les prix baissent de {abs(season['expected_change_pct'])}% "
                f"dans les 3 prochains mois. Creux habituel en {season['trough_month']} "
                f"({season['trough_price']} FCFA/kg en moyenne)."
            ),
        })

    # Signal 3: Arbitrage (>10% gap)
    if arbitrage.get("opportunity"):
        signals.append({
            "signal": "Opportunite d'arbitrage regional",
            "impact": "positif",
            "detail": (
                f"Le meme produit se vend {arbitrage['diff_pct']}% plus cher en "
                f"{arbitrage['best_country']} ({arbitrage['best_price']} FCFA/kg vs "
                f"{arbitrage['local_price']} FCFA/kg localement)."
            ),
        })

    # Signal 4: Weather impact (16-day forecast precipitation)
    if weather_outlook.get("precipitation_trend"):
        prec = weather_outlook["precipitation_trend"]
        if prec == "deficit":
            score += 1
            signals.append({
                "signal": "Deficit pluviometrique",
                "impact": "positif",
                "detail": (
                    f"Les previsions meteo indiquent un deficit de pluie sur 16 jours. "
                    f"Precipitation moyenne prevue: {weather_outlook.get('avg_precipitation', '?')} mm/jour. "
                    f"Cela pourrait affecter la prochaine recolte et soutenir les prix."
                ),
            })
        elif prec == "exces":
            score -= 1
            signals.append({
                "signal": "Exces pluviometrique",
                "impact": "negatif",
                "detail": (
                    f"Les previsions meteo indiquent de fortes pluies "
                    f"({weather_outlook.get('avg_precipitation', '?')} mm/jour). "
                    f"Risque d'exces d'eau sur les cultures, mais bonne recolte attendue "
                    f"— pression baissiere possible."
                ),
            })

    # Clamp score to [-3, 3]
    score = max(-3, min(3, score))

    # Map score to action
    if score >= 1:
        action = "STOCKER"
    elif score <= -1:
        action = "VENDRE"
    else:
        action = "ATTENDRE"

    # Confidence level
    abs_score = abs(score)
    if abs_score >= 3:
        confidence = "elevee"
    elif abs_score >= 1:
        confidence = "moderee"
    else:
        confidence = "faible"

    # Summary
    if action == "STOCKER" and abs_score >= 3:
        summary = (
            "Les signaux convergent vers une hausse des prix. "
            "Il est recommande de stocker et d'attendre un meilleur prix de vente."
        )
    elif action == "STOCKER":
        summary = (
            "Plusieurs indicateurs suggerent une hausse a venir. "
            "Le stockage est conseille si les conditions le permettent."
        )
    elif action == "VENDRE" and abs_score >= 3:
        summary = (
            "Les signaux indiquent une probable baisse des prix. "
            "Il est recommande de vendre rapidement."
        )
    elif action == "VENDRE":
        summary = (
            "La tendance est a la baisse. "
            "Envisagez de vendre ou de securiser un prix maintenant."
        )
    else:
        summary = (
            "Les signaux sont mitiges. Pas de recommandation forte "
            "— surveillez l'evolution sur les 2 prochaines semaines."
        )

    return {
        "action": action,
        "confidence": confidence,
        "score": score,
        "summary": summary,
        "signals": signals,
    }


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@router.get("/")
async def get_recommendation(
    country: str = Query(..., description="Country key (e.g., 'benin')"),
    commodity: str = Query(..., description="Commodity name (e.g., 'Maize')"),
):
    """
    Generate an intelligent recommendation for a commodity in a country.

    Combines price trends (6-month + 3-month MA), seasonality (multi-year),
    regional arbitrage (>10% gap), and 16-day weather forecast to produce
    an actionable recommendation with factual justification.

    Response: {country, commodity, recommendation: {action, confidence, score, summary, signals}, data: {trend, seasonality, arbitrage, weather}}
    """
    engine = get_engine()

    try:
        # 1. Last 6 months of prices for trend analysis
        q_trend = text(f"""
            SELECT {sql_year_month('date')} as month, AVG(price) as avg_price
            FROM prices
            WHERE country = :country AND commodity LIKE :commodity
            GROUP BY {sql_year_month('date')}
            ORDER BY month DESC
            LIMIT 6
        """)
        with engine.connect() as conn:
            result = conn.execute(q_trend, {
                "country": country,
                "commodity": f"%{commodity}%",
            })
            last_6 = [dict(r._mapping) for r in result]

        last_6.reverse()  # oldest first
        trend = compute_trend([r["avg_price"] for r in last_6])

        # 2. Seasonality: average price per calendar month (all years)
        q_season = text(f"""
            SELECT {sql_month_num('date')} as cal_month,
                   AVG(price) as avg_price
            FROM prices
            WHERE country = :country AND commodity LIKE :commodity
            GROUP BY {sql_month_num('date')}
        """)
        with engine.connect() as conn:
            result = conn.execute(q_season, {
                "country": country,
                "commodity": f"%{commodity}%",
            })
            season_data = {
                r._mapping["cal_month"]: r._mapping["avg_price"] for r in result
            }

        current_month = dt_date.today().month
        season = compute_seasonality(season_data, current_month)

        # 3. Regional comparison for arbitrage detection
        q_compare = text(f"""
            SELECT country, AVG(price) as avg_price
            FROM prices
            WHERE commodity LIKE :commodity
              AND date >= {sql_date_months_ago(6)}
            GROUP BY country
        """)
        with engine.connect() as conn:
            result = conn.execute(q_compare, {"commodity": f"%{commodity}%"})
            compare_rows = [dict(r._mapping) for r in result]

        arbitrage = compute_arbitrage(country, compare_rows)

        # 4. Weather outlook from 16-day forecast
        weather_outlook: dict = {}
        try:
            forecast = meteo_connector.get_forecast(country)
            if forecast and "forecast" in forecast:
                precips = [
                    day.get("precipitation", 0) or 0
                    for day in forecast["forecast"]
                ]
                if precips:
                    avg_prec = sum(precips) / len(precips)
                    weather_outlook["avg_precipitation"] = round(avg_prec, 1)
                    # deficit < 1mm/day, excess > 10mm/day
                    if avg_prec < 1:
                        weather_outlook["precipitation_trend"] = "deficit"
                    elif avg_prec > 10:
                        weather_outlook["precipitation_trend"] = "exces"
                    else:
                        weather_outlook["precipitation_trend"] = "normal"
        except Exception:
            pass  # Weather is optional, don't break recommendation

    finally:
        engine.dispose()

    recommendation = generate_recommendation(trend, season, arbitrage, weather_outlook)

    return {
        "country": country,
        "commodity": commodity,
        "recommendation": recommendation,
        "data": {
            "trend": trend,
            "seasonality": season,
            "arbitrage": arbitrage,
            "weather": weather_outlook,
        },
    }
