"""
Generate weekly PDF price reports for AgroPrix users.
Uses ReportLab for PDF generation.
"""
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm, cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from io import BytesIO
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import os

# AgroPrix brand colors
GREEN = colors.HexColor("#2d8a4e")
NAVY = colors.HexColor("#1a3a5c")
GOLD = colors.HexColor("#f4a261")
LIGHT_BG = colors.HexColor("#f0f7f4")


def create_styles():
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='APTitle', fontSize=20, textColor=GREEN, spaceAfter=6, fontName='Helvetica-Bold'))
    styles.add(ParagraphStyle(name='APSubtitle', fontSize=11, textColor=NAVY, spaceAfter=12))
    styles.add(ParagraphStyle(name='APSection', fontSize=14, textColor=NAVY, spaceBefore=16, spaceAfter=8, fontName='Helvetica-Bold'))
    styles.add(ParagraphStyle(name='APBody', fontSize=10, textColor=colors.black, spaceAfter=6, leading=14))
    styles.add(ParagraphStyle(name='APFooter', fontSize=8, textColor=colors.grey, alignment=TA_CENTER))
    return styles


def generate_price_report(
    country: str,
    commodity: str,
    prices: List[Dict],  # [{date, price, market}]
    recommendation: Optional[Dict] = None,
    weather: Optional[Dict] = None,
    user_name: str = "Utilisateur"
) -> bytes:
    """Generate a PDF price report and return as bytes."""
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=2*cm, bottomMargin=2*cm, leftMargin=2*cm, rightMargin=2*cm)
    styles = create_styles()
    elements = []

    # Header
    elements.append(Paragraph("AgroPrix — Rapport Hebdomadaire", styles['APTitle']))
    elements.append(Paragraph(
        f"Généré le {datetime.now().strftime('%d/%m/%Y à %H:%M')} pour {user_name}",
        styles['APSubtitle']
    ))
    elements.append(Spacer(1, 8*mm))

    # Summary section
    elements.append(Paragraph("Résumé", styles['APSection']))
    country_names = {"benin": "Bénin", "burkina": "Burkina Faso", "cote_ivoire": "Côte d'Ivoire", "guinee_bissau": "Guinée-Bissau", "mali": "Mali", "niger": "Niger", "senegal": "Sénégal", "togo": "Togo"}
    cn = country_names.get(country, country.title())
    elements.append(Paragraph(f"<b>Pays :</b> {cn} | <b>Culture :</b> {commodity.title()} | <b>Période :</b> 7 derniers jours", styles['APBody']))
    elements.append(Spacer(1, 4*mm))

    # Price table
    if prices:
        elements.append(Paragraph("Évolution des Prix", styles['APSection']))
        table_data = [["Date", "Marché", "Prix (FCFA/kg)"]]
        for p in prices[-10:]:  # Last 10 entries
            table_data.append([
                p.get("date", "—"),
                p.get("market", "—"),
                f"{p.get('price', 0):,.0f}"
            ])

        # Stats row
        all_prices = [p.get("price", 0) for p in prices if p.get("price")]
        if all_prices:
            avg_price = sum(all_prices) / len(all_prices)
            min_price = min(all_prices)
            max_price = max(all_prices)
            table_data.append(["", "Moyenne", f"{avg_price:,.0f}"])
            table_data.append(["", "Min / Max", f"{min_price:,.0f} / {max_price:,.0f}"])

        t = Table(table_data, colWidths=[80, 180, 100])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), GREEN),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('ALIGN', (2, 0), (2, -1), 'RIGHT'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.lightgrey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -3), [colors.white, LIGHT_BG]),
            ('BACKGROUND', (0, -2), (-1, -1), colors.HexColor("#e8f5e9")),
            ('FONTNAME', (0, -2), (-1, -1), 'Helvetica-Bold'),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        elements.append(t)
        elements.append(Spacer(1, 6*mm))

    # Recommendation
    if recommendation:
        elements.append(Paragraph("Recommandation IA", styles['APSection']))
        action = recommendation.get("action", "ATTENDRE")
        confidence = recommendation.get("confidence", "modérée")
        elements.append(Paragraph(
            f"<b>Action recommandée :</b> {action} (confiance : {confidence})",
            styles['APBody']
        ))
        if recommendation.get("strategy"):
            elements.append(Paragraph(f"<b>Stratégie :</b> {recommendation['strategy']}", styles['APBody']))
        elements.append(Spacer(1, 4*mm))

    # Weather
    if weather:
        elements.append(Paragraph("Conditions Météo", styles['APSection']))
        elements.append(Paragraph(
            f"<b>Température :</b> {weather.get('temp', '—')}°C | "
            f"<b>Précipitations :</b> {weather.get('precipitation', '—')} mm | "
            f"<b>Humidité :</b> {weather.get('humidity', '—')}%",
            styles['APBody']
        ))
        elements.append(Spacer(1, 6*mm))

    # Footer
    elements.append(Spacer(1, 12*mm))
    elements.append(Paragraph("AgroPrix by 33 Lab — Cotonou, Bénin — www.agroprix.com", styles['APFooter']))
    elements.append(Paragraph("Ce rapport est généré automatiquement. Les données proviennent du WFP et de sources publiques.", styles['APFooter']))

    doc.build(elements)
    return buffer.getvalue()
