"""
AgroPrix - Configuration module.
Loads environment variables and defines API endpoints and reference data.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from backend root
_env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_env_path)

# ---------------------------------------------------------------------------
# External API base URLs
# ---------------------------------------------------------------------------
WFP_API_BASE = "https://api.wfp.org/vam-data-bridges/4.0.0"
WFP_TOKEN_URL = "https://api.wfp.org/token"
NASA_POWER_BASE = "https://power.larc.nasa.gov/api/temporal/monthly/point"
OPEN_METEO_BASE = "https://api.open-meteo.com/v1/forecast"
EXCHANGERATE_BASE = "https://v6.exchangerate-api.com/v6"

# ---------------------------------------------------------------------------
# Environment variables
# ---------------------------------------------------------------------------
WFP_API_KEY = os.getenv("WFP_API_KEY", "")
WFP_API_SECRET = os.getenv("WFP_API_SECRET", "")
EXCHANGERATE_API_KEY = os.getenv("EXCHANGERATE_API_KEY", "")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./data/agroprix.db")

# Heroku/Railway-style postgres:// -> postgresql:// (SQLAlchemy 2.x requirement)
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))
DEBUG = os.getenv("DEBUG", "true").lower() in ("true", "1", "yes")

# ---------------------------------------------------------------------------
# JWT / Authentication
# ---------------------------------------------------------------------------
JWT_SECRET = os.getenv("JWT_SECRET", "agroprix-dev-secret-change-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_HOURS = int(os.getenv("JWT_EXPIRE_HOURS", "72"))

# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

# ---------------------------------------------------------------------------
# Rate limiting (per role)
# ---------------------------------------------------------------------------
RATE_LIMIT_FREE = os.getenv("RATE_LIMIT_FREE", "50/hour")
RATE_LIMIT_PRO = os.getenv("RATE_LIMIT_PRO", "500/hour")
RATE_LIMIT_EXPERT = os.getenv("RATE_LIMIT_EXPERT", "2000/hour")

# ---------------------------------------------------------------------------
# UEMOA countries  (key -> ISO3 / ISO2)
# ---------------------------------------------------------------------------
UEMOA_COUNTRIES = {
    "benin":         {"iso3": "BEN", "iso2": "BJ"},
    "burkina_faso":  {"iso3": "BFA", "iso2": "BF"},
    "cote_divoire":  {"iso3": "CIV", "iso2": "CI"},
    "guinee_bissau": {"iso3": "GNB", "iso2": "GW"},
    "mali":          {"iso3": "MLI", "iso2": "ML"},
    "niger":         {"iso3": "NER", "iso2": "NE"},
    "senegal":       {"iso3": "SEN", "iso2": "SN"},
    "togo":          {"iso3": "TGO", "iso2": "TG"},
}

# ---------------------------------------------------------------------------
# WFP commodity IDs -> human-readable names
# ---------------------------------------------------------------------------
WFP_COMMODITIES = {
    51:  "Maize",
    52:  "Millet",
    58:  "Sorghum",
    56:  "Rice (local)",
    57:  "Rice (imported)",
    183: "Cowpeas",
    78:  "Tomatoes",
    63:  "Onions",
    41:  "Groundnuts",
    86:  "Cassava",
    82:  "Yam",
    68:  "Maize flour",
}
