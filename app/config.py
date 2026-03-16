import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    # WFP DataBridges
    WFP_API_KEY: str = os.getenv("WFP_API_KEY", "")
    WFP_API_SECRET: str = os.getenv("WFP_API_SECRET", "")
    WFP_BASE_URL: str = "https://api.wfp.org/vam-data-bridges/4.0.0"
    WFP_TOKEN_URL: str = "https://api.wfp.org/token"

    # NASA POWER (no key needed)
    NASA_POWER_URL: str = "https://power.larc.nasa.gov/api/temporal/monthly/point"

    # Open-Meteo (no key needed)
    OPEN_METEO_URL: str = "https://api.open-meteo.com/v1/forecast"

    # ExchangeRate
    EXCHANGERATE_API_KEY: str = os.getenv("EXCHANGERATE_API_KEY", "")
    EXCHANGERATE_URL: str = "https://v6.exchangerate-api.com/v6"

    # World Bank
    WORLD_BANK_URL: str = "https://api.worldbank.org/v2"

    # Database
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./data/agroprix.db")

    # Server
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8000"))
    DEBUG: bool = os.getenv("DEBUG", "true").lower() == "true"

    # UEMOA country codes (ISO3 for WFP API)
    COUNTRIES = {
        "benin":         {"iso3": "BEN", "iso2": "BJ", "name": "Bénin"},
        "burkina_faso":  {"iso3": "BFA", "iso2": "BF", "name": "Burkina Faso"},
        "cote_divoire":  {"iso3": "CIV", "iso2": "CI", "name": "Côte d'Ivoire"},
        "guinee_bissau": {"iso3": "GNB", "iso2": "GW", "name": "Guinée-Bissau"},
        "mali":          {"iso3": "MLI", "iso2": "ML", "name": "Mali"},
        "niger":         {"iso3": "NER", "iso2": "NE", "name": "Niger"},
        "senegal":       {"iso3": "SEN", "iso2": "SN", "name": "Sénégal"},
        "togo":          {"iso3": "TGO", "iso2": "TG", "name": "Togo"},
    }

    # WFP commodity IDs for main crops
    WFP_COMMODITIES = {
        "mais":        {"id": 51,  "name": "Maize"},
        "mil":         {"id": 52,  "name": "Millet"},
        "sorgho":      {"id": 58,  "name": "Sorghum"},
        "riz_local":   {"id": 56,  "name": "Rice (local)"},
        "riz_importe": {"id": 57,  "name": "Rice (imported)"},
        "niebe":       {"id": 183, "name": "Cowpeas"},
        "tomate":      {"id": 78,  "name": "Tomatoes"},
        "oignon":      {"id": 63,  "name": "Onions"},
        "arachide":    {"id": 41,  "name": "Groundnuts"},
        "manioc":      {"id": 86,  "name": "Cassava"},
        "igname":      {"id": 82,  "name": "Yam"},
        "mais_farine": {"id": 68,  "name": "Maize flour"},
    }


settings = Settings()
