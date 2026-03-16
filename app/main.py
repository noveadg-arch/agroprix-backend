"""
AgroPrix Backend — by 33 Lab
Intelligence de prix agricoles pour l'UEMOA

FastAPI server connecting to:
- WFP DataBridges (market prices)
- NASA POWER (historical weather)
- Open-Meteo (weather forecasts)
- ExchangeRate API (currency rates)
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.config import settings
from app.database import init_db
from app.routes import prices, weather, sync


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database on startup."""
    print("=" * 50)
    print("  AgroPrix Backend — 33 Lab")
    print("=" * 50)
    init_db()
    print("[OK] Database initialized")
    print(f"[OK] Server ready at http://{settings.HOST}:{settings.PORT}")
    print(f"[OK] Docs at http://{settings.HOST}:{settings.PORT}/docs")
    print()
    print("NEXT STEP: Call POST /api/sync/all to populate the database")
    print("=" * 50)
    yield


app = FastAPI(
    title="AgroPrix API",
    description="Intelligence de prix agricoles UEMOA — by 33 Lab",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allow frontend to call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production: restrict to your domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routes
app.include_router(prices.router)
app.include_router(weather.router)
app.include_router(sync.router)


@app.get("/")
async def root():
    return {
        "name": "AgroPrix API",
        "version": "1.0.0",
        "by": "33 Lab",
        "docs": "/docs",
        "endpoints": {
            "prices": "/api/prices/?country=benin&commodity=Maize",
            "monthly": "/api/prices/monthly?country=benin&commodity=Maize",
            "markets": "/api/prices/markets?country=benin",
            "commodities": "/api/prices/commodities?country=benin",
            "compare": "/api/prices/compare?commodity=Maize",
            "weather_history": "/api/weather/historical?country=benin",
            "weather_forecast": "/api/weather/forecast?country=benin",
            "correlation": "/api/weather/correlation?country=benin&commodity=Maize",
            "sync_all": "POST /api/sync/all",
            "sync_status": "/api/sync/status",
        },
    }
