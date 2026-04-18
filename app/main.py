"""
AgroPrix API - Main FastAPI application.
Agricultural price intelligence for West Africa (UEMOA).
"""

from contextlib import asynccontextmanager
from pathlib import Path
import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.config import ALLOWED_ORIGINS, DEBUG, DEMO_MODE, HOST, PORT, JWT_SECRET
from app.database import init_db

logger = logging.getLogger("agroprix")
from app.middleware import RateLimitMiddleware, SecurityHeadersMiddleware, limiter
from app.routes.prices import router as prices_router
from app.routes.weather import router as weather_router
from app.routes.sync import router as sync_router
from app.routes.recommendations import router as recommendations_router
from app.routes.auth import router as auth_router
from app.routes.market import router as market_router
from app.routes.health import router as health_router
from app.routes.public_v1 import router as public_v1_router
from app.routes.parcelles import router as parcelles_router
from app.routes.rapports import router as rapports_router
from app.routes.subscriptions import router as subscriptions_router
from app.routes.ndvi import router as ndvi_router


# ---------------------------------------------------------------------------
# Lifespan (startup / shutdown)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run on startup: initialise DB, ensure data directory exists."""
    data_dir = Path(__file__).resolve().parent.parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    init_db()

    # Security checks : en DEBUG on avertit, en prod on refuse le demarrage.
    default_jwt = JWT_SECRET == "agroprix-dev-secret-change-in-production"
    cors_wildcard = "*" in ALLOWED_ORIGINS

    if DEBUG:
        if default_jwt:
            logger.warning("SECURITE : JWT_SECRET par defaut. Definir JWT_SECRET en prod.")
        if cors_wildcard:
            logger.warning("SECURITE : CORS autorise toutes les origines. Definir ALLOWED_ORIGINS.")
        if DEMO_MODE:
            logger.warning("SECURITE : DEMO_MODE actif — l'auth JWT peut etre contournee.")
    else:
        errors = []
        if default_jwt:
            errors.append("JWT_SECRET par defaut en production — refuse de demarrer.")
        if cors_wildcard:
            errors.append("CORS=* en production — definir ALLOWED_ORIGINS.")
        if DEMO_MODE:
            errors.append("DEMO_MODE=true en production — interdit.")
        if errors:
            for err in errors:
                logger.error("SECURITE FATALE : %s", err)
            raise RuntimeError(
                "Refus de demarrer en production avec ces parametres : "
                + " / ".join(errors)
            )

    yield


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="AgroPrix API",
    description=(
        "## Agricultural price intelligence for West Africa — UEMOA zone\n\n"
        "**Provider:** 33Lab · Cotonou, Bénin · agroprix.app\n\n"
        "### Public API v1 (`/api/v1/`)\n"
        "Open data endpoints for institutional partners. Requires a free API key.\n"
        "Register: `POST /api/v1/keys/register`\n\n"
        "**Compatibility roadmap (phase 2):** ECOAGRIS/ECOWAS · World Bank AgriConnect · "
        "USAID Digital Frontiers · Enabel · GIZ · AFD · IFAD · AfDB\n\n"
        "**Standards:** OpenAPI 3.0 · GeoJSON RFC 7946 · AGROVOC · EUDR compatibility in progress\n\n"
        "**EUDR-relevant commodities tracked:** cacao · cajou · café · soja · hévéa"
    ),
    version="2.0.0",
    contact={"name": "33Lab", "url": "https://agroprix.app", "email": "api@agroprix.app"},
    license_info={"name": "CC BY 4.0", "url": "https://creativecommons.org/licenses/by/4.0/"},
    lifespan=lifespan,
)

# --- slowapi rate limiter (shared state for @limiter.limit() decorators) ------
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# --- Middleware stack (order matters: last added = first executed) ------------
# 1. Security headers on every response
app.add_middleware(SecurityHeadersMiddleware)
# 2. Role-based rate limiting
app.add_middleware(RateLimitMiddleware)

# --- CORS (configurable via ALLOWED_ORIGINS env var) -------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Routers (API routes are registered BEFORE the static-files catch-all)
# ---------------------------------------------------------------------------

app.include_router(auth_router,            prefix="/api/auth")
app.include_router(prices_router,          prefix="/api/prices")
app.include_router(weather_router,         prefix="/api/weather")
app.include_router(sync_router,            prefix="/api/sync")
app.include_router(recommendations_router, prefix="/api/recommendations")
app.include_router(market_router,          prefix="/api/market")
app.include_router(health_router,          prefix="/api")
app.include_router(public_v1_router,       prefix="/api/v1")
app.include_router(parcelles_router,       prefix="/api/parcelles")
app.include_router(rapports_router,        prefix="/api/rapports")
app.include_router(subscriptions_router,   prefix="/api")
app.include_router(ndvi_router,            prefix="/api")


# ---------------------------------------------------------------------------
# Root endpoint
# ---------------------------------------------------------------------------

@app.get("/api")
async def api_root():
    """API information and available endpoints."""
    return {
        "name": "AgroPrix API",
        "version": "2.0.0",
        "provider": "33Lab — Cotonou, Bénin",
        "documentation": "/docs",
        "ecoagris_compatible": "phase_2_planned",
        "eudr_ready": "phase_2_planned",
        "endpoints": {
            "public_v1": "/api/v1/ (API key required — POST /api/v1/keys/register)",
            "auth": "/api/auth",
            "prices": "/api/prices",
            "parcelles": "/api/parcelles (JWT required)",
            "rapports": "/api/rapports/pdf | /api/rapports/excel",
            "weather": "/api/weather",
            "recommendations": "/api/recommendations",
            "market": "/api/market",
            "health": "/api/health",
        },
    }


@app.get("/")
async def root():
    """Root redirect / health-check."""
    return {
        "name": "AgroPrix API",
        "version": "2.0.0",
        "provider": "33Lab — Cotonou, Bénin",
        "documentation": "/docs",
        "ecoagris_compatible": "phase_2_planned",
        "eudr_ready": "phase_2_planned",
        "endpoints": {
            "public_v1": "/api/v1/ (API key required — POST /api/v1/keys/register)",
            "auth": "/api/auth",
            "prices": "/api/prices",
            "parcelles": "/api/parcelles (JWT required)",
            "rapports": "/api/rapports/pdf | /api/rapports/excel",
            "weather": "/api/weather",
            "recommendations": "/api/recommendations",
            "market": "/api/market",
            "health": "/api/health",
        },
    }


# ---------------------------------------------------------------------------
# Serve frontend static files (mounted AFTER API routes so /api/* wins)
# ---------------------------------------------------------------------------

_frontend_dir = Path(__file__).resolve().parent.parent.parent / "frontend"
if _frontend_dir.is_dir():
    app.mount("/static", StaticFiles(directory=str(_frontend_dir), html=True), name="frontend")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host=HOST, port=PORT, reload=DEBUG)
