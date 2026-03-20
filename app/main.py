"""
AgroPrix API - Main FastAPI application.
Agricultural price intelligence for West Africa (UEMOA).
"""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.config import ALLOWED_ORIGINS, DEBUG, HOST, PORT
from app.database import init_db
from app.middleware import RateLimitMiddleware, SecurityHeadersMiddleware, limiter
from app.routes.prices import router as prices_router
from app.routes.weather import router as weather_router
from app.routes.sync import router as sync_router
from app.routes.recommendations import router as recommendations_router
from app.routes.auth import router as auth_router
from app.routes.market import router as market_router


# ---------------------------------------------------------------------------
# Lifespan (startup / shutdown)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run on startup: initialise DB, ensure data directory exists."""
    data_dir = Path(__file__).resolve().parent.parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    init_db()
    yield


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="AgroPrix API",
    description="Agricultural price intelligence for West Africa (UEMOA)",
    version="1.0.0",
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

app.include_router(auth_router, prefix="/api/auth")
app.include_router(prices_router, prefix="/api/prices")
app.include_router(weather_router, prefix="/api/weather")
app.include_router(sync_router, prefix="/api/sync")
app.include_router(recommendations_router, prefix="/api/recommendations")
app.include_router(market_router, prefix="/api/market")


# ---------------------------------------------------------------------------
# Root endpoint
# ---------------------------------------------------------------------------

@app.get("/api")
async def api_root():
    """API information and available endpoints."""
    return {
        "name": "AgroPrix API",
        "version": "1.0.0",
        "endpoints": {
            "auth": "/api/auth",
            "prices": "/api/prices",
            "weather": "/api/weather",
            "sync": "/api/sync",
            "recommendations": "/api/recommendations",
            "market": "/api/market",
        },
    }


@app.get("/")
async def root():
    """Root redirect / health-check."""
    return {
        "name": "AgroPrix API",
        "version": "1.0.0",
        "endpoints": {
            "auth": "/api/auth",
            "prices": "/api/prices",
            "weather": "/api/weather",
            "sync": "/api/sync",
            "recommendations": "/api/recommendations",
            "market": "/api/market",
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
