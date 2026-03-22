"""
AgroPrix - Middleware: role-based rate limiting and security headers.
"""

import time
from collections import defaultdict
from typing import Optional

from jose import JWTError, jwt
from slowapi import Limiter
from slowapi.util import get_remote_address
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from app.config import (
    JWT_ALGORITHM,
    JWT_SECRET,
    RATE_LIMIT_EXPERT,
    RATE_LIMIT_FREE,
    RATE_LIMIT_PRO,
)


# ---------------------------------------------------------------------------
# Parse "N/hour" format into max requests per hour
# ---------------------------------------------------------------------------

def _parse_limit(limit_str):
    """Parse '50/hour' -> 50, '500/hour' -> 500."""
    try:
        parts = limit_str.split("/")
        return int(parts[0])
    except (ValueError, IndexError):
        return 50  # fallback


ROLE_MAX_REQUESTS = {
    "free": _parse_limit(RATE_LIMIT_FREE),
    "pro": _parse_limit(RATE_LIMIT_PRO),
    "expert": _parse_limit(RATE_LIMIT_EXPERT),
    "admin": None,  # unlimited
}

ROLE_LIMITS = {
    "free": RATE_LIMIT_FREE,
    "pro": RATE_LIMIT_PRO,
    "expert": RATE_LIMIT_EXPERT,
    "admin": None,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_role_from_request(request):
    """Try to extract the user role from a Bearer token (best-effort)."""
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return None
    token = auth_header[7:]
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload.get("role")
    except JWTError:
        return None


def rate_limit_key(request):
    """Key function for slowapi."""
    role = _extract_role_from_request(request) or "free"
    ip = get_remote_address(request) or "unknown"
    return "{}:{}".format(role, ip)


def get_rate_limit_for_request(request):
    """Return the applicable rate-limit string based on the caller's role."""
    role = _extract_role_from_request(request) or "free"
    return ROLE_LIMITS.get(role) or "100000/hour"


# ---------------------------------------------------------------------------
# slowapi Limiter instance (for @limiter.limit() on specific routes)
# ---------------------------------------------------------------------------

limiter = Limiter(
    key_func=rate_limit_key,
    default_limits=[],
    storage_uri="memory://",
)


# ---------------------------------------------------------------------------
# Simple in-memory rate limiter (sliding window per hour)
# ---------------------------------------------------------------------------

_request_log = defaultdict(list)  # key -> list of timestamps


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Apply role-aware rate limiting using a simple sliding window."""

    async def dispatch(self, request, call_next):
        role = _extract_role_from_request(request) or "free"
        max_requests = ROLE_MAX_REQUESTS.get(role)

        # Admin = unlimited
        if max_requests is not None:
            ip = get_remote_address(request) or "unknown"
            key = "{}:{}".format(role, ip)
            now = time.time()
            window = 3600  # 1 hour

            # Clean old entries
            _request_log[key] = [t for t in _request_log[key] if now - t < window]

            if len(_request_log[key]) >= max_requests:
                return JSONResponse(
                    status_code=429,
                    content={
                        "detail": "Limite de requêtes dépassée: {}/heure pour le plan '{}'".format(
                            max_requests, role
                        ),
                    },
                    headers={"Retry-After": "3600"},
                )

            _request_log[key].append(now)

        response = await call_next(request)
        return response


# ---------------------------------------------------------------------------
# Security headers middleware
# ---------------------------------------------------------------------------

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add standard security headers to every response."""

    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Strict-Transport-Security"] = (
            "max-age=31536000; includeSubDomains"
        )
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://unpkg.com; "
            "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://unpkg.com; "
            "font-src 'self' https://fonts.gstatic.com; "
            "img-src 'self' data: https://*.tile.openstreetmap.org; "
            "connect-src 'self' http://localhost:* https://*.railway.app https://*.vercel.app https://agroprix.app https://*.agroprix.app"
        )
        return response
