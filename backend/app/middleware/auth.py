"""API key authentication middleware for /api/* endpoints."""

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from app.config import settings


class APIKeyMiddleware(BaseHTTPMiddleware):
    """Require X-API-Key header on all /api/* requests.

    - If ELEPHANT_API_KEY is not configured, all requests pass through (dev mode).
    - GET /api/health is always exempt.
    - All other /api/* requests require a matching X-API-Key header; 401 otherwise.
    """

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        if not path.startswith("/api/"):
            return await call_next(request)

        if path == "/api/health" and request.method == "GET":
            return await call_next(request)

        if not settings.api_key:
            return await call_next(request)

        if request.headers.get("X-API-Key") != settings.api_key:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or missing API key"},
            )

        return await call_next(request)
