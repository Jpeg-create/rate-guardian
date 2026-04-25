from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from .limiter import RateGuardian


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Global rate limiting by client IP. Respects x-forwarded-for."""

    def __init__(self, app, limiter: RateGuardian, limit: int, window: int):
        super().__init__(app)
        self.limiter = limiter
        self.limit = limit
        self.window = window

    async def dispatch(self, request: Request, call_next):
        ip = (
            request.headers.get("x-forwarded-for", "").split(",")[0].strip()
            or (request.client.host if request.client else "unknown")
        )

        try:
            allowed, headers = await self.limiter.is_allowed(
                f"middleware:{ip}", self.limit, self.window
            )
        except Exception:
            return await call_next(request)  # fail open if Redis is down

        if not allowed:
            return JSONResponse(
                status_code=429,
                content={"error": "Too many requests"},
                headers=headers,
            )

        response = await call_next(request)
        for k, v in headers.items():
            response.headers[k] = v
        return response
