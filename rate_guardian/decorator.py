from functools import wraps
from fastapi import Request
from fastapi.responses import JSONResponse

from .limiter import RateGuardian


def rate_limit(limiter: RateGuardian, limit: int, window: int):
    """Per-route rate limiting. Keys by function name + client IP."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            request = kwargs.get("request")
            if request is None:
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break

            if request:
                ip = (
                    request.headers.get("x-forwarded-for", "").split(",")[0].strip()
                    or (request.client.host if request.client else "unknown")
                )
                key = f"route:{func.__name__}:{ip}"
                try:
                    allowed, headers = await limiter.is_allowed(key, limit, window)
                    if not allowed:
                        return JSONResponse(
                            status_code=429,
                            content={"error": "Too many requests"},
                            headers=headers,
                        )
                except Exception:
                    pass  # fail open

            return await func(*args, **kwargs)
        return wrapper
    return decorator
