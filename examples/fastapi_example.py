import redis.asyncio as aioredis
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request

from rate_guardian import RateGuardian, RateLimitExceeded, RateLimitMiddleware, rate_limit

_redis_client = aioredis.from_url("redis://localhost:6379", decode_responses=True)
limiter = RateGuardian(redis=_redis_client, prefix="myapp")


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await _redis_client.aclose()


app = FastAPI(lifespan=lifespan)

# global limit on every request
app.add_middleware(RateLimitMiddleware, limiter=limiter, limit=200, window=60)


# per-route bucket keyed by route + IP
@app.get("/search")
@rate_limit(limiter, limit=20, window=60)
async def search(request: Request, q: str):
    return {"results": []}


# manual check — useful when the key depends on request data
@app.post("/shorten")
async def shorten(request: Request, url: str, tenant_id: int):
    try:
        await limiter.check(f"tenant:{tenant_id}", limit=10, window=60)
    except RateLimitExceeded as e:
        raise HTTPException(status_code=429, detail="Rate limit exceeded", headers=e.headers)

    return {"short_url": "https://short.ly/abc123"}
