import os
import pytest
import redis.asyncio as aioredis

from rate_guardian import RateGuardian

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


@pytest.fixture
async def redis_client():
    client = aioredis.from_url(REDIS_URL, decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture
async def limiter(redis_client):
    return RateGuardian(redis=redis_client, prefix="test")
