import time
import uuid
from typing import Tuple

import redis.asyncio as aioredis

# Atomic sliding window check. Evicts expired entries, counts remainder,
# and only records the request if it's within the limit.
# Returns [count_before, allowed]  (allowed: 1 = yes, 0 = no)
_LUA_SCRIPT = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local oldest = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local window = tonumber(ARGV[4])
local req_id = ARGV[5]

redis.call('ZREMRANGEBYSCORE', key, 0, oldest)
local count = tonumber(redis.call('ZCARD', key))

if count < limit then
    redis.call('ZADD', key, now, req_id)
    redis.call('EXPIRE', key, window)
    return {count, 1}
else
    return {count, 0}
end
"""


class RateLimitExceeded(Exception):
    """Raised by RateGuardian.check() when the rate limit is exceeded."""

    def __init__(self, headers: dict):
        self.headers = headers
        super().__init__("Rate limit exceeded")


class RateGuardian:
    """Async sliding window rate limiter backed by Redis."""

    def __init__(self, redis: aioredis.Redis, prefix: str = "rg"):
        self._redis = redis
        self._prefix = prefix
        self._script = redis.register_script(_LUA_SCRIPT)

    def _key(self, key: str) -> str:
        return f"{self._prefix}:{key}"

    async def is_allowed(self, key: str, limit: int, window: int) -> Tuple[bool, dict]:
        now = int(time.time() * 1000)
        oldest = now - (window * 1000)
        request_id = str(uuid.uuid4())

        count, allowed_int = await self._script(
            keys=[self._key(key)],
            args=[now, oldest, limit, window, request_id],
        )
        count = int(count)
        allowed = bool(int(allowed_int))
        remaining = max(0, limit - count - 1) if allowed else 0
        reset_at = int(time.time()) + window

        headers = {
            "X-RateLimit-Limit": str(limit),
            "X-RateLimit-Remaining": str(remaining),
            "X-RateLimit-Reset": str(reset_at),
        }
        if not allowed:
            headers["Retry-After"] = str(window)

        return allowed, headers

    async def check(self, key: str, limit: int, window: int) -> dict:
        """Like is_allowed() but raises RateLimitExceeded instead of returning False."""
        allowed, headers = await self.is_allowed(key, limit, window)
        if not allowed:
            raise RateLimitExceeded(headers)
        return headers

    async def reset(self, key: str) -> None:
        await self._redis.delete(self._key(key))


class RateGuardianSync:
    """
    Synchronous v1 compatibility layer using the Upstash HTTP client.

    Requires the optional 'sync' extra:
        pip install rate-guardian[sync]
    """

    def __init__(self, redis_url: str, redis_token: str, prefix: str = "rg"):
        try:
            from upstash_redis import Redis as UpstashRedis
        except ImportError as exc:
            raise ImportError(
                "upstash-redis is required for RateGuardianSync. "
                "Install it with: pip install rate-guardian[sync]"
            ) from exc

        self._redis = UpstashRedis(url=redis_url, token=redis_token)
        self._prefix = prefix

    def _key(self, key: str) -> str:
        return f"{self._prefix}:{key}"

    def is_allowed(self, key: str, limit: int, window: int) -> Tuple[bool, dict]:
        now = int(time.time() * 1000)
        oldest = now - (window * 1000)
        full_key = self._key(key)

        pipe = self._redis.pipeline()
        pipe.zremrangebyscore(full_key, 0, oldest)
        pipe.zcard(full_key)
        results = pipe.exec()

        count = results[1]
        allowed = count < limit

        if allowed:
            request_id = str(uuid.uuid4())
            write_pipe = self._redis.pipeline()
            write_pipe.zadd(full_key, {request_id: now})
            write_pipe.expire(full_key, window)
            write_pipe.exec()

        remaining = max(0, limit - count - 1) if allowed else 0
        reset_at = int(time.time()) + window

        headers = {
            "X-RateLimit-Limit": str(limit),
            "X-RateLimit-Remaining": str(remaining),
            "X-RateLimit-Reset": str(reset_at),
        }
        if not allowed:
            headers["Retry-After"] = str(window)

        return allowed, headers

    def reset(self, key: str) -> None:
        self._redis.delete(self._key(key))
