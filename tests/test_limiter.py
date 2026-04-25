import time
import pytest
import redis.asyncio as aioredis

from rate_guardian import RateGuardian, RateLimitExceeded


async def test_first_request_allowed(limiter):
    await limiter.reset("first")
    allowed, headers = await limiter.is_allowed("first", limit=5, window=60)

    assert allowed is True
    assert headers["X-RateLimit-Limit"] == "5"
    assert headers["X-RateLimit-Remaining"] == "4"
    assert "Retry-After" not in headers


async def test_limit_enforced(limiter):
    await limiter.reset("enforce")

    for _ in range(3):
        allowed, _ = await limiter.is_allowed("enforce", limit=3, window=60)
        assert allowed is True

    # 4th request — over the limit
    allowed, headers = await limiter.is_allowed("enforce", limit=3, window=60)
    assert allowed is False
    assert headers["X-RateLimit-Remaining"] == "0"
    assert "Retry-After" in headers


async def test_remaining_decrements(limiter):
    await limiter.reset("decrement")

    _, h1 = await limiter.is_allowed("decrement", limit=5, window=60)
    _, h2 = await limiter.is_allowed("decrement", limit=5, window=60)
    _, h3 = await limiter.is_allowed("decrement", limit=5, window=60)

    assert int(h1["X-RateLimit-Remaining"]) > int(h2["X-RateLimit-Remaining"])
    assert int(h2["X-RateLimit-Remaining"]) > int(h3["X-RateLimit-Remaining"])


async def test_check_raises_when_exceeded(limiter):
    await limiter.reset("check_raise")

    for _ in range(2):
        await limiter.check("check_raise", limit=2, window=60)

    with pytest.raises(RateLimitExceeded) as exc_info:
        await limiter.check("check_raise", limit=2, window=60)

    assert "Retry-After" in exc_info.value.headers


async def test_check_returns_headers_when_allowed(limiter):
    await limiter.reset("check_headers")
    headers = await limiter.check("check_headers", limit=10, window=60)

    assert "X-RateLimit-Limit" in headers
    assert "X-RateLimit-Remaining" in headers
    assert "X-RateLimit-Reset" in headers


async def test_reset_header_is_unix_timestamp(limiter):
    """X-RateLimit-Reset must be a Unix epoch timestamp, not the window duration."""
    await limiter.reset("ts_check")
    _, headers = await limiter.is_allowed("ts_check", limit=5, window=60)

    reset_val = int(headers["X-RateLimit-Reset"])
    now = int(time.time())

    assert now < reset_val <= now + 65, (
        f"X-RateLimit-Reset ({reset_val}) should be a Unix timestamp near {now + 60}"
    )


async def test_blocked_requests_not_written_to_redis(limiter, redis_client):
    """Blocked requests must NOT be added to the sorted set."""
    await limiter.reset("no_write")

    for _ in range(3):
        await limiter.is_allowed("no_write", limit=3, window=60)

    for _ in range(5):
        await limiter.is_allowed("no_write", limit=3, window=60)

    cardinality = await redis_client.zcard("test:no_write")
    assert cardinality == 3, (
        f"Sorted set should have exactly 3 entries (the allowed ones), got {cardinality}"
    )


async def test_keys_are_isolated(limiter):
    await limiter.reset("key_a")
    await limiter.reset("key_b")

    for _ in range(2):
        await limiter.is_allowed("key_a", limit=2, window=60)

    allowed_a, _ = await limiter.is_allowed("key_a", limit=2, window=60)
    assert allowed_a is False

    allowed_b, _ = await limiter.is_allowed("key_b", limit=2, window=60)
    assert allowed_b is True


async def test_reset_clears_counter(limiter):
    await limiter.reset("reset_me")

    for _ in range(3):
        await limiter.is_allowed("reset_me", limit=3, window=60)

    allowed, _ = await limiter.is_allowed("reset_me", limit=3, window=60)
    assert allowed is False

    await limiter.reset("reset_me")

    allowed, _ = await limiter.is_allowed("reset_me", limit=3, window=60)
    assert allowed is True


async def test_prefix_isolates_keys(redis_client):
    limiter_a = RateGuardian(redis=redis_client, prefix="app_a")
    limiter_b = RateGuardian(redis=redis_client, prefix="app_b")

    await limiter_a.reset("shared")
    await limiter_b.reset("shared")

    for _ in range(2):
        await limiter_a.is_allowed("shared", limit=2, window=60)

    allowed_a, _ = await limiter_a.is_allowed("shared", limit=2, window=60)
    assert allowed_a is False

    allowed_b, _ = await limiter_b.is_allowed("shared", limit=2, window=60)
    assert allowed_b is True


async def test_retry_after_only_on_block(limiter):
    await limiter.reset("retry")

    allowed, headers = await limiter.is_allowed("retry", limit=5, window=30)
    assert allowed is True
    assert "Retry-After" not in headers

    for _ in range(4):
        await limiter.is_allowed("retry", limit=5, window=30)

    allowed, headers = await limiter.is_allowed("retry", limit=5, window=30)
    assert allowed is False
    assert headers["Retry-After"] == "30"
