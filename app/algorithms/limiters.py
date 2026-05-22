"""
Rate Limiting Algorithms — all backed by Redis for distributed correctness.

Key insight: every algorithm must be ATOMIC — check + update must be one operation.
Redis Lua scripts achieve this: they run on the server, cannot be interrupted.
"""
import time
import redis
from abc import ABC, abstractmethod
from typing import Tuple
from dataclasses import dataclass


@dataclass
class RateLimitResult:
    allowed: bool
    remaining: int
    reset_at: float      # unix timestamp when limit resets
    retry_after: float   # seconds to wait if denied


class BaseRateLimiter(ABC):
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    @abstractmethod
    def is_allowed(self, identifier: str, limit: int, window_seconds: int) -> RateLimitResult:
        pass


class FixedWindowLimiter(BaseRateLimiter):
    """
    Simple counter reset at fixed intervals.
    Problem: burst at boundary — 200 requests in 2 seconds if straddling window.
    Use when: simplicity > precision (e.g. blocking bots, not API billing).
    """
    def is_allowed(self, identifier: str, limit: int, window_seconds: int) -> RateLimitResult:
        window_start = int(time.time() // window_seconds) * window_seconds
        key = f"fw:{identifier}:{window_start}"

        # Atomic INCR + EXPIRE via Lua
        lua = """
        local count = redis.call('INCR', KEYS[1])
        if count == 1 then
            redis.call('EXPIRE', KEYS[1], tonumber(ARGV[1]))
        end
        return count
        """
        count = self.redis.eval(lua, 1, key, window_seconds)
        remaining = max(0, limit - count)
        reset_at = window_start + window_seconds

        return RateLimitResult(
            allowed=count <= limit,
            remaining=remaining,
            reset_at=reset_at,
            retry_after=reset_at - time.time() if count > limit else 0
        )


class SlidingWindowLogLimiter(BaseRateLimiter):
    """
    Exact sliding window using Redis Sorted Set.
    Key: sw_log:{identifier}
    Members: request timestamps (score = timestamp)

    Memory: O(requests in window) — can be large for high-traffic identifiers.
    Use when: exact limiting required, traffic is low-medium.
    """
    def is_allowed(self, identifier: str, limit: int, window_seconds: int) -> RateLimitResult:
        now = time.time()
        window_start = now - window_seconds
        key = f"sw_log:{identifier}"

        lua = """
        local now = tonumber(ARGV[1])
        local window_start = tonumber(ARGV[2])
        local limit = tonumber(ARGV[3])
        local window_seconds = tonumber(ARGV[4])

        -- Remove expired entries outside the window
        redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', window_start)

        -- Count requests in window
        local count = redis.call('ZCARD', KEYS[1])

        if count < limit then
            -- Add current request with timestamp as score
            redis.call('ZADD', KEYS[1], now, now)
            redis.call('EXPIRE', KEYS[1], window_seconds + 1)
            return {1, limit - count - 1}
        else
            return {0, 0}
        end
        """
        result = self.redis.eval(lua, 1, key, now, window_start, limit, window_seconds)
        allowed = result[0] == 1

        return RateLimitResult(
            allowed=allowed,
            remaining=int(result[1]),
            reset_at=now + window_seconds,
            retry_after=0 if allowed else 1.0
        )


class TokenBucketLimiter(BaseRateLimiter):
    """
    Token bucket: tokens refill at constant rate, requests consume tokens.
    Allows burst up to bucket capacity, enforces average rate.

    Best for: API rate limiting where occasional burst is acceptable.
    Used by: Stripe, GitHub, most production APIs.

    Redis stores: {tokens, last_refill_timestamp} as a Hash.
    """
    def is_allowed(self, identifier: str, limit: int, window_seconds: int) -> RateLimitResult:
        now = time.time()
        refill_rate = limit / window_seconds  # tokens per second
        key = f"tb:{identifier}"

        lua = """
        local now = tonumber(ARGV[1])
        local capacity = tonumber(ARGV[2])
        local refill_rate = tonumber(ARGV[3])

        local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'last_refill')
        local tokens = tonumber(bucket[1]) or capacity
        local last_refill = tonumber(bucket[2]) or now

        -- Refill tokens based on elapsed time
        local elapsed = now - last_refill
        tokens = math.min(capacity, tokens + elapsed * refill_rate)

        if tokens >= 1 then
            tokens = tokens - 1
            redis.call('HMSET', KEYS[1], 'tokens', tokens, 'last_refill', now)
            redis.call('EXPIRE', KEYS[1], 3600)
            return {1, math.floor(tokens)}
        else
            redis.call('HMSET', KEYS[1], 'tokens', tokens, 'last_refill', now)
            return {0, 0}
        end
        """
        result = self.redis.eval(lua, 1, key, now, limit, refill_rate)
        allowed = result[0] == 1

        return RateLimitResult(
            allowed=allowed,
            remaining=int(result[1]),
            reset_at=now + (1.0 / refill_rate),
            retry_after=0 if allowed else round(1.0 / refill_rate, 2)
        )


class SlidingWindowCounterLimiter(BaseRateLimiter):
    """
    Approximation of sliding window using two fixed windows.
    Memory: O(1) — just two counters.
    Accuracy: ~10% error at window boundary (usually acceptable).

    Calculation:
    current_window_requests + previous_window_requests * (overlap_fraction)
    """
    def is_allowed(self, identifier: str, limit: int, window_seconds: int) -> RateLimitResult:
        now = time.time()
        current_window = int(now // window_seconds)
        prev_window = current_window - 1
        elapsed_in_window = now % window_seconds
        prev_weight = 1 - (elapsed_in_window / window_seconds)

        curr_key = f"swc:{identifier}:{current_window}"
        prev_key = f"swc:{identifier}:{prev_window}"

        lua = """
        local curr_key = KEYS[1]
        local prev_key = KEYS[2]
        local limit = tonumber(ARGV[1])
        local prev_weight = tonumber(ARGV[2])
        local window_seconds = tonumber(ARGV[3])

        local curr_count = tonumber(redis.call('GET', curr_key)) or 0
        local prev_count = tonumber(redis.call('GET', prev_key)) or 0

        local weighted_count = curr_count + (prev_count * prev_weight)

        if weighted_count < limit then
            local new_curr = redis.call('INCR', curr_key)
            if new_curr == 1 then
                redis.call('EXPIRE', curr_key, window_seconds * 2)
            end
            return {1, math.floor(limit - weighted_count - 1)}
        else
            return {0, 0}
        end
        """
        result = self.redis.eval(lua, 2, curr_key, prev_key, limit, prev_weight, window_seconds)
        allowed = result[0] == 1

        return RateLimitResult(
            allowed=allowed,
            remaining=int(result[1]),
            reset_at=now + (window_seconds - elapsed_in_window),
            retry_after=0 if allowed else round(window_seconds - elapsed_in_window, 1)
        )
