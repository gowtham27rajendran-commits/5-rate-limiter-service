# Rate Limiter as a Service

A standalone rate limiting microservice implementing four algorithms, distributable across N workers via Redis. Used as middleware by other services.

## Algorithms Implemented

| Algorithm | Best For | Weakness |
|---|---|---|
| Fixed Window | Simple counters | Burst at window boundary (2x limit) |
| Sliding Window Log | Exact limiting | High memory (stores all timestamps) |
| Sliding Window Counter | Approx. sliding, low memory | ~10% error at boundary |
| Token Bucket | Smooth burst handling | Slightly complex |
| Leaky Bucket | Strict output rate | No burst tolerance |

## Architecture

```
Client Request → Rate Limiter Service (FastAPI)
                        ↓
                   Redis (shared state across N workers)
                        ↓
                 Allow / Deny + Headers
                 (X-RateLimit-Remaining, X-RateLimit-Reset)
```

## Why Redis for distributed rate limiting?

Each worker process has no shared memory. Redis provides atomic operations (INCR, SETNX, Lua scripts) that work correctly even with 100 workers hitting the same key simultaneously.

## Running Locally

```bash
docker-compose up -d redis
pip install -r requirements.txt
uvicorn app.main:app --reload
```

## API

```
POST /check              — check if request is allowed
GET  /config/{key}       — get rate limit config for a key
POST /config             — set rate limit config
GET  /stats/{identifier} — current usage stats
```

## Interview Talking Points

**"What's the boundary burst problem with fixed windows?"**
Window resets at T=0 and T=60. A user can make 100 requests at T=59 and 100 more at T=61 — 200 requests in 2 seconds. Sliding window eliminates this.

**"How do you make rate limiting atomic in distributed systems?"**
Use Redis Lua scripts — they execute atomically on the Redis server. No race condition between CHECK and INCREMENT because they're a single atomic operation.

**"How does Stripe/Cloudflare do it?"**
Token bucket with Redis. Each identifier (IP/API key) has a bucket. Tokens refill at a constant rate. Request costs 1 token. Atomic via Redis Lua scripts.
