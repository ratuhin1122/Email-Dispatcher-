# 📨 High-Throughput Concurrent Email Dispatcher

A high-performance, concurrent email dispatching engine written in Go. Built using Go's native CSP (Communicating Sequential Processes) concurrency primitives (Goroutines, Channels, and WaitGroups), this service reads recipient lists, durably persists jobs to **PostgreSQL**, dynamically compiles responsive HTML email templates, dispatches messages concurrently across a **3-worker pool** to **Mailpit** via SMTP, and routes failures to both PostgreSQL and a dedicated **Dead Letter Queue (DLQ)**.

**Redis** provides a shared rate limiter across all 3 consumers, enforcing a configurable send rate without losing email jobs.

---

## 🏗️ Architecture

```
[ emails.csv ]
      │
      ▼
┌──────────────┐       1. INSERT ('pending')
│   Producer   │ ──────────────────────────────┐
└──────┬───────┘                               │
       │  2. Passes EmailJob{ID, Recipient}    ▼
       ▼                               ┌──────────────────┐
┌────────────────────────┐             │    PostgreSQL    │
│   Unbuffered Channel   │             │   (email_jobs)   │
└──────┬─────────────────┘             └────────┬─────────┘
       ├───────────────────┬───────────────────┐│
       ▼                   ▼                   ▼│
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  Consumer 1  │    │  Consumer 2  │    │  Consumer 3  │   Concurrent Worker Pool (3 workers)
└──────┬───────┘    └──────┬───────┘    └──────┬───────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
                           ▼
                 ┌───────────────────┐
                 │  Redis Rate       │
                 │  Limiter (shared) │
                 └────────┬──────────┘
                          │
             ┌────────────┴────────────┐
             ▼                         ▼
     [ Success Path ]          [ Failure Path ]
             │                         │
     ┌───────────────┐         ┌───────────────┐
     │  SMTP Server  │         │  Dead Letter  │
     │   (Mailpit)   │         │  Queue (DLQ)  │
     └───────────────┘         └───────┬───────┘
                                       │
                                       ▼
                               failed_emails.csv
```

### Architectural Flow & Concurrency Mechanics
1. **Producer Persistence**: The producer reads recipient records from CSV, inserts each job into PostgreSQL in `pending` status using `database/sql` raw SQL, and retrieves its generated UUID.
2. **Channel Dispatch**: The producer immediately sends an `EmailJob{ID, Recipient}` into the unbuffered Go channel. This ensures durability before in-process communication.
3. **Concurrent Consumers**: The 3 worker goroutines receive jobs from the channel.
   - Go channels guarantee that each job is received by **exactly one consumer**.
   - As an additional concurrency guard, status transitions use row-level atomic conditional updates (`WHERE id = $1 AND status = 'pending'`).
4. **Shared Rate Limiting**: Before sending, each consumer calls `rl.Wait()` which atomically checks a Redis counter. All 3 consumers share the **same** rate limit via a single Redis key. If the limit is exceeded, the consumer waits with exponential backoff until the window resets.
5. **Execution & Delivery**:
   - The consumer marks the job as `processing` in PostgreSQL.
   - The consumer renders the HTML template, waits for rate-limit clearance, and transmits the email via SMTP to Mailpit.
   - **On Success**: The job is marked as `sent`, storing `sent_at = NOW()` and the rendered body.
   - **On Failure**: The job is marked as `failed`, incrementing `attempts = attempts + 1` and storing the error message. The job is also pushed to `dlqChan` for disk logging.

---

## 🔴 Redis: Shared Rate Limiting

### Why Redis?

The 3 email consumers run as separate goroutines. A naive in-process rate limiter (e.g., `time.Sleep` or `sync.Mutex` with counters) would only limit each consumer independently — they wouldn't share a single global rate. Redis provides:

- **Shared state**: All consumers check the same Redis key, enforcing a single global limit.
- **Atomicity**: A Lua script runs `INCR` + `EXPIRE` atomically on the Redis server, preventing race conditions.
- **Ephemeral TTL**: Rate-limit keys auto-expire, requiring no cleanup.

### Rate-Limiting Algorithm: Fixed-Window Counter

The rate limiter uses a **fixed-window counter** implemented with an atomic Redis Lua script:

```lua
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local current = redis.call("INCR", key)
if current == 1 then
    redis.call("EXPIRE", key, window)
end

return current
```

**How it works:**
1. Key format: `ratelimit:email:<window_timestamp>` (e.g., `ratelimit:email:1696982400`).
2. `INCR` atomically increments the counter. On the first request in a window, it creates the key with value `1`.
3. `EXPIRE` sets the TTL to the window duration, so the key auto-deletes when the window ends.
4. If `current > limit`, the consumer's `Wait()` method backs off with exponential delay and retries until the window resets.

**Why fixed-window over token-bucket:** A fixed-window counter is the simplest atomic Redis pattern. It requires a single Lua script with no floating-point timestamps or refill calculations. For a local development email dispatcher, it provides sufficient accuracy without unnecessary complexity.

### PostgreSQL vs Redis Responsibilities

| System | Responsibility | Durability |
|---|---|---|
| **PostgreSQL** | Email job state (create, processing, sent, failed) | ✅ Durable, persistent |
| **Redis** | Rate-limit counters with TTL | ❌ Ephemeral, auto-expiring |
| **Go channels** | In-process worker communication | ❌ In-memory only |
| **Mailpit** | Local SMTP server for email delivery | N/A |

**PostgreSQL is always the source of truth.** Redis only stores temporary rate-limit counters that are safe to lose. If Redis goes down, no email job data is affected.

### How the 3 Consumers Share the Rate Limiter

```
Consumer 1 ─┐
Consumer 2 ─┼──> rl.Wait(ctx) ──> Redis INCR "ratelimit:email:1696982400" ──> Mailpit
Consumer 3 ─┘
```

All 3 consumers call the same `RateLimiter.Wait()` method, which executes the same Lua script against the same Redis key. Because `INCR` is atomic in Redis, concurrent calls from different goroutines never produce duplicate counts or race conditions.

### Redis Failure Behavior: Fail Open

If Redis becomes unavailable:
- The rate limiter **allows** sends to proceed (returns `true` with a logged warning).
- **No email jobs are lost.**
- PostgreSQL continues to track all job state regardless of Redis availability.

**Tradeoff:** Under Redis failure, all 3 consumers send without rate limiting. This is acceptable for a local development environment sending to Mailpit. In production with a real SMTP provider (billing/reputation), you might prefer fail-closed.

---

## 🔌 Redis Connection Pool Configuration

The Redis client (`go-redis/v9`) manages its own connection pool, separate from PostgreSQL's pool.

| Parameter | Default | Environment Variable | Purpose |
|---|---|---|---|
| `PoolSize` | `5` | `REDIS_POOL_SIZE` | 3 consumers + headroom for health checks |
| `MinIdleConns` | `2` | `REDIS_MIN_IDLE_CONNS` | Keeps warm connections to avoid cold-start latency |
| `DialTimeout` | `5s` | `REDIS_DIAL_TIMEOUT` | Maximum time to establish a Redis connection |

**Why these values:**
- **PoolSize = 5**: Each of the 3 consumers makes one Redis call per email. 5 connections provides headroom without waste.
- **MinIdleConns = 2**: Avoids the latency of creating a new TCP connection on burst dispatches.
- These are intentionally small — suitable for local development. Scale up for production.

---

## 🗄️ Database Schema & Lifecycle

### `email_jobs` Table

```sql
CREATE TABLE email_jobs (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    recipient     VARCHAR(255) NOT NULL,
    subject       VARCHAR(500) NOT NULL,
    body          TEXT NOT NULL DEFAULT '',
    status        VARCHAR(20)  NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'processing', 'sent', 'failed')),
    attempts      INTEGER      NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    sent_at       TIMESTAMPTZ
);

CREATE INDEX idx_email_jobs_status ON email_jobs(status);
CREATE INDEX idx_email_jobs_created_at ON email_jobs(created_at);
```

### Email Lifecycle

```
pending ──► processing ──┬──► sent
                         └──► failed
```

* **`pending`**: Job created and persisted by the producer.
* **`processing`**: Consumer dequeued the job from the channel and began template rendering & SMTP dispatch.
* **`sent`**: Successfully accepted by the SMTP server (Mailpit). Delivery timestamp recorded in `sent_at`.
* **`failed`**: Encountered an error (template parsing error or SMTP transmission failure). `attempts` is incremented and `error_message` is persisted.

### Concurrency Safety
* **Channel Partitioning**: In-process Go channel semantics guarantee that only one goroutine receives any individual job from `jobChan`.
* **Conditional Atomic Updates**: All SQL update statements specify state preconditions:
  ```sql
  UPDATE email_jobs SET status = 'processing', updated_at = NOW()
  WHERE id = $1 AND status = 'pending';
  ```
  If two workers were ever to race on the same ID, exactly one will update 1 row, while the second will encounter `RowsAffected == 0` and safely abort.

---

## 🔌 PostgreSQL Connection Pool Configuration

Connection pooling is initialized once in [`db.go`](db.go) during application bootstrap using Go's standard library `database/sql`. The returned `*sql.DB` connection pool is thread-safe and shared across the producer and all 3 consumer goroutines.

| Parameter | Default | Environment Variable | Purpose |
|---|---|---|---|
| `MaxOpenConns` | `10` | `DB_MAX_OPEN_CONNS` | Limits total active connections across 3 workers + producer |
| `MaxIdleConns` | `5` | `DB_MAX_IDLE_CONNS` | Keeps idle connections ready for burst dispatches |
| `ConnMaxLifetime` | `30m` | `DB_CONN_MAX_LIFETIME` | Recycles stale connections to prevent stale socket leaks |
| `ConnMaxIdleTime` | `5m` | `DB_CONN_MAX_IDLE_TIME` | Reclaims excess idle connections during low dispatch activity |

All operations utilize `context.Context` with appropriate cancellation and timeouts.

---

## 🐳 Docker Infrastructure

The local development environment runs **Mailpit**, **PostgreSQL**, and **Redis** via Docker Compose.

### Starting the Infrastructure

```bash
docker compose up -d
```

Services exposed:
* **Mailpit SMTP Server**: `localhost:1025`
* **Mailpit Web UI**: [http://localhost:8025](http://localhost:8025)
* **PostgreSQL Server**: `localhost:5433` (defaults to 5433 to avoid port collisions with host PostgreSQL)
* **Redis Server**: `localhost:6379`

### Docker Services

| Service | Image | Container Name | Ports | Persistent Volume |
|---|---|---|---|---|
| Mailpit | `axllent/mailpit` | `mailpit` | 8025, 1025 | — |
| PostgreSQL | `postgres:16-alpine` | `email_dispatcher_db` | 5433→5432 | `pgdata` |
| Redis | `redis:7-alpine` | `email_dispatcher_redis` | 6379→6379 | `redisdata` |

All services include healthchecks (except Mailpit which doesn't require one).

---

## 🔄 Database Migrations

Migrations are stored as raw SQL files in the [`migrations/`](migrations/) directory:
* [`001_create_email_jobs.up.sql`](migrations/001_create_email_jobs.up.sql)
* [`001_create_email_jobs.down.sql`](migrations/001_create_email_jobs.down.sql)

Migrations are tracked in the database using a `schema_migrations` table and are executed inside an atomic transaction.

### Apply Migrations (Up)
```bash
go run . migrate
```

### Rollback Migrations (Down)
```bash
go run . migrate down
```

---

## ⚙️ Environment Variables

Configuration is loaded from environment variables with `.env` file fallback via [`config.go`](config.go). Copy `.env.example` to `.env`:

```bash
cp .env.example .env
```

| Variable | Default | Description |
|---|---|---|
| `SMTP_HOST` | `localhost` | Mailpit SMTP hostname |
| `SMTP_PORT` | `1025` | Mailpit SMTP port |
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5433` | PostgreSQL port |
| `DB_NAME` | `email_dispatcher` | Database name |
| `DB_USER` | `postgres` | Database username |
| `DB_PASSWORD` | `postgres` | Database password |
| `DB_SSLMODE` | `disable` | SSL mode (`disable` for local dev) |
| `DB_MAX_OPEN_CONNS` | `10` | Maximum open database connections |
| `DB_MAX_IDLE_CONNS` | `5` | Maximum idle connections in pool |
| `DB_CONN_MAX_LIFETIME` | `30m` | Maximum connection reuse duration |
| `DB_CONN_MAX_IDLE_TIME` | `5m` | Maximum idle connection duration |
| `REDIS_HOST` | `localhost` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_PASSWORD` | *(empty)* | Redis password (blank for local dev) |
| `REDIS_POOL_SIZE` | `5` | Redis connection pool size |
| `REDIS_MIN_IDLE_CONNS` | `2` | Minimum idle Redis connections |
| `REDIS_DIAL_TIMEOUT` | `5s` | Redis connection timeout |
| `EMAIL_RATE_LIMIT` | `10` | Max emails allowed per rate window |
| `EMAIL_RATE_WINDOW` | `1s` | Duration of the rate-limit window |

---

## 🚀 Running the Project

### 1. Start Docker Containers
```bash
docker compose up -d
```

### 2. Apply Database Migrations
```bash
go run . migrate
```

### 3. Run the Email Dispatcher
```bash
go run .
```

### 4. Verify Delivery
* **Mailpit Web UI**: Open [http://localhost:8025](http://localhost:8025) to view all delivered HTML emails.
* **Database Inspection**:
  ```bash
  docker exec email_dispatcher_db psql -U postgres -d email_dispatcher -c "SELECT id, recipient, status, sent_at FROM email_jobs LIMIT 10;"
  ```
* **Redis Inspection**:
  ```bash
  docker exec email_dispatcher_redis redis-cli KEYS "ratelimit:*"
  ```

---

## 🧪 Testing

Run all unit and integration tests:

```bash
go test -v ./...
```

**Requires**: PostgreSQL and Redis running via `docker compose up -d`.

The test suite covers:
* Job creation with UUID generation (`TestCreateJob`)
* Complete success lifecycle `pending` → `processing` → `sent` (`TestEmailJobLifecycle_Sent`)
* Failure lifecycle with error capture and attempt tracking (`TestEmailJobLifecycle_Failed`)
* Concurrency idempotency and guard conditions (`TestMarkProcessingIdempotency`, `TestMarkSentRequiresProcessing`)
* Connection pool configuration verification (`TestConnectionPoolConfig`)
* Worker error handling and DLQ emission (`TestEmailWorkerFailure`)
* Rate limiter: requests below limit (`TestRateLimiter_BelowLimit`)
* Rate limiter: requests exceeding limit (`TestRateLimiter_ExceedsLimit`)
* Rate limiter: window reset (`TestRateLimiter_WindowReset`)
* Rate limiter: multiple consumers sharing one limit (`TestRateLimiter_MultipleConsumersSharedLimit`)
* Rate limiter: concurrent requests (`TestRateLimiter_ConcurrentRequests`)
* Rate limiter: Redis unavailable fail-open (`TestRateLimiter_RedisUnavailable_FailOpen`)
* Rate limiter: Wait blocking behavior (`TestRateLimiter_Wait`)
* Rate limiter: context cancellation (`TestRateLimiter_WaitContextCancelled`)
