# 📨 High-Throughput Concurrent Email Dispatcher

A high-performance, concurrent email dispatching engine written in Go. Built using Go's native CSP (Communicating Sequential Processes) concurrency primitives (Goroutines, Channels, and WaitGroups), this service reads recipient lists, durably persists jobs to **PostgreSQL**, dynamically compiles responsive HTML email templates, dispatches messages concurrently across a **3-worker pool** to **Mailpit** via SMTP, and routes failures to both PostgreSQL and a dedicated **Dead Letter Queue (DLQ)**.

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
       │ 3. Mark 'processing'                  │
       │ 4. Send SMTP via Mailpit              │
       │ 5. Mark 'sent' / 'failed' in DB       │
       └───────────────────┼───────────────────┘
                           │
            ┌──────────────┴──────────────┐
            ▼                             ▼
    [ Success Path ]              [ Failure Path ]
            │                             │
    ┌───────────────┐             ┌───────────────┐
    │  SMTP Server  │             │  Dead Letter  │
    │   (Mailpit)   │             │  Queue (DLQ)  │
    └───────────────┘             └───────┬───────┘
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
4. **Execution & Delivery**:
   - The consumer marks the job as `processing` in PostgreSQL.
   - The consumer renders the HTML template and transmits the email via SMTP to Mailpit.
   - **On Success**: The job is marked as `sent`, storing `sent_at = NOW()` and the rendered body.
   - **On Failure**: The job is marked as `failed`, incrementing `attempts = attempts + 1` and storing the error message. The job is also pushed to `dlqChan` for disk logging.

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

## 🔌 Connection Pooling Configuration

Connection pooling is initialized once in [`db.go`](file:///c:/Users/FC/Downloads/Go%20Projects/email-dispatcher/db.go) during application bootstrap using Go's standard library `database/sql`. The returned `*sql.DB` connection pool is thread-safe and shared across the producer and all 3 consumer goroutines.

| Parameter | Default | Environment Variable | Purpose |
|---|---|---|---|
| `MaxOpenConns` | `10` | `DB_MAX_OPEN_CONNS` | Limits total active connections across 3 workers + producer |
| `MaxIdleConns` | `5` | `DB_MAX_IDLE_CONNS` | Keeps idle connections ready for burst dispatches |
| `ConnMaxLifetime` | `30m` | `DB_CONN_MAX_LIFETIME` | Recycles stale connections to prevent stale socket leaks |
| `ConnMaxIdleTime` | `5m` | `DB_CONN_MAX_IDLE_TIME` | Reclaims excess idle connections during low dispatch activity |

All operations utilize `context.Context` with appropriate cancellation and timeouts.

---

## 🐳 Docker Infrastructure

The local development environment runs both **Mailpit** and **PostgreSQL** via Docker Compose.

### Starting the Infrastructure

```bash
docker compose up -d
```

Services exposed:
* **Mailpit SMTP Server**: `localhost:1025`
* **Mailpit Web UI**: [http://localhost:8025](http://localhost:8025)
* **PostgreSQL Server**: `localhost:5433` (defaults to 5433 to avoid port collisions with host PostgreSQL)

---

## 🔄 Database Migrations

Migrations are stored as raw SQL files in the [`migrations/`](file:///c:/Users/FC/Downloads/Go%20Projects/email-dispatcher/migrations) directory:
* [`001_create_email_jobs.up.sql`](file:///c:/Users/FC/Downloads/Go%20Projects/email-dispatcher/migrations/001_create_email_jobs.up.sql)
* [`001_create_email_jobs.down.sql`](file:///c:/Users/FC/Downloads/Go%20Projects/email-dispatcher/migrations/001_create_email_jobs.down.sql)

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

Configuration is loaded from environment variables with `.env` file fallback via [`config.go`](file:///c:/Users/FC/Downloads/Go%20Projects/email-dispatcher/config.go). Copy `.env.example` to `.env`:

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

---

## 🧪 Testing

Run all unit and integration tests:

```bash
go test -v ./...
```

The test suite covers:
* Job creation with UUID generation (`TestCreateJob`)
* Complete success lifecycle `pending` → `processing` → `sent` (`TestEmailJobLifecycle_Sent`)
* Failure lifecycle with error capture and attempt tracking (`TestEmailJobLifecycle_Failed`)
* Concurrency idempotency and guard conditions (`TestMarkProcessingIdempotency`, `TestMarkSentRequiresProcessing`)
* Connection pool configuration verification (`TestConnectionPoolConfig`)
* Worker error handling and DLQ emission (`TestEmailWorkerFailure`)
