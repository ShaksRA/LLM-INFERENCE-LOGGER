# LLM Inference Logger

A production-grade inference logging and ingestion system for LLM applications — built as a take-home assignment demonstration.

---

> **📌 Note on Demo / Hosting**
>
> The JD listed a hosted demo link as **optional**. This project runs fully locally via Docker Compose
> and is not deployed to a public URL. This was a deliberate choice for the following reasons:
>
> - The backend requires a **persistent server and file system** (for SQLite) which is incompatible
>   with free serverless platforms like Vercel or Netlify
> - Hosting on a paid platform (Railway, Render, Fly.io) would require embedding a **live API key**
>   in a public environment, which is a security risk for a demo submission
> - The entire system spins up with a **single command** (`docker compose up --build`) and is
>   fully reviewable locally in under 5 minutes — see Quick Start below
>
> All seven bonus deliverables (multi-provider, streaming, dashboards, Docker Compose,
> event-based architecture, PII redaction, k8s manifests) are fully implemented and verifiable locally.
> Screenshots of the running application are included at the bottom of this README.

**To run the demo:** Clone this repo, add your API key to `.env`, and run `docker compose up --build`. Open http://localhost:3000.

---
## Demo Screenshots: 
<img width="2559" height="1472" alt="image" src="https://github.com/user-attachments/assets/5456b76a-cf34-49cc-9bae-5ff10166cc40" />

---

## Table of Contents

1. [What This Project Does](#what-this-project-does)
2. [Tech Stack](#tech-stack)
3. [Quick Start](#quick-start)
4. [Architecture Overview](#architecture-overview)
5. [Bonus Features — Completed](#bonus-features--completed)
6. [Schema Design](#schema-design)
7. [Ingestion Flow](#ingestion-flow)
8. [Logging Strategy](#logging-strategy)
9. [PII Redaction](#pii-redaction)
10. [Streaming Responses](#streaming-responses)
11. [Multi-Provider Support](#multi-provider-support)
12. [Dashboards](#dashboards)
13. [Kubernetes Deployment](#kubernetes-deployment)
14. [Scaling Considerations](#scaling-considerations)
15. [Failure Handling](#failure-handling)
16. [Tradeoffs Made](#tradeoffs-made)
17. [What I'd Improve With More Time](#what-id-improve-with-more-time)
18. [API Reference](#api-reference)
19. [Project Structure](#project-structure)

---

## What This Project Does

This system wraps any LLM provider API with a lightweight SDK that:

- Captures inference metadata on every request (latency, tokens, cost, timestamps, errors)
- Redacts PII before sending data to providers or storing it
- Streams responses to the user in real time via Server-Sent Events
- Sends logs to an ingestion pipeline in near real time (non-blocking)
- Stores everything in a structured database with analytics-ready schema
- Exposes a chat UI with conversation management (list, resume, cancel)
- Shows live dashboards for latency, throughput, and error rate

---

## Tech Stack

| Layer | Technology |
|---|---|
| Backend | Python 3.11, FastAPI, uvicorn |
| LLM SDK | Custom wrapper (httpx async) |
| Database | SQLite (dev) / PostgreSQL (prod) via aiosqlite |
| Frontend | Vanilla HTML/CSS/JS + Chart.js (zero build step) |
| Reverse proxy | nginx (proxies /api → backend, handles SSE) |
| Containerisation | Docker + Docker Compose |
| Orchestration | Kubernetes (self-hosted manifests included) |

---

## Quick Start

### Prerequisites

- Docker Desktop installed and running
- An API key from at least one provider (Anthropic recommended)

### One-command setup

```bash
# 1. Clone / unzip the project
cd llm-inference-logger

# 2. Configure your API key
cp .env.example .env
# Open .env and set:
# ANTHROPIC_API_KEY=sk-ant-api03-xxxxxxxx

# 3. Start everything
docker compose up --build
```

That's it. Open **http://localhost:3000** in your browser.

| Service | URL |
|---|---|
| Chat UI | http://localhost:3000 |
| API (REST) | http://localhost:8000 |
| Interactive API docs | http://localhost:8000/docs |
| Health check | http://localhost:8000/health |

### Stop

```bash
docker compose down          # stop containers
docker compose down -v       # stop + wipe database
docker compose down --rmi all  # stop + delete images (full reset)
```

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│  Browser (localhost:3000)                                        │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────────────────┐   │
│  │  Chat UI    │  │  Dashboards  │  │  Logs viewer          │   │
│  │  + Stream   │  │  Latency /   │  │  Per-request metadata │   │
│  │  + Convs    │  │  Throughput /│  │  Paginated table      │   │
│  │  mgmt       │  │  Errors      │  │                       │   │
│  └──────┬──────┘  └──────┬───────┘  └───────────┬───────────┘   │
└─────────┼────────────────┼──────────────────────┼───────────────┘
          │ HTTP / SSE     │ GET /api/metrics/*    │ GET /api/logs
          ▼                ▼                       ▼
┌──────────────────────────────────────────────────────────────────┐
│  nginx (port 80→3000)                                            │
│  • Serves static frontend                                        │
│  • Proxies /api/* → FastAPI backend (port 8000)                  │
│  • proxy_buffering off  (required for SSE streaming)             │
└──────────────────────────────┬───────────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────────┐
│  FastAPI Backend (port 8000)                                     │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  LLM SDK Wrapper  (src/sdk/wrapper.py)                  │    │
│  │                                                         │    │
│  │  • Anthropic Claude  • OpenAI GPT-4.1                   │    │
│  │  • Google Gemini     • DeepSeek                         │    │
│  │  • xAI Grok                                             │    │
│  │                                                         │    │
│  │  Per-request captures:                                  │    │
│  │  latency_ms │ tokens │ timestamps │ status │ previews   │    │
│  └──────────────────────┬──────────────────────────────────┘    │
│                         │                                        │
│  ┌──────────────────────▼──────────────────────────────────┐    │
│  │  PII Redactor  (src/pii/redactor.py)                    │    │
│  │  Runs BEFORE provider call and BEFORE storage           │    │
│  │  Strips: email, phone, SSN, credit card, API keys       │    │
│  └──────────────────────┬──────────────────────────────────┘    │
│                         │                                        │
│  ┌──────────────────────▼──────────────────────────────────┐    │
│  │  Ingestion Pipeline  (src/pipeline/ingestion.py)        │    │
│  │                                                         │    │
│  │  asyncio.create_task() ← non-blocking, fire & forget    │    │
│  │                                                         │    │
│  │  1. validate  — enforce required fields + type safety   │    │
│  │  2. enrich    — cost estimate, latency bucket, TPS      │    │
│  │  3. persist   — write to SQLite / PostgreSQL            │    │
│  └──────────────────────┬──────────────────────────────────┘    │
│                         │                                        │
│  ┌──────────────────────▼──────────────────────────────────┐    │
│  │  Database  (src/db/database.py)                         │    │
│  │                                                         │    │
│  │  conversations   messages   inference_logs              │    │
│  └─────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────┘
```

---

## Bonus Features — Completed

### ✅ Multi-provider support
Five providers wired up in `src/sdk/wrapper.py`:

| Provider | Model | API Format |
|---|---|---|
| Anthropic | claude-haiku-4-5-20251001 | Native Messages API |
| OpenAI | gpt-4.1 | OpenAI Chat Completions |
| Google | gemini-2.0-flash | Gemini GenerateContent |
| DeepSeek | deepseek-chat | OpenAI-compatible |
| xAI | grok-3 | OpenAI-compatible |

Switch providers live from the dropdown in the chat UI. Add keys for each provider in `.env`.

### ✅ Streaming Responses
Implemented via Server-Sent Events (SSE):
- Backend: `StreamingResponse` in FastAPI yielding `data: {...}\n\n` chunks
- Frontend: `ReadableStream` reader consuming chunks token by token
- nginx: `proxy_buffering off` + `chunked_transfer_encoding on` for correct SSE passthrough
- Toggle the **Stream** checkbox in the UI to enable

### ✅ Latency + Throughput + Errors Dashboards
Live charts in the Dashboards tab powered by Chart.js:
- **Latency**: avg/min/max per hour over last 24h
- **Throughput**: requests + tokens per hour
- **Error rate**: percentage of failed requests per hour
- **Provider breakdown**: per-provider request count, avg latency, error rate
- Backed by indexed SQL analytics queries in `src/db/database.py`

### ✅ Docker Compose one-command setup
```bash
docker compose up --build
```
Single command starts both services. Frontend waits for backend health check before starting. Persistent volume for the database. CORS and proxy fully configured.

### ✅ Event-based architecture
The ingestion pipeline uses `asyncio.create_task()` — a non-blocking fire-and-forget pattern:

```python
# User gets their response immediately
# Log is shipped to pipeline independently
asyncio.create_task(self.pipeline.process(log))
```

This is the same contract as publishing to a Kafka topic. Swapping to a real event bus requires only changing `pipeline.process()` to publish to a topic — the validation/enrichment/persistence stages remain identical consumers.

The pipeline also exposes a queue-based worker (`start_worker()`) for batch consumption patterns.

### ✅ PII Redaction
Runs in `src/pii/redactor.py` at two points:
1. **Before the LLM call** — user message is redacted before leaving your infrastructure
2. **Before storage** — `content_pii_redacted` flag stored per message for audit

Patterns covered by regex:
- Email addresses → `[EMAIL]`
- US/international phone numbers → `[PHONE]`
- Credit card numbers (Visa, MC, Amex, Discover) → `[CREDIT_CARD]`
- US Social Security Numbers → `[SSN]`
- IPv4 addresses → `[IP_ADDRESS]`
- API keys (Bearer tokens, sk- prefixed keys) → `[API_KEY]`

### ✅ Self-hosted Kubernetes
Full manifests in `k8s/manifests.yaml`:
- Namespace isolation (`llm-logger`)
- Deployments for API (2 replicas) and frontend (2 replicas)
- `HorizontalPodAutoscaler` — scales API from 2 to 10 replicas at 70% CPU
- `PersistentVolumeClaim` — 10Gi for SQLite data
- `Ingress` with nginx annotations for SSE timeout handling
- `ConfigMap` + `Secret` for configuration separation
- Resource requests and limits on every container
- Liveness and readiness probes

---

## Schema Design

Three tables, intentionally normalized:

```sql
-- Anchor table — cheap to list/paginate without touching messages
conversations (
    session_id    TEXT PRIMARY KEY,
    title         TEXT,
    provider      TEXT,
    model         TEXT,
    status        TEXT,          -- active | cancelled
    created_at    TEXT,
    updated_at    TEXT,
    message_count INTEGER,
    total_tokens  INTEGER
)

-- Full message content stored separately
-- Allows PII auditing without touching the log table
messages (
    id                   TEXT PRIMARY KEY,
    session_id           TEXT REFERENCES conversations,
    role                 TEXT,   -- user | assistant | system
    content              TEXT,
    content_pii_redacted INTEGER,
    created_at           TEXT,
    token_count          INTEGER
)

-- One row per LLM API call — analytics-optimised
inference_logs (
    id                TEXT PRIMARY KEY,
    session_id        TEXT REFERENCES conversations,
    request_id        TEXT UNIQUE,   -- idempotency key
    provider          TEXT,
    model             TEXT,
    latency_ms        REAL,
    prompt_tokens     INTEGER,
    completion_tokens INTEGER,
    total_tokens      INTEGER,
    timestamp_start   TEXT,
    timestamp_end     TEXT,
    status            TEXT,          -- success | error | timeout
    error             TEXT,
    input_preview     TEXT,
    output_preview    TEXT,
    metadata          TEXT           -- JSON blob, extensible
)

-- Indexes for dashboard query performance
CREATE INDEX idx_logs_timestamp ON inference_logs(timestamp_start);
CREATE INDEX idx_logs_provider  ON inference_logs(provider);
CREATE INDEX idx_logs_status    ON inference_logs(status);
```

**Key decisions:**
- `request_id UNIQUE` makes the ingestion endpoint idempotent — safe for at-least-once delivery
- `metadata TEXT` JSON column means new fields never require migrations
- `conversations` counter columns (`message_count`, `total_tokens`) allow O(1) list queries without scanning `messages`
- SQLite WAL mode enabled for better concurrent read performance
- Same SQL runs on PostgreSQL — swap `aiosqlite` for `asyncpg` in ~20 lines

---

## Ingestion Flow

```
User sends message
       │
       ▼
FastAPI /api/chat receives request
       │
       ▼
PII Redactor scrubs message
       │
       ▼
LLM SDK calls provider API (Anthropic / OpenAI / etc.)
       │
       ├── On response ──────────────────────────────────────────┐
       │                                                         │
       ▼                                                         ▼
Return response to user                           asyncio.create_task(pipeline.process(log))
  (user is not blocked)                                          │
                                                                 ▼
                                                    1. Validate (required fields, type coercion)
                                                                 │
                                                                 ▼
                                                    2. Enrich
                                                       • tokens_per_second
                                                       • latency_bucket (<500ms, 500ms-1s, etc.)
                                                       • estimated_cost_usd
                                                       • ingested_at timestamp
                                                                 │
                                                                 ▼
                                                    3. Persist to SQLite
                                                       inference_logs table
```

---

## Logging Strategy

Every LLM call captures:

| Field | How captured |
|---|---|
| `latency_ms` | `time.monotonic()` diff around the HTTP call |
| `prompt_tokens` | From provider's usage field in response |
| `completion_tokens` | From provider's usage field in response |
| `timestamp_start` | `datetime.utcnow()` before call |
| `timestamp_end` | `datetime.utcnow()` after call |
| `status` | `success` / `error` / `timeout` |
| `error` | Full exception message if failed |
| `input_preview` | First 200 chars of user message (post-PII-redaction) |
| `output_preview` | First 200 chars of response |
| `session_id` | UUID per conversation, persistent across turns |
| `request_id` | UUID per LLM call, used as idempotency key |
| `provider` + `model` | Explicit from wrapper configuration |

Enrichment adds: `tokens_per_second`, `latency_bucket`, `estimated_cost_usd`.

---

## Scaling Considerations

**Horizontal scaling:** The API is stateless (all state in DB). Add replicas freely. The k8s HPA handles this automatically.

**Write throughput:** At high volume (>1k RPS), replace `asyncio.create_task` with a Kafka publish. A dedicated consumer group handles persistence in batches, enabling back-pressure and replay.

**Read path:** Dashboard queries use indexed columns. At large scale, add a PostgreSQL read replica and route all `GET /api/metrics/*` queries there.

**Streaming:** Each SSE connection holds an open HTTP connection. Use async uvicorn workers (already configured) and set connection limits per pod. The nginx `proxy_read_timeout 300s` handles long-running streams.

**PII:** Regex redaction is CPU-bound. At >10k RPS, extract into a sidecar service or use a compiled regex engine.

---

## Failure Handling

| Failure | Current behaviour | Production improvement |
|---|---|---|
| Provider API down | Error captured in log, returned to user | Retry with exponential backoff + circuit breaker |
| Log ingestion fails | Silently dropped (fire-and-forget) | Dead-letter queue, alerting |
| Database write fails | Exception logged | Retry queue, WAL replay |
| 529 Overloaded (rate limit) | Error returned immediately | Automatic retry with jitter |
| Process crash mid-request | In-flight log lost | Publish to durable queue before processing |
| PII redaction failure | Unredacted text stored | Fail closed — reject request, alert |

---

## Tradeoffs Made

| Decision | Tradeoff |
|---|---|
| SQLite over PostgreSQL | Zero-dependency local setup; swap 20 lines for prod |
| `asyncio.create_task` over Kafka | No infrastructure dependency; loses durability on crash |
| Regex PII redaction | Fast, no ML needed; misses contextual PII ("my name is John") |
| 10-turn context window | Keeps token costs predictable; configurable constant |
| SSE over WebSockets | Simpler for unidirectional server→client streaming |
| Single-file frontend | No build step, no npm, no Node version issues |
| Vanilla JS frontend | Zero dependencies; swap to React without touching backend |
| SQLite WAL mode | Better concurrent reads without switching DB engines |

---

## What I'd Improve With More Time

1. **Kafka/SQS event bus** — Replace `asyncio.create_task` with a durable topic for guaranteed delivery, replay, and multi-consumer fan-out (metrics + alerting + audit simultaneously)
2. **PostgreSQL** — Proper `PERCENTILE_CONT`, partitioned log table by day, connection pooling via PgBouncer
3. **ML-based PII detection** — Use spaCy or Microsoft Presidio for contextual entity recognition (names, addresses, account numbers)
4. **Authentication** — JWT + per-user conversation isolation; the DB schema already supports adding `user_id`
5. **Actual cost tracking** — Wire up provider billing APIs for real vs estimated cost reconciliation
6. **Alerting** — Configurable thresholds (p95 > 5s, error rate > 5%) with Slack/webhook delivery
7. **OpenTelemetry** — Emit traces and metrics in OTEL format for Grafana/Datadog/Honeycomb integration
8. **Log replay** — Reprocess historical logs through an updated enrichment pipeline without data loss
9. **Provider fallback** — Automatically retry on a secondary provider when the primary returns 529/503
10. **Rate limit handling** — Per-provider rate limiter with token bucket algorithm and backpressure to the client

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/chat` | Send message. Body: `{content, session_id?, provider, stream?}` |
| `GET` | `/api/conversations` | List all active conversations |
| `GET` | `/api/conversations/:id` | Get conversation with full message history |
| `DELETE` | `/api/conversations/:id` | Cancel a conversation |
| `POST` | `/api/ingest/log` | Direct log ingestion (used by SDK internally) |
| `GET` | `/api/metrics/summary` | 24h summary: requests, latency, errors, tokens |
| `GET` | `/api/metrics/latency?hours=24` | Avg/min/max latency per hour |
| `GET` | `/api/metrics/throughput?hours=24` | Requests + tokens per hour |
| `GET` | `/api/metrics/errors?hours=24` | Error rate per hour |
| `GET` | `/api/metrics/providers` | Per-provider breakdown |
| `GET` | `/api/logs?limit=50&offset=0` | Paginated raw inference logs |
| `GET` | `/health` | Health check |

---

## Project Structure

```
llm-inference-logger/
├── docker-compose.yml              # One-command startup
├── .env.example                    # API key template
├── README.md
│
├── backend/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
│       ├── main.py                 # FastAPI app, all routes
│       ├── sdk/
│       │   └── wrapper.py          # Multi-provider LLM wrapper + streaming
│       ├── pipeline/
│       │   └── ingestion.py        # Validate → enrich → persist pipeline
│       ├── db/
│       │   └── database.py         # Schema, queries, analytics
│       └── pii/
│           └── redactor.py         # PII regex redaction
│
├── frontend/
│   ├── Dockerfile                  # nginx serving static HTML
│   ├── nginx.conf                  # SSE proxy config
│   └── index.html                  # Full UI — chat, dashboards, logs
│
└── k8s/
    └── manifests.yaml              # Namespace, Deployments, HPA, Ingress, PVC
```

---

## Environment Variables

```bash
# Required — at least one provider key needed
ANTHROPIC_API_KEY=sk-ant-api03-...
OPENAI_API_KEY=sk-...
GEMINI_API_KEY=...
DEEPSEEK_API_KEY=...
XAI_API_KEY=...

# Optional
DB_PATH=/data/inference_logger.db   # default path inside container
```

---

## Running Tests (manual)

```bash
# Health check
curl http://localhost:8000/health

# Send a chat message
curl -X POST http://localhost:8000/api/chat \
  -H "Content-Type: application/json" \
  -d '{"content": "Hello!", "provider": "anthropic"}'

# List conversations
curl http://localhost:8000/api/conversations

# View logs
curl http://localhost:8000/api/logs

# Dashboard metrics
curl http://localhost:8000/api/metrics/summary
```
