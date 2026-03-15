# Secure Task System

A secure distributed job scheduling system built with FastAPI, Redis, and an async worker.

This project demonstrates:
- Producer-consumer scheduling with queue-backed background processing
- JWT-based authentication with access + refresh token lifecycle
- Security controls (rate limits, token revocation, role-based access, user-level data isolation)
- Reliability patterns (retries, timeout handling, cancellation, dead-letter queue)
- Observability (structured logs, request IDs, health checks, metrics)

## Architecture

Services:
- `api` (FastAPI): accepts jobs, authenticates users, exposes status/metrics/admin endpoints
- `worker` (Python async process): consumes queued jobs and executes task handlers
- `redis` (queue + state): stores job metadata, queue items, idempotency keys, metrics, rate-limit counters

Flow:
1. Client gets tokens from `POST /token`
2. Client submits job to `POST /jobs/` with Bearer token
3. API stores job state in Redis and pushes queue item
4. Worker pulls queue item (`BRPOP`), processes task, updates status
5. Client polls `GET /jobs/{job_id}` until terminal state

## Security Features

- Password hashing with bcrypt (`passlib`)
- Access + refresh JWTs with token type claims (`typ`), `jti`, and expiration
- Refresh-token rotation on `/token/refresh`
- Token revocation store in Redis (`/token/revoke`)
- Role-based access (`admin` endpoints for DLQ + metrics)
- User-level job ownership checks
- Rate limiting for auth and job submissions
- Idempotency keys (`Idempotency-Key`) to prevent duplicate job creation
- Docker hardening:
  - non-root user
  - dropped capabilities
  - `no-new-privileges`

## Reliability Features

- Retry on worker failures (up to `max_attempts`)
- Timeout-aware execution (`timeout_seconds`)
- Job cancellation endpoint (`POST /jobs/{job_id}/cancel`)
- Dead-letter queue for permanently failed/timed-out jobs (`/admin/dead-letter`)

## Task Types and Payload Validation

Supported task categories with per-type validation:
- `media_transcode`: requires `input_format`, `output_format`
- `thumbnail_generate`: requires `video_url`, `frame_second`
- `sleep`: requires `seconds` in range 0..120
- Any other `task_type` is accepted as generic task

## API Summary

Auth:
- `POST /token`
- `POST /token/refresh`
- `POST /token/revoke`
- `GET /users/me`

Jobs:
- `POST /jobs/` (alias: `POST /tasks/`)
- `GET /jobs/{job_id}` (alias: `GET /tasks/{job_id}`)
- `POST /jobs/{job_id}/cancel`

System:
- `GET /health/liveness`
- `GET /health/readiness`
- `GET /metrics` (admin)
- `GET /admin/dead-letter` (admin)

## Local Run

### 1) Install dependencies

```bash
pip install -r requirements.txt
```

### 2) Start API + Worker + Redis

```bash
docker compose -f app/docker-compose.yml up --build
```

### 3) Open docs

- Swagger UI: `http://localhost:8000/docs`

## Testing

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest -q
```

## Default Local Credentials

- Username: `admin`
- Password: `password123`

Override via environment variables in production-like setups:
- `ADMIN_USERNAME`
- `ADMIN_PASSWORD`
- `SECRET_KEY`

## Design Decisions

- Redis chosen for both queue and fast mutable job-state tracking
- Async worker loop to model real non-blocking background processing
- Explicit dead-letter queue to preserve failed work items for investigation
- Access/refresh token split to balance short-lived API access and re-auth UX
- Request IDs and JSON logs to support debugging in distributed runs
