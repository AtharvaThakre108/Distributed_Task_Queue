# Distributed Task Queue

A Redis-backed distributed task queue built from scratch — priority queues, delayed execution, exponential backoff retries, dead letter queue, and a REST API for job management. Conceptually similar to Celery, built to understand what Celery actually does under the hood.

## Architecture

```
┌─────────────┐     POST /jobs      ┌─────────────────────────────────────┐
│   Client    │ ──────────────────▶ │            FastAPI (API)             │
└─────────────┘                     └──────────────┬──────────────────────┘
                                                   │ LPUSH job_id
                                    ┌──────────────▼──────────────────────┐
                                    │              Redis                   │
                                    │                                      │
                                    │  queue:high    ──┐                   │
                                    │  queue:default ──┼──▶ BRPOP (atomic) │
                                    │  queue:low     ──┘                   │
                                    │                                      │
                                    │  delayed (Sorted Set, score=run_at)  │
                                    │  job:{uuid}  (Hash, TTL 7d)          │
                                    │  dlq         (List, failed jobs)     │
                                    └──────┬──────────────────┬────────────┘
                                           │                  │
                              ┌────────────▼───┐    ┌─────────▼──────────┐
                              │   Worker ×N    │    │     Scheduler      │
                              │                │    │                    │
                              │ BRPOP loop     │    │ ZRANGEBYSCORE poll │
                              │ Execute task   │    │ every 1s           │
                              │ Retry/DLQ      │    │ Promotes delayed   │
                              └────────────────┘    └────────────────────┘
```

**Why Redis Lists for queues:** `BRPOP` is a blocking pop — workers sleep at the OS level with zero CPU usage until a job arrives. Single-threaded Redis command execution means no two workers ever dequeue the same job without needing distributed locks.

**Why a Sorted Set for delayed jobs:** Score = Unix timestamp. `ZRANGEBYSCORE delayed 0 {now}` pulls all jobs ready to execute in O(log N). The scheduler promotes them to the main queue every second.

**Why separate worker processes (not threads):** Python GIL. CPU-bound tasks don't parallelize in threads. Process isolation also means a crashed worker doesn't take down the API server.

**Why a Dead Letter Queue:** Failed jobs after retries are preserved for inspection and replay. Deleting them silently makes production debugging impossible.

## Features

- **3-level priority queue** — `high`, `default`, `low`. BRPOP polls them in order so high-priority jobs always dequeue first.
- **Delayed execution** — submit a job with `delay_seconds`, the scheduler promotes it when ready.
- **Exponential backoff retries** — configurable `max_retries`, delay = `2^attempt` seconds (2s → 4s → 8s).
- **Dead Letter Queue** — jobs that exhaust retries land in DLQ. Inspect and replay via API.
- **Batch submission** — submit up to 100 jobs in a single POST.
- **Job cancellation** — cancel queued or delayed jobs before a worker picks them up.
- **Graceful shutdown** — workers catch SIGTERM, finish the current job, then exit cleanly. No mid-job data loss on `docker compose down`.
- **Metrics endpoint** — queue depths and global counters for every service (enqueued/processed/failed/dlq_total).
- **Task registry** — decorator-based handler registration (`@task("name")`). Add new task types without touching the dispatcher.

## Benchmark

Hardware: Windows, GTX 1660Ti, 16GB RAM, Docker Desktop  
Workload: 50 × `send_email` jobs (100ms simulated I/O latency each), 2 workers

| Workers | Jobs | Total Time | Throughput      |
|---------|------|------------|-----------------|
| 2       | 50   | ~2.5s      | ~20 jobs/sec    |
| 4       | 50   | ~1.3s      | ~38 jobs/sec    |

Throughput scales linearly with worker count for I/O-bound tasks. For CPU-bound tasks (e.g. `generate_report` at 1–3s), horizontal scaling via `docker compose up --scale worker=N` provides the same linear improvement since each worker is a separate process with its own GIL.

Redis `LPUSH` + `BRPOP` throughput ceiling (in-process benchmark, no task execution): **~80,000 ops/sec** on this hardware.

## Key Redis Schema

| Key | Type | Purpose |
|-----|------|---------|
| `queue:high/default/low` | List | Active job queues (LPUSH in, BRPOP out) |
| `delayed` | Sorted Set | Delayed jobs, score = Unix timestamp |
| `job:{uuid}` | Hash | Full job state, 7-day TTL |
| `dlq` | List | Dead jobs awaiting inspection/replay |
| `metrics:*` | String | Incrementing counters |

## API

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/jobs` | Submit a job |
| `GET` | `/jobs/{id}` | Get job status and result |
| `DELETE` | `/jobs/{id}` | Cancel a queued job |
| `POST` | `/jobs/batch` | Submit up to 100 jobs |
| `GET` | `/dlq` | Inspect dead letter queue |
| `POST` | `/dlq/{id}/replay` | Re-enqueue a dead job |
| `GET` | `/metrics` | Queue depths and counters |
| `GET` | `/tasks` | List registered task handlers |
| `GET` | `/healthz` | Redis connectivity check |

## Running

```bash
# Start all services (API + 2 workers + scheduler + Redis)
docker compose up --build

# Scale workers
docker compose up --scale worker=4

# API docs
open http://localhost:8000/docs
```

## Project Structure

```
├── api/
│   ├── main.py        # FastAPI routes
│   └── models.py      # Pydantic request/response schemas
├── worker/
│   ├── worker.py      # BRPOP loop, retry logic, graceful shutdown
│   └── tasks.py       # Task registry and handlers
├── scheduler/
│   └── scheduler.py   # Delayed job promotion loop
├── common/
│   ├── job.py         # Job model and status enum
│   └── queue.py       # All Redis operations
├── Dockerfile.api
├── Dockerfile.worker
├── Dockerfile.scheduler
└── docker-compose.yml
```

## CS Concepts Demonstrated

- **Producer-consumer pattern** with blocking I/O (`BRPOP`)
- **Priority queue** implementation using ordered Redis key polling
- **Exponential backoff** for retry jitter and thundering herd prevention
- **Dead letter queue** pattern for fault-tolerant async systems
- **Process-based concurrency** vs thread-based — GIL implications
- **Atomic dequeue** — Redis single-threaded execution as an implicit distributed lock
- **Event-driven scheduling** — sorted set as a time-indexed priority queue
- **Graceful shutdown** — SIGTERM handling in containerized processes