import os
import sys
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.job import Job, JobStatus, Priority
from common.queue import (
    get_redis, enqueue, load_job, get_queue_depths,
    get_metrics, get_dlq_jobs, replay_dlq_job, cancel_job,
)
from api.models import (
    EnqueueRequest, EnqueueResponse, BatchEnqueueResponse,
    CancelResponse, ReplayResponse, HealthResponse,
    MetricsResponse, TaskListResponse, DLQResponse,
)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

r = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global r
    r = get_redis(REDIS_HOST, REDIS_PORT)
    r.ping()
    yield

app = FastAPI(
    title="Distributed Task Queue",
    description="Redis-backed task queue with priority, delayed execution, retries, and DLQ.",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Jobs ──────────────────────────────────────────────────────────────────────

@app.post("/jobs", response_model=EnqueueResponse, status_code=202)
def submit_job(req: EnqueueRequest):
    run_at = (time.time() + req.delay_seconds) if req.delay_seconds > 0 else None
    job = Job(
        task_name   = req.task_name,
        args        = req.args,
        kwargs      = req.kwargs,
        priority    = req.priority,
        max_retries = req.max_retries,
        run_at      = run_at,
    )
    job_id = enqueue(r, job)
    return EnqueueResponse(
        job_id = job_id,
        status = "queued" if not run_at else "delayed",
        queue  = f"queue:{req.priority}" if not run_at else "delayed",
        eta    = run_at,
    )


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = load_job(r, job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return job.to_dict()


@app.delete("/jobs/{job_id}", response_model=CancelResponse)
def cancel(job_id: str):
    ok = cancel_job(r, job_id)
    if not ok:
        raise HTTPException(
            status_code=409,
            detail="Cannot cancel: job is already running, succeeded, or dead",
        )
    return CancelResponse(job_id=job_id, status="cancelled")


@app.post("/jobs/batch", response_model=BatchEnqueueResponse, status_code=202)
def submit_batch(jobs: list[EnqueueRequest]):
    if len(jobs) > 100:
        raise HTTPException(status_code=400, detail="Max 100 jobs per batch")
    results = []
    for req in jobs:
        run_at = (time.time() + req.delay_seconds) if req.delay_seconds > 0 else None
        job = Job(
            task_name   = req.task_name,
            args        = req.args,
            kwargs      = req.kwargs,
            priority    = req.priority,
            max_retries = req.max_retries,
            run_at      = run_at,
        )
        results.append({"job_id": enqueue(r, job), "task_name": req.task_name})
    return BatchEnqueueResponse(submitted=len(results), jobs=results)


# ── DLQ ───────────────────────────────────────────────────────────────────────

@app.get("/dlq", response_model=DLQResponse)
def inspect_dlq(limit: int = Query(default=20, ge=1, le=100)):
    return DLQResponse(jobs=get_dlq_jobs(r, limit), total=r.llen("dlq"))


@app.post("/dlq/{job_id}/replay", response_model=ReplayResponse)
def replay_job(job_id: str):
    ok = replay_dlq_job(r, job_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Job not found in DLQ or not in dead state")
    return ReplayResponse(job_id=job_id, status="re-enqueued")


# ── Observability ─────────────────────────────────────────────────────────────

@app.get("/metrics", response_model=MetricsResponse)
def metrics():
    return MetricsResponse(
        queue_depths=get_queue_depths(r),
        counters=get_metrics(r),
    )


@app.get("/healthz", response_model=HealthResponse)
def health():
    try:
        r.ping()
        return HealthResponse(status="ok", redis="connected")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Redis unreachable: {e}")


@app.get("/tasks", response_model=TaskListResponse)
def list_registered_tasks():
    from worker.tasks import list_tasks
    return TaskListResponse(tasks=list_tasks())