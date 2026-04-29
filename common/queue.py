import json
import time
import redis
from common.job import Job, JobStatus, Priority

QUEUE_KEYS = {
    Priority.HIGH:    "queue:high",
    Priority.DEFAULT: "queue:default",
    Priority.LOW:     "queue:low",
}
DELAYED_KEY   = "delayed"
DLQ_KEY       = "dlq"
JOB_KEY       = lambda job_id: f"job:{job_id}"
METRIC_KEYS   = {
    "enqueued":  "metrics:enqueued",
    "processed": "metrics:processed",
    "failed":    "metrics:failed",
    "dlq_total": "metrics:dlq_total",
}

def get_redis(host: str = "redis", port: int = 6379, db: int = 0) -> redis.Redis:
    return redis.Redis(host=host, port=port, db=db, decode_responses=True)


# ── Job state ─────────────────────────────────────────────────────────────────

def save_job(r: redis.Redis, job: Job) -> None:
    r.hset(JOB_KEY(job.job_id), mapping={k: json.dumps(v) for k, v in job.to_dict().items()})
    r.expire(JOB_KEY(job.job_id), 86400 * 7)  # TTL: 7 days

def load_job(r: redis.Redis, job_id: str) -> Job | None:
    raw = r.hgetall(JOB_KEY(job_id))
    if not raw:
        return None
    return Job.from_dict({k: json.loads(v) for k, v in raw.items()})

def update_job_fields(r: redis.Redis, job_id: str, **fields) -> None:
    r.hset(JOB_KEY(job_id), mapping={k: json.dumps(v) for k, v in fields.items()})


# ── Enqueue ───────────────────────────────────────────────────────────────────

def enqueue(r: redis.Redis, job: Job) -> str:
    """Push a job to the appropriate queue. Handles delayed jobs via sorted set."""
    job.status = JobStatus.QUEUED
    save_job(r, job)

    if job.run_at and job.run_at > time.time():
        # Delayed: goes into sorted set, scheduler promotes it when ready
        r.zadd(DELAYED_KEY, {job.job_id: job.run_at})
    else:
        queue_key = QUEUE_KEYS[Priority(job.priority)]
        r.lpush(queue_key, job.job_id)

    r.incr(METRIC_KEYS["enqueued"])
    return job.job_id


# ── Dequeue ───────────────────────────────────────────────────────────────────

def dequeue(r: redis.Redis, timeout: int = 5) -> Job | None:
    """
    Blocking pop from queues in priority order.
    BRPOP accepts multiple keys — Redis pops from the FIRST non-empty one.
    This gives us priority queuing for free without polling.
    """
    result = r.brpop(
        [QUEUE_KEYS[Priority.HIGH], QUEUE_KEYS[Priority.DEFAULT], QUEUE_KEYS[Priority.LOW]],
        timeout=timeout,
    )
    if not result:
        return None
    _, job_id = result
    return load_job(r, job_id)


# ── Retry / DLQ ───────────────────────────────────────────────────────────────

def requeue_for_retry(r: redis.Redis, job: Job, error: str, backoff_base: int = 2) -> None:
    """Exponential backoff retry. Job sits in delayed set until ready."""
    job.retries += 1
    job.error   = error
    job.status  = JobStatus.RETRYING

    delay   = backoff_base ** job.retries          # 2, 4, 8 seconds
    run_at  = time.time() + delay
    job.run_at = run_at

    save_job(r, job)
    r.zadd(DELAYED_KEY, {job.job_id: run_at})

def send_to_dlq(r: redis.Redis, job: Job, error: str) -> None:
    """Job exhausted retries. Move to DLQ for manual inspection/replay."""
    job.status      = JobStatus.DEAD
    job.error       = error
    job.finished_at = _now()
    save_job(r, job)
    r.lpush(DLQ_KEY, job.job_id)
    r.incr(METRIC_KEYS["dlq_total"])


# ── Scheduler support ─────────────────────────────────────────────────────────

def promote_delayed_jobs(r: redis.Redis) -> int:
    """Pull all delayed jobs whose run_at <= now and push them to their queue."""
    now      = time.time()
    job_ids  = r.zrangebyscore(DELAYED_KEY, 0, now)
    promoted = 0
    for job_id in job_ids:
        job = load_job(r, job_id)
        if not job:
            r.zrem(DELAYED_KEY, job_id)
            continue
        r.zrem(DELAYED_KEY, job_id)
        queue_key   = QUEUE_KEYS[Priority(job.priority)]
        job.status  = JobStatus.QUEUED
        job.run_at  = None
        save_job(r, job)
        r.lpush(queue_key, job_id)
        promoted += 1
    return promoted


# ── Metrics & inspection ──────────────────────────────────────────────────────

def get_queue_depths(r: redis.Redis) -> dict:
    return {
        "high":    r.llen(QUEUE_KEYS[Priority.HIGH]),
        "default": r.llen(QUEUE_KEYS[Priority.DEFAULT]),
        "low":     r.llen(QUEUE_KEYS[Priority.LOW]),
        "delayed": r.zcard(DELAYED_KEY),
        "dlq":     r.llen(DLQ_KEY),
    }

def get_metrics(r: redis.Redis) -> dict:
    return {k: int(r.get(v) or 0) for k, v in METRIC_KEYS.items()}

def get_dlq_jobs(r: redis.Redis, limit: int = 50) -> list[dict]:
    job_ids = r.lrange(DLQ_KEY, 0, limit - 1)
    jobs = []
    for job_id in job_ids:
        job = load_job(r, job_id)
        if job:
            jobs.append(job.to_dict())
    return jobs

def replay_dlq_job(r: redis.Redis, job_id: str) -> bool:
    """Re-enqueue a DLQ job. Resets retry count."""
    job = load_job(r, job_id)
    if not job or job.status != JobStatus.DEAD:
        return False
    r.lrem(DLQ_KEY, 1, job_id)
    job.retries  = 0
    job.error    = None
    job.status   = JobStatus.PENDING
    job.run_at   = None
    enqueue(r, job)
    return True

def cancel_job(r: redis.Redis, job_id: str) -> bool:
    job = load_job(r, job_id)
    if not job or job.status in (JobStatus.RUNNING, JobStatus.SUCCESS, JobStatus.DEAD):
        return False
    job.status = JobStatus.CANCELLED
    save_job(r, job)
    return True


# ── Internal ──────────────────────────────────────────────────────────────────

def _now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()