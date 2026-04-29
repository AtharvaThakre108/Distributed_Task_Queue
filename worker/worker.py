import os
import sys
import signal
import logging
import time
from datetime import datetime, timezone

# Allow imports from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import redis as redis_lib
from common.job import Job, JobStatus
from common.queue import (
    get_redis, dequeue, save_job, update_job_fields,
    requeue_for_retry, send_to_dlq, get_queue_depths, get_metrics,
    METRIC_KEYS,
)
from worker.tasks import get_task, list_tasks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WORKER-%(process)d] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

WORKER_ID      = os.getenv("WORKER_ID", f"worker-{os.getpid()}")
REDIS_HOST     = os.getenv("REDIS_HOST", "redis")
REDIS_PORT     = int(os.getenv("REDIS_PORT", 6379))
BRPOP_TIMEOUT  = int(os.getenv("BRPOP_TIMEOUT", 5))   # seconds to block before loop check
STATS_INTERVAL = int(os.getenv("STATS_INTERVAL", 30))  # log stats every N seconds


class Worker:
    def __init__(self):
        self.r            = get_redis(REDIS_HOST, REDIS_PORT)
        self.running      = True
        self.jobs_done    = 0
        self.jobs_failed  = 0
        self.last_stat_ts = time.time()

        # Graceful shutdown: finish current job, then stop.
        # Without this, Docker kill -s SIGTERM mid-job corrupts job state.
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT,  self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logger.info(f"[{WORKER_ID}] Received shutdown signal. Finishing current job then exiting...")
        self.running = False

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _execute(self, job: Job) -> None:
        handler = get_task(job.task_name)
        if not handler:
            raise LookupError(f"No handler registered for task '{job.task_name}'. "
                              f"Available: {list_tasks()}")
        result = handler(*job.args, **job.kwargs)
        job.result = result

    def _process(self, job: Job) -> None:
        logger.info(f"[{WORKER_ID}] START job={job.job_id} task={job.task_name} "
                    f"retry={job.retries}/{job.max_retries}")

        job.status      = JobStatus.RUNNING
        job.started_at  = self._now()
        save_job(self.r, job)

        t0 = time.perf_counter()
        try:
            self._execute(job)
            elapsed_ms = round((time.perf_counter() - t0) * 1000)

            job.status      = JobStatus.SUCCESS
            job.finished_at = self._now()
            save_job(self.r, job)
            self.r.incr(METRIC_KEYS["processed"])
            self.jobs_done += 1

            logger.info(f"[{WORKER_ID}] DONE  job={job.job_id} task={job.task_name} "
                        f"duration_ms={elapsed_ms}")

        except Exception as e:
            elapsed_ms = round((time.perf_counter() - t0) * 1000)
            error_str  = f"{type(e).__name__}: {e}"

            if job.retries < job.max_retries:
                requeue_for_retry(self.r, job, error_str)
                logger.warning(f"[{WORKER_ID}] RETRY job={job.job_id} task={job.task_name} "
                               f"attempt={job.retries}/{job.max_retries} error={error_str}")
            else:
                send_to_dlq(self.r, job, error_str)
                self.r.incr(METRIC_KEYS["failed"])
                self.jobs_failed += 1
                logger.error(f"[{WORKER_ID}] DEAD  job={job.job_id} task={job.task_name} "
                             f"error={error_str} → DLQ")

    def _log_stats(self) -> None:
        now = time.time()
        if now - self.last_stat_ts < STATS_INTERVAL:
            return
        self.last_stat_ts = now
        depths  = get_queue_depths(self.r)
        metrics = get_metrics(self.r)
        logger.info(
            f"[{WORKER_ID}] STATS "
            f"local_done={self.jobs_done} local_failed={self.jobs_failed} "
            f"queues={depths} global={metrics}"
        )

    def run(self):
        logger.info(f"[{WORKER_ID}] Started. Registered tasks: {list_tasks()}")
        try:
            self.r.ping()
        except redis_lib.ConnectionError as e:
            logger.critical(f"Cannot connect to Redis: {e}")
            sys.exit(1)

        while self.running:
            try:
                job = dequeue(self.r, timeout=BRPOP_TIMEOUT)
                if job:
                    # Skip cancelled jobs silently
                    if job.status == JobStatus.CANCELLED:
                        logger.info(f"[{WORKER_ID}] SKIP cancelled job={job.job_id}")
                        continue
                    self._process(job)
                self._log_stats()
            except redis_lib.ConnectionError as e:
                logger.error(f"[{WORKER_ID}] Redis connection lost: {e}. Retrying in 5s...")
                time.sleep(5)
            except Exception as e:
                logger.exception(f"[{WORKER_ID}] Unexpected error: {e}")

        logger.info(f"[{WORKER_ID}] Shutdown complete. "
                    f"Processed={self.jobs_done} Failed={self.jobs_failed}")


if __name__ == "__main__":
    Worker().run()