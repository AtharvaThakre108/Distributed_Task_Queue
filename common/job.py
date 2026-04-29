import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

class JobStatus(str, Enum):
    PENDING   = "pending"
    QUEUED    = "queued"
    RUNNING   = "running"
    SUCCESS   = "success"
    FAILED    = "failed"
    RETRYING  = "retrying"
    CANCELLED = "cancelled"
    DEAD      = "dead"       # exhausted retries → DLQ

class Priority(str, Enum):
    HIGH    = "high"
    DEFAULT = "default"
    LOW     = "low"

class Job:
    def __init__(
        self,
        task_name: str,
        args: list[Any]        = None,
        kwargs: dict[str, Any] = None,
        priority: Priority     = Priority.DEFAULT,
        max_retries: int       = 3,
        run_at: float | None   = None,   # unix timestamp, None = now
        job_id: str            = None,
    ):
        self.job_id      = job_id or str(uuid.uuid4())
        self.task_name   = task_name
        self.args        = args   or []
        self.kwargs      = kwargs or {}
        self.priority    = priority
        self.max_retries = max_retries
        self.retries     = 0
        self.status      = JobStatus.PENDING
        self.run_at      = run_at
        self.error       = None
        self.result      = None
        self.created_at  = datetime.now(timezone.utc).isoformat()
        self.started_at  = None
        self.finished_at = None

    def to_dict(self) -> dict:
        return {
            "job_id":      self.job_id,
            "task_name":   self.task_name,
            "args":        self.args,
            "kwargs":      self.kwargs,
            "priority":    self.priority,
            "max_retries": self.max_retries,
            "retries":     self.retries,
            "status":      self.status,
            "run_at":      self.run_at,
            "error":       self.error,
            "result":      self.result,
            "created_at":  self.created_at,
            "started_at":  self.started_at,
            "finished_at": self.finished_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Job":
        job = cls(
            task_name   = data["task_name"],
            args        = data.get("args", []),
            kwargs      = data.get("kwargs", {}),
            priority    = Priority(data.get("priority", Priority.DEFAULT)),
            max_retries = data.get("max_retries", 3),
            run_at      = data.get("run_at"),
            job_id      = data["job_id"],
        )
        job.retries     = data.get("retries", 0)
        job.status      = JobStatus(data.get("status", JobStatus.PENDING))
        job.error       = data.get("error")
        job.result      = data.get("result")
        job.created_at  = data.get("created_at")
        job.started_at  = data.get("started_at")
        job.finished_at = data.get("finished_at")
        return job