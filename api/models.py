from typing import Any
from pydantic import BaseModel, Field
from common.job import Priority


class EnqueueRequest(BaseModel):
    task_name:      str            = Field(..., example="send_email")
    args:           list[Any]      = Field(default_factory=list)
    kwargs:         dict[str, Any] = Field(default_factory=dict)
    priority:       Priority       = Priority.DEFAULT
    max_retries:    int            = Field(default=3, ge=0, le=10)
    delay_seconds:  float          = Field(default=0.0, ge=0, description="Seconds before execution")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "task_name": "send_email",
                    "kwargs": {"to": "user@example.com", "subject": "Welcome"},
                    "priority": "high",
                    "max_retries": 3,
                    "delay_seconds": 0,
                },
                {
                    "task_name": "generate_report",
                    "kwargs": {"report_type": "revenue", "date_range": {"from": "2024-01-01", "to": "2024-03-31"}},
                    "priority": "low",
                    "delay_seconds": 30,
                },
            ]
        }
    }


class EnqueueResponse(BaseModel):
    job_id:  str
    status:  str
    queue:   str
    eta:     float | None = None


class BatchEnqueueResponse(BaseModel):
    submitted: int
    jobs:      list[dict[str, str]]


class CancelResponse(BaseModel):
    job_id:  str
    status:  str


class ReplayResponse(BaseModel):
    job_id:  str
    status:  str


class HealthResponse(BaseModel):
    status:  str
    redis:   str


class MetricsResponse(BaseModel):
    queue_depths: dict[str, int]
    counters:     dict[str, int]


class TaskListResponse(BaseModel):
    tasks: list[str]


class DLQResponse(BaseModel):
    jobs:  list[dict]
    total: int