import time
import random
import logging
from typing import Callable

logger = logging.getLogger(__name__)

# ── Registry ──────────────────────────────────────────────────────────────────

_REGISTRY: dict[str, Callable] = {}

def task(name: str = None):
    """Decorator to register a function as a named task."""
    def decorator(fn: Callable) -> Callable:
        task_name = name or fn.__name__
        _REGISTRY[task_name] = fn
        logger.info(f"Registered task: {task_name}")
        return fn
    return decorator

def get_task(name: str) -> Callable | None:
    return _REGISTRY.get(name)

def list_tasks() -> list[str]:
    return list(_REGISTRY.keys())


# ── Sample Tasks ──────────────────────────────────────────────────────────────
# These simulate real workloads. Replace with actual business logic.

@task("send_email")
def send_email(to: str, subject: str, body: str = "") -> dict:
    """Simulates sending an email. ~100ms latency."""
    time.sleep(0.1)
    logger.info(f"Email sent to {to}: {subject}")
    return {"status": "sent", "to": to, "subject": subject}


@task("resize_image")
def resize_image(image_path: str, width: int, height: int) -> dict:
    """Simulates CPU-bound image processing. 200-500ms."""
    duration = random.uniform(0.2, 0.5)
    time.sleep(duration)
    return {"status": "resized", "path": image_path, "dimensions": f"{width}x{height}"}


@task("process_payment")
def process_payment(amount: float, currency: str = "INR", user_id: str = None) -> dict:
    """Simulates payment processing. 5% random failure rate for testing retries."""
    time.sleep(0.15)
    if random.random() < 0.05:
        raise ValueError(f"Payment gateway timeout for user {user_id}")
    return {"status": "processed", "amount": amount, "currency": currency}


@task("generate_report")
def generate_report(report_type: str, date_range: dict = None) -> dict:
    """Simulates a slow report generation task. 1-3 seconds."""
    duration = random.uniform(1.0, 3.0)
    time.sleep(duration)
    return {
        "status":      "generated",
        "report_type": report_type,
        "rows":        random.randint(100, 10000),
        "duration_ms": round(duration * 1000),
    }


@task("always_fails")
def always_fails(*args, **kwargs) -> None:
    """Used to test DLQ behavior — always raises."""
    raise RuntimeError("This task always fails. Used for DLQ testing.")


@task("flaky_task")
def flaky_task(fail_rate: float = 0.5, *args, **kwargs) -> dict:
    """Fails at given rate. Used to test retry/backoff behavior."""
    if random.random() < fail_rate:
        raise RuntimeError(f"Flaky failure (rate={fail_rate})")
    return {"status": "ok", "message": "Survived the flakiness"}