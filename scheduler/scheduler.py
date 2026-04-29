import os
import sys
import signal
import logging
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.queue import get_redis, promote_delayed_jobs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SCHEDULER] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

REDIS_HOST     = os.getenv("REDIS_HOST", "redis")
REDIS_PORT     = int(os.getenv("REDIS_PORT", 6379))
POLL_INTERVAL  = float(os.getenv("SCHEDULER_POLL_INTERVAL", 1.0))   # seconds


class Scheduler:
    def __init__(self):
        self.r       = get_redis(REDIS_HOST, REDIS_PORT)
        self.running = True
        signal.signal(signal.SIGTERM, self._stop)
        signal.signal(signal.SIGINT,  self._stop)

    def _stop(self, *_):
        logger.info("Scheduler shutting down...")
        self.running = False

    def run(self):
        logger.info(f"Scheduler started. Poll interval: {POLL_INTERVAL}s")
        while self.running:
            try:
                promoted = promote_delayed_jobs(self.r)
                if promoted:
                    logger.info(f"Promoted {promoted} delayed job(s) to queue")
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
            time.sleep(POLL_INTERVAL)
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    Scheduler().run()