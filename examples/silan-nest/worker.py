"""
BullMQ worker for the silan-nest translation pipeline.

Connects to the same Redis instance as the NestJS backend and listens to
the ``novel-translation-queue`` queue for ``translate-novel`` jobs.

Job payload:
    {
        "novelId": "uuid-string",
        "targetLang": "en"
    }

Environment variables:
    REDIS_HOST      – Redis host           (default: localhost)
    REDIS_PORT      – Redis port           (default: 6379)
    REDIS_PASSWORD  – Redis password       (default: empty)
    DATABASE_URL    – PostgreSQL DSN        (required)
    SOURCE_LANG     – Source language code  (default: en)
    OUTPUT_DIR      – Artefact directory    (default: outputs/novel_jobs)

Usage:
    python worker.py
"""

import os
import sys
import signal
import asyncio
import logging

# Ensure the project root is on the Python path so top-level packages
# (translator, providers, configs) are importable.
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
# Also add the current directory for local imports (postgres_client, silan-nest parser)
_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if _CURRENT_DIR not in sys.path:
    sys.path.insert(0, _CURRENT_DIR)

from bullmq import Worker

from postgres_client import PostgresClient
from silan_nest import SilanNest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
# Suppress noisy HTTP request logs from the googletrans httpx client
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

logger = logging.getLogger("silan_nest_worker")

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
SOURCE_LANG = os.environ.get("SOURCE_LANG", "en")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "outputs/novel_jobs")

QUEUE_NAME = "novel-translation"
JOB_NAME = "process-novel-translation"


# ---------------------------------------------------------------------------
# Job processor
# ---------------------------------------------------------------------------

async def process_job(job, token):
    """Process a single ``translate-novel`` job from the BullMQ queue.

    Parameters
    ----------
    job : bullmq.Job
        The BullMQ job instance.  ``job.data`` contains:
        - ``novelId`` (str): UUID of the novel to translate.
        - ``targetLang`` (str): Target language code (e.g. ``"en"``, ``"id"``).
    token : str
        The BullMQ worker token (unused but required by the callback signature).
    """
    data = job.data
    novel_id = data.get("novelId")
    target_lang = data.get("targetLang")

    if not novel_id or not target_lang:
        error_msg = f"Invalid job payload – missing novelId or targetLang: {data}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Job ID follows the NestJS convention: translate-<novelId>
    job_id = f"translate-{novel_id}"

    logger.info(
        "Received job %s – novel=%s target_lang=%s",
        job_id,
        novel_id,
        target_lang,
    )

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is not set")

    db_client = PostgresClient(dsn=DATABASE_URL)

    try:
        await job.updateProgress(5)

        parser = SilanNest(
            job_id=job_id,
            novel_id=novel_id,
            target_lang=target_lang,
            db_client=db_client,
            source_lang=SOURCE_LANG,
            output_dir=OUTPUT_DIR,
        )

        await job.updateProgress(10)

        # read → convert → translate (save) → persist
        parser.run()

        await job.updateProgress(100)
        logger.info("Job %s completed successfully.", job_id)

    except Exception:
        logger.exception("Job %s failed.", job_id)
        raise


# ---------------------------------------------------------------------------
# Worker entry-point
# ---------------------------------------------------------------------------

async def main() -> None:
    """Start the BullMQ worker and block until interrupted."""
    redis_opts = {
        "host": REDIS_HOST,
        "port": REDIS_PORT,
    }
    if REDIS_PASSWORD:
        redis_opts["password"] = REDIS_PASSWORD

    logger.info(
        "Starting worker on queue=%s  redis=%s:%s",
        QUEUE_NAME,
        REDIS_HOST,
        REDIS_PORT,
    )

    worker = Worker(
        QUEUE_NAME,
        process_job,
        {"connection": redis_opts},
    )

    # Graceful shutdown on SIGINT / SIGTERM
    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("Shutdown signal received – closing worker …")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    logger.info("Worker is ready. Waiting for jobs …")

    # Block until a shutdown signal is received
    await stop_event.wait()

    await worker.close()
    logger.info("Worker shut down cleanly.")


if __name__ == "__main__":
    asyncio.run(main())

