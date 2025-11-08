import os
import json
import time
import math
import logging
import subprocess
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("QUEUECTL_REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("QUEUECTL_REDIS_PORT", "6379"))
REDIS_DECODE = os.getenv("QUEUECTL_REDIS_DECODE_RESPONSES", "true").lower() in ("1","true","yes")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=REDIS_DECODE)

QUEUE = os.getenv("QUEUECTL_QUEUE_KEY", "jobs")
PROCESSING = os.getenv("QUEUECTL_PROCESSING_KEY", "jobs:processing")
COMPLETED = os.getenv("QUEUECTL_COMPLETED_KEY", "jobs:completed")
FAILED = os.getenv("QUEUECTL_FAILED_KEY", "jobs:failed")
DEAD = os.getenv("QUEUECTL_DEAD_KEY", "jobs:dlq")
DELAYED = os.getenv("QUEUECTL_DELAYED_KEY", "jobs:delayed")
CONFIG_HASH = os.getenv("QUEUECTL_CONFIG_HASH", "queuectl:config")

MAX_BACKOFF_SECONDS = int(os.getenv("QUEUECTL_MAX_BACKOFF_SECONDS", "3600"))

def utcnow_iso_z():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def get_max_retries():
    v = r.hget(CONFIG_HASH, "max-retries")
    try:
        return int(v) if v is not None else int(os.getenv("QUEUECTL_DEFAULT_MAX_RETRIES", "3"))
    except Exception:
        return int(os.getenv("QUEUECTL_DEFAULT_MAX_RETRIES", "3"))

def get_backoff_base():
    v = r.hget(CONFIG_HASH, "backoff_base")
    try:
        return int(v) if v is not None else int(os.getenv("QUEUECTL_DEFAULT_BACKOFF_BASE", "2"))
    except Exception:
        return int(os.getenv("QUEUECTL_DEFAULT_BACKOFF_BASE", "2"))

def execute(job):
    try:
        logger.info("Executing job %s cmd=%s", job.get("id"), job.get("command"))
        subprocess.run(job["command"], shell=True, check=True, timeout=120)
        logger.info("Job done %s", job.get("id"))
        return True
    except Exception as e:
        logger.exception("Job failed %s: %s", job.get("id"), e)
        return False

def mark_processing_replace(raw, job):
    try:
        r.lrem(PROCESSING, 1, raw)
        r.lpush(PROCESSING, json.dumps(job))
    except Exception:
        logger.exception("Failed to replace processing entry for %s", job.get("id"))

def mark_completed(job):
    job["state"] = "completed"
    job["updated_at"] = utcnow_iso_z()
    r.lpush(COMPLETED, json.dumps(job))
    logger.info("Marked completed %s", job.get("id"))

def record_failed(job):
    job["state"] = "failed"
    job["updated_at"] = utcnow_iso_z()
    r.lpush(FAILED, json.dumps(job))
    logger.info("Recorded failed %s attempts=%s", job.get("id"), job.get("attempts"))

def push_to_dead(job):
    job["state"] = "dead"
    job["updated_at"] = utcnow_iso_z()
    r.lpush(DEAD, json.dumps(job))
    logger.info("Pushed to DLQ %s", job.get("id"))

def schedule_delayed(job, delay_seconds):
    if delay_seconds > MAX_BACKOFF_SECONDS:
        delay_seconds = MAX_BACKOFF_SECONDS
    available_at = time.time() + delay_seconds
    job["state"] = "delayed"
    job["updated_at"] = utcnow_iso_z()
    member = json.dumps(job)
    r.zadd(DELAYED, {member: available_at})
    logger.info("Scheduled delayed job %s for %s sec (at %s)", job.get("id"), delay_seconds, available_at)

def requeue_immediate(job):
    job["state"] = "pending"
    job["updated_at"] = utcnow_iso_z()
    r.lpush(QUEUE, json.dumps(job))
    logger.info("Requeued job immediate %s", job.get("id"))

if __name__ == "__main__":
    while True:
        res = r.brpoplpush(QUEUE, PROCESSING, timeout=5)
        if not res:
            time.sleep(0.5)
            continue
        raw = res
        try:
            job = json.loads(raw)
        except Exception:
            logger.exception("Bad payload; moving raw to DLQ")
            r.lrem(PROCESSING, 1, raw)
            r.lpush(DEAD, raw)
            continue

        # update to processing state and replace the entry in PROCESSING with updated job json
        job["state"] = "processing"
        job["updated_at"] = utcnow_iso_z()
        mark_processing_replace(raw, job)

        attempts = int(job.get("attempts", 0))
        # execute
        ok = execute(job)

        # remove current job entry from processing (attempt to remove the updated JSON)
        current_serialized = json.dumps(job)
        r.lrem(PROCESSING, 1, current_serialized)

        if ok:
            mark_completed(job)
            continue

        # failure handling
        max_retries = job.get("max_retries") if job.get("max_retries") is not None else get_max_retries()
        base = get_backoff_base()
        job["attempts"] = attempts + 1

        if job["attempts"] <= int(max_retries):
            record_failed(job)
            # exponential backoff: delay = base ** attempts
            delay_seconds = int(math.pow(base, job["attempts"]))
            schedule_delayed(job, delay_seconds)
        else:
            # exceeded retries -> send to dead-letter queue
            push_to_dead(job)
