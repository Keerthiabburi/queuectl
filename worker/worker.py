import redis, json, subprocess, time, os
import sys, logging
import math
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

QUEUE = "jobs"
PROCESSING = "jobs:processing"
COMPLETED = "jobs:completed"
FAILED = "jobs:failed"
DEAD = "jobs:dlq"
DELAYED = "jobs:delayed"
CONFIG_HASH = "queuectl:config"

def utcnow_iso_z():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def get_max_retries():
    v = r.hget(CONFIG_HASH, "max-retries")
    try:
        return int(v) if v is not None else 3
    except Exception:
        return 3

def get_backoff_base():
    v = r.hget(CONFIG_HASH, "backoff_base")
    try:
        return int(v) if v is not None else 2
    except Exception:
        return 2

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
    """Replace the raw entry in PROCESSING with updated job JSON."""
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
    available_at = time.time() + delay_seconds
    job["state"] = "delayed"
    job["updated_at"] = utcnow_iso_z()
    member = json.dumps(job)
    r.zadd(DELAYED, {member: available_at})
    logger.info("Scheduled delayed job %s for in %s sec", job.get("id"), delay_seconds)

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
            logger.exception("Bad job payload, sending to DLQ raw")
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
