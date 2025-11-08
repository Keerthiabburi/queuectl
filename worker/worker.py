import redis, json, subprocess, time, os
import sys, logging
import math

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

def push_to_dead(job):
    logger.info("Pushing job to DEAD/DLQ %s", job.get("id"))
    r.lpush(DEAD, json.dumps(job))

def record_failed(job):
    logger.info("Recording failed job %s attempts=%s", job.get("id"), job.get("attempts"))
    r.lpush(FAILED, json.dumps(job))

def mark_completed(job):
    logger.info("Marking job completed %s", job.get("id"))
    r.lpush(COMPLETED, json.dumps(job))

def schedule_delayed(job, delay_seconds):
    
    available_at = time.time() + delay_seconds
    member = json.dumps(job)
    r.zadd(DELAYED, {member: available_at})
    logger.info("Scheduled job %s to run in %s sec (at %s)", job.get("id"), delay_seconds, available_at)

def requeue_job_immediate(job):
    logger.info("Requeueing job immediately %s attempts=%s", job.get("id"), job.get("attempts"))
    r.lpush(QUEUE, json.dumps(job))

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

        attempts = int(job.get("attempts", 0))
        # execute
        ok = execute(job)
        # remove from processing
        r.lrem(PROCESSING, 1, raw)

        if ok:
            # success
            mark_completed(job)
            continue

        # failure handling
        max_retries = get_max_retries()
        base = get_backoff_base()
        new_attempts = attempts + 1
        job["attempts"] = new_attempts

        if new_attempts <= max_retries:
            # schedule with exponential backoff: delay = base ** attempts
            try:
                delay_seconds = int(math.pow(base, new_attempts))
            except Exception:
                delay_seconds = base ** new_attempts
            record_failed(job)
            schedule_delayed(job, delay_seconds)
        else:
            # exceeded retries -> send to dead-letter queue
            push_to_dead(job)
