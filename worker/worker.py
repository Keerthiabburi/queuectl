import redis, json, subprocess, time, os
import sys, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
QUEUE = "jobs"
DLQ = "jobs:dlq"
CONFIG_HASH = "queuectl:config"

def get_max_retries():
    v = r.hget(CONFIG_HASH, "max-retries")
    try:
        return int(v) if v is not None else 3
    except Exception:
        return 3

def execute(job):
    try:
        logger.info("Executing job %s cmd=%s", job.get("id"), job.get("command"))
        subprocess.run(job["command"], shell=True, check=True, timeout=120)
        logger.info("Job done %s", job.get("id"))
        return True
    except Exception as e:
        logger.exception("Job failed %s: %s", job.get("id"), e)
        return False

def push_to_dlq(job):
    logger.info("Pushing job to DLQ %s", job.get("id"))
    r.lpush(DLQ, json.dumps(job))

def requeue_job(job):
    logger.info("Requeueing job %s attempts=%s", job.get("id"), job.get("attempts"))
    r.lpush(QUEUE, json.dumps(job))

if __name__ == "__main__":
    while True:
        res = r.brpop(QUEUE, timeout=5)
        if not res:
            time.sleep(0.5)
            continue
        _, data = res
        try:
            job = json.loads(data)
        except Exception:
            logger.exception("Bad job payload, sending to DLQ raw")
            r.lpush(DLQ, data)
            continue

        # ensure attempts field
        attempts = int(job.get("attempts", 0))
        job["attempts"] = attempts

        ok = execute(job)
        if ok:
            continue
        # failed -> check retries
        max_retries = get_max_retries()
        job["attempts"] = attempts + 1
        if job["attempts"] <= max_retries:
            # backoff: simple immediate requeue (could add delay)
            requeue_job(job)
        else:
            push_to_dlq(job)
