# worker/worker.py
import os
import json
import time
import math
import uuid
import logging
import subprocess
from datetime import datetime, timezone
from threading import Thread

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
REDIS_DECODE = os.getenv("QUEUECTL_REDIS_DECODE_RESPONSES", "true").lower() in ("1", "true", "yes")

QUEUE = os.getenv("QUEUECTL_QUEUE_KEY", "jobs")
PROCESSING = os.getenv("QUEUECTL_PROCESSING_KEY", "jobs:processing")
COMPLETED = os.getenv("QUEUECTL_COMPLETED_KEY", "jobs:completed")
FAILED = os.getenv("QUEUECTL_FAILED_KEY", "jobs:failed")
DEAD = os.getenv("QUEUECTL_DEAD_KEY", "jobs:dlq")
DELAYED = os.getenv("QUEUECTL_DELAYED_KEY", "jobs:delayed")
CONFIG_HASH = os.getenv("QUEUECTL_CONFIG_HASH", "queuectl:config")
WORKERS_SET = os.getenv("QUEUECTL_WORKERS_SET", "workers:set")

HEARTBEAT_TTL = int(os.getenv("QUEUECTL_WORKER_HEARTBEAT_TTL", "12"))
HEARTBEAT_INTERVAL = int(os.getenv("QUEUECTL_WORKER_HEARTBEAT_INTERVAL", "4"))

VISIBILITY_TIMEOUT = int(os.getenv("QUEUECTL_VISIBILITY_TIMEOUT", "30"))
LOCK_EXTEND_INTERVAL = int(os.getenv("QUEUECTL_LOCK_EXTEND_INTERVAL", "8"))
LOCK_EXTEND_MAX_MISSES = int(os.getenv("QUEUECTL_LOCK_EXTEND_MAX_MISSES", "2"))

MAX_BACKOFF_SECONDS = int(os.getenv("QUEUECTL_MAX_BACKOFF_SECONDS", "3600"))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=REDIS_DECODE)

def utcnow_iso_z():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

WORKER_ID = str(uuid.uuid4())
HOSTNAME = os.getenv("HOSTNAME", "local")
PID = str(os.getpid())

# --- Lua scripts ---

_CLAIM_AND_UPDATE_LUA = r"""
-- KEYS[1] = processing_list
-- KEYS[2] = lock_key
-- ARGV[1] = old_member
-- ARGV[2] = new_member
-- ARGV[3] = token
-- ARGV[4] = visibility_secs
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
redis.call("SET", KEYS[2], ARGV[3], "EX", tonumber(ARGV[4]))
redis.call("LREM", KEYS[1], 1, ARGV[1])
redis.call("LPUSH", KEYS[1], ARGV[2])
return 1
"""

_EXTEND_LOCK_LUA = r"""
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
end
return 0
"""

# NEW: require token match before moving. ARGV: token, processing_member, completed_member
_FINALIZE_SUCCESS_LUA = r"""
-- KEYS[1] = processing_list
-- KEYS[2] = completed_list
-- KEYS[3] = lock_key
-- ARGV[1] = token
-- ARGV[2] = processing_member_value
-- ARGV[3] = completed_member_value
if redis.call('GET', KEYS[3]) == ARGV[1] then
  redis.call('DEL', KEYS[3])
  redis.call('LREM', KEYS[1], 1, ARGV[2])
  redis.call('LPUSH', KEYS[2], ARGV[3])
  return 1
end
return 0
"""

# DEAD finalizer with token check
_FINALIZE_DEAD_LUA = r"""
-- KEYS[1] = processing_list
-- KEYS[2] = dead_list
-- KEYS[3] = lock_key
-- ARGV[1] = token
-- ARGV[2] = processing_member_value
-- ARGV[3] = dead_member_value
if redis.call('GET', KEYS[3]) == ARGV[1] then
  redis.call('DEL', KEYS[3])
  redis.call('LREM', KEYS[1], 1, ARGV[2])
  redis.call('LPUSH', KEYS[2], ARGV[3])
  return 1
end
return 0
"""

_RELEASE_LOCK_LUA = r"""
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""

def register_worker():
    meta_key = f"workers:{WORKER_ID}:meta"
    heartbeat_key = f"worker:heartbeat:{WORKER_ID}"
    now = utcnow_iso_z()
    try:
        r.hset(meta_key, mapping={
            "id": WORKER_ID,
            "host": HOSTNAME,
            "pid": PID,
            "started_at": now,
            "updated_at": now
        })
        r.sadd(WORKERS_SET, WORKER_ID)
        r.set(heartbeat_key, now, ex=HEARTBEAT_TTL)
        logger.info("Registered worker %s", WORKER_ID)
    except Exception:
        logger.exception("Worker registration failed")

def heartbeat_loop(stop_flag):
    heartbeat_key = f"worker:heartbeat:{WORKER_ID}"
    meta_key = f"workers:{WORKER_ID}:meta"
    while not stop_flag():
        try:
            now = utcnow_iso_z()
            r.set(heartbeat_key, now, ex=HEARTBEAT_TTL)
            r.hset(meta_key, "updated_at", now)
        except Exception:
            logger.exception("Heartbeat error for %s", WORKER_ID)
        time.sleep(HEARTBEAT_INTERVAL)

def unregister_worker():
    meta_key = f"workers:{WORKER_ID}:meta"
    heartbeat_key = f"worker:heartbeat:{WORKER_ID}"
    now = utcnow_iso_z()
    try:
        r.delete(heartbeat_key)
    except Exception:
        logger.exception("Failed to delete heartbeat for %s", WORKER_ID)
    try:
        r.hset(meta_key, "stopped_at", now)
        r.hset(meta_key, "updated_at", now)
    except Exception:
        logger.exception("Failed to set stopped_at for %s", WORKER_ID)
    logger.info("Unregistered worker (heartbeat removed) %s", WORKER_ID)

def extend_lock(job_id, token, visibility=VISIBILITY_TIMEOUT):
    lock_key = f"lock:job:{job_id}"
    try:
        res = r.eval(_EXTEND_LOCK_LUA, 1, lock_key, token, visibility)
        return bool(res)
    except Exception:
        logger.exception("extend_lock lua error for %s", job_id)
        return False

def release_lock_if_owned(job_id, token):
    lock_key = f"lock:job:{job_id}"
    try:
        res = r.eval(_RELEASE_LOCK_LUA, 1, lock_key, token)
        return bool(res)
    except Exception:
        logger.exception("release_lock lua error for %s", job_id)
        return False

def finalize_success(processing_member, completed_member, token):
    lock_key = f"lock:job:{json.loads(processing_member).get('id')}"
    try:
        res = r.eval(_FINALIZE_SUCCESS_LUA, 3, PROCESSING, COMPLETED, lock_key, token, processing_member, completed_member)
        return bool(res)
    except Exception:
        logger.exception("Finalize success lua error")
        return False

def finalize_dead(processing_member, dead_member, token):
    lock_key = f"lock:job:{json.loads(processing_member).get('id')}"
    try:
        res = r.eval(_FINALIZE_DEAD_LUA, 3, PROCESSING, DEAD, lock_key, token, processing_member, dead_member)
        return bool(res)
    except Exception:
        logger.exception("Finalize dead lua error")
        return False

def run_job_loop():
    stop = False
    def stop_flag(): return stop

    hb_stop = False
    def hb_flag(): return hb_stop
    hb_thread = Thread(target=heartbeat_loop, args=(hb_flag,), daemon=True)
    hb_thread.start()

    try:
        while True:
            try:
                res = r.brpoplpush(QUEUE, PROCESSING, timeout=5)
            except Exception:
                logger.exception("Redis BRPOPLPUSH error")
                time.sleep(1)
                continue

            if not res:
                time.sleep(0.2)
                continue

            raw = res
            try:
                job = json.loads(raw)
            except Exception:
                logger.exception("Bad job payload; moving raw to DLQ")
                try:
                    r.lrem(PROCESSING, 1, raw)
                    r.lpush(DEAD, raw)
                except Exception:
                    logger.exception("Failed to move bad payload to DLQ")
                continue

            job_id = job.get("id")
            token = str(uuid.uuid4())

            # prepare updated processing member
            job["state"] = "processing"
            job["updated_at"] = utcnow_iso_z()
            updated_raw = json.dumps(job)
            lock_key = f"lock:job:{job_id}"

            # claim & update atomically
            try:
                ok = r.eval(_CLAIM_AND_UPDATE_LUA, 2, PROCESSING, lock_key,
                            raw, updated_raw, token, str(VISIBILITY_TIMEOUT))
            except Exception:
                logger.exception("Redis error while claiming job %s", job_id)
                try:
                    r.lrem(PROCESSING, 1, raw)
                    r.lpush(QUEUE, raw)
                except Exception:
                    logger.exception("Failed fallback requeue for %s", job_id)
                continue

            if not ok:
                logger.warning("Could not claim job %s (lock exists), requeueing", job_id)
                try:
                    r.lrem(PROCESSING, 1, raw)
                    r.lpush(QUEUE, raw)
                except Exception:
                    logger.exception("Failed to requeue %s after failed claim", job_id)
                continue

            # raw in processing is now updated_raw (canonical)
            raw = updated_raw

            # lock extender
            extender_stop = False
            def extender():
                missed = 0
                while not extender_stop:
                    time.sleep(LOCK_EXTEND_INTERVAL)
                    ok_ext = extend_lock(job_id, token, VISIBILITY_TIMEOUT)
                    if not ok_ext:
                        missed += 1
                        logger.warning("Lock extend failed for job %s (missed=%s)", job_id, missed)
                        if missed >= LOCK_EXTEND_MAX_MISSES:
                            logger.error("Lock extend failed repeatedly for %s — will stop attempting further extends", job_id)
                            break
                    else:
                        missed = 0
            ext_t = Thread(target=extender, daemon=True)
            ext_t.start()

            try:
                logger.info("Worker %s executing job %s: %s", WORKER_ID, job_id, job.get("command"))
                subprocess.run(job["command"], shell=True, check=True, timeout=120)

                # prepare completed member (reflect final state)
                job["state"] = "completed"
                job["updated_at"] = utcnow_iso_z()
                completed_member = json.dumps(job)

                moved = finalize_success(raw, completed_member, token)
                if moved:
                    logger.info("Job %s completed", job_id)
                else:
                    # finalize failed: likely lock mismatch / reaper already requeued
                    logger.warning("Finalize for job %s returned false — job may have been requeued by reaper", job_id)

            except Exception as e:
                logger.exception("Job %s failed: %s", job_id, e)
                attempts = int(job.get("attempts", 0)) + 1
                job["attempts"] = attempts
                job["updated_at"] = utcnow_iso_z()
                try:
                    max_retries = job.get("max_retries", None)
                    if max_retries is None:
                        max_retries = int(r.hget(CONFIG_HASH, "max-retries") or 3)
                except Exception:
                    max_retries = 3

                if attempts <= int(max_retries):
                    try:
                        base = int(r.hget(CONFIG_HASH, "backoff_base") or 2)
                    except Exception:
                        base = 2
                    try:
                        delay_seconds = int(math.pow(base, attempts))
                    except Exception:
                        delay_seconds = base ** attempts
                    if delay_seconds > MAX_BACKOFF_SECONDS:
                        delay_seconds = MAX_BACKOFF_SECONDS

                    # attempt to release lock and move to delayed (best-effort)
                    try:
                        r.eval(_RELEASE_LOCK_LUA, 1, lock_key, token)
                    except Exception:
                        logger.exception("Failed to release lock for failed job %s", job_id)
                    try:
                        r.lrem(PROCESSING, 1, raw)
                        r.zadd(DELAYED, {json.dumps(job): time.time() + delay_seconds})
                        r.lpush(FAILED, json.dumps(job))
                        logger.info("Job %s scheduled delayed for %s seconds (attempt %s)", job_id, delay_seconds, attempts)
                    except Exception:
                        logger.exception("Failed to schedule delayed for %s", job_id)
                else:
                    try:
                        r.eval(_RELEASE_LOCK_LUA, 1, lock_key, token)
                    except Exception:
                        logger.exception("Failed to release lock for dead job %s", job_id)
                    try:
                        r.lrem(PROCESSING, 1, raw)
                        job["state"] = "dead"
                        job["updated_at"] = utcnow_iso_z()
                        r.lpush(DEAD, json.dumps(job))
                        logger.info("Job %s moved to DLQ (dead)", job_id)
                    except Exception:
                        logger.exception("Failed to move job %s to DLQ", job_id)

            finally:
                extender_stop = True
                try:
                    release_lock_if_owned(job_id, token)
                except Exception:
                    pass

    except KeyboardInterrupt:
        logger.info("Worker interrupted, shutting down")
    finally:
        hb_stop = True
        unregister_worker()

if __name__ == "__main__":
    register_worker()
    run_job_loop()
