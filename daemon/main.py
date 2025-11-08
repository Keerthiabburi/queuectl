import os
import sys
import json
import time
import threading
import logging
import subprocess
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
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
IDS_SET = os.getenv("QUEUECTL_IDS_SET", "jobs:ids")
CONFIG_HASH = os.getenv("QUEUECTL_CONFIG_HASH", "queuectl:config")
WORKERS_SET = os.getenv("QUEUECTL_WORKERS_SET", "workers:set")

DELAYED_BATCH = int(os.getenv("QUEUECTL_DELAYED_MOVER_BATCH", "100"))
DELAYED_INTERVAL = float(os.getenv("QUEUECTL_DELAYED_MOVER_INTERVAL", "1.0"))

DEFAULT_MAX_RETRIES = int(os.getenv("QUEUECTL_DEFAULT_MAX_RETRIES", "3"))
DEFAULT_BACKOFF_BASE = int(os.getenv("QUEUECTL_DEFAULT_BACKOFF_BASE", "2"))
MAX_BACKOFF_SECONDS = int(os.getenv("QUEUECTL_MAX_BACKOFF_SECONDS", "3600"))

VISIBILITY_TIMEOUT = int(os.getenv("QUEUECTL_VISIBILITY_TIMEOUT", "30"))
REAPER_INTERVAL = int(os.getenv("QUEUECTL_REAPER_INTERVAL", "5"))
REAPER_BATCH = int(os.getenv("QUEUECTL_REAPER_BATCH", "100"))

WORKER_MODULE = os.getenv("QUEUECTL_WORKER_MODULE", "worker.worker")


r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=REDIS_DECODE)

app = FastAPI()
LOCAL_WORKERS = {}

# Ensure defaults
DEFAULT_CONFIG = {"max-retries": DEFAULT_MAX_RETRIES, "backoff_base": DEFAULT_BACKOFF_BASE}
for k, v in DEFAULT_CONFIG.items():
    if r.hget(CONFIG_HASH, k) is None:
        r.hset(CONFIG_HASH, k, v)

def utcnow_iso_z():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def get_config_int(key: str, default: int):
    v = r.hget(CONFIG_HASH, key)
    try:
        return int(v) if v is not None else default
    except Exception:
        return default

def set_config(key: str, value: str):
    r.hset(CONFIG_HASH, key, value)


_ENQUEUE_LUA = """
-- KEYS[1] = ids set
-- KEYS[2] = queue list
-- ARGV[1] = job_id
-- ARGV[2] = job_json
local added = redis.call('SADD', KEYS[1], ARGV[1])
if added == 0 then
  return 0
end
redis.call('LPUSH', KEYS[2], ARGV[2])
return 1
"""

_REQUEUE_MEMBER_LUA = """
-- KEYS[1] = processing_list
-- KEYS[2] = queue_list
-- ARGV[1] = member_value
local removed = redis.call('lrem', KEYS[1], 1, ARGV[1])
if removed > 0 then
  return redis.call('lpush', KEYS[2], ARGV[1])
end
return 0
"""

class JobModel(BaseModel):
    id: str
    command: str
    state: Optional[str] = "pending"
    attempts: Optional[int] = 0
    max_retries: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

def _delayed_mover():
    """Move ready items from DELAYED zset back to QUEUE (pending)."""
    while True:
        try:
            now = time.time()
            ready = r.zrangebyscore(DELAYED, "-inf", now, start=0, num=DELAYED_BATCH)
            for member in ready:
                removed = r.zrem(DELAYED, member)
                if not removed:
                    continue
                try:
                    job = json.loads(member)
                except Exception:
                    r.lpush(DEAD, member)
                    continue
                job["state"] = "pending"
                job["updated_at"] = utcnow_iso_z()
                r.lpush(QUEUE, json.dumps(job))
                logger.info("Moved delayed job back to pending: %s", job.get("id"))
            time.sleep(DELAYED_INTERVAL)
        except Exception:
            logger.exception("Delayed mover error; sleeping")
            time.sleep(DELAYED_INTERVAL)

def _reaper_loop():
    """
    Reclaim processing jobs whose lock expired: move them back to QUEUE.
    """
    while True:
        try:
            items = r.lrange(PROCESSING, 0, REAPER_BATCH - 1)
            if not items:
                time.sleep(REAPER_INTERVAL)
                continue
            for member in items:
                try:
                    job = json.loads(member)
                    job_id = job.get("id")
                except Exception:
                    r.lrem(PROCESSING, 1, member)
                    r.lpush(DEAD, member)
                    continue
                lock_key = f"lock:job:{job_id}"
                if r.exists(lock_key):
                    continue
                try:
                    moved = r.eval(_REQUEUE_MEMBER_LUA, 2, PROCESSING, QUEUE, member)
                    if moved:
                        logger.info("Requeued stale processing job %s back to pending", job_id)
                except Exception:
                    logger.exception("Reaper error moving job %s", job_id)
            time.sleep(REAPER_INTERVAL)
        except Exception:
            logger.exception("Reaper loop error")
            time.sleep(REAPER_INTERVAL)

@app.on_event("startup")
def startup_tasks():
    t1 = threading.Thread(target=_delayed_mover, daemon=True)
    t1.start()
    logger.info("Started delayed mover")
    t2 = threading.Thread(target=_reaper_loop, daemon=True)
    t2.start()
    logger.info("Started reaper loop")

@app.post("/enqueue")
def enqueue(job: JobModel):
    now = utcnow_iso_z()
    j = job.dict()
    j.setdefault("state", "pending")
    j.setdefault("attempts", 0)
    if j.get("max_retries") is None:
        j["max_retries"] = get_config_int("max-retries", DEFAULT_CONFIG["max-retries"])
    j.setdefault("created_at", now)
    j["updated_at"] = now

    job_id = j["id"]
    job_json = json.dumps(j)
    try:
        res = r.eval(_ENQUEUE_LUA, 2, IDS_SET, QUEUE, job_id, job_json)
    except Exception:
        logger.exception("Redis Lua enqueue failed")
        raise HTTPException(status_code=500, detail="redis error")

    if res == 0:
        raise HTTPException(status_code=409, detail=f"job id '{job_id}' already exists")
    return {"status": "ok", "id": job_id}

@app.post("/worker/start")
def worker_start(payload: dict):
    count = int(payload.get("count", 1))
    pids = []
    for _ in range(count):
        try:
            cmd = [sys.executable, "-m", WORKER_MODULE]
            p = subprocess.Popen(cmd)
            LOCAL_WORKERS[p.pid] = p
            pids.append(p.pid)
            logger.info("Started local worker pid=%s", p.pid)
        except Exception:
            logger.exception("Failed to start worker subprocess")
    return {"started": pids}

@app.post("/worker/stop")
def worker_stop(payload: dict):
    
    stopped = []
    
    for pid, p in list(LOCAL_WORKERS.items()):
        try:
            p.terminate()
            stopped.append(pid)
            LOCAL_WORKERS.pop(pid, None)
            logger.info("Terminated local worker pid=%s", pid)
        except Exception:
            logger.exception("Failed to terminate worker pid=%s", pid)

    requeued = []
    skipped_locked = 0

    try:
        items = r.lrange(PROCESSING, 0, -1)
    except Exception:
        logger.exception("Failed to read processing list")
        items = []

    for member in items:
        try:
            j = json.loads(member)
            job_id = j.get("id")
        except Exception:
            try:
                r.lrem(PROCESSING, 1, member)
                r.lpush(DEAD, member)
                logger.warning("Corrupt processing member moved to DLQ")
            except Exception:
                logger.exception("Failed to move corrupt member to DLQ")
            continue

        lock_key = f"lock:job:{job_id}"
        try:
            if r.exists(lock_key):
                skipped_locked += 1
                continue

            moved = r.eval(_REQUEUE_MEMBER_LUA, 2, PROCESSING, QUEUE, member)
            if moved:
                requeued.append(job_id)
                logger.info("Requeued processing job %s back to pending (stop)", job_id)
            else:
                logger.debug("Member %s was not moved (already removed?); skipping", job_id)
        except Exception:
            logger.exception("Error while attempting to requeue job %s", job_id)

    return {"stopped": stopped, "requeued": requeued, "skipped_locked": skipped_locked}


@app.get("/status")
def status():
    try:
        return {
            "queue_pending": r.llen(QUEUE),
            "processing": r.llen(PROCESSING),
            "completed": r.llen(COMPLETED),
            "failed": r.llen(FAILED),
            "dead": r.llen(DEAD),
            "delayed": r.zcard(DELAYED),
            "workers_local": list(LOCAL_WORKERS.keys())
        }
    except Exception:
        raise HTTPException(status_code=500, detail="redis error")

@app.get("/list")
def list_jobs(state: str = "pending"):
    s = state.lower()
    if s == "pending":
        items = r.lrange(QUEUE, 0, -1)
    elif s == "processing":
        items = r.lrange(PROCESSING, 0, -1)
    elif s == "completed":
        items = r.lrange(COMPLETED, 0, -1)
    elif s == "failed":
        items = r.lrange(FAILED, 0, -1)
    elif s == "dead":
        items = r.lrange(DEAD, 0, -1)
    elif s == "delayed":
        items = r.zrange(DELAYED, 0, -1, withscores=True)
        out = []
        for member, score in items:
            try:
                out.append({"job": json.loads(member), "available_at": score})
            except Exception:
                out.append({"raw": member, "available_at": score})
        return {"state": s, "count": len(out), "jobs": out}
    else:
        raise HTTPException(status_code=400, detail=f"unsupported state: {state}")

    out = []
    for it in items:
        try:
            out.append(json.loads(it))
        except Exception:
            out.append({"raw": it})
    return {"state": s, "count": len(out), "jobs": out}

@app.get("/dlq/list")
def dlq_list():
    items = r.lrange(DEAD, 0, -1)
    jobs = []
    for s in items:
        try:
            jobs.append(json.loads(s))
        except Exception:
            jobs.append({"raw": s})
    return {"count": len(jobs), "jobs": jobs}

@app.post("/dlq/retry/{job_id}")
def dlq_retry(job_id: str):
    items = r.lrange(DEAD, 0, -1)
    target = None
    for s in items:
        try:
            j = json.loads(s)
        except Exception:
            continue
        if j.get("id") == job_id:
            target = s
            break
    if not target:
        raise HTTPException(status_code=404, detail="job not found in DLQ")
    r.lrem(DEAD, 1, target)
    j = json.loads(target)
    j["state"] = "pending"
    j["updated_at"] = utcnow_iso_z()
    r.lpush(QUEUE, json.dumps(j))
    return {"status": "retried", "id": job_id}

@app.post("/config/set")
def config_set(payload: BaseModel):
    body = payload.dict()
    key = body.get("key")
    value = body.get("value")
    if not key:
        raise HTTPException(status_code=400, detail="missing key")
    set_config(key, value)
    return {"status": "ok", "key": key, "value": value}

@app.get("/workers")
def list_workers():
    ids = r.smembers(WORKERS_SET) or set()
    out = []
    for wid in ids:
        meta = r.hgetall(f"workers:{wid}:meta") or {}
        heartbeat = r.get(f"worker:heartbeat:{wid}")
        meta["heartbeat"] = heartbeat
        out.append(meta)
    return {"count": len(out), "workers": out}

@app.get("/health")
def health():
    # basic healthcheck: ping redis
    try:
        r.ping()
        return {"status": "ok"}
    except Exception:
        raise HTTPException(status_code=503, detail="redis unreachable")
