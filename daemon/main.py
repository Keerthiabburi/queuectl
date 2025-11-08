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

DELAYED_BATCH = int(os.getenv("QUEUECTL_DELAYED_MOVER_BATCH", "100"))
DELAYED_INTERVAL = float(os.getenv("QUEUECTL_DELAYED_MOVER_INTERVAL", "1.0"))

DEFAULT_MAX_RETRIES = int(os.getenv("QUEUECTL_DEFAULT_MAX_RETRIES", "3"))
DEFAULT_BACKOFF_BASE = int(os.getenv("QUEUECTL_DEFAULT_BACKOFF_BASE", "2"))
MAX_BACKOFF_SECONDS = int(os.getenv("QUEUECTL_MAX_BACKOFF_SECONDS", "3600"))

WORKER_MODULE = os.getenv("QUEUECTL_WORKER_MODULE", "worker.worker")


r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=REDIS_DECODE)

app = FastAPI()
WORKERS = {}

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

class JobModel(BaseModel):
    id: str
    command: str
    state: Optional[str] = "pending"
    attempts: Optional[int] = 0
    max_retries: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

@app.on_event("startup")
def start_background_tasks():
    t = threading.Thread(target=_delayed_mover, daemon=True)
    t.start()
    logger.info("Started delayed mover thread")

def _delayed_mover():
    """
    Move ready jobs from DELAYED zset to QUEUE (pending).
    """
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
            logger.exception("Delayed mover error; sleeping %s", DELAYED_INTERVAL)
            time.sleep(DELAYED_INTERVAL)
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
    job_json = json.dumps(j)
    job_id = j["id"]

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
            WORKERS[p.pid] = p
            pids.append(p.pid)
            logger.info("Started worker pid=%s", p.pid)
        except Exception:
            logger.exception("Failed to start worker")
    return {"started": pids}

@app.post("/worker/stop")
def worker_stop(payload: dict):
    killed = []
    for pid, p in list(WORKERS.items()):
        try:
            p.terminate()
            killed.append(pid)
            WORKERS.pop(pid, None)
        except Exception:
            logger.exception("Failed to terminate pid=%s", pid)
    return {"stopped": killed}

@app.get("/status")
def status():
    try:
        qlen = r.llen(QUEUE)
        proc_len = r.llen(PROCESSING)
        completed_len = r.llen(COMPLETED)
        failed_len = r.llen(FAILED)
        dlq_len = r.llen(DEAD)
        delayed_len = r.zcard(DELAYED)
    except Exception:
        qlen = proc_len = completed_len = failed_len = dlq_len = delayed_len = None
    return {
        "queue_pending": qlen,
        "processing": proc_len,
        "completed": completed_len,
        "failed": failed_len,
        "dead": dlq_len,
        "delayed": delayed_len,
        "workers": list(WORKERS.keys())
    }

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

class ConfigSet(BaseModel):
    key: str
    value: str

@app.post("/config/set")
def config_set(payload: ConfigSet):
    set_config(payload.key, payload.value)
    return {"status": "ok", "key": payload.key, "value": payload.value}
