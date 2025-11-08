from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis, os, subprocess, json, sys, logging
from typing import List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

QUEUE = "jobs"
DLQ = "jobs:dlq"
CONFIG_HASH = "queuectl:config"
WORKERS = {}  

# Default config
DEFAULT_CONFIG = {"max-retries": 3}

def get_config(key: str, default=None):
    v = r.hget(CONFIG_HASH, key)
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return v

def set_config(key: str, value):
    r.hset(CONFIG_HASH, key, value)

# Ensure defaults
for k, v in DEFAULT_CONFIG.items():
    if r.hget(CONFIG_HASH, k) is None:
        r.hset(CONFIG_HASH, k, v)

class JobModel(BaseModel):
    id: str
    command: str
    attempts: Optional[int] = 0

@app.post("/enqueue")
def enqueue(job: JobModel):
    job_json = json.dumps(job.dict())
    r.lpush(QUEUE, job_json)
    return {"status": "ok", "id": job.id}

@app.post("/worker/start")
def worker_start(payload: dict):
    count = int(payload.get("count", 1))
    pids = []
    for _ in range(count):
        try:
            cmd = [sys.executable, "-m", "worker.worker"]
            p = subprocess.Popen(cmd)
            WORKERS[p.pid] = p
            pids.append(p.pid)
            logger.info("Started worker pid=%s cmd=%s", p.pid, cmd)
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
            logger.info("Terminated worker pid=%s", pid)
        except Exception:
            logger.exception("Failed to terminate pid=%s", pid)
    return {"stopped": killed}

@app.get("/status")
def status():
    try:
        qlen = r.llen(QUEUE)
        dlq_len = r.llen(DLQ)
    except Exception:
        qlen = dlq_len = None
    return {"queue": qlen, "dlq": dlq_len, "workers": list(WORKERS.keys())}


@app.get("/list")
def list_jobs(state: str = "pending"):
    """
    state: pending | dlq
    pending -> jobs in main queue
    dlq -> dead-letter queue
    """
    if state == "pending":
        items = r.lrange(QUEUE, 0, -1)
    elif state == "dlq":
        items = r.lrange(DLQ, 0, -1)
    else:
        raise HTTPException(status_code=400, detail="unsupported state")
    out = []
    for s in items:
        try:
            out.append(json.loads(s))
        except Exception:
            out.append({"raw": s})
    return {"state": state, "count": len(out), "jobs": out}

@app.get("/dlq/list")
def dlq_list():
    items = r.lrange(DLQ, 0, -1)
    jobs = []
    for s in items:
        try:
            jobs.append(json.loads(s))
        except Exception:
            jobs.append({"raw": s})
    return {"count": len(jobs), "jobs": jobs}

@app.post("/dlq/retry/{job_id}")
def dlq_retry(job_id: str):
    """
    Find the first job in DLQ with id == job_id, remove it from DLQ and push back to main queue.
    """
    items = r.lrange(DLQ, 0, -1)
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
    # remove first occurrence and push back to queue (reset attempts or keep attempts)
    r.lrem(DLQ, 1, target)
    # optionally reset attempts to 0 when retrying from DLQ
    j = json.loads(target)
    j["attempts"] = j.get("attempts", 0)
    r.lpush(QUEUE, json.dumps(j))
    return {"status": "retried", "id": job_id}

class ConfigSet(BaseModel):
    key: str
    value: str

@app.post("/config/set")
def config_set(payload: ConfigSet):
    set_config(payload.key, payload.value)
    return {"status": "ok", "key": payload.key, "value": payload.value}
