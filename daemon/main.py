from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis, os, subprocess, json, sys, logging
from typing import List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

QUEUE = "jobs"
PROCESSING = "jobs:processing"
COMPLETED = "jobs:completed"
FAILED = "jobs:failed"
DEAD = "jobs:dlq"
CONFIG_HASH = "queuectl:config"
WORKERS = {}  

# Default config
DEFAULT_CONFIG = {"max-retries": 3}

# Ensure defaults
for k, v in DEFAULT_CONFIG.items():
    if r.hget(CONFIG_HASH, k) is None:
        r.hset(CONFIG_HASH, k, v)

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
    attempts: Optional[int] = 0

@app.post("/enqueue")
def enqueue(job: JobModel):
    j = job.dict()
    j.setdefault("attempts", 0)
    job_json = json.dumps(j)
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
        proc_len = r.llen(PROCESSING)
        completed_len = r.llen(COMPLETED)
        failed_len = r.llen(FAILED)
        dlq_len = r.llen(DEAD)
    except Exception:
        qlen = proc_len = completed_len = failed_len = dlq_len = None
    return {
        "queue_pending": qlen,
        "processing": proc_len,
        "completed": completed_len,
        "failed": failed_len,
        "dead": dlq_len,
        "workers": list(WORKERS.keys())
    }


@app.get("/list")
def list_jobs(state: str = "pending"):
    state = state.lower()
    if state == "pending":
        items = r.lrange(QUEUE, 0, -1)
    elif state == "processing":
        items = r.lrange(PROCESSING, 0, -1)
    elif state == "completed":
        items = r.lrange(COMPLETED, 0, -1)
    elif state == "failed":
        items = r.lrange(FAILED, 0, -1)
    elif state == "dead":
        items = r.lrange(DEAD, 0, -1)
    else:
        raise HTTPException(status_code=400, detail=f"unsupported state: {state}")
    out = []
    for s in items:
        try:
            out.append(json.loads(s))
        except Exception:
            out.append({"raw": s})
    return {"state": state, "count": len(out), "jobs": out}

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
    j["attempts"] = j.get("attempts", 0)
    # push back to pending (reset attempts optionally)
    r.lpush(QUEUE, json.dumps(j))
    return {"status": "retried", "id": job_id}

class ConfigSet(BaseModel):
    key: str
    value: str

@app.post("/config/set")
def config_set(payload: ConfigSet):
    set_config(payload.key, payload.value)
    return {"status": "ok", "key": payload.key, "value": payload.value}
