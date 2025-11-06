from fastapi import FastAPI
from pydantic import BaseModel
import redis, os, subprocess, json

app = FastAPI()
r = redis.Redis(host="localhost", port=6379)

QUEUE = "jobs"
DLQ = "jobs:dlq"
WORKERS = {}

class Job(BaseModel):
    id: str
    command: str

@app.post("/enqueue")
def enqueue(job: Job):
    r.lpush(QUEUE, job.json())
    return {"status": "ok", "id": job.id}

@app.post("/worker/start")
def worker_start(payload: dict):
    count = payload.get("count", 1)
    pids = []
    for _ in range(count):
        p = subprocess.Popen(["python", "-m", "worker.worker"])
        WORKERS[p.pid] = p
        pids.append(p.pid)
    return {"started": pids}

@app.post("/worker/stop")
def worker_stop(payload: dict):
    killed = []
    for pid, p in list(WORKERS.items()):
        p.terminate()
        killed.append(pid)
        WORKERS.pop(pid)
    return {"stopped": killed}

@app.get("/status")
def status():
    return {
        "queue": r.llen(QUEUE),
        "dlq": r.llen(DLQ),
        "workers": list(WORKERS.keys())
    }

