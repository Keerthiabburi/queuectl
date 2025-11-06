import redis, json, subprocess, time

r = redis.Redis(host="localhost", port=6379)
QUEUE = "jobs"
DLQ = "jobs:dlq"

def execute(job):
    try:
        subprocess.run(job["command"], shell=True, check=True, timeout=120)
    except Exception:
        r.lpush(DLQ, json.dumps(job))

if __name__ == "__main__":
    while True:
        result = r.brpop(QUEUE, timeout=5)
        if not result:
            time.sleep(1)
            continue
        _, data = result
        execute(json.loads(data))

