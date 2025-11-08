# queuectl — Distributed Job Queue with Workers, Backoff & DLQ

**📹 Demo Video:** [Watch on Google Drive](https://drive.google.com/file/d/1AipUjS91-B4sM-jI9OAXbtFlEPGRSzUh/view?usp=sharing)

`queuectl` is a lightweight job queue and worker system built using **Python**, **Redis**, **FastAPI**, and **Typer**.

## Features

- Enqueue jobs with custom commands
- Multiple concurrent workers
- Crash-safe locks with visibility timeout
- Retry mechanism with exponential backoff
- Delayed job scheduling (Redis ZSET)
- Dead Letter Queue (DLQ) for failed jobs
- Automatic reaper for stuck jobs
- Worker heartbeat and metadata persistence

---

## 1. Setup Instructions

### Prerequisites

- Python 3.10+
- Redis running on `localhost:6379`
- Git Bash / PowerShell / Linux shell

### Clone the Repository

```bash
git clone https://github.com/Keerthiabburi/queuectl.git
cd queuectl
```

### Create and Activate Virtual Environment

**Linux / Mac:**

```bash
python -m venv venv
source venv/bin/activate
```

**Windows (PowerShell):**

```bash
python -m venv venv
venv\Scripts\activate
```

### Install Dependencies

```bash
pip install -r requirements.txt
pip install -e .
```

### Create a `.env` File

Place this in the project root:

```env
QUEUECTL_REDIS_HOST=localhost
QUEUECTL_REDIS_PORT=6379
QUEUECTL_REDIS_DECODE_RESPONSES=true

QUEUECTL_QUEUE_KEY=jobs
QUEUECTL_PROCESSING_KEY=jobs:processing
QUEUECTL_COMPLETED_KEY=jobs:completed
QUEUECTL_FAILED_KEY=jobs:failed
QUEUECTL_DEAD_KEY=jobs:dlq
QUEUECTL_DELAYED_KEY=jobs:delayed
QUEUECTL_IDS_SET=jobs:ids
QUEUECTL_CONFIG_HASH=queuectl:config

QUEUECTL_WORKERS_SET=workers:set
QUEUECTL_WORKER_HEARTBEAT_TTL=12
QUEUECTL_WORKER_HEARTBEAT_INTERVAL=4

QUEUECTL_VISIBILITY_TIMEOUT=30
QUEUECTL_LOCK_EXTEND_INTERVAL=8
QUEUECTL_LOCK_EXTEND_MAX_MISSES=2

QUEUECTL_DEFAULT_MAX_RETRIES=3
QUEUECTL_DEFAULT_BACKOFF_BASE=2
QUEUECTL_MAX_BACKOFF_SECONDS=3600

QUEUECTL_DELAYED_MOVER_BATCH=100
QUEUECTL_DELAYED_MOVER_INTERVAL=1.0

QUEUECTL_REAPER_BATCH=100
QUEUECTL_REAPER_INTERVAL=5

QUEUECTL_WORKER_MODULE=worker.worker
QUEUECTL_DAEMON_URL=http://127.0.0.1:9000
QUEUECTL_LOG_LEVEL=INFO
```

### Start the Daemon (API Server)

```bash
uvicorn daemon.main:app --reload --port 9000
```

This launches:

- REST API
- Delayed job mover
- Job reaper
- Worker metadata persistence

### Start Workers

```bash
queuectl worker start --count 2
```

---

## 2. Usage Examples

### Enqueue a Job

```bash
queuectl enqueue "{\"id\":\"job1\",\"command\":\"sleep 2\"}"
```

### Check System Status

```bash
queuectl status
```

**Example output:**

```json
{
  "queue_pending": 1,
  "processing": 0,
  "completed": 3,
  "failed": 0,
  "dead": 0,
  "delayed": 0,
  "workers_local": [9134, 9135]
}
```

### List Jobs by State

```bash
queuectl list --state pending
queuectl list --state processing
queuectl list --state delayed
queuectl list --state dead
```

### Stop Workers

```bash
queuectl worker stop
```

### Retry a DLQ Job

```bash
queuectl dlq retry job1
```

### Set Configuration

```bash
queuectl config set max-retries 5
```

---

## 3. Architecture Overview

### Components

- **daemon** — FastAPI server with delayed mover and reaper
- **worker** — Executes jobs, extends locks, runs commands
- **client (CLI)** — Typer-based interface
- **Redis** — Persistence for queues, locks, heartbeats, and job metadata

### Redis Data Structures

| Key                     | Type   | Purpose                    |
| ----------------------- | ------ | -------------------------- |
| `jobs`                  | LIST   | Pending jobs               |
| `jobs:processing`       | LIST   | In-progress jobs           |
| `jobs:completed`        | LIST   | Successfully finished jobs |
| `jobs:failed`           | LIST   | Per-attempt failure logs   |
| `jobs:dlq`              | LIST   | Dead letter queue          |
| `jobs:delayed`          | ZSET   | Retry scheduling           |
| `jobs:ids`              | SET    | Job ID uniqueness          |
| `lock:job:<id>`         | STRING | Job ownership lock         |
| `worker:heartbeat:<id>` | STRING | TTL heartbeat              |
| `workers:set`           | SET    | All known workers          |
| `workers:<id>:meta`     | HASH   | Worker metadata            |

---

## 4. Job Lifecycle

```
pending → processing → completed
           │
           └── failure → failed → delayed → pending (retry)
                                │
                                └── max retries exceeded → dead (DLQ)
```

---

## 5. Worker Logic

### 1. Claim Job

Worker uses:

```
BRPOPLPUSH jobs → jobs:processing
```

### 2. Atomic Claim-Update (Lua)

- Create lock: `lock:job:<id> = token EX <visibility>`
- Replace job JSON inside processing list

### 3. Lock Extension

Worker regularly extends the job lock to prevent reprocessing.

### 4. Execute Job

```python
subprocess.run(job["command"], shell=True)
```

### 5. Finalization

**Success:** Move to `jobs:completed`

**Failure:** Increment attempts and:

- If `attempts ≤ max_retries` → Schedule in delayed queue and push to `jobs:failed`
- Else → Move to `jobs:dlq`

### 6. Reaper (Daemon)

Jobs in processing with expired locks get requeued back to pending.

---

## 6. Assumptions & Trade-offs

- Redis acts as the single source of truth
- Guarantees **at-least-once delivery**, not exactly-once
- Locks and Lua scripts minimize duplicates but cannot fully guarantee elimination
- Worker crashes are automatically handled by the reaper

---

## 7. Testing Instructions

### ✅ Success Case

```bash
queuectl enqueue "{\"id\":\"ok1\",\"command\":\"echo hello\"}"
queuectl worker start --count 1
queuectl status
```

### ✅ Failure + Delayed Backoff

```bash
queuectl enqueue "{\"id\":\"fail1\",\"command\":\"python -c \\\"import sys; sys.exit(1)\\\"\",\"max_retries\":2}"
```

**Monitor:**

```bash
redis-cli LRANGE jobs:failed 0 -1
redis-cli ZRANGE jobs:delayed 0 -1 WITHSCORES
```

**After retries exhausted:**

```bash
redis-cli LRANGE jobs:dlq 0 -1
```

### ✅ Reaper Test (Worker Crash)

**Start long-running job:**

```bash
queuectl enqueue "{\"id\":\"long\",\"command\":\"sleep 40\"}"
```

**Kill worker:**

Kill the worker process. After `VISIBILITY_TIMEOUT`, the reaper should requeue the job.

---
