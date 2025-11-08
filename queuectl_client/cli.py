import typer, json, shlex, requests
from pathlib import Path

app = typer.Typer()
DAEMON = "http://127.0.0.1:9000"

def load_payload(raw: str):
    # file syntax @file.json
    if raw.startswith("@"):
        return json.loads(Path(raw[1:]).read_text())
    # strip outer quotes if present
    if (raw.startswith("'") and raw.endswith("'")) or (raw.startswith('"') and raw.endswith('"')):
        raw = raw[1:-1]
    return json.loads(raw)

@app.command()
def enqueue(payload: str):
    data = load_payload(payload)
    r = requests.post(f"{DAEMON}/enqueue", json=data, timeout=3)
    r.raise_for_status()
    typer.echo(r.json())

@app.command()
def worker(action: str, count: int = typer.Option(1, "--count")):
    r = requests.post(f"{DAEMON}/worker/{action}", json={"count": count}, timeout=3)
    r.raise_for_status()
    typer.echo(r.json())

@app.command()
def status():
    r = requests.get(f"{DAEMON}/status", timeout=3)
    r.raise_for_status()
    typer.echo(r.json())


@app.command("list")
def cli_list(state: str = typer.Option("pending", "--state", "-s", help="pending|dlq")):
    r = requests.get(f"{DAEMON}/list", params={"state": state}, timeout=3)
    r.raise_for_status()
    typer.echo(r.json())

@app.command("dlq-list")
def cli_dlq_list():
    r = requests.get(f"{DAEMON}/dlq/list", timeout=3)
    r.raise_for_status()
    typer.echo(r.json())

@app.command("dlq-retry")
def cli_dlq_retry(job_id: str):
    r = requests.post(f"{DAEMON}/dlq/retry/{job_id}", timeout=3)
    if r.status_code == 404:
        typer.echo(f"job {job_id} not found in DLQ")
        raise typer.Exit(code=1)
    r.raise_for_status()
    typer.echo(r.json())

@app.command("config-set")
def cli_config_set(key: str, value: str):
    r = requests.post(f"{DAEMON}/config/set", json={"key": key, "value": value}, timeout=3)
    r.raise_for_status()
    typer.echo(r.json())

if __name__ == "__main__":
    app()
