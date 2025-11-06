import typer, json, shlex, requests
from pathlib import Path

app = typer.Typer()
DAEMON = "http://127.0.0.1:9000"

def load_payload(raw: str):
    # file syntax @file.json
    if raw.startswith("@"):
        return json.loads(Path(raw[1:]).read_text())

    # If the whole argument was wrapped in single or double quotes by the shell,
    # strip those outer quotes before parsing.
    if (raw.startswith("'") and raw.endswith("'")) or (raw.startswith('"') and raw.endswith('"')):
        raw = raw[1:-1]

    # Try to parse as-is
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Try to fix typical shell-escaping issues:
        fixed = raw.replace('\\"', '"').replace("\\'", "'").replace('\\\\', '\\')
        return json.loads(fixed)  # if this fails it will raise a JSONDecodeError

@app.command()
def enqueue(payload: str):
    data = load_payload(payload)
    r = requests.post(f"{DAEMON}/enqueue", json=data)
    typer.echo(r.json())

@app.command()
def worker(action: str, count: int = typer.Option(1, "--count")):
    r = requests.post(f"{DAEMON}/worker/{action}", json={"count": count})
    typer.echo(r.json())

@app.command()
def status():
    typer.echo(requests.get(f"{DAEMON}/status").json())

