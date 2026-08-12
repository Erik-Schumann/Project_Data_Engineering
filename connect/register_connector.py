"""Waits for the Kafka Connect REST API, then registers (or updates) every
*-connector.json found under /scripts (mounted read-only from each bounded
context's own connect/ directory — catalog-input, reporting-output, ...).
Runs once as a one-off container; safe to re-run (PUT .../config is an
idempotent upsert)."""
import glob
import json
import time
import urllib.error
import urllib.request

CONNECT_URL = "http://connect:8083"
CONFIG_GLOB = "/scripts/**/*-connector.json"


def wait_for_connect(timeout_s: int = 180) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"{CONNECT_URL}/connectors", timeout=5)
            print("Kafka Connect is up.")
            return
        except (urllib.error.URLError, ConnectionError):
            time.sleep(3)
    raise SystemExit("Timed out waiting for Kafka Connect REST API")


def register(name: str, config: dict) -> None:
    body = json.dumps(config).encode("utf-8")
    req = urllib.request.Request(
        f"{CONNECT_URL}/connectors/{name}/config",
        data=body,
        method="PUT",
        headers={"Content-Type": "application/json"},
    )
    try:
        resp = urllib.request.urlopen(req, timeout=10)
        print(f"Registration HTTP status: {resp.status}")
        print(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print(f"Registration HTTP status: {e.code}")
        print(e.read().decode("utf-8"))
        raise SystemExit(1)


def print_status(name: str) -> None:
    try:
        resp = urllib.request.urlopen(f"{CONNECT_URL}/connectors/{name}/status", timeout=10)
        print("Connector status:")
        print(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print(f"Could not fetch status: {e.code} {e.read().decode('utf-8')}")


def main() -> None:
    config_paths = sorted(glob.glob(CONFIG_GLOB, recursive=True))
    if not config_paths:
        raise SystemExit(f"No connector configs found matching {CONFIG_GLOB}")

    wait_for_connect()
    for path in config_paths:
        with open(path) as f:
            spec = json.load(f)
        print(f"Registering {spec['name']} from {path}")
        register(spec["name"], spec["config"])
        print_status(spec["name"])


if __name__ == "__main__":
    main()
