"""Pre-deployment checks for the compose lakehouse stack."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

LOCAL_ONLY_VALUES = {
    "S3_ACCESS_KEY": {"minioadmin", ""},
    "S3_SECRET_KEY": {"minioadmin", ""},
    "GRAFANA_ADMIN_PASSWORD": {"admin", ""},
    "AIRFLOW_ADMIN_PASSWORD": {"admin", ""},
    "AIRFLOW__WEBSERVER__SECRET_KEY": {"local-dev-secret", "replace-me-with-a-random-secret", ""},
}

REQUIRED_PATHS = [
    Path("docker-compose.yml"),
    Path("docker/spark/Dockerfile"),
    Path("docker/producer/Dockerfile"),
    Path("docker/airflow/Dockerfile"),
    Path("docker/metrics-publisher/Dockerfile"),
    Path("observability/prometheus/prometheus.yml"),
    Path("observability/grafana/dashboards/lakehouse-overview.json"),
]


def _load_dotenv(path: Path = Path(".env")) -> dict[str, str]:
    values = dict(os.environ)
    if not path.exists():
        return values
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        values.setdefault(name.strip(), value.strip().strip("'\""))
    return values


def _run_compose_config() -> bool:
    result = subprocess.run(
        ["docker", "compose", "config", "--quiet"],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if result.returncode != 0:
        print(result.stdout.rstrip())
        return False
    return True


def _missing_paths() -> list[Path]:
    return [path for path in REQUIRED_PATHS if not path.exists()]


def _unsafe_env_values(env: dict[str, str]) -> list[str]:
    unsafe = []
    for name, local_values in LOCAL_ONLY_VALUES.items():
        value = env.get(name, "")
        if value in local_values:
            unsafe.append(name)
    return unsafe


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--strict",
        action="store_true",
        help="fail when local-only credentials are still configured",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="require credentials for external live-data producers",
    )
    args = parser.parse_args()

    failures = []
    env = _load_dotenv()
    if not _run_compose_config():
        failures.append("docker compose config failed")

    missing = _missing_paths()
    if missing:
        failures.append("missing required paths: " + ", ".join(str(path) for path in missing))

    unsafe = _unsafe_env_values(env)
    if unsafe and args.strict:
        failures.append("strict mode rejects local-only values: " + ", ".join(sorted(unsafe)))
    elif unsafe:
        print("WARN local-only values still configured: " + ", ".join(sorted(unsafe)))

    if args.live:
        missing_live = [
            name for name in ("ALPACA_API_KEY", "ALPACA_API_SECRET") if not env.get(name)
        ]
        if missing_live:
            failures.append("missing live producer credentials: " + ", ".join(missing_live))

    if failures:
        for failure in failures:
            print(f"FAIL {failure}")
        return 1

    print("deploy check passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
