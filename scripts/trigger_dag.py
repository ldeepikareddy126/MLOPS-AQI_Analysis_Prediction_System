"""Trigger an Airflow DAG with JSON-safe configuration from the CLI."""

from __future__ import annotations

import argparse
import json
import subprocess


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("dag_id")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--extra-conf", default="{}")
    args = parser.parse_args()

    conf = json.loads(args.extra_conf)
    if not isinstance(conf, dict):
        parser.error("--extra-conf must be a JSON object")
    if args.start_date:
        conf["start_date"] = args.start_date
    if args.end_date:
        conf["end_date"] = args.end_date

    command = ["airflow", "dags", "trigger", args.dag_id]
    if conf:
        command.extend(["--conf", json.dumps(conf, separators=(",", ":"))])
    return subprocess.run(command, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
