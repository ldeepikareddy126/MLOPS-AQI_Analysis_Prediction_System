"""CLI wrapper for Stage A ETL."""

from __future__ import annotations

import argparse
import json

from aqi_pipeline.transform.pipeline import run_stage_a


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--skip-ingest", action="store_true")
    args = parser.parse_args()
    summary = run_stage_a(
        run_id=args.run_id,
        start_date=args.start_date,
        end_date=args.end_date,
        ingest=not args.skip_ingest,
    )
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
