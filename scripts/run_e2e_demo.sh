#!/usr/bin/env bash
set -euo pipefail

RUN_ID="${PIPELINE_RUN_ID:-$(python - <<'PY'
from uuid import uuid4
print(uuid4())
PY
)}"
START_DATE="${PIPELINE_BACKFILL_START:-2022-01-01}"
END_DATE="${PIPELINE_BACKFILL_END:-$(date -I)}"

export PIPELINE_RUN_ID="$RUN_ID"

echo "Running Stage A ETL"
echo "run_id=$RUN_ID"
echo "date_range=$START_DATE..$END_DATE"

python -m aqi_pipeline.transform.pipeline_runner \
  --run-id "$RUN_ID" \
  --start-date "$START_DATE" \
  --end-date "$END_DATE"

python /opt/airflow/scripts/verify_sources.py --run-id "$RUN_ID" --strict
