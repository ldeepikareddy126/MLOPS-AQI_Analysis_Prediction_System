"""Raw file landing utilities."""

from __future__ import annotations

import hashlib
import json
import shutil
from datetime import date
from pathlib import Path
from typing import Any

from aqi_pipeline.utils.paths import DATA_DIR

RAW_DIR = DATA_DIR / "raw"


def land_json(source: str, run_id: str, name: str, payload: Any, run_date: date | None = None) -> tuple[Path, str]:
    run_date = run_date or date.today()
    directory = RAW_DIR / source / run_date.isoformat() / run_id
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{name}.json"
    data = json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    path.write_bytes(data)
    return path, hashlib.sha256(data).hexdigest()


def land_file(source: str, run_id: str, path: Path, run_date: date | None = None) -> tuple[Path, str]:
    run_date = run_date or date.today()
    directory = RAW_DIR / source / run_date.isoformat() / run_id
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / path.name
    shutil.copy2(path, target)
    data = target.read_bytes()
    return target, hashlib.sha256(data).hexdigest()
