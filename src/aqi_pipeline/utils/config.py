"""Configuration helpers for the AQI pipeline."""

from __future__ import annotations

from functools import lru_cache
from typing import Any

import yaml

from aqi_pipeline.utils.paths import CONFIG_DIR


@lru_cache(maxsize=32)
def load_yaml(name: str) -> dict[str, Any]:
    path = CONFIG_DIR / name
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def load_cities() -> list[dict[str, Any]]:
    return load_yaml("cities.yaml").get("cities", [])


def load_pollutants() -> dict[str, Any]:
    return load_yaml("pollutants.yaml").get("pollutants", {})


def load_source_priority() -> dict[str, int]:
    cfg = load_yaml("source_priority.yaml")
    return {
        source: int(details["priority"])
        for source, details in cfg.get("priority", {}).items()
        if details.get("priority") is not None
    }
