"""Pollutant name and unit normalisation."""

from __future__ import annotations

import math
import re
from typing import Any

from aqi_pipeline.utils.config import load_pollutants


def _norm(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def pollutant_lookup() -> dict[str, str]:
    mapping: dict[str, str] = {}
    for code, spec in load_pollutants().items():
        names = {code, spec.get("canonical_code", code)}
        names.update(spec.get("openaq_parameter_names", []))
        names.update(spec.get("cpcb_column_aliases", []))
        for name in names:
            mapping[_norm(str(name))] = code
    return mapping


def canonical_pollutant(value: str | None) -> str | None:
    if not value:
        return None
    return pollutant_lookup().get(_norm(value))


def normalise_unit(unit: str | None) -> str:
    unit = (unit or "").strip()
    replacements = {
        "ug/m3": "µg/m³",
        "µg/m3": "µg/m³",
        "ug/m³": "µg/m³",
        "microgram/m3": "µg/m³",
        "mg/m3": "mg/m³",
    }
    return replacements.get(unit, replacements.get(unit.lower(), unit))


def convert_value(value: Any, pollutant_code: str, unit: str | None) -> tuple[float | None, str]:
    if value is None or value == "":
        return None, load_pollutants()[pollutant_code]["standard_unit"]
    try:
        numeric = float(value)
        if not math.isfinite(numeric):
            return None, load_pollutants()[pollutant_code]["standard_unit"]
    except (TypeError, ValueError):
        return None, load_pollutants()[pollutant_code]["standard_unit"]

    spec = load_pollutants()[pollutant_code]
    standard_unit = spec["standard_unit"]
    unit_key = normalise_unit(unit or standard_unit)
    conversions = spec.get("unit_conversions", {})
    factor = conversions.get(unit_key)
    if factor is None:
        factor = conversions.get(unit_key.lower())
    if factor is None:
        factor = 1.0 if unit_key == standard_unit else None
    if factor is None:
        return None, standard_unit
    return numeric * float(factor), standard_unit


def is_valid_range(pollutant_code: str, value: float | None) -> tuple[bool, str | None]:
    if value is None or not math.isfinite(value):
        return False, "missing or non-numeric value"
    spec = load_pollutants()[pollutant_code]
    if value in set(float(v) for v in spec.get("sentinel_values", [])):
        return False, "sentinel value"
    min_value = spec.get("physical_min")
    max_value = spec.get("physical_max")
    if min_value is not None and value < float(min_value):
        return False, f"value below physical minimum {min_value}"
    if max_value is not None and value > float(max_value):
        return False, f"value above physical maximum {max_value}"
    return True, None
