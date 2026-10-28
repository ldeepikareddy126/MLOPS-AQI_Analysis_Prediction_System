"""Indian CPCB AQI calculations."""

from __future__ import annotations

import math

from aqi_pipeline.utils.config import load_yaml

MIN_AQI_POLLUTANTS = 3
REQUIRED_PARTICULATE_POLLUTANTS = {"PM2.5", "PM10"}


def sub_index(pollutant_code: str, concentration: float | None) -> float | None:
    if concentration is None:
        return None
    if not math.isfinite(float(concentration)):
        return None
    cfg = load_yaml("aqi_breakpoints.yaml")
    spec = cfg.get("breakpoints", {}).get(pollutant_code)
    if not spec:
        return None
    ranges = spec["ranges"]
    conc = float(concentration)
    if conc < 0.0:
        return None
    if conc > float(ranges[-1]["bp_high"]):
        return 500.0
    for idx, row in enumerate(ranges):
        bp_low = float(row["bp_low"])
        bp_high = float(row["bp_high"])
        if bp_low <= conc <= bp_high:
            aqi_low = float(row["aqi_low"])
            aqi_high = float(row["aqi_high"])
            return ((aqi_high - aqi_low) / (bp_high - bp_low)) * (conc - bp_low) + aqi_low
        if idx < len(ranges) - 1:
            next_low = float(ranges[idx + 1]["bp_low"])
            if bp_high < conc < next_low:
                if (conc - bp_high) <= (next_low - conc):
                    return float(row["aqi_high"])
                else:
                    return float(ranges[idx + 1]["aqi_low"])
    return None


def category(aqi: float | None) -> tuple[str | None, int | None]:
    if aqi is None:
        return None, None
    if not math.isfinite(float(aqi)):
        return None, None
    for idx, row in enumerate(load_yaml("aqi_breakpoints.yaml").get("categories", []), start=1):
        if float(row["aqi_low"]) <= aqi <= float(row["aqi_high"]):
            return row["name"], idx
    return "Severe", 6


def final_aqi(sub_indices: dict[str, float | None]) -> tuple[float | None, str | None]:
    present = {k: v for k, v in sub_indices.items() if v is not None and math.isfinite(float(v))}
    if (
        len(present) < MIN_AQI_POLLUTANTS
        or not REQUIRED_PARTICULATE_POLLUTANTS.intersection(present)
    ):
        return None, None
    pollutant, value = max(present.items(), key=lambda item: item[1])
    return value, pollutant
