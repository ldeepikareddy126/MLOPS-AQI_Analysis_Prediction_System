from aqi_pipeline.transform.units import canonical_pollutant, convert_value, is_valid_range


def test_pollutant_aliases():
    assert canonical_pollutant("pm25") == "PM2.5"
    assert canonical_pollutant("Ozone") == "O3"


def test_unit_conversion_co_ug_to_mg():
    value, unit = convert_value(1000, "CO", "ug/m3")
    assert unit == "mg/m³"
    assert value == 1.0


def test_range_rejects_negative():
    ok, reason = is_valid_range("PM10", -1)
    assert not ok
    assert "minimum" in reason


def test_range_rejects_nan_and_none():
    import math

    ok, reason = is_valid_range("PM2.5", float("nan"))
    assert not ok
    assert "non-numeric" in reason

    ok, reason = is_valid_range("PM2.5", None)
    assert not ok

    val, _ = convert_value(float("nan"), "PM2.5", "µg/m³")
    assert val is None


def test_range_rejects_sentinel():
    ok, reason = is_valid_range("PM2.5", -999.0)
    assert not ok
    assert "sentinel" in reason

