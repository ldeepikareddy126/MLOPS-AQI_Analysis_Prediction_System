from unittest.mock import patch

from aqi_pipeline.transform import pipeline


def test_match_city_loads_config_once_for_repeated_measurements():
    cities = [
        {"id": "delhi", "name": "Delhi", "lat": 28.6, "lon": 77.2},
        {"id": "mumbai", "name": "Mumbai", "lat": 19.1, "lon": 72.9},
    ]
    pipeline._configured_cities.cache_clear()
    try:
        with patch.object(pipeline, "load_cities", return_value=cities) as load:
            assert pipeline._match_city("Delhi station", None, None) == "delhi"
            assert pipeline._match_city("Mumbai station", None, None) == "mumbai"
            assert load.call_count == 1
    finally:
        pipeline._configured_cities.cache_clear()
