import json
import math

from aqi_pipeline.quality.ops import json_dumps


def test_json_dumps_converts_non_finite_numbers_to_null():
    result = json.loads(
        json_dumps({"nan": math.nan, "positive_infinity": math.inf, "nested": [1.0, -math.inf]})
    )

    assert result == {
        "nan": None,
        "positive_infinity": None,
        "nested": [1.0, None],
    }
