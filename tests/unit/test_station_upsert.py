from unittest.mock import MagicMock, patch

from aqi_pipeline.transform import pipeline


def test_station_upsert_deduplicates_station_ids():
    first = ("openaq_1", "Station 1", "delhi", "IN", 28.6, 77.2, "openaq", 1)
    duplicate = ("openaq_1", "Updated Station 1", "delhi", "IN", 28.6, 77.2, "openaq", 1)
    second = ("openaq_2", "Station 2", "mumbai", "IN", 19.1, 72.9, "openaq", 2)
    connection = MagicMock()
    connection.__enter__.return_value = connection

    with (
        patch.object(pipeline, "get_raw_connection", return_value=connection),
        patch.object(pipeline.psycopg2.extras, "execute_values") as execute_values,
    ):
        pipeline._upsert_station_rows([first, duplicate, second])

    assert execute_values.call_args.args[2] == [first, second]
    connection.commit.assert_called_once()
