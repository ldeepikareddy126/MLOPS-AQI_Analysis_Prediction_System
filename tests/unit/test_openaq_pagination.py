from unittest.mock import Mock

import requests

from aqi_pipeline.collectors.openaq import (
    MEASUREMENT_PAGE_SIZE,
    OpenAQError,
    _get,
    _get_pages,
    _resume_cursor,
    _retry_wait,
)


def test_get_pages_requests_every_full_page():
    session = Mock()
    first = Mock()
    first.status_code = 200
    first.json.return_value = {"results": [{"id": 1}, {"id": 2}]}
    second = Mock()
    second.status_code = 200
    second.json.return_value = {"results": [{"id": 3}]}
    session.get.side_effect = [first, second]

    pages = list(_get_pages(session, "/locations", {"limit": 2}))

    assert [page[3] for page in pages] == [1, 2]
    assert [len(page[0]["results"]) for page in pages] == [2, 1]
    assert session.get.call_args_list[0].kwargs["params"] == {"limit": 2, "page": 1}
    assert session.get.call_args_list[1].kwargs["params"] == {"limit": 2, "page": 2}


def test_get_pages_stops_after_short_page():
    session = Mock()
    response = Mock()
    response.status_code = 200
    response.json.return_value = {"results": [{"id": 1}]}
    session.get.return_value = response

    pages = list(_get_pages(session, "/locations", {"limit": 2}))

    assert len(pages) == 1
    session.get.assert_called_once()


def test_get_pages_can_resume_from_a_later_page():
    session = Mock()
    response = Mock()
    response.status_code = 200
    response.json.return_value = {"results": [{"id": 3}]}
    session.get.return_value = response

    pages = list(_get_pages(session, "/locations", {"limit": 2}, start_page=3))

    assert [page[3] for page in pages] == [3]
    assert session.get.call_args.kwargs["params"] == {"limit": 2, "page": 3}


def test_resume_cursor_skips_full_pages_and_detects_completed_sensor():
    assert _resume_cursor({1: MEASUREMENT_PAGE_SIZE, 2: MEASUREMENT_PAGE_SIZE}) == (3, False)
    assert _resume_cursor({1: MEASUREMENT_PAGE_SIZE, 2: 17}) == (2, True)


def test_retry_wait_honors_open_aq_retry_after():
    retry_state = Mock()
    retry_state.outcome.exception.return_value = OpenAQError(
        "rate limited", retry_after=45
    )

    assert _retry_wait(retry_state) == 45


def test_retry_wait_uses_bounded_exponential_backoff_without_retry_after():
    retry_state = Mock()
    retry_state.outcome.exception.return_value = OpenAQError("temporary failure")
    retry_state.attempt_number = 4

    assert _retry_wait(retry_state) == 40


def test_get_retries_after_rate_limit_delay():
    session = Mock()
    rate_limited = Mock()
    rate_limited.status_code = 429
    rate_limited.headers = {"Retry-After": "0"}
    success = Mock()
    success.status_code = 200
    success.json.return_value = {"results": []}
    session.get.side_effect = [rate_limited, success]

    payload, status_code, _ = _get(session, "/locations", {"limit": 1})

    assert payload == {"results": []}
    assert status_code == 200
    assert session.get.call_count == 2


def test_get_retries_after_incomplete_response():
    session = Mock()
    success = Mock()
    success.status_code = 200
    success.json.return_value = {"results": []}
    session.get.side_effect = [
        requests.exceptions.ChunkedEncodingError("incomplete response"),
        success,
    ]

    payload, status_code, _ = _get(session, "/locations", {"limit": 1})

    assert payload == {"results": []}
    assert status_code == 200
    assert session.get.call_count == 2
