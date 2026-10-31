import pytest

from fred_pipeline.fred_client import FredAPIError, FredClient, RateLimiter


def _make_client(session, **kw):
    return FredClient(
        api_key="test-key",
        session=session,
        sleep=lambda _s: None,  # no real sleeping in tests
        **kw,
    )


def test_requires_api_key():
    with pytest.raises(FredAPIError):
        FredClient(api_key="", session=object())


def test_get_observations_success(
    fake_session_cls, fake_response_cls, observations_payload
):
    session = fake_session_cls([fake_response_cls(observations_payload)])
    client = _make_client(session)
    out = client.get_observations("DGS10")
    assert out == observations_payload
    # api_key + file_type are injected into every request
    params = session.calls[0]["params"]
    assert params["api_key"] == "test-key"
    assert params["file_type"] == "json"
    assert params["series_id"] == "DGS10"


def test_vintage_params_passed_through(fake_session_cls, fake_response_cls):
    session = fake_session_cls([fake_response_cls({"count": 0, "observations": []})])
    client = _make_client(session)
    client.get_observations(
        "GDP", realtime_start="1776-07-04", realtime_end="9999-12-31"
    )
    params = session.calls[0]["params"]
    assert params["realtime_start"] == "1776-07-04"
    assert params["realtime_end"] == "9999-12-31"


def test_retries_on_transient_then_succeeds(fake_session_cls, fake_response_cls):
    session = fake_session_cls(
        [
            fake_response_cls(None, status_code=503),
            fake_response_cls(None, status_code=500),
            fake_response_cls({"count": 0, "observations": []}, status_code=200),
        ]
    )
    client = _make_client(session, max_retries=5)
    out = client.get_observations("DGS10")
    assert out == {"count": 0, "observations": []}
    assert len(session.calls) == 3


def test_non_retryable_error_raises_immediately(fake_session_cls, fake_response_cls):
    session = fake_session_cls(
        [
            fake_response_cls(
                {"error_message": "Bad Request. Series does not exist."},
                status_code=400,
            ),
        ]
    )
    client = _make_client(session)
    with pytest.raises(FredAPIError) as exc:
        client.get_observations("NOPE")
    assert exc.value.status_code == 400
    assert len(session.calls) == 1


def test_exhausts_retries_and_raises(fake_session_cls, fake_response_cls):
    session = fake_session_cls(
        [fake_response_cls(None, status_code=503) for _ in range(3)]
    )
    client = _make_client(session, max_retries=2)
    with pytest.raises(FredAPIError):
        client.get_observations("DGS10")
    assert len(session.calls) == 3  # initial + 2 retries


_VINTAGE_CAP_BODY = {
    "error_message": "Bad Request. Exceeded maximum number of vintage dates "
    "allowed. The maximum is 2000."
}


def test_vintage_cap_falls_back_to_bounded_window(fake_session_cls, fake_response_cls):
    session = fake_session_cls(
        [
            fake_response_cls(_VINTAGE_CAP_BODY, status_code=400),  # full window caps
            fake_response_cls(
                {
                    "count": 1,
                    "observations": [
                        {
                            "date": "2024-01-01",
                            "value": "1",
                            "realtime_start": "2020-01-01",
                            "realtime_end": "9999-12-31",
                        }
                    ],
                }
            ),  # bounded succeeds
        ]
    )
    client = _make_client(session)
    out = client.get_observations(
        "ICSA", realtime_start="1776-07-04", realtime_end="9999-12-31"
    )
    assert out["observations"]
    # the retry narrowed realtime_start away from the full-history sentinel
    assert session.calls[1]["params"]["realtime_start"] != "1776-07-04"


def test_vintage_cap_final_fallback_to_latest_only(fake_session_cls, fake_response_cls):
    caps = [fake_response_cls(_VINTAGE_CAP_BODY, status_code=400) for _ in range(4)]
    session = fake_session_cls(
        caps + [fake_response_cls({"count": 0, "observations": []})]
    )
    client = _make_client(session)
    out = client.get_observations(
        "ICSA", realtime_start="1776-07-04", realtime_end="9999-12-31"
    )
    assert out == {"count": 0, "observations": []}
    # last resort drops the realtime window entirely (latest revision only)
    assert "realtime_start" not in session.calls[-1]["params"]


def test_non_vintage_400_still_raises(fake_session_cls, fake_response_cls):
    # a 400 that is NOT the vintage cap must not trigger the fallback
    session = fake_session_cls(
        [
            fake_response_cls(
                {"error_message": "Bad Request. Series does not exist."},
                status_code=400,
            ),
        ]
    )
    client = _make_client(session)
    with pytest.raises(FredAPIError):
        client.get_observations(
            "NOPE", realtime_start="1776-07-04", realtime_end="9999-12-31"
        )
    assert len(session.calls) == 1  # no fallback attempts


def test_get_vintage_dates_paginates(fake_session_cls, fake_response_cls):
    session = fake_session_cls(
        [
            fake_response_cls({"vintage_dates": ["2020-01-01", "2020-06-01"]}),
            fake_response_cls({"vintage_dates": ["2021-01-01"]}),
        ]
    )
    client = FredClient(api_key="k", session=session, sleep=lambda s: None)
    # tiny page size forces a second page
    dates = client.get_vintage_dates("ICSA", page_size=2)
    assert dates == ["2020-01-01", "2020-06-01", "2021-01-01"]


def test_all_vintages_batches_and_coalesces(fake_session_cls, fake_response_cls):
    # 4 vintage dates, batch_size 2 -> 1 vintage-date call + 2 observation calls
    vintage_dates = fake_response_cls(
        {"vintage_dates": ["2020-01-01", "2020-02-01", "2020-03-01", "2020-03-02"]}
    )
    # batch 1 (windows 01-01..02-01): value 100 across both vintages (clipped)
    batch1 = fake_response_cls(
        {
            "count": 1,
            "observations": [
                {
                    "date": "2019-12-01",
                    "value": "100",
                    "realtime_start": "2020-01-01",
                    "realtime_end": "2020-02-29",
                },
            ],
        }
    )
    # batch 2 (window 03-01): value revised to 105
    batch2 = fake_response_cls(
        {
            "count": 2,
            "observations": [
                {
                    "date": "2019-12-01",
                    "value": "100",
                    "realtime_start": "2020-03-01",
                    "realtime_end": "2020-03-01",
                },
                {
                    "date": "2019-12-01",
                    "value": "105",
                    "realtime_start": "2020-03-02",
                    "realtime_end": "9999-12-31",
                },
            ],
        }
    )
    session = fake_session_cls([vintage_dates, batch1, batch2])
    client = FredClient(api_key="k", session=session, sleep=lambda s: None)
    out = client.get_observations_all_vintages("X", batch_size=2)

    obs = out["observations"]
    # coalescing collapses the repeated value=100 vintages into one, keeps the 105
    values = [(o["value"], o["realtime_start"]) for o in obs]
    assert ("100", "2020-01-01") in values
    assert ("105", "2020-03-02") in values
    assert sum(1 for v, _ in values if v == "100") == 1  # not fragmented
    assert len(session.calls) == 3


def test_coalesce_observations_merges_equal_runs():
    from fred_pipeline.fred_client import _coalesce_observations

    rows = [
        {
            "date": "d",
            "value": "1",
            "realtime_start": "2020-01-01",
            "realtime_end": "2020-02-01",
        },
        {
            "date": "d",
            "value": "1",
            "realtime_start": "2020-02-02",
            "realtime_end": "2020-03-01",
        },
        {
            "date": "d",
            "value": "2",
            "realtime_start": "2020-03-02",
            "realtime_end": "9999-12-31",
        },
    ]
    out = _coalesce_observations(rows)
    assert len(out) == 2
    assert out[0]["value"] == "1"
    assert out[0]["realtime_end"] == "2020-03-01"  # window extended across the run
    assert out[1]["value"] == "2"


def test_metadata_extraction(fake_session_cls, fake_response_cls):
    session = fake_session_cls(
        [
            fake_response_cls({"seriess": [{"id": "DGS10", "title": "10Y"}]}),
        ]
    )
    client = _make_client(session)
    meta = client.get_series_metadata("DGS10")
    assert meta["title"] == "10Y"


def test_list_series_paginates(fake_session_cls, fake_response_cls):
    # page 1 fills the page (limit=2) -> keep going; page 2 short -> stop
    session = fake_session_cls(
        [
            fake_response_cls({"seriess": [{"id": "A"}, {"id": "B"}]}),
            fake_response_cls({"seriess": [{"id": "C"}]}),
        ]
    )
    client = _make_client(session)
    out = client.list_series(
        "category/series", {"category_id": 1}, max_results=None, page_size=2
    )
    assert [s["id"] for s in out] == ["A", "B", "C"]
    assert len(session.calls) == 2
    # offset advanced on the second call
    assert session.calls[1]["params"]["offset"] == 2


def test_list_series_respects_max_results(fake_session_cls, fake_response_cls):
    session = fake_session_cls(
        [
            fake_response_cls({"seriess": [{"id": "A"}, {"id": "B"}]}),
        ]
    )
    client = _make_client(session)
    out = client.search_series("treasury", max_results=1, page_size=2)
    assert [s["id"] for s in out] == ["A"]


def test_get_release_dates_paginates(fake_session_cls, fake_response_cls):
    # page 1 fills the page (limit=2) -> keep going; page 2 short -> stop
    session = fake_session_cls(
        [
            fake_response_cls(
                {
                    "release_dates": [
                        {"release_id": 10, "release_name": "CPI", "date": "2026-08-12"},
                        {
                            "release_id": 50,
                            "release_name": "Employment",
                            "date": "2026-08-07",
                        },
                    ]
                }
            ),
            fake_response_cls(
                {
                    "release_dates": [
                        {"release_id": 53, "release_name": "GDP", "date": "2026-08-27"},
                    ]
                }
            ),
        ]
    )
    client = _make_client(session)
    out = client.get_release_dates(
        realtime_start="2026-07-17",
        realtime_end="2026-11-17",
        limit=2,
    )
    assert [r["release_id"] for r in out] == [10, 50, 53]
    assert len(session.calls) == 2
    assert session.calls[1]["params"]["offset"] == 2


def test_get_release_dates_passes_params(fake_session_cls, fake_response_cls):
    session = fake_session_cls([fake_response_cls({"release_dates": []})])
    client = _make_client(session)
    client.get_release_dates(
        realtime_start="2026-07-17",
        realtime_end="2026-11-17",
        include_release_dates_with_no_data=True,
        sort_order="asc",
    )
    params = session.calls[0]["params"]
    assert params["realtime_start"] == "2026-07-17"
    assert params["realtime_end"] == "2026-11-17"
    assert params["sort_order"] == "asc"
    assert params["include_release_dates_with_no_data"] == "true"


def test_get_release_dates_without_release_id_hits_plural_endpoint(
    fake_session_cls,
    fake_response_cls,
):
    session = fake_session_cls([fake_response_cls({"release_dates": []})])
    client = _make_client(session)
    client.get_release_dates(realtime_start="2026-07-17", realtime_end="2026-11-17")
    assert session.calls[0]["url"].endswith("/releases/dates")
    assert "release_id" not in session.calls[0]["params"]


def test_get_release_dates_with_release_id_hits_singular_scoped_endpoint(
    fake_session_cls,
    fake_response_cls,
):
    """release_id set -> the singular, server-side-scoped endpoint (fast),
    not the unfiltered plural one (confirmed live to degrade badly under
    offset pagination — see get_release_dates's docstring)."""
    session = fake_session_cls(
        [
            fake_response_cls(
                {
                    "release_dates": [
                        {"release_id": 101, "date": "2026-07-29"},
                    ]
                }
            )
        ]
    )
    client = _make_client(session)
    out = client.get_release_dates(
        release_id=101,
        realtime_start="2026-07-17",
        realtime_end="2026-11-17",
    )
    assert session.calls[0]["url"].endswith("/release/dates")
    assert session.calls[0]["params"]["release_id"] == 101
    assert out == [{"release_id": 101, "date": "2026-07-29"}]


def test_rate_limiter_sleeps_when_too_fast():
    slept = []
    now = [0.0]
    rl = RateLimiter(per_minute=60, _sleep=slept.append, _now=lambda: now[0])
    rl.acquire()  # first call, no sleep
    now[0] = 0.1  # only 0.1s elapsed, need 1.0s
    rl.acquire()
    assert slept and slept[-1] == pytest.approx(0.9, abs=1e-6)


def test_rate_limiter_serializes_concurrent_acquires():
    """Threaded extraction workers share one RateLimiter; acquire() must not
    race (e.g. two threads both reading a stale _last_call and skipping the
    sleep). A real clock/sleep exercises actual lock contention."""
    import threading
    import time as real_time

    rl = RateLimiter(per_minute=1000)  # 60ms min interval
    calls: list[float] = []
    lock = threading.Lock()

    def worker():
        rl.acquire()
        with lock:
            calls.append(real_time.monotonic())

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    calls.sort()
    min_interval = 60.0 / 1000
    gaps = [b - a for a, b in zip(calls, calls[1:])]
    assert all(g >= min_interval * 0.9 for g in gaps)
