"""Regression coverage for observation-row pagination in every vintage path."""

from unittest import TestCase
from fred_pipeline.sources.fred import FredClient, FredAPIError


class ObservationPaginationTests(TestCase):
    def client(self, payloads):
        calls = []
        iterator = iter(payloads)
        client = object.__new__(FredClient)

        def request(endpoint, params):
            calls.append((endpoint, dict(params)))
            payload = next(iterator)
            if isinstance(payload, Exception):
                raise payload
            return payload

        client._request = request
        return client, calls

    def pages(self):
        return [
            {
                "count": 3,
                "offset": 0,
                "observations": [{"date": "2000-01-01"}, {"date": "2001-01-01"}],
            },
            {"count": 3, "offset": 2, "observations": [{"date": "2026-10-23"}]},
        ]

    def test_normal_request_reads_tail_and_retains_response_pages(self):
        pages = self.pages()
        client, calls = self.client(pages)
        result = client.get_observations("ANFCI")
        self.assertEqual(len(result["observations"]), 3)
        self.assertEqual(result["observations"][-1]["date"], "2026-10-23")
        self.assertEqual([p[1]["offset"] for p in calls], [0, 2])
        self.assertEqual(result["response_pages"], pages)

    def test_vintage_window_reads_all_observation_pages(self):
        client, calls = self.client(self.pages())
        client.get_vintage_dates = lambda _: ["2024-01-01", "2026-10-30"]
        result = client.get_observations_all_vintages("X")
        self.assertEqual(len(result["observations"]), 3)
        self.assertEqual(calls[-1][1]["realtime_end"], "9999-12-31")

    def test_cap_fallback_still_reads_all_rows(self):
        error = FredAPIError(
            "Exceeded maximum number of vintage dates", status_code=400
        )
        client, calls = self.client([error, *self.pages()])
        result = client.get_observations("X", realtime_start="1776-07-04")
        self.assertEqual(len(result["observations"]), 3)
        self.assertEqual(calls[-1][1]["offset"], 2)

    def test_missing_count_or_excessive_count_refused(self):
        for payload in ({"observations": []}, {"count": 1000001, "observations": []}):
            client, calls = self.client([payload])
            with self.assertRaises(FredAPIError):
                client.get_observations("X")
            self.assertEqual(len(calls), 1)

    def test_incomplete_or_drifting_pages_refused(self):
        for page in (
            {"count": 3, "offset": 2, "observations": []},
            {"count": 4, "offset": 2, "observations": [{"date": "x"}]},
            {"count": 3, "offset": 0, "observations": [{"date": "x"}]},
        ):
            client, _ = self.client([self.pages()[0], page])
            with self.assertRaises(FredAPIError):
                client.get_observations("X")

    def test_vintage_batches_cover_gaps_and_keep_final_interval_open(self):
        client, calls = self.client(
            [
                {"count": 0, "observations": []},
                {"count": 0, "observations": []},
            ]
        )
        client.get_vintage_dates = lambda _: ["2020-01-01", "2020-02-01", "2020-05-01"]
        client.get_observations_all_vintages("X", batch_size=2)
        self.assertEqual(calls[0][1]["realtime_end"], "2020-04-30")
        self.assertEqual(calls[1][1]["realtime_start"], "2020-05-01")
        self.assertEqual(calls[1][1]["realtime_end"], "9999-12-31")

    def test_equal_values_with_unknown_gap_do_not_manufacture_continuity(self):
        from fred_pipeline.sources.fred import _coalesce_observations

        rows = [
            {
                "date": "2020-01-01",
                "value": "1",
                "realtime_start": "2020-01-01",
                "realtime_end": "2020-02-01",
            },
            {
                "date": "2020-01-01",
                "value": "1",
                "realtime_start": "2020-03-01",
                "realtime_end": "9999-12-31",
            },
        ]
        self.assertEqual(len(_coalesce_observations(rows)), 2)

    def test_vintage_window_reserves_boundary_state_under_api_cap(self):
        from datetime import date, timedelta

        client, calls = self.client(
            [{"count": 0, "observations": []}, {"count": 0, "observations": []}]
        )
        dates = [
            (date(2000, 1, 1) + timedelta(days=i)).isoformat() for i in range(2000)
        ]
        client.get_vintage_dates = lambda _: dates
        client.get_observations_all_vintages("X", batch_size=2000)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1][1]["realtime_start"], dates[1999])
