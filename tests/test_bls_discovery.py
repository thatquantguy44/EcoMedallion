import pytest

from fred_pipeline.catalogs.bls_discovery import (
    BLSDiscoveryError,
    BLSFlatFileClient,
    BLSSeriesRow,
    bls_manifest_to_yaml,
    build_bls_manifest_dict,
    filter_series_rows,
    filter_surveys,
    generate_bls_candidate_specs,
    infer_bls_category,
    inspect_series_catalog,
    parse_bls_series_flatfile,
    parse_column_filters,
    parse_surveys_payload,
)
from fred_pipeline.manifest import Manifest

CU_FLATFILE = (
    "series_id\tarea_code\titem_code\tseasonal\tperiodicity_code\tbase_period\t"
    "footnote_codes\tbegin_year\tbegin_period\tend_year\tend_period\tseries_title\n"
    "CUSR0000SA0\t0000\tSA0\tS\tR\t1982-84=100\t\t1947\tM01\t2026\tM06\t"
    "All items in U.S. city average, seasonally adjusted\n"
    "CUUR0000SA0\t0000\tSA0\tU\tR\t1982-84=100\t\t1913\tM01\t2026\tM06\t"
    "All items in U.S. city average, not seasonally adjusted\n"
    "CUSR0000SAF\t0000\tSAF\tS\tR\t1982-84=100\t\t1947\tM01\t2026\tM06\t"
    "Food and beverages in U.S. city average, seasonally adjusted\n"
    "\n"
)

SURVEYS_PAYLOAD = {
    "status": "REQUEST_SUCCEEDED",
    "Results": {
        "survey": [
            {"survey_abbreviation": "CU", "survey_name": "Consumer Price Index"},
            {
                "survey_abbreviation": "CE",
                "survey_name": "Employment, Hours, and Earnings",
            },
            {"survey_abbreviation": ""},
        ]
    },
}


class _Response:
    def __init__(self, *, text="", payload=None):
        self.status_code = 200
        self.text = text
        self._payload = payload

    def json(self):
        return self._payload


class _Session:
    def __init__(self, response):
        self._response = response
        self.calls = []

    def get(self, url, params=None, timeout=None, headers=None):
        self.calls.append(
            {"url": url, "params": params, "timeout": timeout, "headers": headers}
        )
        return self._response


def test_parse_bls_series_flatfile_maps_rows_and_skips_blank_lines():
    rows = parse_bls_series_flatfile(CU_FLATFILE)
    assert [r.series_id for r in rows] == ["CUSR0000SA0", "CUUR0000SA0", "CUSR0000SAF"]
    assert rows[0].series_title == "All items in U.S. city average, seasonally adjusted"
    assert rows[0].fields["area_code"] == "0000"
    assert rows[0].fields["seasonal"] == "S"
    assert "series_id" not in rows[0].fields
    assert "series_title" not in rows[0].fields


def test_parse_bls_series_flatfile_requires_series_id_column():
    with pytest.raises(BLSDiscoveryError, match="no series_id column"):
        parse_bls_series_flatfile("area_code\titem_code\n0000\tSA0\n")


def test_parse_bls_series_flatfile_empty_text_returns_empty_list():
    assert parse_bls_series_flatfile("") == []


def test_parse_surveys_payload_drops_blank_abbreviations_and_sorts():
    surveys = parse_surveys_payload(SURVEYS_PAYLOAD)
    assert [s.abbreviation for s in surveys] == ["CE", "CU"]
    assert surveys[1].name == "Consumer Price Index"


def test_filter_surveys_searches_and_caps():
    surveys = parse_surveys_payload(SURVEYS_PAYLOAD)
    assert [s.abbreviation for s in filter_surveys(surveys, search="employment")] == [
        "CE"
    ]
    assert len(filter_surveys(surveys, max_results=1)) == 1


def test_parse_column_filters_valid_and_invalid():
    assert parse_column_filters(["seasonal=S,U", "area_code=0000"]) == {
        "seasonal": {"S", "U"},
        "area_code": {"0000"},
    }
    assert parse_column_filters(None) == {}
    with pytest.raises(BLSDiscoveryError, match="expected COLUMN=VALUE"):
        parse_column_filters(["seasonal"])


def test_filter_series_rows_by_column_and_search():
    rows = parse_bls_series_flatfile(CU_FLATFILE)
    sa_only = filter_series_rows(rows, column_filters={"seasonal": {"S"}})
    assert [r.series_id for r in sa_only] == ["CUSR0000SA0", "CUSR0000SAF"]

    food = filter_series_rows(rows, search="food")
    assert [r.series_id for r in food] == ["CUSR0000SAF"]

    both = filter_series_rows(
        rows, column_filters={"seasonal": {"U"}}, search="all items"
    )
    assert [r.series_id for r in both] == ["CUUR0000SA0"]


def test_inspect_series_catalog_summarizes_columns_and_samples():
    rows = parse_bls_series_flatfile(CU_FLATFILE)
    summary = inspect_series_catalog(rows, sample_size=2)
    assert summary["row_count"] == 3
    by_column = {c["column"]: c for c in summary["columns"]}
    assert by_column["seasonal"]["sample_values"] == ["S", "U"]
    assert by_column["seasonal"]["distinct_count_in_sample"] == 2


def test_infer_bls_category_uses_title_keywords():
    cpi_row = BLSSeriesRow(
        series_id="CUSR0000SA0", series_title="Consumer Price Index item"
    )
    labor_row = BLSSeriesRow(series_id="LNS14000000", series_title="Unemployment Rate")
    assert infer_bls_category("CU", cpi_row) == "inflation"
    assert infer_bls_category("LN", labor_row) == "labor"


def test_generate_bls_candidate_specs_requires_valid_frequency():
    rows = parse_bls_series_flatfile(CU_FLATFILE)
    with pytest.raises(BLSDiscoveryError, match="--frequency must be one of"):
        generate_bls_candidate_specs("CU", rows, frequency="bogus")


def test_generate_bls_candidate_specs_bounds_and_excludes_existing():
    rows = parse_bls_series_flatfile(CU_FLATFILE)
    specs, skipped = generate_bls_candidate_specs(
        "CU",
        rows,
        frequency="m",
        max_results=2,
        exclude_ids={"CUUR0000SA0"},
    )
    assert [s.series_id for s in specs] == ["CUSR0000SA0"]
    assert all(not s.active for s in specs)
    assert all(s.source == "bls" for s in specs)
    assert all(s.vintage_enabled is False for s in specs)
    reasons = {row["series_id"]: row["reason"] for row in skipped}
    assert reasons["CUUR0000SA0"] == "already in manifest"


def test_build_and_serialize_bls_manifest_yaml_round_trips():
    rows = parse_bls_series_flatfile(CU_FLATFILE)
    specs, _skipped = generate_bls_candidate_specs(
        "CU", rows, frequency="m", max_results=1
    )
    manifest_dict = build_bls_manifest_dict(
        "bls_cu_candidates", specs, description="test"
    )
    yaml_text = bls_manifest_to_yaml(manifest_dict)
    manifest = Manifest.from_dict(manifest_dict)
    assert manifest.name == "bls_cu_candidates"
    assert "CUSR0000SA0" in yaml_text


def test_flatfile_client_fetches_series_catalog_with_user_agent_header():
    session = _Session(_Response(text=CU_FLATFILE))
    client = BLSFlatFileClient(
        base_url="https://example.test/pub/time.series",
        user_agent="test-agent/1.0",
        session=session,
        sleep=lambda _seconds: None,
    )
    rows = client.fetch_series_catalog("CU")
    assert [r.series_id for r in rows] == ["CUSR0000SA0", "CUUR0000SA0", "CUSR0000SAF"]
    assert session.calls == [
        {
            "url": "https://example.test/pub/time.series/cu/cu.series",
            "params": {},
            "timeout": 30,
            "headers": {"User-Agent": "test-agent/1.0"},
        }
    ]


def test_flatfile_client_lists_surveys_from_absolute_v2_url():
    session = _Session(_Response(payload=SURVEYS_PAYLOAD))
    client = BLSFlatFileClient(session=session, sleep=lambda _seconds: None)
    surveys = client.list_surveys()
    assert [s.abbreviation for s in surveys] == ["CE", "CU"]
    assert session.calls[0]["url"] == "https://api.bls.gov/publicAPI/v2/surveys"
