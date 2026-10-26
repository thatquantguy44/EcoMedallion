from dataclasses import replace

import pytest

from fred_pipeline.catalogs.manifest import Manifest, load_manifests
from fred_pipeline.gold_config.global_config import load_global_config
from fred_pipeline.catalogs.series_geography import (
    SeriesGeographyError,
    build_initial_config_candidates,
)


def test_initial_config_candidates_resolve_the_six_approved_seed_records():
    candidates = build_initial_config_candidates(
        load_global_config("config/global_series.yml"),
        load_manifests("manifests"),
        evidence_ref="config/global_series.yml",
    )

    assert [
        (
            candidate.country_iso3,
            candidate.product,
            candidate.series_id,
            candidate.source,
        )
        for candidate in candidates
    ] == [
        ("USA", "inflation", "CPIAUCSL", "fred"),
        ("USA", "policy_rate", "FEDFUNDS", "fred"),
        ("CAN", "inflation", "CANCPIALLMINMEI", "fred"),
        ("CAN", "policy_rate", "BIS:CA", "bis"),
        ("AUS", "inflation", "AUS:FP.CPI.TOTL.ZG", "worldbank"),
        ("AUS", "policy_rate", "BIS:AU", "bis"),
    ]
    assert {candidate.status for candidate in candidates} == {"candidate"}
    assert {candidate.evidence_kind for candidate in candidates} == {"global_config"}
    assert all(candidate.evidence_hash for candidate in candidates)


def test_initial_config_candidates_are_deterministic_and_exclude_other_countries():
    config = load_global_config("config/global_series.yml")
    manifests = load_manifests("manifests")

    first = build_initial_config_candidates(config, manifests)
    second = build_initial_config_candidates(config, manifests)

    assert first == second
    assert {candidate.country_iso3 for candidate in first} == {"USA", "CAN", "AUS"}
    assert "MEX" not in {candidate.country_iso3 for candidate in first}


def test_initial_config_candidates_refuse_a_changed_selected_series_id():
    config = load_global_config("config/global_series.yml")
    changed = replace(config.inflation[0], series_id="NOT_CPIAUCSL")
    invalid_config = replace(config, inflation=(changed, *config.inflation[1:]))

    with pytest.raises(SeriesGeographyError, match="expected series_id"):
        build_initial_config_candidates(invalid_config, load_manifests("manifests"))


def test_initial_config_candidates_refuse_a_missing_selected_entry():
    config = load_global_config("config/global_series.yml")
    invalid_config = replace(config, inflation=config.inflation[1:])

    with pytest.raises(SeriesGeographyError, match="exactly one config entry"):
        build_initial_config_candidates(invalid_config, load_manifests("manifests"))


def test_initial_config_candidates_refuse_a_source_mismatch():
    config = load_global_config("config/global_series.yml")
    manifests = load_manifests("manifests")
    replacement = Manifest.from_dict(
        {
            "name": "replacement",
            "series": [
                {
                    "series_id": "CPIAUCSL",
                    "title": "replacement",
                    "frequency": "m",
                    "source": "bls",
                }
            ],
        }
    )
    manifests = [
        replacement if manifest.name == "inflation" else manifest
        for manifest in manifests
    ]

    with pytest.raises(SeriesGeographyError, match="expected source"):
        build_initial_config_candidates(config, manifests)


def test_initial_config_candidates_refuse_duplicate_manifest_series():
    config = load_global_config("config/global_series.yml")
    manifests = load_manifests("manifests")
    duplicate = Manifest.from_dict(
        {
            "name": "duplicate",
            "series": [
                {
                    "series_id": "CPIAUCSL",
                    "title": "duplicate",
                    "frequency": "m",
                    "source": "fred",
                }
            ],
        }
    )

    with pytest.raises(SeriesGeographyError, match="exactly one manifest series"):
        build_initial_config_candidates(config, [*manifests, duplicate])


def test_initial_config_candidates_hash_the_exact_config_evidence():
    config = load_global_config("config/global_series.yml")
    manifests = load_manifests("manifests")
    changed = replace(config.inflation[0], country="United States of America")
    changed_config = replace(config, inflation=(changed, *config.inflation[1:]))

    original = build_initial_config_candidates(config, manifests)[0]
    changed_candidate = build_initial_config_candidates(changed_config, manifests)[0]

    assert original.evidence_hash != changed_candidate.evidence_hash
