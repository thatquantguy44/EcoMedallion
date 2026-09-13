"""Phase-6 global inflation/policy engines + the Power BI catalog."""

from __future__ import annotations

import pytest

from fred_pipeline.global_config import (
    GlobalConfig,
    GlobalConfigError,
    GlobalInflationDef,
    GlobalPolicyRateDef,
    load_global_config,
)
from fred_pipeline.global_views import (
    POWERBI_CATALOG,
    compute_global_inflation,
    compute_global_policy_rates,
    powerbi_catalog_rows,
)
from tests.test_terminal_views import _monthly, _row

# ---- config loader ---------------------------------------------------------


def test_global_loader_missing_and_repo_file(tmp_path):
    cfg = load_global_config(str(tmp_path / "nope.yml"))
    assert cfg.inflation == () and cfg.policy_rates == ()
    cfg = load_global_config("config/global_series.yml")
    us = next(d for d in cfg.inflation if d.iso3 == "USA")
    assert us.series_id == "CPIAUCSL" and us.transform == "yoy_from_index"
    assert us.target == pytest.approx(2.0)
    assert any(d.series_id == "CP0000EZ19M086NEST" for d in cfg.inflation)
    assert any(d.series_id == "SAU:FP.CPI.TOTL.ZG" for d in cfg.inflation)
    assert len(cfg.inflation) == 38
    assert {d.region for d in cfg.inflation} == {"AMER", "EMEA", "APAC"}
    assert any(d.series_id == "FEDFUNDS" for d in cfg.policy_rates)


@pytest.mark.parametrize(
    "body",
    [
        "inflation:\n  - {country: X, iso3: XXX, region: MARS, series_id: S}\n",
        "inflation:\n  - {country: X, iso3: XXX, region: AMER, series_id: S, transform: log}\n",
        (
            "inflation:\n  - {country: X, iso3: XXX, region: AMER, series_id: S}\n"
            "  - {country: Y, iso3: XXX, region: AMER, series_id: T}\n"
        ),  # dup iso3
        "policy_rates:\n  - {country: X, iso3: XXX, region: AMER, series_id: S, bogus: 1}\n",
    ],
)
def test_global_loader_rejects_malformed(tmp_path, body):
    p = tmp_path / "global.yml"
    p.write_text(body)
    with pytest.raises(GlobalConfigError):
        load_global_config(str(p))


# ---- global inflation --------------------------------------------------------


def _annual(series_id, values, start_year=2018):
    return [_row(series_id, f"{start_year + i}-01-01", v) for i, v in enumerate(values)]


def test_global_inflation_trend_streaks_and_target():
    cfg = GlobalConfig(
        inflation=(
            GlobalInflationDef(
                "Euro Area", "EMU", "EMEA", "EMU:FP.CPI.TOTL.ZG", target=2.0
            ),
        )
    )
    # 1.5 -> 1.5 (flat) -> 2.5 -> 5.0 (accelerating x2) -> 3.0 (cooling)
    rows = _annual("EMU:FP.CPI.TOTL.ZG", [1.5, 1.5, 2.5, 5.0, 3.0])
    out = compute_global_inflation(rows, cfg)
    assert [r["cpi_yoy_pct"] for r in out] == pytest.approx([1.5, 1.5, 2.5, 5.0, 3.0])
    assert [r["trend"] for r in out] == [
        None,
        "flat",
        "accelerating",
        "accelerating",
        "cooling",
    ]
    assert [r["streak"] for r in out] == [0, 0, 1, 2, -1]
    assert out[3]["vs_target_pp"] == pytest.approx(3.0)
    assert out[0]["region"] == "EMEA" and out[0]["change_pp"] is None


def test_global_inflation_yoy_from_index():
    cfg = GlobalConfig(
        inflation=(
            GlobalInflationDef(
                "United States",
                "USA",
                "AMER",
                "CPIAUCSL",
                transform="yoy_from_index",
                target=2.0,
            ),
        )
    )
    # 13 months of an index rising 0.25%/mo -> YoY ~ 1.0025^12 - 1
    rows = _monthly("CPIAUCSL", 2023, [100.0 * 1.0025**i for i in range(13)])
    out = compute_global_inflation(rows, cfg)
    (r,) = out  # only the 13th month has a year-ago match
    assert r["observation_date"] == "2024-01-01"
    assert r["cpi_yoy_pct"] == pytest.approx((1.0025**12 - 1) * 100.0)
    assert r["vs_target_pp"] == pytest.approx(r["cpi_yoy_pct"] - 2.0)


def test_global_inflation_absent_series_and_empty_config():
    cfg = GlobalConfig(
        inflation=(GlobalInflationDef("Japan", "JPN", "APAC", "JPN:FP.CPI.TOTL.ZG"),)
    )
    assert compute_global_inflation([_row("OTHER", "2024-01-01", 1.0)], cfg) == []
    assert compute_global_inflation([], GlobalConfig()) == []


# ---- global policy rates -------------------------------------------------------


def test_global_policy_rates_stance_and_real_rate():
    cfg = GlobalConfig(
        inflation=(
            GlobalInflationDef(
                "United States", "USA", "AMER", "USA:FP.CPI.TOTL.ZG", target=2.0
            ),
        ),
        policy_rates=(GlobalPolicyRateDef("United States", "USA", "AMER", "FEDFUNDS"),),
    )
    rows = _annual("USA:FP.CPI.TOTL.ZG", [3.0, 4.0], start_year=2022)
    rows += [
        _row("FEDFUNDS", "2022-06-01", 1.00),
        _row("FEDFUNDS", "2022-12-01", 4.00),  # +300bp -> hiking
        _row("FEDFUNDS", "2023-06-01", 4.00),  # hold, stance carried
        _row("FEDFUNDS", "2023-12-01", 3.50),  # -50bp -> cutting
    ]
    out = compute_global_policy_rates(rows, cfg)
    assert [r["change_bps"] for r in out] == [None, 300.0, 0.0, -50.0]
    assert [r["stance"] for r in out] == [None, "hiking", "hiking", "cutting"]
    assert [r["last_move_bps"] for r in out] == [None, 300.0, 300.0, -50.0]
    # real rate: policy minus the latest CPI print on-or-before each date
    assert out[0]["real_rate_pct"] == pytest.approx(1.00 - 3.0)
    assert out[3]["real_rate_pct"] == pytest.approx(3.50 - 4.0)


def test_global_policy_rates_no_inflation_pairing():
    cfg = GlobalConfig(
        policy_rates=(GlobalPolicyRateDef("Euro Area", "EMU", "EMEA", "ECBDFR"),)
    )
    rows = [_row("ECBDFR", "2024-01-01", 4.0), _row("ECBDFR", "2024-02-01", 4.0)]
    out = compute_global_policy_rates(rows, cfg)
    assert all(r["real_rate_pct"] is None for r in out)
    # flat print before any move -> on-hold
    assert out[1]["stance"] == "on-hold"


def test_global_policy_rates_pairs_annual_worldbank_cpi_with_midyear_bis_rate():
    cfg = GlobalConfig(
        inflation=(
            GlobalInflationDef(
                "Brazil", "BRA", "AMER", "BRA:FP.CPI.TOTL.ZG", target=3.0
            ),
        ),
        policy_rates=(GlobalPolicyRateDef("Brazil", "BRA", "AMER", "BIS:BR"),),
    )
    rows = [
        _row("BRA:FP.CPI.TOTL.ZG", "2025-01-01", 5.01675279604836),
        _row("BIS:BR", "2026-06-01", 14.25),
    ]

    out = compute_global_policy_rates(rows, cfg)

    assert out[0]["real_rate_pct"] == pytest.approx(14.25 - 5.01675279604836)


# ---- Power BI catalog ------------------------------------------------------------


def test_powerbi_catalog_rows_are_unique_and_typed():
    rows = powerbi_catalog_rows()
    names = [r["object_name"] for r in rows]
    assert len(names) == len(set(names))
    # 'view' is computed on read rather than materialized -- worth flagging to a
    # report author, who may want to import rather than DirectQuery it.
    assert all(
        r["object_type"] in {"dimension", "fact", "reference", "view"} for r in rows
    )
    assert all(r["grain"] and r["intended_visual"] and r["description"] for r in rows)


def test_powerbi_catalog_covers_gold_tables():
    """Every gold_* table AND view in the local schema must have a catalog row.

    gold.powerbi_catalog is what a report author browses to discover what the
    Gold layer offers (it is rendered as the documentation page of the Pipeline
    Health report), so an object missing from it is effectively invisible to
    them. Views count: the PIT and governance reports bind to several.

    The exempt list is deliberately short — only objects no report should be
    built on. Adding to it is a claim that the object is not report-facing;
    if a report binds to it, catalog it instead.
    """
    import re

    from fred_pipeline import local_store

    exempt = {
        # spec003 Phase 3: internal bookkeeping for incremental Gold rebuilds
        # (per-entity resumable computation state, last-build watermark) --
        # not a reportable object, no report should bind to it.
        "incremental_checkpoint",
        "build_watermark",
        # Superseded for reporting by the terminal views; kept for backward
        # compatibility with earlier consumers.
        "fred_macro_feature_daily",
        "fred_curve_spread",
        # Silver-backed convenience lenses; reports use the Gold tables that
        # are built from them.
        "v_latest_revised",
        "v_point_in_time",
        "v_series_latest_value",
        # SEC company financials — out of scope for the v1 Power BI suite
        # (manifests/sec_financials.yml carries three demo companies). Catalog
        # these when the SEC manifest is generated at scale.
        "fred_company_fundamentals",
        "fred_company_ratios",
        "v_company_ratio_ranks",
    }
    objects = set(
        re.findall(r"CREATE TABLE IF NOT EXISTS gold_(\w+)", local_store._SCHEMA)
    ) | set(re.findall(r"CREATE VIEW IF NOT EXISTS gold_(\w+)", local_store._SCHEMA))
    cataloged = {r["object_name"] for r in POWERBI_CATALOG}
    missing = objects - cataloged - exempt
    assert not missing, (
        "Gold objects missing from POWERBI_CATALOG (report authors cannot "
        f"discover them): {sorted(missing)}"
    )


def test_powerbi_catalog_has_no_phantom_entries():
    """The inverse guard: a catalog row for an object that no longer exists
    sends a report author to a table they cannot query."""
    import re

    from fred_pipeline import local_store

    objects = set(
        re.findall(r"CREATE TABLE IF NOT EXISTS gold_(\w+)", local_store._SCHEMA)
    ) | set(re.findall(r"CREATE VIEW IF NOT EXISTS gold_(\w+)", local_store._SCHEMA))
    phantom = {r["object_name"] for r in POWERBI_CATALOG} - objects
    assert not phantom, (
        f"POWERBI_CATALOG lists objects that do not exist in Gold: {sorted(phantom)}"
    )
