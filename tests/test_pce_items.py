"""Tests for PCE item-level BEA integration.

Covers config/inflation_items.yml PCE/SA tree validation and
compute_inflation_explorer behaviour with BEA-sourced PCE rows.
"""

from __future__ import annotations

from datetime import date

import yaml

from fred_pipeline.inflation_config import (
    InflationItemDef,
    _parse_items,
    load_inflation_items,
)
from fred_pipeline.sources.bea import _parse_series_id
from fred_pipeline.terminal_views import compute_inflation_explorer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BEA_PCE_SERIES = [
    "NIUnderlyingDetail:U20404:2:M",
    "NIUnderlyingDetail:U20404:3:M",
    "NIUnderlyingDetail:U20404:4:M",
    "NIUnderlyingDetail:U20404:23:M",
    "NIUnderlyingDetail:U20404:38:M",
    "NIUnderlyingDetail:U20404:62:M",
    "NIUnderlyingDetail:U20404:72:M",
    "NIUnderlyingDetail:U20404:73:M",
    "NIUnderlyingDetail:U20404:104:M",
    "NIUnderlyingDetail:U20404:113:M",
    "NIUnderlyingDetail:U20404:120:M",
    "NIUnderlyingDetail:U20404:150:M",
    "NIUnderlyingDetail:U20404:151:M",
    "NIUnderlyingDetail:U20404:152:M",
    "NIUnderlyingDetail:U20404:172:M",
    "NIUnderlyingDetail:U20404:190:M",
    "NIUnderlyingDetail:U20404:209:M",
    "NIUnderlyingDetail:U20404:234:M",
    "NIUnderlyingDetail:U20404:252:M",
    "NIUnderlyingDetail:U20404:280:M",
    "NIUnderlyingDetail:U20404:342:M",
]

FRED_PCE_SERIES = ["PCEPI", "PCEPILFE"]

# Approximate PCE expenditure weights for the 15 waterfall items.
_WATERFALL_LINES = {
    4,
    23,
    38,
    62,
    73,
    104,
    113,
    120,
    152,
    172,
    190,
    209,
    234,
    252,
    280,
    342,
}


def _load_pce_items() -> list[InflationItemDef]:
    with open("config/inflation_items.yml", "r") as fh:
        data = yaml.safe_load(fh)
    all_items = _parse_items(data["items"], source="config/inflation_items.yml")
    return [i for i in all_items if i.basket == "PCE"]


def _make_bea_row(series_id: str, obs_date: date, value: float) -> dict:
    return {
        "series_id": series_id,
        "observation_date": obs_date.isoformat(),
        "value": value,
        "is_missing": False,
        "realtime_start": "",
    }


def _make_fred_row(series_id: str, obs_date: date, value: float) -> dict:
    return {
        "series_id": series_id,
        "observation_date": obs_date.isoformat(),
        "value": value,
        "is_missing": False,
        "realtime_start": None,
    }


def _monthly_series(
    series_id: str,
    n_months: int,
    base: float = 100.0,
    growth: float = 0.002,
    start: date = date(2021, 1, 1),
    *,
    fred: bool = False,
) -> list[dict]:
    rows = []
    v = base
    d = start
    make = _make_fred_row if fred else _make_bea_row
    for _ in range(n_months):
        rows.append(make(series_id, d, round(v, 4)))
        v *= 1 + growth
        if d.month == 12:
            d = date(d.year + 1, 1, 1)
        else:
            d = date(d.year, d.month + 1, 1)
    return rows


# ---------------------------------------------------------------------------
# Config validation tests
# ---------------------------------------------------------------------------


def test_pce_tree_parses_without_error():
    """Loading the actual config file raises no InflationConfigError."""
    items = load_inflation_items("config/inflation_items.yml")
    pce = [i for i in items if i.basket == "PCE"]
    assert len(pce) > 2  # more than just headline + core


def test_pce_tree_has_headline_and_core():
    items = _load_pce_items()
    sids = {i.series_id for i in items}
    assert "PCEPI" in sids
    assert "PCEPILFE" in sids


def test_pce_tree_contains_all_bea_series():
    items = _load_pce_items()
    sids = {i.series_id for i in items}
    for sid in BEA_PCE_SERIES:
        assert sid in sids, f"{sid} missing from PCE tree"


def test_pce_headline_is_level_0():
    items = _load_pce_items()
    root = next(i for i in items if i.series_id == "PCEPI")
    assert root.level == 0
    assert root.parent == ""


def test_pce_core_is_child_of_headline():
    items = _load_pce_items()
    core = next(i for i in items if i.series_id == "PCEPILFE")
    assert core.level == 1
    assert core.parent == "PCEPI"


def test_bea_goods_is_level1_under_pcepi():
    items = _load_pce_items()
    goods = next(i for i in items if i.series_id == "NIUnderlyingDetail:U20404:2:M")
    assert goods.level == 1
    assert goods.parent == "PCEPI"


def test_bea_services_is_level1_under_pcepi():
    items = _load_pce_items()
    svc = next(i for i in items if i.series_id == "NIUnderlyingDetail:U20404:150:M")
    assert svc.level == 1
    assert svc.parent == "PCEPI"


def test_bea_durable_goods_is_level2_under_goods():
    items = _load_pce_items()
    dur = next(i for i in items if i.series_id == "NIUnderlyingDetail:U20404:3:M")
    assert dur.level == 2
    assert dur.parent == "NIUnderlyingDetail:U20404:2:M"


def test_bea_housing_is_level3():
    items = _load_pce_items()
    housing = next(i for i in items if i.series_id == "NIUnderlyingDetail:U20404:152:M")
    assert housing.level == 3
    assert housing.parent == "NIUnderlyingDetail:U20404:151:M"


def test_pce_waterfall_items_have_weights():
    """Every waterfall item has a non-None weight."""
    items = _load_pce_items()
    wf = [i for i in items if i.waterfall]
    assert len(wf) > 0, "Expected at least one waterfall item in PCE tree"
    for i in wf:
        assert i.weight is not None, f"{i.series_id} is waterfall but has no weight"


def test_pce_waterfall_weights_sum_near_100():
    """Waterfall weights for the PCE/SA tree should sum to approximately 100%."""
    items = _load_pce_items()
    wf = [i for i in items if i.waterfall]
    total = sum(i.weight for i in wf)
    assert abs(total - 100.0) < 1.0, (
        f"PCE waterfall weights sum to {total}, expected ~100"
    )


def test_pce_non_waterfall_intermediates_have_no_weight():
    """Intermediate aggregates (Goods, Services, Durable Goods, etc.) should not have waterfall=True."""
    items = _load_pce_items()
    # Goods, durables, nondurables, services, and household services are aggregates.
    intermediate_sids = {
        "NIUnderlyingDetail:U20404:2:M",
        "NIUnderlyingDetail:U20404:3:M",
        "NIUnderlyingDetail:U20404:72:M",
        "NIUnderlyingDetail:U20404:150:M",
        "NIUnderlyingDetail:U20404:151:M",
    }
    for i in items:
        if i.series_id in intermediate_sids:
            assert not i.waterfall, f"{i.series_id} should not be waterfall"


def test_bea_series_ids_parse_correctly():
    """Every BEA series_id in the PCE tree is well-formed for _parse_series_id."""
    items = _load_pce_items()
    for i in items:
        if i.series_id in BEA_PCE_SERIES:
            dataset, table, line, freq = _parse_series_id(i.series_id)
            assert dataset == "NIUnderlyingDetail"
            assert table == "U20404"
            assert line.isdigit()
            assert freq == "M"


def test_pce_tree_parent_referential_integrity():
    """Every parent reference in the PCE tree points to a known series_id."""
    items = _load_pce_items()
    sids = {i.series_id for i in items}
    for i in items:
        if i.parent:
            assert i.parent in sids, (
                f"{i.series_id} parent {i.parent!r} not in PCE tree"
            )


# ---------------------------------------------------------------------------
# compute_inflation_explorer with BEA PCE rows
# ---------------------------------------------------------------------------

START = date(2021, 1, 1)
N = 24  # 2 years of monthly data


def _pce_items() -> list[InflationItemDef]:
    return _load_pce_items()


def _minimal_pce_rows() -> list[dict]:
    """Synthetic latest_rows for PCEPI + a couple of BEA sub-items."""
    rows = []
    rows.extend(_monthly_series("PCEPI", N, base=100.0, growth=0.003, fred=True))
    rows.extend(
        _monthly_series("NIUnderlyingDetail:U20404:2:M", N, base=95.0, growth=0.003)
    )
    rows.extend(
        _monthly_series("NIUnderlyingDetail:U20404:150:M", N, base=98.0, growth=0.003)
    )
    return rows


def test_explorer_returns_pcepi_rows():
    items = _pce_items()
    rows = _minimal_pce_rows()
    result = compute_inflation_explorer(rows, items)
    sids = {r["series_id"] for r in result["explorer"]}
    assert "PCEPI" in sids


def test_explorer_returns_bea_goods_rows():
    items = _pce_items()
    rows = _minimal_pce_rows()
    result = compute_inflation_explorer(rows, items)
    sids = {r["series_id"] for r in result["explorer"]}
    assert "NIUnderlyingDetail:U20404:2:M" in sids


def test_explorer_basket_is_pce():
    items = _pce_items()
    rows = _minimal_pce_rows()
    result = compute_inflation_explorer(rows, items)
    for r in result["explorer"]:
        if r["series_id"] in ("PCEPI", "NIUnderlyingDetail:U20404:2:M"):
            assert r["basket"] == "PCE"
            assert r["sa_nsa"] == "SA"


def test_explorer_mom_computed_from_bea_levels():
    """MoM for BEA PCE goods should be close to the synthetic growth rate."""
    items = _pce_items()
    growth = 0.003
    rows = _monthly_series("NIUnderlyingDetail:U20404:2:M", N, base=95.0, growth=growth)
    result = compute_inflation_explorer(rows, items)
    goods_rows = [
        r
        for r in result["explorer"]
        if r["series_id"] == "NIUnderlyingDetail:U20404:2:M"
    ]
    # skip the first month (no prior)
    later_rows = [r for r in goods_rows if r["mom_pct"] is not None]
    assert len(later_rows) > 0
    for r in later_rows:
        # mom_pct is a decimal fraction (0.003), not a percent (0.3)
        assert abs(r["mom_pct"] - growth) < 0.001


def test_explorer_hierarchy_metadata():
    """hierarchy_level and parent_item are set correctly for BEA sub-items."""
    items = _pce_items()
    rows = []
    rows.extend(_monthly_series("PCEPI", N, base=100.0, fred=True))
    rows.extend(_monthly_series("NIUnderlyingDetail:U20404:2:M", N, base=95.0))
    rows.extend(_monthly_series("NIUnderlyingDetail:U20404:3:M", N, base=90.0))
    result = compute_inflation_explorer(rows, items)
    ex = result["explorer"]
    goods = next(r for r in ex if r["series_id"] == "NIUnderlyingDetail:U20404:2:M")
    durable = next(r for r in ex if r["series_id"] == "NIUnderlyingDetail:U20404:3:M")
    assert goods["hierarchy_level"] == 1
    assert goods["parent_item"] == "PCEPI"
    assert durable["hierarchy_level"] == 2
    assert durable["parent_item"] == "NIUnderlyingDetail:U20404:2:M"


def test_waterfall_not_produced_without_weights():
    """If BEA waterfall items have no data, contribution is empty for PCE."""
    items = _pce_items()
    # Only headline (no waterfall items ingested)
    rows = _monthly_series("PCEPI", N, base=100.0, fred=True)
    result = compute_inflation_explorer(rows, items)
    pce_contrib = [
        r
        for r in result["contribution"]
        if r["basket"] == "PCE" and not r["is_headline_total"]
    ]
    assert pce_contrib == []


def test_waterfall_produced_when_waterfall_items_present():
    """With waterfall items ingested, contribution rows appear for PCE."""
    items = _pce_items()
    rows = []
    rows.extend(_monthly_series("PCEPI", N, base=100.0, growth=0.003, fred=True))
    # One waterfall item: Motor Vehicles (line 4, weight=4.0)
    rows.extend(
        _monthly_series("NIUnderlyingDetail:U20404:4:M", N, base=90.0, growth=0.002)
    )
    result = compute_inflation_explorer(rows, items)
    pce_contrib = [r for r in result["contribution"] if r["basket"] == "PCE"]
    assert len(pce_contrib) > 0
    # Should have both waterfall rows and headline-total rows
    wf_rows = [r for r in pce_contrib if not r["is_headline_total"]]
    hl_rows = [r for r in pce_contrib if r["is_headline_total"]]
    assert len(wf_rows) > 0
    assert len(hl_rows) > 0


def test_mixed_fred_and_bea_sources():
    """PCEPI from FRED + BEA sub-items all flow through the same engine."""
    items = _pce_items()
    rows = []
    rows.extend(_monthly_series("PCEPI", N, base=100.0, fred=True))
    rows.extend(_monthly_series("PCEPILFE", N, base=98.0, fred=True))
    rows.extend(_monthly_series("NIUnderlyingDetail:U20404:2:M", N, base=95.0))
    rows.extend(_monthly_series("NIUnderlyingDetail:U20404:150:M", N, base=98.0))
    rows.extend(
        _monthly_series("NIUnderlyingDetail:U20404:152:M", N, base=99.0)
    )  # Housing
    result = compute_inflation_explorer(rows, items)
    sids = {r["series_id"] for r in result["explorer"]}
    assert "PCEPI" in sids
    assert "PCEPILFE" in sids
    assert "NIUnderlyingDetail:U20404:2:M" in sids
    assert "NIUnderlyingDetail:U20404:150:M" in sids
    assert "NIUnderlyingDetail:U20404:152:M" in sids


def test_contribution_pp_uses_weight():
    """contribution_pp for BEA waterfall item = weight × MoM."""
    items = _pce_items()
    rows = []
    rows.extend(_monthly_series("PCEPI", N, base=100.0, growth=0.003, fred=True))
    growth = 0.004
    rows.extend(
        _monthly_series("NIUnderlyingDetail:U20404:4:M", N, base=90.0, growth=growth)
    )
    result = compute_inflation_explorer(rows, items)
    motor_rows = [
        r
        for r in result["explorer"]
        if r["series_id"] == "NIUnderlyingDetail:U20404:4:M"
    ]
    item = next(i for i in items if i.series_id == "NIUnderlyingDetail:U20404:4:M")
    for r in motor_rows:
        if r["mom_pct"] is not None and r["weight"] is not None:
            expected = item.weight * r["mom_pct"]
            assert abs(r["contribution_pp"] - expected) < 1e-9


def test_yoy_computed_for_bea_series():
    """YoY should be populated after 12 months of BEA data."""
    items = _pce_items()
    rows = _monthly_series(
        "NIUnderlyingDetail:U20404:172:M", N, base=100.0, growth=0.003
    )  # Health Care
    result = compute_inflation_explorer(rows, items)
    health_rows = [
        r
        for r in result["explorer"]
        if r["series_id"] == "NIUnderlyingDetail:U20404:172:M"
    ]
    rows_with_yoy = [r for r in health_rows if r["yoy_pct"] is not None]
    assert len(rows_with_yoy) > 0


def test_three_month_annualized_for_bea_series():
    """three_month_annualized should be non-null for BEA PCE items with 3+ months."""
    items = _pce_items()
    rows = _monthly_series(
        "NIUnderlyingDetail:U20404:73:M", N, base=100.0, growth=0.003
    )  # Food off-prem
    result = compute_inflation_explorer(rows, items)
    food_rows = [
        r
        for r in result["explorer"]
        if r["series_id"] == "NIUnderlyingDetail:U20404:73:M"
    ]
    rows_with_ann = [r for r in food_rows if r["three_month_annualized"] is not None]
    assert len(rows_with_ann) > 0


def test_bea_series_not_ingested_emits_no_rows():
    """If a BEA PCE series has no data, the explorer emits no rows for it."""
    items = _pce_items()
    rows = _monthly_series("PCEPI", N, base=100.0, fred=True)
    result = compute_inflation_explorer(rows, items)
    sids = {r["series_id"] for r in result["explorer"]}
    # None of the BEA sub-items (which have no data) should appear
    for sid in BEA_PCE_SERIES:
        assert sid not in sids


def test_full_pce_tree_all_items_rendered():
    """With all 21 PCE series ingested, explorer rows appear for every one."""
    items = _pce_items()
    rows = []
    all_sids = FRED_PCE_SERIES + BEA_PCE_SERIES
    for i, sid in enumerate(all_sids):
        fred = sid in FRED_PCE_SERIES
        rows.extend(_monthly_series(sid, N, base=100.0 + i, growth=0.003, fred=fred))
    result = compute_inflation_explorer(rows, items)
    rendered = {r["series_id"] for r in result["explorer"]}
    for sid in all_sids:
        assert sid in rendered, f"{sid} not in explorer output"


def test_full_pce_waterfall_all_15_items():
    """With all waterfall items ingested, each month has 15 contribution rows."""
    items = _pce_items()
    rows = []
    rows.extend(_monthly_series("PCEPI", N, base=100.0, growth=0.003, fred=True))
    # Inject all 15 waterfall items
    waterfall_items = [i for i in items if i.waterfall and i.basket == "PCE"]
    assert len(waterfall_items) == 16  # 4 durable + 4 nondurable + 7 services + 1 NPISH
    for j, item in enumerate(waterfall_items):
        rows.extend(
            _monthly_series(item.series_id, N, base=90.0 + j, growth=0.003 + j * 0.0001)
        )
    result = compute_inflation_explorer(rows, items)
    pce_wf = [
        r
        for r in result["contribution"]
        if r["basket"] == "PCE" and not r["is_headline_total"]
    ]
    # Should have 15 waterfall items × (N-1) months with a prior
    months_with_data = len({r["observation_date"] for r in pce_wf})
    assert months_with_data >= N - 1
    # Check each month has 15 contributions
    from collections import Counter

    counts = Counter(r["observation_date"] for r in pce_wf)
    for month, cnt in counts.items():
        assert cnt == 16, f"Month {month} has {cnt} waterfall rows, expected 16"
