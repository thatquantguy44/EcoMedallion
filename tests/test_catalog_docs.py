"""Keeps docs/catalog/*.md's stated active-series counts honest.

This is the gate for the drift docs/catalog/README.md warns about: a catalog
page is hand-written prose, and nothing stops it from staying frozen at
whatever count it had when last edited while manifests/*.yml moves on. This
runs in CI on every push/PR (see .github/workflows/ci.yml's `pytest -q`
step), so a manifest change that flips a source's active count without a
matching catalog-page edit fails here rather than shipping undetected.

FRED is deliberately excluded: its own catalog page says outright that full
enumeration doesn't fit (thousands of series across many manifests), so
there is no single stated count to check against reality.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from fred_pipeline.manifest import all_series, load_manifests

CATALOG_DIR = Path("docs/catalog")
MANIFESTS_DIR = "manifests"

# source key -> catalog markdown filename. Keep in sync with
# docs/catalog/README.md's source table.
SOURCE_TO_DOC = {
    "bea": "bea.md",
    "bis": "bis.md",
    "bls": "bls.md",
    "census": "census.md",
    "ecb": "ecb.md",
    "eia": "eia.md",
    "ishares": "ishares.md",
    "sec": "sec.md",
    "stooq": "stooq.md",
    "tiingo": "tiingo.md",
    "treasury": "treasury.md",
    "worldbank": "worldbank.md",
}

# Sources whose catalog page exists but is explicitly exempt from the
# active-count check (documented as too large to state a single number
# meaningfully). Still required to exist -- see
# test_every_active_source_has_a_catalog_page.
COUNT_EXEMPT_SOURCES = {"fred"}

_ACTIVE_COUNT_RE = re.compile(r"(\d+)\s+active", re.IGNORECASE)


@pytest.fixture(scope="module")
def active_counts_by_source() -> dict[str, int]:
    active = [s for s in all_series(load_manifests(MANIFESTS_DIR)) if s.active]
    counts: dict[str, int] = {}
    for spec in active:
        counts[spec.source] = counts.get(spec.source, 0) + 1
    return counts


def _documented_active_count(doc_path: Path) -> int:
    """Read the count out of the page's '## Current Series/Coverage (N
    active ...)' heading -- not just the first 'N active' text anywhere in
    the file, since a caveats section can legitimately mention other counts
    (e.g. stooq.md's "61 tickers overlap ... active list")."""
    text = doc_path.read_text()
    for line in text.splitlines():
        if line.startswith("#") and (
            "Current Series" in line or "Current Coverage" in line
        ):
            match = _ACTIVE_COUNT_RE.search(line)
            if match:
                return int(match.group(1))
    raise AssertionError(
        f"{doc_path}: no '## Current Series (N active ...)' / "
        f"'## Current Coverage (N active ...)' heading found to check against "
        f"the real manifest count"
    )


@pytest.mark.parametrize("source", sorted(SOURCE_TO_DOC))
def test_catalog_doc_active_count_matches_manifests(source, active_counts_by_source):
    doc_path = CATALOG_DIR / SOURCE_TO_DOC[source]
    assert doc_path.exists(), f"missing catalog page for source {source!r}: {doc_path}"

    real_count = active_counts_by_source.get(source, 0)
    documented_count = _documented_active_count(doc_path)
    assert documented_count == real_count, (
        f"{doc_path} says {documented_count} active series but manifests/*.yml "
        f"currently has {real_count} active {source!r} series -- update the "
        f"doc (or the manifest, if the change was accidental) so they match."
    )


def test_every_active_source_has_a_catalog_page(active_counts_by_source):
    """A brand-new source (not just a new series on an existing source)
    should ship with a catalog page too, not silently go undocumented."""
    documented = set(SOURCE_TO_DOC) | COUNT_EXEMPT_SOURCES
    missing = set(active_counts_by_source) - documented
    assert not missing, (
        f"active manifest series use source(s) {sorted(missing)} with no "
        f"docs/catalog/<source>.md page -- add one and register it in "
        f"SOURCE_TO_DOC here plus docs/catalog/README.md's source table"
    )
