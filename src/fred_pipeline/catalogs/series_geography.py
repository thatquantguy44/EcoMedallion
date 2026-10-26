"""Deterministic, review-required geography candidates for series metadata.

The first release deliberately seeds only the six USA/CAN/AUS entries used by
the global inflation and policy-rate products. Configuration is evidence, not
approval: every generated record has ``status='candidate'``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from typing import Iterable

from fred_pipeline.catalogs.manifest import Manifest
from fred_pipeline.gold_config.global_config import (
    DEFAULT_GLOBAL_PATH,
    GlobalConfig,
    GlobalInflationDef,
    GlobalPolicyRateDef,
)

GlobalConfigEntry = GlobalInflationDef | GlobalPolicyRateDef


class SeriesGeographyError(ValueError):
    """Raised when geography evidence cannot safely resolve to one series."""


@dataclass(frozen=True)
class ConfigCandidateSelection:
    """One explicitly approved first-release config entry to inspect."""

    country_iso3: str
    product: str
    expected_series_id: str
    expected_source: str


INITIAL_CONFIG_CANDIDATE_SELECTIONS = (
    ConfigCandidateSelection("USA", "inflation", "CPIAUCSL", "fred"),
    ConfigCandidateSelection("USA", "policy_rate", "FEDFUNDS", "fred"),
    ConfigCandidateSelection("CAN", "inflation", "CANCPIALLMINMEI", "fred"),
    ConfigCandidateSelection("CAN", "policy_rate", "BIS:CA", "bis"),
    ConfigCandidateSelection("AUS", "inflation", "AUS:FP.CPI.TOTL.ZG", "worldbank"),
    ConfigCandidateSelection("AUS", "policy_rate", "BIS:AU", "bis"),
)


@dataclass(frozen=True)
class SeriesGeographyCandidate:
    """A candidate mapping that requires an explicit later review action."""

    source: str
    series_id: str
    country_iso3: str
    product: str
    geo_type: str
    geo_code: str
    status: str
    evidence_kind: str
    evidence_ref: str
    evidence_hash: str


def build_initial_config_candidates(
    config: GlobalConfig,
    manifests: Iterable[Manifest],
    *,
    evidence_ref: str = DEFAULT_GLOBAL_PATH,
) -> tuple[SeriesGeographyCandidate, ...]:
    """Return the six configured first-release geography candidates.

    Each selected config entry must still name its expected series, and that
    series must resolve exactly once to its expected source in the supplied
    manifest collection. This makes stale config or catalog identity changes a
    refusal rather than a silently changed geography mapping.
    """
    manifest_list = tuple(manifests)
    candidates = []
    for selection in INITIAL_CONFIG_CANDIDATE_SELECTIONS:
        entry = _selected_config_entry(config, selection)
        if entry.series_id != selection.expected_series_id:
            raise SeriesGeographyError(
                f"{selection.product} {selection.country_iso3} expected series_id "
                f"{selection.expected_series_id!r}, got {entry.series_id!r}"
            )
        source = validate_manifest_source(
            manifest_list,
            series_id=entry.series_id,
            expected_source=selection.expected_source,
        )
        evidence_hash = _evidence_hash(selection, entry, evidence_ref)
        candidates.append(
            SeriesGeographyCandidate(
                source=source,
                series_id=entry.series_id,
                country_iso3=entry.iso3,
                product=selection.product,
                geo_type="country",
                geo_code=entry.iso3,
                status="candidate",
                evidence_kind="global_config",
                evidence_ref=evidence_ref,
                evidence_hash=evidence_hash,
            )
        )
    return tuple(candidates)


def _selected_config_entry(
    config: GlobalConfig, selection: ConfigCandidateSelection
) -> GlobalConfigEntry:
    entries = (
        config.inflation if selection.product == "inflation" else config.policy_rates
    )
    matches = [entry for entry in entries if entry.iso3 == selection.country_iso3]
    if len(matches) != 1:
        raise SeriesGeographyError(
            f"{selection.product} {selection.country_iso3} must resolve to exactly "
            f"one config entry, found {len(matches)}"
        )
    return matches[0]


def validate_manifest_source(
    manifests: Iterable[Manifest], *, series_id: str, expected_source: str
) -> str:
    matches = [
        spec
        for manifest in manifests
        for spec in manifest.series
        if spec.series_id == series_id
    ]
    if len(matches) != 1:
        raise SeriesGeographyError(
            f"series_id {series_id!r} must resolve to exactly one manifest series, "
            f"found {len(matches)}"
        )
    source = matches[0].source
    if source != expected_source:
        raise SeriesGeographyError(
            f"series_id {series_id!r} expected source {expected_source!r}, "
            f"got {source!r}"
        )
    return source


def _evidence_hash(
    selection: ConfigCandidateSelection,
    entry: GlobalConfigEntry,
    evidence_ref: str,
) -> str:
    payload = {
        "evidence_ref": evidence_ref,
        "selection": asdict(selection),
        "config_entry": asdict(entry),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return sha256(encoded.encode("utf-8")).hexdigest()
