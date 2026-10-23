"""Reviewed schema registry and normalizer for historical ECB asset snapshots."""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping

import yaml

from fred_pipeline.ecb_eligible_assets import (
    ParsedAssets,
    _asset_key,
    _canonical_hash,
    _date_value,
    _number_value,
)

REGISTRY_VERSION = 1
DEFAULT_REGISTRY_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "ecb_eligible_asset_schemas.yml"
)

CANONICAL_FIELDS = (
    "isin_code",
    "other_reg_number",
    "haircut_category",
    "asset_type",
    "reference_market",
    "denomination",
    "issuance_date",
    "maturity_date",
    "issuer_csd",
    "coupon_definition",
    "coupon_rate",
    "issuer_name",
    "issuer_group",
    "issuer_residence",
    "guarantor_name",
    "guarantor_group",
    "guarantor_residence",
    "non_own_use_haircut",
    "own_use_haircut",
    "potentially_own_usable_covered_bond",
    "climate_factor",
)
NUMERIC_FIELDS = frozenset({"coupon_rate", "non_own_use_haircut", "own_use_haircut"})
DATE_FIELDS = frozenset({"issuance_date", "maturity_date"})
CODE_FIELDS = frozenset(
    {
        "isin_code",
        "other_reg_number",
        "haircut_category",
        "asset_type",
        "reference_market",
        "denomination",
        "issuer_csd",
        "coupon_definition",
        "issuer_group",
        "issuer_residence",
        "guarantor_group",
        "guarantor_residence",
        "potentially_own_usable_covered_bond",
        "climate_factor",
    }
)


class ECBHistoricalSchemaError(RuntimeError):
    """A historical snapshot does not match a reviewed schema contract."""


def column_set_hash(columns: tuple[str, ...] | list[str]) -> str:
    """Return the order-sensitive hash used by the archive inventory."""
    body = json.dumps(tuple(columns), ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class HistoricalSchema:
    schema_id: str
    source_schema_hash: str
    observed_from: date
    observed_through: date
    valid_to: date | None
    columns: tuple[str, ...]
    mappings: Mapping[str, str]
    intentionally_unmapped: Mapping[str, str]
    formats: tuple[str, ...]
    encodings: tuple[str, ...]
    notes: str


@dataclass(frozen=True)
class HistoricalSchemaRegistry:
    schemas: tuple[HistoricalSchema, ...]
    source_path: Path
    inventory_generated_at_utc: str
    inventory_snapshot_count: int

    def by_hash(self, source_schema_hash: str) -> HistoricalSchema:
        for schema in self.schemas:
            if schema.source_schema_hash == source_schema_hash:
                return schema
        raise ECBHistoricalSchemaError(
            "unregistered ECB historical schema " + source_schema_hash
        )

    def resolve(
        self, columns: tuple[str, ...] | list[str], *, snapshot_date: date | None = None
    ) -> HistoricalSchema:
        normalized = tuple(value.strip().lstrip("\ufeff") for value in columns)
        schema = self.by_hash(column_set_hash(normalized))
        if normalized != schema.columns:
            raise ECBHistoricalSchemaError(
                f"column hash collision or registry drift for {schema.schema_id}"
            )
        if snapshot_date is not None and (
            snapshot_date < schema.observed_from
            or (schema.valid_to is not None and snapshot_date > schema.valid_to)
        ):
            raise ECBHistoricalSchemaError(
                f"schema {schema.schema_id} is not registered for "
                f"{snapshot_date.isoformat()}"
            )
        return schema


_TOP_LEVEL_KEYS = {"version", "inventory", "canonical_fields", "schemas"}
_SCHEMA_KEYS = {
    "schema_id",
    "source_schema_hash",
    "observed_from",
    "observed_through",
    "valid_to",
    "formats",
    "encodings",
    "columns",
    "mappings",
    "intentionally_unmapped",
    "notes",
}


def _exact_keys(value: Mapping[str, Any], expected: set[str], label: str) -> None:
    actual = set(value)
    if actual != expected:
        raise ECBHistoricalSchemaError(
            f"{label} keys differ: missing={sorted(expected - actual)} "
            f"unknown={sorted(actual - expected)}"
        )


def _parse_date(value: Any, label: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ECBHistoricalSchemaError(f"invalid {label}: {value!r}") from exc


def _parse_optional_date(value: Any, label: str) -> date | None:
    return None if value is None else _parse_date(value, label)


def load_schema_registry(
    path: str | Path = DEFAULT_REGISTRY_PATH,
) -> HistoricalSchemaRegistry:
    """Load and fully validate the checked-in historical schema registry."""
    source_path = Path(path)
    try:
        raw = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ECBHistoricalSchemaError(
            f"cannot load ECB schema registry {source_path}: {exc}"
        ) from exc
    if not isinstance(raw, dict):
        raise ECBHistoricalSchemaError("ECB schema registry must be a mapping")
    _exact_keys(raw, _TOP_LEVEL_KEYS, "registry")
    if raw["version"] != REGISTRY_VERSION:
        raise ECBHistoricalSchemaError(
            f"unsupported ECB schema registry version {raw['version']!r}"
        )
    if tuple(raw["canonical_fields"]) != CANONICAL_FIELDS:
        raise ECBHistoricalSchemaError("canonical_fields differs from parser contract")
    inventory = raw["inventory"]
    if not isinstance(inventory, dict):
        raise ECBHistoricalSchemaError("inventory provenance must be a mapping")

    schemas: list[HistoricalSchema] = []
    ids: set[str] = set()
    hashes: set[str] = set()
    for index, item in enumerate(raw["schemas"]):
        if not isinstance(item, dict):
            raise ECBHistoricalSchemaError(f"schemas[{index}] must be a mapping")
        _exact_keys(item, _SCHEMA_KEYS, f"schemas[{index}]")
        schema_id = str(item["schema_id"])
        source_hash = str(item["source_schema_hash"])
        columns = tuple(str(value) for value in item["columns"])
        mappings = {str(key): str(value) for key, value in item["mappings"].items()}
        unmapped = {
            str(key): str(value)
            for key, value in item["intentionally_unmapped"].items()
        }
        observed_from = _parse_date(item["observed_from"], "observed_from")
        observed_through = _parse_date(item["observed_through"], "observed_through")
        valid_to = _parse_optional_date(item["valid_to"], "valid_to")
        if observed_through < observed_from:
            raise ECBHistoricalSchemaError(f"reversed date range for {schema_id}")
        if valid_to is not None and observed_through > valid_to:
            raise ECBHistoricalSchemaError(
                f"observations exceed validity range for {schema_id}"
            )
        if schema_id in ids or source_hash in hashes:
            raise ECBHistoricalSchemaError(f"duplicate schema id/hash for {schema_id}")
        ids.add(schema_id)
        hashes.add(source_hash)
        if len(columns) != len(set(columns)):
            raise ECBHistoricalSchemaError(f"duplicate columns in {schema_id}")
        calculated = column_set_hash(columns)
        if calculated != source_hash:
            raise ECBHistoricalSchemaError(
                f"source_schema_hash mismatch for {schema_id}: {calculated}"
            )
        mapped_sources = set(mappings)
        unmapped_sources = set(unmapped)
        if mapped_sources & unmapped_sources:
            raise ECBHistoricalSchemaError(
                f"mapped and unmapped columns overlap for {schema_id}"
            )
        covered = mapped_sources | unmapped_sources
        if covered != set(columns):
            raise ECBHistoricalSchemaError(
                f"column coverage differs for {schema_id}: "
                f"missing={sorted(set(columns) - covered)} "
                f"unknown={sorted(covered - set(columns))}"
            )
        unknown_targets = set(mappings.values()) - set(CANONICAL_FIELDS)
        if unknown_targets:
            raise ECBHistoricalSchemaError(
                f"unknown canonical targets for {schema_id}: {sorted(unknown_targets)}"
            )
        if len(mappings.values()) != len(set(mappings.values())):
            raise ECBHistoricalSchemaError(
                f"multiple source columns target one canonical field in {schema_id}"
            )
        if not {"isin_code", "other_reg_number"}.issubset(mappings.values()):
            raise ECBHistoricalSchemaError(
                f"identifier mappings are incomplete for {schema_id}"
            )
        schemas.append(
            HistoricalSchema(
                schema_id=schema_id,
                source_schema_hash=source_hash,
                observed_from=observed_from,
                observed_through=observed_through,
                valid_to=valid_to,
                columns=columns,
                mappings=mappings,
                intentionally_unmapped=unmapped,
                formats=tuple(str(value) for value in item["formats"]),
                encodings=tuple(str(value) for value in item["encodings"]),
                notes=str(item["notes"]),
            )
        )

    schemas.sort(key=lambda value: value.observed_from)
    for previous, current in zip(schemas, schemas[1:]):
        if previous.valid_to is None:
            raise ECBHistoricalSchemaError(
                f"open-ended schema is not last: {previous.schema_id}"
            )
        if current.observed_from <= previous.valid_to:
            raise ECBHistoricalSchemaError(
                f"schema date ranges overlap: {previous.schema_id}, {current.schema_id}"
            )
    return HistoricalSchemaRegistry(
        schemas=tuple(schemas),
        source_path=source_path,
        inventory_generated_at_utc=str(inventory.get("generated_at_utc") or ""),
        inventory_snapshot_count=int(inventory.get("snapshot_count") or 0),
    )


def validate_inventory_report(
    report: Mapping[str, Any], registry: HistoricalSchemaRegistry
) -> dict[str, Any]:
    """Fail closed when an inventory contains unknown or date-drifted schemas."""
    observed: set[str] = set()
    snapshot_count = 0
    for annual in report.get("years", []):
        for item in annual.get("schemas", []):
            source_hash = str(item["schema_hash"])
            schema = registry.by_hash(source_hash)
            columns = tuple(str(value) for value in item["columns"])
            if columns != schema.columns:
                raise ECBHistoricalSchemaError(
                    f"inventory columns differ for {schema.schema_id}"
                )
            first = _parse_date(item["first_snapshot_date"], "first_snapshot_date")
            last = _parse_date(item["last_snapshot_date"], "last_snapshot_date")
            if first < schema.observed_from or (
                schema.valid_to is not None and last > schema.valid_to
            ):
                raise ECBHistoricalSchemaError(
                    f"inventory date range {first}..{last} exceeds registry range "
                    f"for {schema.schema_id}"
                )
            observed.add(source_hash)
            snapshot_count += int(item.get("snapshot_count") or 0)
    return {
        "status": "valid",
        "registry_version": REGISTRY_VERSION,
        "registry_path": str(registry.source_path),
        "observed_schema_count": len(observed),
        "observed_schema_hashes": sorted(observed),
        "snapshot_count": snapshot_count,
    }


def _encoding(data: bytes) -> str:
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "utf-16"
    if b"\x00" in data[:200]:
        return "utf-16le"
    return "utf-8-sig"


def parse_historical_assets_csv(
    csv_bytes: bytes,
    *,
    snapshot_date: date,
    registry: HistoricalSchemaRegistry | None = None,
) -> ParsedAssets:
    """Normalize one registered historical full snapshot into canonical rows."""
    registry = registry or load_schema_registry()
    raw = gzip.decompress(csv_bytes) if csv_bytes[:2] == b"\x1f\x8b" else csv_bytes
    text = raw.decode(_encoding(raw))
    header = text.partition("\n")[0]
    delimiter = max(("\t", ";", ","), key=header.count)
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    columns = tuple(
        str(value or "").strip().lstrip("\ufeff") for value in (reader.fieldnames or [])
    )
    schema = registry.resolve(columns, snapshot_date=snapshot_date)
    rows: dict[str, dict[str, Any]] = {}
    for record_number, source_record in enumerate(reader, start=2):
        if None in source_record:
            raise ECBHistoricalSchemaError(
                f"row {record_number} has more values than registered columns"
            )
        source = {
            str(key).strip().lstrip("\ufeff"): str(value or "").strip()
            for key, value in source_record.items()
        }
        row: dict[str, Any] = {field: None for field in CANONICAL_FIELDS}
        for source_name, target_name in schema.mappings.items():
            value = source[source_name]
            if target_name in NUMERIC_FIELDS:
                row[target_name] = _number_value(value, target_name)
            elif target_name in DATE_FIELDS:
                row[target_name] = _date_value(value, target_name)
            elif target_name in CODE_FIELDS:
                row[target_name] = value.upper() or None
            else:
                row[target_name] = value or None
        row["asset_key"] = _asset_key(row.get("isin_code"), row.get("other_reg_number"))
        row["declared_change_type"] = None
        row["source_fields_json"] = json.dumps(
            source, sort_keys=True, ensure_ascii=False, separators=(",", ":")
        )
        row["source_schema_version"] = schema.schema_id
        row["row_hash"] = _canonical_hash(row)
        if row["asset_key"] in rows:
            raise ECBHistoricalSchemaError(
                f"duplicate eligible-asset key {row['asset_key']}"
            )
        rows[row["asset_key"]] = row
    return ParsedAssets(
        rows=rows,
        schema_hash=schema.source_schema_hash,
        source_fields=schema.columns,
    )
