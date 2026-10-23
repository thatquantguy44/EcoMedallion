from __future__ import annotations

import json
from datetime import date

import pytest

from fred_pipeline.ecb_asset_schema_registry import (
    DEFAULT_REGISTRY_PATH,
    ECBHistoricalSchemaError,
    column_set_hash,
    load_schema_registry,
    parse_historical_assets_csv,
    validate_inventory_report,
)


def _utf16_snapshot(columns, values):
    return ("\t".join(columns) + "\n" + "\t".join(values) + "\n").encode("utf-16")


def _values(columns, **overrides):
    defaults = {column: "" for column in columns}
    defaults.update(overrides)
    return [defaults[column] for column in columns]


def test_checked_in_registry_covers_full_live_inventory_contract():
    registry = load_schema_registry()

    assert len(registry.schemas) == 7
    assert registry.inventory_snapshot_count == 4256
    assert registry.schemas[0].observed_from == date(2010, 4, 8)
    assert registry.schemas[-1].observed_through == date(2026, 10, 22)
    assert registry.schemas[-1].valid_to is None
    assert len({schema.source_schema_hash for schema in registry.schemas}) == 7
    assert all(
        column_set_hash(schema.columns) == schema.source_schema_hash
        for schema in registry.schemas
    )


def test_inventory_validation_accepts_registered_schema_and_rejects_date_drift():
    registry = load_schema_registry()
    schema = registry.schemas[0]
    report = {
        "years": [
            {
                "schemas": [
                    {
                        "schema_hash": schema.source_schema_hash,
                        "columns": list(schema.columns),
                        "first_snapshot_date": "2010-04-08",
                        "last_snapshot_date": "2010-04-09",
                        "snapshot_count": 2,
                    }
                ]
            }
        ]
    }

    result = validate_inventory_report(report, registry)
    assert result["status"] == "valid"
    assert result["observed_schema_count"] == 1
    assert result["snapshot_count"] == 2

    report["years"][0]["schemas"][0]["first_snapshot_date"] = "2010-04-07"
    with pytest.raises(ECBHistoricalSchemaError, match="exceeds registry range"):
        validate_inventory_report(report, registry)


def test_legacy_schema_normalizes_renames_and_preserves_retired_fields():
    registry = load_schema_registry()
    schema = registry.schemas[0]
    values = _values(
        schema.columns,
        ISIN_CODE="at0000131966",
        LIQUIDITY_CLASS="l1c",
        TYPE="at10",
        REFERENCE_MARKET="rmat04",
        PRICE_SPECIFICATION="PSEU01",
        DENOMINATION="eur",
        ISSUANCE_DATE="07/12/1999 00:00:00",
        MATURITY_DATE="07/12/2019 00:00:00",
        COUNTRY_OF_LOCATION="clat",
        **{
            "COUPON_RATE (%)": "2,81",
            "ISSUER_NAME": "Oberosterreichische Landesbank AG",
            "ISSUER_RESIDENCE": "irat",
            "ISSUER_GROUP": "ig4",
            "COUPON_DEFINITION": "cd4",
            "VALUATION_HAIRCUT": "4.5",
            "UCITS_COMPLIANT": "Y",
        },
    )

    parsed = parse_historical_assets_csv(
        _utf16_snapshot(schema.columns, values),
        snapshot_date=date(2010, 4, 8),
        registry=registry,
    )

    row = parsed.rows["ISIN:AT0000131966"]
    assert parsed.schema_hash == schema.source_schema_hash
    assert row["haircut_category"] == "L1C"
    assert row["issuer_csd"] == "CLAT"
    assert row["coupon_rate"] == 2.81
    assert row["non_own_use_haircut"] == 4.5
    assert row["own_use_haircut"] is None
    assert row["climate_factor"] is None
    assert row["source_schema_version"] == schema.schema_id
    source = json.loads(row["source_fields_json"])
    assert source["PRICE_SPECIFICATION"] == "PSEU01"
    assert source["UCITS_COMPLIANT"] == "Y"


def test_latest_schema_normalizes_climate_factor_and_absent_values():
    registry = load_schema_registry()
    schema = registry.schemas[-1]
    values = _values(
        schema.columns,
        ISIN_CODE="DE0001",
        HAIRCUT_CATEGORY="L1A",
        TYPE="AT01",
        DENOMINATION="eur",
        ISSUANCE_DATE="2026-01-02",
        HAIRCUT="1.25",
        CLIMATE_FACTOR="y",
    )

    parsed = parse_historical_assets_csv(
        _utf16_snapshot(schema.columns, values),
        snapshot_date=date(2026, 10, 22),
        registry=registry,
    )
    row = parsed.rows["ISIN:DE0001"]
    assert row["climate_factor"] == "Y"
    assert row["non_own_use_haircut"] == 1.25
    assert row["issuer_name"] is None


def test_unregistered_header_fails_closed():
    registry = load_schema_registry()
    columns = [*registry.schemas[-1].columns, "UNREVIEWED_FIELD"]
    values = _values(columns, ISIN_CODE="DE0001")

    with pytest.raises(ECBHistoricalSchemaError, match="unregistered"):
        parse_historical_assets_csv(
            _utf16_snapshot(columns, values),
            snapshot_date=date(2026, 10, 22),
            registry=registry,
        )


def test_registry_rejects_hash_drift(tmp_path):
    registry_text = DEFAULT_REGISTRY_PATH.read_text(encoding="utf-8")
    changed = registry_text.replace(
        "7e7f9310994b8ef0dccf2a76946285648ccf47a916b8236833014989f009f330",
        "0" * 64,
        1,
    )
    path = tmp_path / "schemas.yml"
    path.write_text(changed, encoding="utf-8")

    with pytest.raises(ECBHistoricalSchemaError, match="hash mismatch"):
        load_schema_registry(path)
