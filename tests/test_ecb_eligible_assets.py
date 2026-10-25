"""Spec011 point-in-time ECB eligible-assets tests (offline fixtures only)."""

from __future__ import annotations

import csv
import gzip
import io
from datetime import datetime, timezone

import pytest

from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.ecb_eligible_assets import (
    ECBEligibleAssetRepository,
    ECBEligibleAssetsPipeline,
    ECBReconciliationError,
    ECBReleaseValidationError,
    SignatureResult,
    parse_assets_xml,
    parse_release_message,
    parse_rss,
    reconcile_update,
    verify_attachment_signature,
)
from fred_pipeline.local_store import LocalWarehouse


def _asset(
    isin: str,
    *,
    haircut: str = "1.0",
    issuer: str = "Issuer",
    change: str | None = None,
    extra: str = "",
) -> str:
    change_xml = f"<change>{change}</change>" if change else ""
    return f"""
    <asset{"Update" if change else ""}>
      <id><isinCode>{isin}</isinCode><otherRegNumber/></id>
      <haircutCategory>L1A</haircutCategory>
      <type>AT01</type><referenceMarket>RMDE01</referenceMarket>
      <denomination>EUR</denomination>
      <issuanceDate>01/01/2024 00:00:00</issuanceDate>
      <maturityDate>01/01/2030 00:00:00</maturityDate>
      <issuerCsd>CLDE01</issuerCsd><couponDefinition>CD4</couponDefinition>
      <couponRate>2.5</couponRate><issuerName>{issuer}</issuerName>
      <issuerGroup>IG1</issuerGroup><issuerResidence>IRDE</issuerResidence>
      <guarantorName/><guarantorGroup/><guarantorResidence/>
      <nonOwnUseHaircut>{haircut}</nonOwnUseHaircut><ownUseHaircut/>
      <potentiallyOwnUsableCoveredBond>N</potentiallyOwnUsableCoveredBond>
      <climateFactor>N</climateFactor>{extra}{change_xml}
    </asset{"Update" if change else ""}>
    """


def _full(*assets: str) -> bytes:
    return gzip.compress(("<assets>" + "".join(assets) + "</assets>").encode())


def _update(*assets: str) -> bytes:
    body = "".join(assets).replace("<assetUpdateUpdate>", "<assetUpdate>")
    body = body.replace("</assetUpdateUpdate>", "</assetUpdate>")
    return gzip.compress(("<assetsUpdates>" + body + "</assetsUpdates>").encode())


def _csv_payload(xml_payload: bytes, *, update: bool = False) -> bytes:
    rows = parse_assets_xml(xml_payload).rows.values()
    fields = [
        ("ISIN_CODE", "isin_code"),
        ("OTHER_REG_NUMBER", "other_reg_number"),
        ("HAIRCUT_CATEGORY", "haircut_category"),
        ("TYPE", "asset_type"),
        ("REFERENCE_MARKET", "reference_market"),
        ("DENOMINATION", "denomination"),
        ("ISSUANCE_DATE", "issuance_date"),
        ("MATURITY_DATE", "maturity_date"),
        ("ISSUER_CSD", "issuer_csd"),
        ("COUPON_RATE (%)", "coupon_rate"),
        ("ISSUER_NAME", "issuer_name"),
        ("ISSUER_RESIDENCE", "issuer_residence"),
        ("ISSUER_GROUP", "issuer_group"),
        ("GUARANTOR_NAME", "guarantor_name"),
        ("GUARANTOR_RESIDENCE", "guarantor_residence"),
        ("GUARANTOR_GROUP", "guarantor_group"),
        ("COUPON_DEFINITION", "coupon_definition"),
        ("HAIRCUT", "non_own_use_haircut"),
        ("HAIRCUT_OWN_USE", "own_use_haircut"),
        ("POTENTIALLY_OWN_USABLE_COVERED_BOND", "potentially_own_usable_covered_bond"),
        ("CLIMATE_FACTOR", "climate_factor"),
    ]
    if update:
        fields.append(("CHANGE", "declared_change_type"))
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, delimiter="\t", lineterminator="\n")
    writer.writerow([header for header, _key in fields])
    for row in rows:
        writer.writerow(
            ["" if row.get(key) is None else row.get(key) for _header, key in fields]
        )
    return gzip.compress(stream.getvalue().encode("utf-16le"))


DICTIONARY = b"""<?xml version="1.0" encoding="UTF-8"?>
<dictionary>
  <assetTypes><assetType><code>AT01</code><definition>Bond</definition></assetType></assetTypes>
  <assetHaircutCategories><assetHaircutCategory><code>L1A</code><definition>Category I</definition></assetHaircutCategory></assetHaircutCategories>
  <assetReferenceMarkets><assetReferenceMarket><code>RMDE01</code><definition>German regulated market</definition></assetReferenceMarket></assetReferenceMarkets>
  <assetCouponDefinitions><assetCouponDefinition><code>CD4</code><definition>Fixed</definition></assetCouponDefinition></assetCouponDefinitions>
</dictionary>"""


def _release_xml(day: str, urls: dict[str, str]) -> bytes:
    attachments = "".join(
        f"""
        <escb:attachment>
          <escb:fileName>{name}</escb:fileName>
          <escb:linkFile>{url}</escb:linkFile>
          <escb:linkSignature>ZmFrZQ==</escb:linkSignature>
        </escb:attachment>
        """
        for name, url in urls.items()
    )
    return f"""<?xml version="1.0"?>
    <escb:externalMessage xmlns:escb="http://escb.ecb.int/MarketInformationDissemination">
      <escb:header>
        <escb:releaseType>EligibleAssetsList</escb:releaseType>
        <escb:releaseTitle>List of eligible marketable assets</escb:releaseTitle>
        <escb:releaseDateTime>{day}T17:56:07</escb:releaseDateTime>
        <escb:publicationDateTime>{day}T17:56:07</escb:publicationDateTime>
        {attachments}
      </escb:header><escb:body/><escb:footer/>
    </escb:externalMessage>""".encode()


def _urls(prefix: str) -> dict[str, str]:
    return {
        f"ea_csv_{prefix}.csv.gz": f"https://example/{prefix}/full.csv.gz",
        f"ea_xml_{prefix}.xml.gz": f"https://example/{prefix}/full.xml.gz",
        f"ea_csv_update_{prefix}.csv.gz": f"https://example/{prefix}/update.csv.gz",
        f"ea_xml_update_{prefix}.xml.gz": f"https://example/{prefix}/update.xml.gz",
        f"ea_dictionary_{prefix}.xml": f"https://example/{prefix}/dictionary.xml",
    }


class _Client:
    def __init__(self, releases, artifacts, rss=b""):
        self.releases = releases
        self.artifacts = artifacts
        self.rss = rss

    def fetch_rss(self):
        return self.rss

    def fetch_release(self, url):
        return self.releases[url]

    def fetch_artifact(self, url):
        return self.artifacts[url]


def _verified(_payload, _signature):
    return SignatureResult(True, "verified", signer_subject="ECB test signer")


def _warehouse(tmp_path):
    config = PipelineConfig(environment=Environment.TEST, fred_api_key="test")
    return LocalWarehouse(config, db_path=str(tmp_path / "ecb.db"))


def _fixture_client(second_update=None):
    release_1_url = "https://example/release/1.xml"
    release_2_url = "https://example/release/2.xml"
    urls_1 = _urls("240101")
    urls_2 = _urls("240102")
    full_1 = _full(_asset("DE0000000001"), _asset("DE0000000002", haircut="2.0"))
    full_2 = _full(
        _asset("DE0000000001", haircut="1.5"),
        _asset("DE0000000003", issuer="New issuer"),
    )
    update_1 = _update()
    update_2 = second_update or _update(
        _asset("DE0000000001", haircut="1.5", change="U"),
        _asset("DE0000000002", haircut="2.0", change="D"),
        _asset("DE0000000003", issuer="New issuer", change="N"),
    )
    artifacts = {}
    for urls, full, update in (
        (urls_1, full_1, update_1),
        (urls_2, full_2, update_2),
    ):
        values = list(urls.values())
        artifacts.update(
            {
                values[0]: _csv_payload(full),
                values[1]: full,
                values[2]: _csv_payload(update, update=True),
                values[3]: update,
                values[4]: DICTIONARY,
            }
        )
    releases = {
        release_1_url: _release_xml("2024-01-01", urls_1),
        release_2_url: _release_xml("2024-01-02", urls_2),
    }
    return _Client(releases, artifacts), release_1_url, release_2_url


def test_rss_filters_exact_title_and_normalizes_time_and_https():
    rss = b"""<rss><channel>
      <item><title>Other</title><link>https://example/other</link><pubDate>Mon, 01 Jan 2024 17:00:00 +0100</pubDate></item>
      <item><title>List of eligible marketable assets</title><link>http://example/release</link><pubDate>Mon, 01 Jan 2024 17:56:07 +0100</pubDate></item>
    </channel></rss>"""
    items = parse_rss(rss)
    assert len(items) == 1
    assert items[0].url == "https://example/release"
    assert items[0].published_at == "2024-01-01T16:56:07+00:00"


def test_release_parser_requires_exact_identity_and_all_five_attachments():
    urls = _urls("240101")
    release = parse_release_message(
        _release_xml("2024-01-01", urls), url="https://example/release.xml"
    )
    assert {attachment.kind for attachment in release.attachments} == {
        "full_xml",
        "full_csv",
        "update_xml",
        "update_csv",
        "dictionary_xml",
    }
    assert release.publication_at_utc == "2024-01-01T16:56:07+00:00"


def test_release_parser_rejects_cross_origin_attachment():
    urls = _urls("240101")
    urls["ea_xml_240101.xml.gz"] = "https://untrusted.example/full.xml.gz"
    with pytest.raises(ECBReleaseValidationError, match="origin"):
        parse_release_message(
            _release_xml("2024-01-01", urls), url="https://example/release.xml"
        )


def test_parser_rejects_unknown_forward_schema_and_duplicate_keys():
    with pytest.raises(ECBReleaseValidationError, match="unknown"):
        parse_assets_xml(_full(_asset("DE0000000001", extra="<surprise>Y</surprise>")))
    with pytest.raises(ECBReleaseValidationError, match="duplicate"):
        parse_assets_xml(_full(_asset("DE0000000001"), _asset("DE0000000001")))


def test_two_releases_build_point_in_time_history_and_are_idempotent(tmp_path):
    client, release_1_url, release_2_url = _fixture_client()
    warehouse = _warehouse(tmp_path)
    pipeline = ECBEligibleAssetsPipeline(
        warehouse, client=client, signature_verifier=_verified
    )
    try:
        first = pipeline.ingest_release(release_1_url)
        second = pipeline.ingest_release(release_2_url)
        duplicate = pipeline.ingest_release(release_2_url)
        assert first.to_dict() | {} == {
            "release_id": first.release_id,
            "status": "accepted",
            "full_count": 2,
            "update_count": 0,
            "new_count": 2,
            "updated_count": 0,
            "deleted_count": 0,
            "coverage_status": "starts_here",
            "reconciliation_status": "initial_snapshot",
        }
        assert (second.new_count, second.updated_count, second.deleted_count) == (
            1,
            1,
            1,
        )
        assert second.reconciliation_status == "matched"
        assert duplicate.status == "duplicate"

        repo = ECBEligibleAssetRepository(warehouse)
        before = repo.as_of("2024-01-01T17:00:00+00:00")
        after = repo.as_of("2024-01-02T17:00:00+00:00")
        assert {row["asset_key"] for row in before} == {
            "ISIN:DE0000000001",
            "ISIN:DE0000000002",
        }
        assert {row["asset_key"] for row in after} == {
            "ISIN:DE0000000001",
            "ISIN:DE0000000003",
        }
        assert next(row for row in before if row["asset_key"] == "ISIN:DE0000000001")[
            "non_own_use_haircut"
        ] == pytest.approx(1.0)
        assert next(row for row in after if row["asset_key"] == "ISIN:DE0000000001")[
            "non_own_use_haircut"
        ] == pytest.approx(1.5)
        tombstone = warehouse.query(
            "SELECT * FROM silver_ecb_eligible_asset_version "
            "WHERE asset_key = ? ORDER BY valid_from_utc DESC LIMIT 1",
            ("ISIN:DE0000000002",),
        )[0]
        assert tombstone["is_eligible"] == 0
        assert tombstone["change_type"] == "D"
        assert repo.status()["current_eligible_count"] == 2
        assert (
            warehouse.query("SELECT COUNT(*) n FROM silver_ecb_eligible_asset_release")[
                0
            ]["n"]
            == 2
        )
    finally:
        warehouse.close()


def test_poll_resumes_after_latest_release_instead_of_replaying_older_items(tmp_path):
    client, release_1_url, release_2_url = _fixture_client()
    client.rss = f"""<rss><channel>
      <item><title>List of eligible marketable assets</title>
        <link>{release_2_url}</link><pubDate>Tue, 02 Jan 2024 17:56:07 +0100</pubDate></item>
      <item><title>List of eligible marketable assets</title>
        <link>{release_1_url}</link><pubDate>Mon, 01 Jan 2024 17:56:07 +0100</pubDate></item>
    </channel></rss>""".encode()
    warehouse = _warehouse(tmp_path)
    pipeline = ECBEligibleAssetsPipeline(
        warehouse, client=client, signature_verifier=_verified
    )
    try:
        pipeline.ingest_release(release_1_url)
        results = pipeline.poll()
        assert [result.release_id for result in results] == [
            parse_release_message(
                client.releases[release_2_url], url=release_2_url
            ).release_id
        ]
        assert results[0].status == "accepted"
        assert pipeline.poll() == []
    finally:
        warehouse.close()


def test_update_mismatch_quarantines_without_advancing_current_state(tmp_path):
    bad_update = _update(_asset("DE0000000003", change="N"))
    client, release_1_url, release_2_url = _fixture_client(bad_update)
    warehouse = _warehouse(tmp_path)
    pipeline = ECBEligibleAssetsPipeline(
        warehouse, client=client, signature_verifier=_verified
    )
    try:
        pipeline.ingest_release(release_1_url)
        with pytest.raises(ECBReconciliationError):
            pipeline.ingest_release(release_2_url)
        repo = ECBEligibleAssetRepository(warehouse)
        assert set(repo.current_assets()) == {
            "ISIN:DE0000000001",
            "ISIN:DE0000000002",
        }
        status = repo.status()
        assert len(status["quarantined"]) == 1
        assert "reconciliation failed" in status["quarantined"][0]["error_detail"]
    finally:
        warehouse.close()


def test_declared_unchanged_update_is_audited_without_creating_a_version(tmp_path):
    client, release_1_url, release_2_url = _fixture_client()
    unchanged = _asset("DE0000000004", issuer="Unchanged issuer")
    client.artifacts["https://example/240101/full.xml.gz"] = _full(
        _asset("DE0000000001"),
        _asset("DE0000000002", haircut="2.0"),
        unchanged,
    )
    client.artifacts["https://example/240101/full.csv.gz"] = _csv_payload(
        client.artifacts["https://example/240101/full.xml.gz"]
    )
    client.artifacts["https://example/240102/full.xml.gz"] = _full(
        _asset("DE0000000001", haircut="1.5"),
        _asset("DE0000000003", issuer="New issuer"),
        unchanged,
    )
    client.artifacts["https://example/240102/full.csv.gz"] = _csv_payload(
        client.artifacts["https://example/240102/full.xml.gz"]
    )
    client.artifacts["https://example/240102/update.xml.gz"] = _update(
        _asset("DE0000000001", haircut="1.5", change="U"),
        _asset("DE0000000002", haircut="2.0", change="D"),
        _asset("DE0000000003", issuer="New issuer", change="N"),
        _asset("DE0000000004", issuer="Unchanged issuer", change="U"),
    )
    client.artifacts["https://example/240102/update.csv.gz"] = _csv_payload(
        client.artifacts["https://example/240102/update.xml.gz"], update=True
    )
    warehouse = _warehouse(tmp_path)
    pipeline = ECBEligibleAssetsPipeline(
        warehouse, client=client, signature_verifier=_verified
    )
    try:
        pipeline.ingest_release(release_1_url)
        result = pipeline.ingest_release(release_2_url)

        assert result.reconciliation_status == "matched_with_source_noops"
        noop_versions = warehouse.query(
            "SELECT * FROM silver_ecb_eligible_asset_version "
            "WHERE asset_key = ? ORDER BY valid_from_utc",
            ("ISIN:DE0000000004",),
        )
        assert len(noop_versions) == 1
        assert noop_versions[0]["valid_to_utc"] is None
        noop_change = warehouse.query(
            "SELECT * FROM silver_ecb_eligible_asset_change "
            "WHERE release_id = ? AND asset_key = ?",
            (result.release_id, "ISIN:DE0000000004"),
        )[0]
        assert noop_change["declared_change_type"] == "U"
        assert noop_change["computed_change_type"] is None
        assert noop_change["change_origin"] == "source_noop"
        assert noop_change["prior_row_hash"] == noop_change["new_row_hash"]
    finally:
        warehouse.close()


@pytest.mark.parametrize("declared_change_type", ["N", "D"])
def test_reconciliation_rejects_extra_non_update_codes(declared_change_type):
    prior = parse_assets_xml(_full(_asset("DE0000000004"))).rows
    current = parse_assets_xml(_full(_asset("DE0000000004"))).rows
    update = parse_assets_xml(
        _update(_asset("DE0000000004", change=declared_change_type))
    ).rows

    result = reconcile_update({}, update, prior=prior, current=current)

    assert not result.matched
    assert result.source_noop_updates == ()


def test_reconciliation_rejects_extra_update_with_different_payload():
    prior = parse_assets_xml(_full(_asset("DE0000000004", haircut="1.0"))).rows
    current = parse_assets_xml(_full(_asset("DE0000000004", haircut="1.0"))).rows
    update = parse_assets_xml(
        _update(_asset("DE0000000004", haircut="9.0", change="U"))
    ).rows

    result = reconcile_update({}, update, prior=prior, current=current)

    assert not result.matched
    assert result.source_noop_updates == ()


def test_xml_csv_mismatch_quarantines_before_publish(tmp_path):
    client, release_1_url, _release_2_url = _fixture_client()
    full_csv_url = next(iter(_urls("240101").values()))
    client.artifacts[full_csv_url] = _csv_payload(_full(_asset("DE9999999999")))
    warehouse = _warehouse(tmp_path)
    pipeline = ECBEligibleAssetsPipeline(
        warehouse, client=client, signature_verifier=_verified
    )
    try:
        with pytest.raises(ECBReleaseValidationError, match="XML/CSV key mismatch"):
            pipeline.ingest_release(release_1_url)
        assert (
            ECBEligibleAssetRepository(warehouse).status()["current_eligible_count"]
            == 0
        )
        assert ECBEligibleAssetRepository(warehouse).status()["quarantined"]
    finally:
        warehouse.close()


def test_allow_gap_accepts_authoritative_full_diff_with_visible_coverage(tmp_path):
    bad_update = _update(_asset("DE0000000003", change="N"))
    client, release_1_url, release_2_url = _fixture_client(bad_update)
    warehouse = _warehouse(tmp_path)
    pipeline = ECBEligibleAssetsPipeline(
        warehouse, client=client, signature_verifier=_verified
    )
    try:
        pipeline.ingest_release(release_1_url)
        result = pipeline.ingest_release(release_2_url, allow_gap=True)
        assert result.status == "accepted"
        assert result.coverage_status == "gapped"
        assert result.reconciliation_status == "derived_full_diff"
    finally:
        warehouse.close()


def test_publish_failure_rolls_back_interval_changes(tmp_path, monkeypatch):
    client, release_1_url, release_2_url = _fixture_client()
    warehouse = _warehouse(tmp_path)
    pipeline = ECBEligibleAssetsPipeline(
        warehouse, client=client, signature_verifier=_verified
    )
    try:
        pipeline.ingest_release(release_1_url)
        original_insert = warehouse._insert

        def fail_on_change_table(table, rows, upsert_keys=None, **kwargs):
            if table == "silver_ecb_eligible_asset_change":
                raise RuntimeError("simulated interrupted publish")
            return original_insert(table, rows, upsert_keys=upsert_keys, **kwargs)

        monkeypatch.setattr(warehouse, "_insert", fail_on_change_table)
        with pytest.raises(RuntimeError, match="interrupted"):
            pipeline.ingest_release(release_2_url)

        repo = ECBEligibleAssetRepository(warehouse)
        assert set(repo.current_assets()) == {
            "ISIN:DE0000000001",
            "ISIN:DE0000000002",
        }
        assert (
            len(warehouse.query("SELECT * FROM silver_ecb_eligible_asset_release")) == 1
        )
        assert len(repo.status()["quarantined"]) == 1
    finally:
        warehouse.close()


def test_out_of_order_forward_release_is_quarantined(tmp_path):
    client, release_1_url, release_2_url = _fixture_client()
    warehouse = _warehouse(tmp_path)
    pipeline = ECBEligibleAssetsPipeline(
        warehouse, client=client, signature_verifier=_verified
    )
    try:
        pipeline.ingest_release(release_2_url)
        with pytest.raises(ECBReleaseValidationError, match="out-of-order"):
            pipeline.ingest_release(release_1_url)
        assert (
            ECBEligibleAssetRepository(warehouse).status()["current_eligible_count"]
            == 2
        )
    finally:
        warehouse.close()


def test_signature_failure_keeps_release_out_of_silver(tmp_path):
    client, release_1_url, _release_2_url = _fixture_client()
    warehouse = _warehouse(tmp_path)

    def failed(_payload, _signature):
        return SignatureResult(False, "digest_mismatch", detail="tampered")

    pipeline = ECBEligibleAssetsPipeline(
        warehouse, client=client, signature_verifier=failed
    )
    try:
        with pytest.raises(ECBReleaseValidationError, match="signature failed"):
            pipeline.ingest_release(release_1_url)
        assert (
            warehouse.query("SELECT COUNT(*) n FROM silver_ecb_eligible_asset_release")[
                0
            ]["n"]
            == 0
        )
        assert ECBEligibleAssetRepository(warehouse).status()["quarantined"]
    finally:
        warehouse.close()


def test_signature_verifier_rejects_invalid_base64_without_openssl():
    result = verify_attachment_signature(b"payload", "not base64!!")
    assert not result.valid
    assert result.status == "invalid_base64"


def test_publication_timestamp_is_not_retrieval_time():
    urls = _urls("240701")
    release = parse_release_message(
        _release_xml("2024-07-01", urls), url="https://example/release.xml"
    )
    published = datetime.fromisoformat(release.publication_at_utc)
    assert published.tzinfo == timezone.utc
    assert published.hour == 15  # 17:56 Europe/Frankfurt during DST
