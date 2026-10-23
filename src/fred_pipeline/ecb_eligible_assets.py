"""Point-in-time ingestion for the ECB list of eligible marketable assets.

This is deliberately separate from the observation-series ``ECBClient``.  MID
publishes a daily security universe, not a time series: the full snapshot is
authoritative, the update attachment is an independent reconciliation input,
and publication time is the point-in-time boundary.
"""

from __future__ import annotations

import base64
import binascii
import csv
import gzip
import hashlib
import io
import json
import re
import subprocess
import tempfile
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from fred_pipeline.sources.base import HTTPSource, SourceError

RSS_URL = "https://mid.ecb.europa.eu/rss/mid.xml"
RELEASE_TITLE = "List of eligible marketable assets"
RELEASE_TYPE = "EligibleAssetsList"
PARSER_VERSION = "spec011-v1"


class _EuropeFrankfurtFallback(tzinfo):
    """EU CET/CEST rules for minimal hosts that ship without an IANA tzdb."""

    @staticmethod
    def _last_sunday(year: int, month: int) -> int:
        if month == 12:
            first_next = datetime(year + 1, 1, 1)
        else:
            first_next = datetime(year, month + 1, 1)
        last = first_next - timedelta(days=1)
        return last.day - ((last.weekday() + 1) % 7)

    def dst(self, value: datetime | None) -> timedelta:
        if value is None:
            return timedelta(0)
        start = datetime(value.year, 3, self._last_sunday(value.year, 3), 2)
        end = datetime(value.year, 10, self._last_sunday(value.year, 10), 3)
        naive = value.replace(tzinfo=None)
        return timedelta(hours=1) if start <= naive < end else timedelta(0)

    def utcoffset(self, value: datetime | None) -> timedelta:
        return timedelta(hours=1) + self.dst(value)

    def tzname(self, value: datetime | None) -> str:
        return "CEST" if self.dst(value) else "CET"


try:
    ECB_ZONE: tzinfo = ZoneInfo("Europe/Frankfurt")
except Exception:  # pragma: no cover - depends on host tzdata installation
    ECB_ZONE = _EuropeFrankfurtFallback()

_NS = {"escb": "http://escb.ecb.int/MarketInformationDissemination"}
_EXPECTED_ARTIFACTS = frozenset(
    {"full_xml", "full_csv", "update_xml", "update_csv", "dictionary_xml"}
)
_ASSET_TAGS = frozenset(
    {
        "id",
        "isinCode",
        "otherRegNumber",
        "haircutCategory",
        "type",
        "referenceMarket",
        "denomination",
        "issuanceDate",
        "maturityDate",
        "issuerCsd",
        "couponDefinition",
        "couponRate",
        "issuerName",
        "issuerGroup",
        "issuerResidence",
        "guarantorName",
        "guarantorGroup",
        "guarantorResidence",
        "nonOwnUseHaircut",
        "ownUseHaircut",
        "potentiallyOwnUsableCoveredBond",
        "climateFactor",
        "change",
    }
)
_FIELD_MAP = {
    "isinCode": "isin_code",
    "otherRegNumber": "other_reg_number",
    "haircutCategory": "haircut_category",
    "type": "asset_type",
    "referenceMarket": "reference_market",
    "denomination": "denomination",
    "issuanceDate": "issuance_date",
    "maturityDate": "maturity_date",
    "issuerCsd": "issuer_csd",
    "couponDefinition": "coupon_definition",
    "couponRate": "coupon_rate",
    "issuerName": "issuer_name",
    "issuerGroup": "issuer_group",
    "issuerResidence": "issuer_residence",
    "guarantorName": "guarantor_name",
    "guarantorGroup": "guarantor_group",
    "guarantorResidence": "guarantor_residence",
    "nonOwnUseHaircut": "non_own_use_haircut",
    "ownUseHaircut": "own_use_haircut",
    "potentiallyOwnUsableCoveredBond": "potentially_own_usable_covered_bond",
    "climateFactor": "climate_factor",
}
_VERSION_COLUMNS = tuple(_FIELD_MAP.values())
_NUMERIC_FIELDS = frozenset({"coupon_rate", "non_own_use_haircut", "own_use_haircut"})
_DATE_FIELDS = frozenset({"issuance_date", "maturity_date"})
_CODE_FIELDS = frozenset(
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


class ECBEligibleAssetsError(RuntimeError):
    """Base error for the dedicated ECB eligible-assets flow."""


class ECBReleaseValidationError(ECBEligibleAssetsError):
    """The release or one of its artifacts failed a source-contract check."""


class ECBReconciliationError(ECBEligibleAssetsError):
    """The source update attachment disagreed with two authoritative full lists."""


class ECBMIDError(SourceError):
    """Transport failure from the ECB MID service."""


@dataclass(frozen=True)
class RSSRelease:
    title: str
    url: str
    published_at: str


@dataclass(frozen=True)
class Attachment:
    kind: str
    file_name: str
    url: str
    signature: str


@dataclass(frozen=True)
class ReleaseMessage:
    release_id: str
    url: str
    release_type: str
    release_title: str
    release_at_local: str
    publication_at_local: str
    publication_at_utc: str
    rss_pub_at: str | None
    attachments: tuple[Attachment, ...]


@dataclass(frozen=True)
class SignatureResult:
    valid: bool
    status: str
    signed_digest: str | None = None
    actual_digest: str | None = None
    signer_subject: str | None = None
    detail: str | None = None


@dataclass(frozen=True)
class ParsedAssets:
    rows: dict[str, dict[str, Any]]
    schema_hash: str
    source_fields: tuple[str, ...]


@dataclass(frozen=True)
class IngestResult:
    release_id: str
    status: str
    full_count: int = 0
    update_count: int = 0
    new_count: int = 0
    updated_count: int = 0
    deleted_count: int = 0
    coverage_status: str = "unknown"
    reconciliation_status: str = "not_run"

    def to_dict(self) -> dict[str, Any]:
        return {
            "release_id": self.release_id,
            "status": self.status,
            "full_count": self.full_count,
            "update_count": self.update_count,
            "new_count": self.new_count,
            "updated_count": self.updated_count,
            "deleted_count": self.deleted_count,
            "coverage_status": self.coverage_status,
            "reconciliation_status": self.reconciliation_status,
        }


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _md5(data: bytes) -> str:
    # MD5 is required by the ECB MID signature protocol.  It is not used for
    # collision-resistant storage identity (SHA-256 above serves that role).
    try:
        return hashlib.md5(data, usedforsecurity=False).hexdigest()  # noqa: S324
    except TypeError:  # pragma: no cover - older/OpenSSL-specific Python builds
        return hashlib.md5(data).hexdigest()  # noqa: S324


def _decode_payload(data: bytes) -> bytes:
    return gzip.decompress(data) if data[:2] == b"\x1f\x8b" else data


def _xml_encoding(data: bytes) -> str:
    head = data[:200]
    match = re.search(rb"encoding=[\"']([^\"']+)", head, re.IGNORECASE)
    if match:
        return match.group(1).decode("ascii", errors="replace")
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "utf-16"
    return "utf-8"


def _parse_mid_local(value: str) -> tuple[str, str]:
    try:
        local = datetime.strptime(value.strip(), "%Y-%m-%dT%H:%M:%S").replace(
            tzinfo=ECB_ZONE
        )
    except ValueError as exc:
        raise ECBReleaseValidationError(f"invalid MID timestamp {value!r}") from exc
    return local.isoformat(), local.astimezone(timezone.utc).isoformat()


def _artifact_kind(file_name: str) -> str:
    lower = file_name.lower()
    if "dictionary" in lower:
        return "dictionary_xml"
    is_update = "update" in lower
    if ".xml" in lower:
        return "update_xml" if is_update else "full_xml"
    if ".csv" in lower:
        return "update_csv" if is_update else "full_csv"
    raise ECBReleaseValidationError(f"unknown MID attachment type: {file_name}")


def parse_rss(xml_bytes: bytes) -> list[RSSRelease]:
    """Return eligible-assets items from the finite, mixed-purpose MID feed."""
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        raise ECBReleaseValidationError(f"invalid MID RSS XML: {exc}") from exc
    releases: list[RSSRelease] = []
    for item in root.findall("./channel/item"):
        title = (item.findtext("title") or "").strip()
        if title != RELEASE_TITLE:
            continue
        url = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        if not url or not pub:
            raise ECBReleaseValidationError(
                "eligible-assets RSS item lacks link/pubDate"
            )
        try:
            published = parsedate_to_datetime(pub)
            if published.tzinfo is None:
                raise ValueError("RSS pubDate has no offset")
        except (TypeError, ValueError) as exc:
            raise ECBReleaseValidationError(f"invalid RSS pubDate {pub!r}") from exc
        releases.append(
            RSSRelease(
                title=title,
                url=url.replace("http://", "https://", 1),
                published_at=published.astimezone(timezone.utc).isoformat(),
            )
        )
    return sorted(releases, key=lambda item: item.published_at)


def parse_release_message(
    xml_bytes: bytes, *, url: str, rss_pub_at: str | None = None
) -> ReleaseMessage:
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        raise ECBReleaseValidationError(f"invalid MID release XML: {exc}") from exc
    release_type = (
        root.findtext("./escb:header/escb:releaseType", namespaces=_NS) or ""
    ).strip()
    release_title = (
        root.findtext("./escb:header/escb:releaseTitle", namespaces=_NS) or ""
    ).strip()
    if release_type != RELEASE_TYPE or release_title != RELEASE_TITLE:
        raise ECBReleaseValidationError(
            f"unexpected release identity: {release_type!r} / {release_title!r}"
        )
    release_raw = (
        root.findtext("./escb:header/escb:releaseDateTime", namespaces=_NS) or ""
    ).strip()
    publication_raw = (
        root.findtext("./escb:header/escb:publicationDateTime", namespaces=_NS) or ""
    ).strip()
    release_local, _release_utc = _parse_mid_local(release_raw)
    publication_local, publication_utc = _parse_mid_local(publication_raw)

    canonical_url = url.replace("http://", "https://", 1)
    release_origin = urlparse(canonical_url).netloc.lower()
    if not release_origin:
        raise ECBReleaseValidationError(f"invalid MID release URL: {url!r}")

    attachments: list[Attachment] = []
    seen_kinds: set[str] = set()
    for node in root.findall("./escb:header/escb:attachment", namespaces=_NS):
        file_name = (node.findtext("escb:fileName", namespaces=_NS) or "").strip()
        file_url = (node.findtext("escb:linkFile", namespaces=_NS) or "").strip()
        signature = "".join(
            (node.findtext("escb:linkSignature", namespaces=_NS) or "").split()
        )
        if not file_name or not file_url or not signature:
            raise ECBReleaseValidationError("MID attachment metadata is incomplete")
        kind = _artifact_kind(file_name)
        if kind in seen_kinds:
            raise ECBReleaseValidationError(f"duplicate MID attachment kind: {kind}")
        seen_kinds.add(kind)
        canonical_file_url = file_url.replace("http://", "https://", 1)
        if urlparse(canonical_file_url).netloc.lower() != release_origin:
            raise ECBReleaseValidationError(
                f"MID attachment origin does not match release: {file_url!r}"
            )
        attachments.append(
            Attachment(
                kind=kind,
                file_name=file_name,
                url=canonical_file_url,
                signature=signature,
            )
        )
    missing = _EXPECTED_ARTIFACTS - seen_kinds
    if missing:
        raise ECBReleaseValidationError(
            f"eligible-assets release is missing attachments: {sorted(missing)}"
        )
    release_id = hashlib.sha256(
        f"{canonical_url}\n{publication_utc}".encode("utf-8")
    ).hexdigest()
    return ReleaseMessage(
        release_id=release_id,
        url=canonical_url,
        release_type=release_type,
        release_title=release_title,
        release_at_local=release_local,
        publication_at_local=publication_local,
        publication_at_utc=publication_utc,
        rss_pub_at=rss_pub_at,
        attachments=tuple(attachments),
    )


def verify_attachment_signature(
    payload: bytes,
    signature_b64: str,
    *,
    openssl_binary: str = "openssl",
) -> SignatureResult:
    """Verify MID's embedded PKCS#7 signature and its signed MD5 hex digest.

    ``openssl cms -verify`` validates the signature and certificate chain using
    the host trust store.  MID signs the 32-character MD5 hex string rather
    than the attachment bytes themselves; both checks are required.
    """
    actual = _md5(payload)
    try:
        signature = base64.b64decode("".join(signature_b64.split()), validate=True)
    except (ValueError, binascii.Error) as exc:
        return SignatureResult(
            False, "invalid_base64", actual_digest=actual, detail=str(exc)
        )
    try:
        with tempfile.TemporaryDirectory(prefix="ecb-mid-signature-") as tmp:
            signature_path = Path(tmp) / "signature.der"
            signature_path.write_bytes(signature)
            verified = subprocess.run(
                [
                    openssl_binary,
                    "cms",
                    "-verify",
                    "-inform",
                    "DER",
                    "-in",
                    str(signature_path),
                    "-purpose",
                    "any",
                ],
                check=False,
                capture_output=True,
            )
            if verified.returncode != 0:
                return SignatureResult(
                    False,
                    "signature_invalid",
                    actual_digest=actual,
                    detail=verified.stderr.decode("utf-8", errors="replace").strip(),
                )
            signed = verified.stdout.decode("ascii", errors="replace").strip().lower()
            certs = subprocess.run(
                [
                    openssl_binary,
                    "pkcs7",
                    "-inform",
                    "DER",
                    "-in",
                    str(signature_path),
                    "-print_certs",
                    "-noout",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            subject = next(
                (
                    line.split("=", 1)[1].strip()
                    for line in certs.stdout.splitlines()
                    if line.startswith("subject=")
                ),
                None,
            )
    except FileNotFoundError as exc:
        return SignatureResult(
            False, "openssl_unavailable", actual_digest=actual, detail=str(exc)
        )
    except OSError as exc:
        return SignatureResult(
            False, "verification_error", actual_digest=actual, detail=str(exc)
        )
    if signed != actual:
        return SignatureResult(
            False,
            "digest_mismatch",
            signed_digest=signed,
            actual_digest=actual,
            signer_subject=subject,
        )
    return SignatureResult(
        True,
        "verified",
        signed_digest=signed,
        actual_digest=actual,
        signer_subject=subject,
    )


def _date_value(value: str, field_name: str) -> str | None:
    text = value.strip()
    if not text:
        return None
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    raise ECBReleaseValidationError(f"invalid {field_name} value {value!r}")


def _number_value(value: str, field_name: str) -> float | None:
    text = value.strip()
    if not text:
        return None
    try:
        return float(Decimal(text.replace(",", ".")))
    except (InvalidOperation, ValueError) as exc:
        raise ECBReleaseValidationError(
            f"invalid numeric {field_name} value {value!r}"
        ) from exc


def _asset_key(isin_code: str | None, other_reg_number: str | None) -> str:
    if isin_code:
        return f"ISIN:{isin_code}"
    if other_reg_number:
        return f"OTHER:{other_reg_number}"
    raise ECBReleaseValidationError("eligible asset has neither ISIN nor other number")


def _canonical_hash(row: Mapping[str, Any], *, is_eligible: bool = True) -> str:
    body = {key: row.get(key) for key in _VERSION_COLUMNS}
    body["is_eligible"] = is_eligible
    return _sha256(
        json.dumps(
            body, sort_keys=True, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
    )


def parse_assets_xml(xml_bytes: bytes, *, strict_schema: bool = True) -> ParsedAssets:
    """Stream a full/update XML attachment into canonical keyed rows."""
    raw = _decode_payload(xml_bytes)
    rows: dict[str, dict[str, Any]] = {}
    fields_seen: set[str] = set()
    try:
        iterator = ET.iterparse(io.BytesIO(raw), events=("end",))
        for _event, elem in iterator:
            if _local_name(elem.tag) not in {"asset", "assetUpdate"}:
                continue
            source: dict[str, str] = {}
            for child in elem.iter():
                name = _local_name(child.tag)
                if child is elem or name == "id":
                    continue
                source[name] = (child.text or "").strip()
                fields_seen.add(name)
            unknown = set(source) - (_ASSET_TAGS - {"id"})
            if strict_schema and unknown:
                raise ECBReleaseValidationError(
                    f"unknown eligible-assets fields: {sorted(unknown)}"
                )
            row: dict[str, Any] = {}
            for source_name, target_name in _FIELD_MAP.items():
                value = source.get(source_name, "")
                if target_name in _NUMERIC_FIELDS:
                    row[target_name] = _number_value(value, target_name)
                elif target_name in _DATE_FIELDS:
                    row[target_name] = _date_value(value, target_name)
                elif target_name in _CODE_FIELDS:
                    row[target_name] = value.strip().upper() or None
                else:
                    row[target_name] = value.strip() or None
            row["asset_key"] = _asset_key(
                row.get("isin_code"), row.get("other_reg_number")
            )
            row["declared_change_type"] = source.get("change", "").upper() or None
            if row["declared_change_type"] not in {None, "N", "U", "D"}:
                raise ECBReleaseValidationError(
                    f"invalid change code {row['declared_change_type']!r} for {row['asset_key']}"
                )
            row["source_fields_json"] = json.dumps(
                source, sort_keys=True, ensure_ascii=False, separators=(",", ":")
            )
            row["row_hash"] = _canonical_hash(row)
            if row["asset_key"] in rows:
                raise ECBReleaseValidationError(
                    f"duplicate eligible-asset key {row['asset_key']}"
                )
            rows[row["asset_key"]] = row
            elem.clear()
    except ET.ParseError as exc:
        raise ECBReleaseValidationError(f"invalid eligible-assets XML: {exc}") from exc
    fields = tuple(sorted(fields_seen))
    return ParsedAssets(
        rows=rows,
        schema_hash=_sha256("\n".join(fields).encode("utf-8")),
        source_fields=fields,
    )


def parse_dictionary_xml(xml_bytes: bytes) -> list[dict[str, Any]]:
    raw = _decode_payload(xml_bytes)
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        raise ECBReleaseValidationError(
            f"invalid eligible-assets dictionary XML: {exc}"
        ) from exc
    rows: list[dict[str, Any]] = []
    for section in list(root):
        domain = _local_name(section.tag)
        for entry in list(section):
            values = {
                _local_name(child.tag): (child.text or "").strip()
                for child in list(entry)
            }
            code = values.get("code", "").upper()
            if not code:
                continue
            rows.append(
                {
                    "domain": domain,
                    "code": code,
                    "definition": values.get("definition") or None,
                    "short_definition": values.get("shortDefinition") or None,
                    "attributes_json": json.dumps(
                        values,
                        sort_keys=True,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                }
            )
    return rows


def _csv_encoding(data: bytes) -> str:
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "utf-16"
    if b"\x00" in data[:200]:
        return "utf-16le"
    return "utf-8-sig"


def parse_assets_csv(csv_bytes: bytes, *, update: bool = False) -> ParsedAssets:
    """Parse current-format archive CSV; retained for bootstrap inventories."""
    raw = _decode_payload(csv_bytes)
    text = raw.decode(_csv_encoding(raw))
    aliases = {
        "ISIN_CODE": "isinCode",
        "OTHER_REG_NUMBER": "otherRegNumber",
        "HAIRCUT_CATEGORY": "haircutCategory",
        "TYPE": "type",
        "REFERENCE_MARKET": "referenceMarket",
        "DENOMINATION": "denomination",
        "ISSUANCE_DATE": "issuanceDate",
        "MATURITY_DATE": "maturityDate",
        "ISSUER_CSD": "issuerCsd",
        "COUPON_DEFINITION": "couponDefinition",
        "COUPON_RATE (%)": "couponRate",
        "ISSUER_NAME": "issuerName",
        "ISSUER_GROUP": "issuerGroup",
        "ISSUER_RESIDENCE": "issuerResidence",
        "GUARANTOR_NAME": "guarantorName",
        "GUARANTOR_GROUP": "guarantorGroup",
        "GUARANTOR_RESIDENCE": "guarantorResidence",
        "HAIRCUT": "nonOwnUseHaircut",
        "HAIRCUT_OWN_USE": "ownUseHaircut",
        "POTENTIALLY_OWN_USABLE_COVERED_BOND": "potentiallyOwnUsableCoveredBond",
        "CLIMATE_FACTOR": "climateFactor",
        "CHANGE": "change",
    }
    header = text.partition("\n")[0]
    delimiter = max(("\t", ";", ","), key=header.count)
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    unknown_headers = {
        str(name or "").strip().lstrip("\ufeff") for name in (reader.fieldnames or [])
    } - set(aliases)
    if unknown_headers:
        raise ECBReleaseValidationError(
            f"unknown eligible-assets CSV fields: {sorted(unknown_headers)}"
        )
    xml_parts = ["<assetsUpdates>" if update else "<assets>"]
    container = "assetUpdate" if update else "asset"
    for rec in reader:
        rec = {
            str(key or "").strip().lstrip("\ufeff"): value for key, value in rec.items()
        }
        xml_parts.append(f"<{container}><id>")
        for header in ("ISIN_CODE", "OTHER_REG_NUMBER"):
            tag = aliases[header]
            value = _xml_escape(rec.get(header) or "")
            xml_parts.append(f"<{tag}>{value}</{tag}>")
        xml_parts.append("</id>")
        for header, tag in aliases.items():
            if header in {"ISIN_CODE", "OTHER_REG_NUMBER"}:
                continue
            if header in rec:
                xml_parts.append(f"<{tag}>{_xml_escape(rec.get(header) or '')}</{tag}>")
        xml_parts.append(f"</{container}>")
    xml_parts.append("</assetsUpdates>" if update else "</assets>")
    return parse_assets_xml("".join(xml_parts).encode("utf-8"), strict_schema=True)


def _xml_escape(value: str) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def diff_assets(
    prior: Mapping[str, Mapping[str, Any]],
    current: Mapping[str, Mapping[str, Any]],
) -> dict[str, str]:
    changes: dict[str, str] = {}
    for key in sorted(set(prior) | set(current)):
        if key not in prior:
            changes[key] = "N"
        elif key not in current:
            changes[key] = "D"
        elif prior[key].get("row_hash") != current[key].get("row_hash"):
            changes[key] = "U"
    return changes


def reconcile_update(
    computed: Mapping[str, str], update_rows: Mapping[str, Mapping[str, Any]]
) -> tuple[bool, str]:
    declared = {
        key: str(row.get("declared_change_type") or "")
        for key, row in update_rows.items()
    }
    if dict(computed) == declared:
        return True, "matched"
    missing = sorted(set(computed) - set(declared))
    extra = sorted(set(declared) - set(computed))
    wrong = sorted(
        key for key in set(computed) & set(declared) if computed[key] != declared[key]
    )
    return False, f"missing={missing[:10]} extra={extra[:10]} wrong={wrong[:10]}"


def _assert_format_parity(
    xml_rows: ParsedAssets, csv_rows: ParsedAssets, *, label: str
) -> None:
    xml_keys = set(xml_rows.rows)
    csv_keys = set(csv_rows.rows)
    if xml_keys != csv_keys:
        raise ECBReleaseValidationError(
            f"{label} XML/CSV key mismatch: "
            f"xml_only={sorted(xml_keys - csv_keys)[:10]} "
            f"csv_only={sorted(csv_keys - xml_keys)[:10]}"
        )
    mismatched = [
        key
        for key in sorted(xml_keys)
        if xml_rows.rows[key]["row_hash"] != csv_rows.rows[key]["row_hash"]
        or xml_rows.rows[key].get("declared_change_type")
        != csv_rows.rows[key].get("declared_change_type")
    ]
    if mismatched:
        raise ECBReleaseValidationError(
            f"{label} XML/CSV value mismatch for {mismatched[:10]}"
        )


class ECBMIDClient(HTTPSource):
    """Small binary/XML client for MID RSS, releases, and attachments."""

    source_name = "ECB MID"
    error_cls = ECBMIDError

    def __init__(
        self,
        base_url: str = "https://mid.ecb.europa.eu",
        *,
        session: Any = None,
        timeout: int = 30,
        max_retries: int = 5,
        rate_limit_per_minute: int = 60,
        sleep: Callable[[float], None] | None = None,
    ):
        kwargs: dict[str, Any] = {
            "base_url": base_url,
            "session": session,
            "timeout": timeout,
            "max_retries": max_retries,
            "rate_limit_per_minute": rate_limit_per_minute,
        }
        if sleep is not None:
            kwargs["sleep"] = sleep
        super().__init__(**kwargs)

    def _request_headers(self) -> dict[str, str]:
        return {"Accept": "application/xml, application/rss+xml, */*"}

    def fetch_rss(self) -> bytes:
        return self._request("rss/mid.xml", {}, as_bytes=True)

    def fetch_release(self, url: str) -> bytes:
        return self._request(url, {}, as_bytes=True)

    def fetch_artifact(self, url: str) -> bytes:
        return self._request(url, {}, as_bytes=True)


class ECBEligibleAssetRepository:
    """Database operations shared by SQLite and PostgreSQL warehouses."""

    def __init__(self, warehouse: Any):
        if not hasattr(warehouse, "conn") or not hasattr(warehouse, "_insert"):
            raise TypeError(
                "ECB eligible-assets currently requires LocalWarehouse or "
                "PostgresWarehouse; Spark uses the shipped Delta DDL but its "
                "writer remains a separate production phase"
            )
        self.warehouse = warehouse

    def _rows(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        result = self.warehouse.conn.execute(sql, params)
        return [dict(row) for row in result.fetchall()]

    def accepted(self, release_id: str) -> bool:
        return bool(
            self._rows(
                "SELECT 1 AS ok FROM silver_ecb_eligible_asset_release "
                "WHERE release_id = ? AND status = 'accepted'",
                (release_id,),
            )
        )

    def latest_release(self) -> dict[str, Any] | None:
        rows = self._rows(
            "SELECT * FROM silver_ecb_eligible_asset_release "
            "WHERE status = 'accepted' ORDER BY publication_at_utc DESC LIMIT 1"
        )
        return rows[0] if rows else None

    def current_assets(self) -> dict[str, dict[str, Any]]:
        return {
            row["asset_key"]: row
            for row in self._rows(
                "SELECT * FROM silver_ecb_eligible_asset_version "
                "WHERE valid_to_utc IS NULL AND is_eligible = 1"
            )
        }

    def record_release(self, release: ReleaseMessage, *, run_id: str) -> None:
        now = _utc_now()
        self.warehouse._insert(
            "bronze_ecb_eligible_asset_release",
            [
                {
                    "release_id": release.release_id,
                    "release_url": release.url,
                    "release_type": release.release_type,
                    "release_title": release.release_title,
                    "release_at_local": release.release_at_local,
                    "publication_at_local": release.publication_at_local,
                    "publication_at_utc": release.publication_at_utc,
                    "rss_pub_at": release.rss_pub_at,
                    "discovered_at": now,
                    "retrieved_at": now,
                    "status": "downloaded",
                    "run_id": run_id,
                    "error_detail": None,
                }
            ],
            upsert_keys=["release_id"],
        )

    def record_artifact(self, row: dict[str, Any]) -> None:
        self.warehouse._insert(
            "bronze_ecb_eligible_asset_artifact",
            [row],
            upsert_keys=["release_id", "artifact_kind"],
        )

    def set_bronze_status(
        self, release_id: str, status: str, error_detail: str | None = None
    ) -> None:
        self.warehouse.conn.execute(
            "UPDATE bronze_ecb_eligible_asset_release "
            "SET status = ?, error_detail = ? WHERE release_id = ?",
            (status, error_detail, release_id),
        )
        self.warehouse.conn.commit()

    def publish(
        self,
        release: ReleaseMessage,
        current: Mapping[str, Mapping[str, Any]],
        prior: Mapping[str, Mapping[str, Any]],
        update_rows: Mapping[str, Mapping[str, Any]],
        changes: Mapping[str, str],
        dictionary_rows: Iterable[Mapping[str, Any]],
        *,
        dictionary_hash: str,
        schema_hash: str,
        reconciliation_status: str,
        coverage_status: str,
        change_origin: str,
        run_id: str,
    ) -> IngestResult:
        if self.accepted(release.release_id):
            return IngestResult(release.release_id, "duplicate")
        latest = self.latest_release()
        now = _utc_now()
        prior_defer = bool(getattr(self.warehouse, "_defer_commits", False))
        self.warehouse._defer_commits = True
        try:
            dict_rows = [
                {
                    **dict(row),
                    "dictionary_hash": dictionary_hash,
                    "release_id": release.release_id,
                    "ingested_at": now,
                }
                for row in dictionary_rows
            ]
            self.warehouse._insert(
                "silver_ecb_eligible_asset_dictionary",
                dict_rows,
                upsert_keys=["dictionary_hash", "domain", "code"],
            )
            change_rows: list[dict[str, Any]] = []
            version_rows: list[dict[str, Any]] = []
            for asset_key, change_type in sorted(changes.items()):
                self.warehouse.conn.execute(
                    "UPDATE silver_ecb_eligible_asset_version "
                    "SET valid_to_release_id = ?, valid_to_utc = ? "
                    "WHERE asset_key = ? AND valid_to_utc IS NULL",
                    (release.release_id, release.publication_at_utc, asset_key),
                )
                source_row = dict(current.get(asset_key) or prior.get(asset_key) or {})
                is_eligible = change_type != "D"
                row_hash = (
                    source_row.get("row_hash")
                    if is_eligible
                    else _canonical_hash(source_row, is_eligible=False)
                )
                version = {
                    "asset_key": asset_key,
                    **{column: source_row.get(column) for column in _VERSION_COLUMNS},
                    "is_eligible": is_eligible,
                    "source_fields_json": source_row.get("source_fields_json"),
                    "source_schema_version": PARSER_VERSION,
                    "dictionary_hash": dictionary_hash,
                    "row_hash": row_hash,
                    "change_type": change_type,
                    "change_origin": change_origin,
                    "valid_from_release_id": release.release_id,
                    "valid_from_utc": release.publication_at_utc,
                    "valid_to_release_id": None,
                    "valid_to_utc": None,
                    "ingested_at": now,
                    "run_id": run_id,
                }
                version_rows.append(version)
                change_rows.append(
                    {
                        "release_id": release.release_id,
                        "asset_key": asset_key,
                        "declared_change_type": (
                            update_rows.get(asset_key, {}).get("declared_change_type")
                        ),
                        "computed_change_type": change_type,
                        "prior_row_hash": prior.get(asset_key, {}).get("row_hash"),
                        "new_row_hash": current.get(asset_key, {}).get("row_hash"),
                        "reconciliation_status": reconciliation_status,
                        "change_origin": change_origin,
                        "publication_at_utc": release.publication_at_utc,
                        "run_id": run_id,
                    }
                )
            self.warehouse._insert(
                "silver_ecb_eligible_asset_version",
                version_rows,
                upsert_keys=["asset_key", "valid_from_release_id"],
            )
            self.warehouse._insert(
                "silver_ecb_eligible_asset_change",
                change_rows,
                upsert_keys=["release_id", "asset_key"],
            )
            counts = {
                code: sum(1 for value in changes.values() if value == code)
                for code in ("N", "U", "D")
            }
            self.warehouse._insert(
                "silver_ecb_eligible_asset_release",
                [
                    {
                        "release_id": release.release_id,
                        "release_url": release.url,
                        "release_at_local": release.release_at_local,
                        "publication_at_local": release.publication_at_local,
                        "publication_at_utc": release.publication_at_utc,
                        "rss_pub_at": release.rss_pub_at,
                        "prior_release_id": latest.get("release_id")
                        if latest
                        else None,
                        "is_consecutive": coverage_status == "complete",
                        "full_count": len(current),
                        "update_count": len(update_rows),
                        "new_count": counts["N"],
                        "updated_count": counts["U"],
                        "deleted_count": counts["D"],
                        "parser_version": PARSER_VERSION,
                        "source_schema_hash": schema_hash,
                        "dictionary_hash": dictionary_hash,
                        "reconciliation_status": reconciliation_status,
                        "coverage_status": coverage_status,
                        "timestamp_precision": "second",
                        "status": "accepted",
                        "accepted_at": now,
                        "run_id": run_id,
                    }
                ],
                upsert_keys=["release_id"],
            )
            self.warehouse.conn.execute(
                "UPDATE bronze_ecb_eligible_asset_release "
                "SET status = 'accepted', error_detail = NULL WHERE release_id = ?",
                (release.release_id,),
            )
            self.warehouse.conn.commit()
        except Exception:
            self.warehouse.conn.rollback()
            raise
        finally:
            self.warehouse._defer_commits = prior_defer
        return IngestResult(
            release.release_id,
            "accepted",
            full_count=len(current),
            update_count=len(update_rows),
            new_count=counts["N"],
            updated_count=counts["U"],
            deleted_count=counts["D"],
            coverage_status=coverage_status,
            reconciliation_status=reconciliation_status,
        )

    def status(self) -> dict[str, Any]:
        latest = self.latest_release()
        quarantined = self._rows(
            "SELECT release_id, release_url, publication_at_utc, error_detail "
            "FROM bronze_ecb_eligible_asset_release "
            "WHERE status = 'quarantined' ORDER BY publication_at_utc DESC"
        )
        count_rows = self._rows(
            "SELECT COUNT(*) AS n FROM silver_ecb_eligible_asset_version "
            "WHERE valid_to_utc IS NULL AND is_eligible = 1"
        )
        return {
            "latest_accepted": latest,
            "current_eligible_count": int(count_rows[0]["n"]) if count_rows else 0,
            "quarantined": quarantined,
        }

    def as_of(self, timestamp: str) -> list[dict[str, Any]]:
        selected = self._rows(
            "SELECT release_id, publication_at_utc, coverage_status "
            "FROM silver_ecb_eligible_asset_release "
            "WHERE status = 'accepted' AND publication_at_utc <= ? "
            "ORDER BY publication_at_utc DESC LIMIT 1",
            (timestamp,),
        )
        if not selected:
            return []
        release = selected[0]
        rows = self._rows(
            "SELECT * FROM gold_ecb_eligible_security_history "
            "WHERE valid_from_utc <= ? "
            "AND (valid_to_utc IS NULL OR ? < valid_to_utc) "
            "AND is_eligible = 1 ORDER BY asset_key",
            (release["publication_at_utc"], release["publication_at_utc"]),
        )
        for row in rows:
            row["selected_release_id"] = release["release_id"]
            row["selected_release_publication_at_utc"] = release["publication_at_utc"]
            row["coverage_status"] = release["coverage_status"]
        return rows


class ECBEligibleAssetsPipeline:
    """Orchestrate one or more signed MID releases into the PIT repository."""

    def __init__(
        self,
        warehouse: Any,
        *,
        client: ECBMIDClient | Any | None = None,
        signature_verifier: Callable[
            [bytes, str], SignatureResult
        ] = verify_attachment_signature,
    ):
        self.repository = ECBEligibleAssetRepository(warehouse)
        self.client = client or ECBMIDClient()
        self.signature_verifier = signature_verifier

    def poll(self, *, allow_gap: bool = False) -> list[IngestResult]:
        items = parse_rss(self.client.fetch_rss())
        latest = self.repository.latest_release()
        if latest:
            cutoff = datetime.fromisoformat(str(latest["publication_at_utc"]))
            items = [
                item
                for item in items
                if datetime.fromisoformat(item.published_at) > cutoff
            ]
        return [
            self.ingest_release(
                item.url, rss_pub_at=item.published_at, allow_gap=allow_gap
            )
            for item in items
        ]

    def ingest_release(
        self,
        url: str,
        *,
        rss_pub_at: str | None = None,
        allow_gap: bool = False,
        run_id: str | None = None,
    ) -> IngestResult:
        run_id = run_id or str(uuid.uuid4())
        release_bytes = self.client.fetch_release(url)
        release = parse_release_message(release_bytes, url=url, rss_pub_at=rss_pub_at)
        if self.repository.accepted(release.release_id):
            return IngestResult(release.release_id, "duplicate")
        self.repository.record_release(release, run_id=run_id)

        artifacts: dict[str, bytes] = {}
        try:
            latest = self.repository.latest_release()
            if latest and datetime.fromisoformat(
                release.publication_at_utc
            ) <= datetime.fromisoformat(str(latest["publication_at_utc"])):
                raise ECBReleaseValidationError(
                    "out-of-order forward release: "
                    f"{release.publication_at_utc} is not after "
                    f"{latest['publication_at_utc']}; use the chronological "
                    "archive rebuild for historical data"
                )
            for attachment in release.attachments:
                payload = self.client.fetch_artifact(attachment.url)
                verification = self.signature_verifier(payload, attachment.signature)
                decoded = _decode_payload(payload)
                self.repository.record_artifact(
                    {
                        "release_id": release.release_id,
                        "artifact_kind": attachment.kind,
                        "file_name": attachment.file_name,
                        "source_url": attachment.url,
                        "content_type": (
                            "application/xml"
                            if "xml" in attachment.kind
                            else "text/csv"
                        ),
                        "encoding": (
                            _xml_encoding(decoded)
                            if "xml" in attachment.kind
                            else _csv_encoding(decoded)
                        ),
                        "compressed_bytes": len(payload),
                        "uncompressed_bytes": len(decoded),
                        "sha256": _sha256(payload),
                        "md5": _md5(payload),
                        "signature_raw": attachment.signature,
                        "signature_status": verification.status,
                        "signer_subject": verification.signer_subject,
                        "verified_at": _utc_now() if verification.valid else None,
                        "archive_path": None,
                        "payload": payload,
                        "retrieved_at": _utc_now(),
                        "run_id": run_id,
                    }
                )
                if not verification.valid:
                    raise ECBReleaseValidationError(
                        f"signature failed for {attachment.file_name}: "
                        f"{verification.status}: {verification.detail or ''}"
                    )
                artifacts[attachment.kind] = payload

            full = parse_assets_xml(artifacts["full_xml"])
            update = parse_assets_xml(artifacts["update_xml"])
            full_csv = parse_assets_csv(artifacts["full_csv"])
            update_csv = parse_assets_csv(artifacts["update_csv"], update=True)
            _assert_format_parity(full, full_csv, label="full")
            _assert_format_parity(update, update_csv, label="update")
            dictionary_bytes = _decode_payload(artifacts["dictionary_xml"])
            dictionary_hash = _sha256(dictionary_bytes)
            dictionary_rows = parse_dictionary_xml(dictionary_bytes)
            prior = self.repository.current_assets()
            changes = diff_assets(prior, full.rows)
            if prior:
                matched, detail = reconcile_update(changes, update.rows)
                if not matched and not allow_gap:
                    raise ECBReconciliationError(
                        f"full/update reconciliation failed: {detail}"
                    )
                reconciliation = "matched" if matched else "derived_full_diff"
                coverage = "complete" if matched else "gapped"
                change_origin = "source_reconciled" if matched else "derived_full_diff"
            else:
                reconciliation = "initial_snapshot"
                coverage = "starts_here"
                change_origin = "initial_full_snapshot"
            return self.repository.publish(
                release,
                full.rows,
                prior,
                update.rows,
                changes,
                dictionary_rows,
                dictionary_hash=dictionary_hash,
                schema_hash=full.schema_hash,
                reconciliation_status=reconciliation,
                coverage_status=coverage,
                change_origin=change_origin,
                run_id=run_id,
            )
        except Exception as exc:
            self.repository.set_bronze_status(
                release.release_id, "quarantined", str(exc)
            )
            raise

    def verify(self, release_id: str) -> list[dict[str, Any]]:
        rows = self.repository._rows(
            "SELECT artifact_kind, file_name, payload, signature_raw "
            "FROM bronze_ecb_eligible_asset_artifact WHERE release_id = ? "
            "ORDER BY artifact_kind",
            (release_id,),
        )
        if not rows:
            raise ECBEligibleAssetsError(f"unknown release_id {release_id}")
        out = []
        for row in rows:
            result = self.signature_verifier(
                bytes(row["payload"]), row["signature_raw"]
            )
            out.append(
                {
                    "artifact_kind": row["artifact_kind"],
                    "file_name": row["file_name"],
                    "valid": result.valid,
                    "status": result.status,
                    "signer_subject": result.signer_subject,
                    "actual_digest": result.actual_digest,
                    "signed_digest": result.signed_digest,
                }
            )
        return out
