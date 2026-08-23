"""ECB metadata discovery helpers.

The observation client in :mod:`fred_pipeline.sources.ecb` ingests one explicit
``ECB:<flow_ref>:<key>`` series at a time. This module is the metadata side of
that story: list ECB dataflows and, in later slices, inspect SDMX structures to
generate candidate manifests.
"""

from __future__ import annotations

import time
import xml.etree.ElementTree as ET
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from typing import Any

from fred_pipeline.sources.base import HTTPSource, SourceError

ECB_STRUCTURE_ACCEPT = "application/vnd.sdmx.structure+xml;version=2.1"

_NS = {
    "str": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "com": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
}


class ECBDiscoveryError(SourceError):
    """Raised when ECB metadata discovery fails."""


@dataclass(frozen=True)
class ECBDataflow:
    """One ECB SDMX dataflow advertised by the metadata endpoint."""

    flow_id: str
    agency_id: str = "ECB"
    version: str = "1.0"
    name: str = ""
    description: str = ""
    structure_url: str = ""

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


class ECBMetadataClient(HTTPSource):
    """Retrying, rate-limited ECB metadata client."""

    source_name = "ECB metadata"
    error_cls = ECBDiscoveryError

    def __init__(
        self,
        base_url: str = "https://data-api.ecb.europa.eu/service",
        *,
        session: Any = None,
        timeout: int = 30,
        max_retries: int = 5,
        rate_limit_per_minute: int = 60,
        sleep: Callable[[float], None] = time.sleep,
    ):
        super().__init__(
            base_url=base_url,
            session=session,
            timeout=timeout,
            max_retries=max_retries,
            rate_limit_per_minute=rate_limit_per_minute,
            sleep=sleep,
        )

    def _request_headers(self) -> dict[str, str]:
        return {"Accept": ECB_STRUCTURE_ACCEPT}

    def _error_detail(self, resp: Any) -> str:
        text = str(getattr(resp, "text", "") or "").strip()
        return text[:500] if text else "<no body>"

    def list_dataflows(self) -> list[ECBDataflow]:
        """Fetch and parse ECB dataflow stubs."""
        xml_text = self._request(
            "dataflow",
            {"detail": "allstubs"},
            as_text=True,
        )
        return parse_dataflows_xml(xml_text)


def _localized_text(parent: ET.Element, tag: str) -> str:
    values: list[tuple[bool, str]] = []
    for elem in parent.findall(f"com:{tag}", _NS):
        text = "".join(elem.itertext()).strip()
        if not text:
            continue
        lang = elem.attrib.get("{http://www.w3.org/XML/1998/namespace}lang", "")
        values.append((lang.lower() == "en", text))
    if not values:
        return ""
    values.sort(key=lambda item: item[0], reverse=True)
    return values[0][1]


def parse_dataflows_xml(xml_text: str) -> list[ECBDataflow]:
    """Parse ECB SDMX dataflow XML into stable records."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise ECBDiscoveryError(f"Could not parse ECB dataflow XML: {exc}") from exc

    flows: list[ECBDataflow] = []
    for elem in root.findall(".//str:Dataflow", _NS):
        flow_id = (elem.attrib.get("id") or "").strip()
        if not flow_id:
            continue
        flows.append(
            ECBDataflow(
                flow_id=flow_id,
                agency_id=(elem.attrib.get("agencyID") or "ECB").strip(),
                version=(elem.attrib.get("version") or "1.0").strip(),
                name=_localized_text(elem, "Name"),
                description=_localized_text(elem, "Description"),
                structure_url=(elem.attrib.get("structureURL") or "").strip(),
            )
        )
    return flows


def filter_dataflows(
    flows: Iterable[ECBDataflow],
    *,
    search: str | None = None,
    max_results: int | None = None,
) -> list[ECBDataflow]:
    """Filter dataflows by text and cap returned rows."""
    needle = (search or "").strip().lower()
    rows: list[ECBDataflow] = []
    for flow in flows:
        haystack = (
            f"{flow.flow_id} {flow.agency_id} {flow.version} "
            f"{flow.name} {flow.description}"
        ).lower()
        if needle and needle not in haystack:
            continue
        rows.append(flow)
        if max_results is not None and len(rows) >= max_results:
            break
    return rows


def dataflows_to_rows(flows: Iterable[ECBDataflow]) -> list[dict[str, str]]:
    """Return JSON/table-friendly dataflow rows."""
    return [flow.to_dict() for flow in flows]
