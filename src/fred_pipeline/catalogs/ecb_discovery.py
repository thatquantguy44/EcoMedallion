"""ECB metadata discovery helpers.

The observation client in :mod:`fred_pipeline.sources.ecb` ingests one explicit
``ECB:<flow_ref>:<key>`` series at a time. This module is the metadata side of
that story: list ECB dataflows, inspect SDMX structures, and generate bounded
inactive candidate manifests for review.
"""

from __future__ import annotations

import time
import xml.etree.ElementTree as ET
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from itertools import islice, product
from math import prod
from typing import Any

import yaml

from fred_pipeline.manifest import Manifest, ManifestError, SeriesSpec
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


@dataclass(frozen=True)
class ECBCode:
    """One code-list value usable in an SDMX dimension."""

    code_id: str
    name: str = ""

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class ECBDimension:
    """One ordered series-key dimension from an ECB SDMX data structure."""

    dimension_id: str
    position: int
    codelist_id: str = ""
    codelist_agency_id: str = ""
    codelist_version: str = ""
    name: str = ""
    codes: tuple[ECBCode, ...] = ()

    def to_dict(self, *, sample_size: int | None = None) -> dict[str, Any]:
        codes = self.codes
        if sample_size is not None:
            codes = codes[:sample_size]
        return {
            "dimension_id": self.dimension_id,
            "position": self.position,
            "codelist_id": self.codelist_id,
            "codelist_agency_id": self.codelist_agency_id,
            "codelist_version": self.codelist_version,
            "name": self.name,
            "code_count": len(self.codes),
            "codes": [code.to_dict() for code in codes],
        }


@dataclass(frozen=True)
class ECBDataflowStructure:
    """Parsed metadata needed to inspect and expand one ECB dataflow."""

    flow_id: str
    agency_id: str = "ECB"
    version: str = "1.0"
    name: str = ""
    description: str = ""
    structure_id: str = ""
    structure_agency_id: str = ""
    structure_version: str = ""
    dimensions: tuple[ECBDimension, ...] = ()

    def to_dict(self, *, sample_size: int | None = 10) -> dict[str, Any]:
        return {
            "flow_id": self.flow_id,
            "agency_id": self.agency_id,
            "version": self.version,
            "name": self.name,
            "description": self.description,
            "structure_id": self.structure_id,
            "structure_agency_id": self.structure_agency_id,
            "structure_version": self.structure_version,
            "dimensions": [
                dim.to_dict(sample_size=sample_size) for dim in self.dimensions
            ],
        }


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

    def get_dataflow_structure(
        self,
        flow_ref: str,
        *,
        agency_id: str = "ECB",
        version: str = "1.0",
    ) -> ECBDataflowStructure:
        """Fetch and parse structure metadata for one ECB dataflow."""
        xml_text = self._request(
            f"dataflow/{agency_id}/{flow_ref}/{version}",
            {"references": "all"},
            as_text=True,
        )
        return parse_dataflow_structure_xml(xml_text, flow_ref=flow_ref)


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


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


def _first_ref(parent: ET.Element) -> ET.Element | None:
    for elem in parent.iter():
        if _local_name(elem.tag) == "Ref":
            return elem
    return None


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


def parse_dataflow_structure_xml(
    xml_text: str,
    *,
    flow_ref: str | None = None,
) -> ECBDataflowStructure:
    """Parse SDMX structure XML into an inspectable ECB dataflow structure."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise ECBDiscoveryError(
            f"Could not parse ECB dataflow structure XML: {exc}"
        ) from exc

    dataflows = root.findall(".//str:Dataflow", _NS)
    flow_elem = None
    if flow_ref:
        for elem in dataflows:
            if (elem.attrib.get("id") or "").strip().upper() == flow_ref.upper():
                flow_elem = elem
                break
    if flow_elem is None and dataflows:
        flow_elem = dataflows[0]
    if flow_elem is None:
        raise ECBDiscoveryError("ECB structure response did not contain a Dataflow")

    structure_ref = flow_elem.find("./str:Structure", _NS)
    ref_elem = _first_ref(structure_ref) if structure_ref is not None else None
    structure_id = (ref_elem.attrib.get("id") if ref_elem is not None else "") or ""
    structure_agency_id = (
        ref_elem.attrib.get("agencyID") if ref_elem is not None else ""
    ) or ""
    structure_version = (
        ref_elem.attrib.get("version") if ref_elem is not None else ""
    ) or ""

    structure_elem = None
    for elem in root.findall(".//str:DataStructure", _NS):
        if structure_id and elem.attrib.get("id") == structure_id:
            structure_elem = elem
            break
    if structure_elem is None:
        structures = root.findall(".//str:DataStructure", _NS)
        structure_elem = structures[0] if structures else None
    if structure_elem is None:
        raise ECBDiscoveryError(
            "ECB structure response did not contain a DataStructure"
        )

    if not structure_id:
        structure_id = (structure_elem.attrib.get("id") or "").strip()
    if not structure_agency_id:
        structure_agency_id = (structure_elem.attrib.get("agencyID") or "").strip()
    if not structure_version:
        structure_version = (structure_elem.attrib.get("version") or "").strip()

    codelists = _parse_codelists(root)
    dimensions = _parse_dimensions(structure_elem, codelists)

    return ECBDataflowStructure(
        flow_id=(flow_elem.attrib.get("id") or flow_ref or "").strip(),
        agency_id=(flow_elem.attrib.get("agencyID") or "ECB").strip(),
        version=(flow_elem.attrib.get("version") or "1.0").strip(),
        name=_localized_text(flow_elem, "Name"),
        description=_localized_text(flow_elem, "Description"),
        structure_id=structure_id,
        structure_agency_id=structure_agency_id,
        structure_version=structure_version,
        dimensions=tuple(dimensions),
    )


def _parse_codelists(
    root: ET.Element,
) -> dict[str, tuple[dict[str, str], list[ECBCode]]]:
    codelists: dict[str, tuple[dict[str, str], list[ECBCode]]] = {}
    for elem in root.findall(".//str:Codelist", _NS):
        codelist_id = (elem.attrib.get("id") or "").strip()
        if not codelist_id:
            continue
        codes: list[ECBCode] = []
        for code_elem in elem.findall("./str:Code", _NS):
            code_id = (code_elem.attrib.get("id") or "").strip()
            if not code_id:
                continue
            codes.append(
                ECBCode(code_id=code_id, name=_localized_text(code_elem, "Name"))
            )
        codelists[codelist_id] = (
            {
                "agency_id": (elem.attrib.get("agencyID") or "").strip(),
                "version": (elem.attrib.get("version") or "").strip(),
            },
            codes,
        )
    return codelists


def _parse_dimensions(
    structure_elem: ET.Element,
    codelists: dict[str, tuple[dict[str, str], list[ECBCode]]],
) -> list[ECBDimension]:
    dimensions: list[ECBDimension] = []
    dim_list = structure_elem.find(".//str:DimensionList", _NS)
    if dim_list is None:
        return dimensions

    ordinal = 0
    for elem in dim_list:
        if _local_name(elem.tag) != "Dimension":
            continue
        ordinal += 1
        dim_id = (elem.attrib.get("id") or "").strip()
        if not dim_id:
            continue
        try:
            position = int(elem.attrib.get("position") or ordinal)
        except ValueError:
            position = ordinal

        enum_elem = elem.find("./str:LocalRepresentation/str:Enumeration", _NS)
        ref_elem = _first_ref(enum_elem) if enum_elem is not None else None
        codelist_id = (ref_elem.attrib.get("id") if ref_elem is not None else "") or ""
        codelist_meta, codes = codelists.get(codelist_id, ({}, []))
        concept_elem = elem.find("./str:ConceptIdentity", _NS)
        concept_ref = _first_ref(concept_elem) if concept_elem is not None else None
        concept_name = (
            concept_ref.attrib.get("id") if concept_ref is not None else ""
        ) or dim_id

        dimensions.append(
            ECBDimension(
                dimension_id=dim_id,
                position=position,
                codelist_id=codelist_id,
                codelist_agency_id=codelist_meta.get("agency_id", ""),
                codelist_version=codelist_meta.get("version", ""),
                name=concept_name,
                codes=tuple(codes),
            )
        )
    return sorted(dimensions, key=lambda dim: dim.position)


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


_ECB_TO_MANIFEST_FREQ = {
    "D": "d",
    "W": "w",
    "M": "m",
    "Q": "q",
    "S": "sa",
    "H": "sa",
    "A": "a",
}
_MANIFEST_TO_ECB_FREQ = {
    manifest: ecb for ecb, manifest in _ECB_TO_MANIFEST_FREQ.items()
}
_MANIFEST_TO_ECB_FREQ["sa"] = "S"
_MANIFEST_TO_ECB_FREQ.update(
    {
        "daily": "D",
        "weekly": "W",
        "monthly": "M",
        "quarterly": "Q",
        "semiannual": "S",
        "annual": "A",
    }
)


def ecb_frequency_to_manifest(value: str) -> str | None:
    """Map an ECB frequency dimension code to the manifest frequency code."""
    return _ECB_TO_MANIFEST_FREQ.get(value.strip().upper())


def parse_dimension_filters(values: Iterable[str] | None) -> dict[str, set[str]]:
    """Parse repeated ``DIM=CODE[,CODE]`` filter arguments."""
    filters: dict[str, set[str]] = {}
    for raw in values or []:
        if "=" not in raw:
            raise ECBDiscoveryError(
                f"Invalid dimension filter {raw!r}; expected DIM=CODE[,CODE]"
            )
        dim, raw_codes = raw.split("=", 1)
        dim = dim.strip().upper()
        codes = {code.strip() for code in raw_codes.split(",") if code.strip()}
        if not dim or not codes:
            raise ECBDiscoveryError(
                f"Invalid dimension filter {raw!r}; expected DIM=CODE[,CODE]"
            )
        filters.setdefault(dim, set()).update(codes)
    return filters


def _selected_codes(
    structure: ECBDataflowStructure,
    *,
    dimension_filters: dict[str, set[str]] | None = None,
    frequencies: Iterable[str] | None = None,
    include_code: Iterable[str] | None = None,
    exclude_code: Iterable[str] | None = None,
) -> tuple[list[tuple[ECBDimension, list[ECBCode]]], list[dict[str, Any]]]:
    filters = {k.upper(): set(v) for k, v in (dimension_filters or {}).items()}
    if frequencies:
        mapped = {
            _MANIFEST_TO_ECB_FREQ.get(freq.strip().lower(), freq.strip().upper())
            for freq in frequencies
            if freq.strip()
        }
        if mapped:
            filters.setdefault("FREQ", set()).update(mapped)

    include_needles = [s.strip().lower() for s in include_code or [] if s.strip()]
    exclude_needles = [s.strip().lower() for s in exclude_code or [] if s.strip()]

    selected: list[tuple[ECBDimension, list[ECBCode]]] = []
    skipped: list[dict[str, Any]] = []
    for dim in structure.dimensions:
        codes = list(dim.codes)
        if not codes:
            skipped.append(
                {"dimension_id": dim.dimension_id, "reason": "dimension has no codes"}
            )
            selected.append((dim, []))
            continue
        requested = filters.get(dim.dimension_id.upper())
        if requested:
            code_lookup = {code.code_id: code for code in codes}
            codes = [
                code_lookup[code_id] for code_id in requested if code_id in code_lookup
            ]
            missing = sorted(requested - set(code_lookup))
            for code_id in missing:
                skipped.append(
                    {
                        "dimension_id": dim.dimension_id,
                        "code_id": code_id,
                        "reason": "filter code not in dimension code list",
                    }
                )
        if include_needles:
            codes = [
                code
                for code in codes
                if any(
                    needle in f"{code.code_id} {code.name}".lower()
                    for needle in include_needles
                )
            ]
        if exclude_needles:
            codes = [
                code
                for code in codes
                if not any(
                    needle in f"{code.code_id} {code.name}".lower()
                    for needle in exclude_needles
                )
            ]
        selected.append((dim, sorted(codes, key=lambda code: code.code_id)))
    return selected, skipped


def estimate_candidate_count(
    structure: ECBDataflowStructure,
    *,
    dimension_filters: dict[str, set[str]] | None = None,
    frequencies: Iterable[str] | None = None,
    include_code: Iterable[str] | None = None,
    exclude_code: Iterable[str] | None = None,
) -> int:
    """Estimate Cartesian expansion count after filters."""
    selected, _skipped = _selected_codes(
        structure,
        dimension_filters=dimension_filters,
        frequencies=frequencies,
        include_code=include_code,
        exclude_code=exclude_code,
    )
    if not selected:
        return 0
    return prod(len(codes) for _dim, codes in selected)


def infer_ecb_category(structure: ECBDataflowStructure) -> str:
    """Infer a broad manifest category from the flow id/name."""
    text = f"{structure.flow_id} {structure.name}".lower()
    if "exr" in text or "exchange" in text:
        return "international"
    if any(token in text for token in ("rate", "yield", "fm", "mir", "rir", "est")):
        return "rates"
    if any(token in text for token in ("price", "hicp", "inflation", "icp")):
        return "inflation"
    if any(token in text for token in ("money", "balance sheet", "bsi", "mfi")):
        return "money"
    if any(token in text for token in ("credit", "bank lending", "stress")):
        return "credit"
    if any(token in text for token in ("gdp", "national accounts", "output")):
        return "growth"
    if any(token in text for token in ("labour", "labor", "employment", "wage")):
        return "labor"
    return "international"


def _expected_update_frequency(frequency: str) -> str:
    return {
        "d": "daily",
        "w": "weekly",
        "m": "monthly",
        "q": "quarterly",
        "sa": "semiannual",
        "a": "annual",
    }.get(frequency, frequency)


def _candidate_title(
    structure: ECBDataflowStructure,
    dim_codes: Iterable[tuple[ECBDimension, ECBCode]],
) -> str:
    parts = [structure.name or structure.flow_id]
    for dim, code in dim_codes:
        if dim.dimension_id.upper() == "FREQ":
            continue
        label = code.name or code.code_id
        if len(label) > 80:
            label = f"{label[:77]}..."
        parts.append(label)
    title = " - ".join(part for part in parts if part)
    return title[:200]


def generate_ecb_candidate_specs(
    structure: ECBDataflowStructure,
    *,
    dimension_filters: dict[str, set[str]] | None = None,
    frequencies: Iterable[str] | None = None,
    include_code: Iterable[str] | None = None,
    exclude_code: Iterable[str] | None = None,
    category: str | None = None,
    max_results: int = 100,
    max_cartesian: int = 10000,
    force: bool = False,
    exclude_ids: Iterable[str] | None = None,
) -> tuple[list[SeriesSpec], list[dict[str, Any]]]:
    """Generate inactive ECB manifest specs from a bounded dimension expansion."""
    selected, skipped = _selected_codes(
        structure,
        dimension_filters=dimension_filters,
        frequencies=frequencies,
        include_code=include_code,
        exclude_code=exclude_code,
    )
    if not selected:
        return [], skipped
    estimate = prod(len(codes) for _dim, codes in selected)
    if estimate > max_cartesian and not force:
        raise ECBDiscoveryError(
            f"ECB candidate expansion for {structure.flow_id} would generate "
            f"{estimate} combinations, above --max-cartesian {max_cartesian}. "
            "Add --dimension filters or pass --force."
        )

    existing = {series_id for series_id in (exclude_ids or [])}
    manifest_category = category or infer_ecb_category(structure)
    specs: list[SeriesSpec] = []
    seen: set[str] = set()
    code_lists = [codes for _dim, codes in selected]
    for combo in islice(product(*code_lists), max_results):
        dim_codes = list(zip((dim for dim, _codes in selected), combo))
        key = ".".join(code.code_id for code in combo)
        series_id = f"ECB:{structure.flow_id}:{key}"
        if series_id in existing:
            skipped.append({"series_id": series_id, "reason": "already in manifest"})
            continue
        if series_id in seen:
            skipped.append({"series_id": series_id, "reason": "duplicate candidate"})
            continue
        freq_code = next(
            (
                code.code_id
                for dim, code in dim_codes
                if dim.dimension_id.upper() == "FREQ"
            ),
            "",
        )
        manifest_frequency = ecb_frequency_to_manifest(freq_code)
        if manifest_frequency is None:
            skipped.append(
                {
                    "series_id": series_id,
                    "reason": f"unsupported frequency {freq_code!r}",
                }
            )
            continue
        spec = {
            "series_id": series_id,
            "title": _candidate_title(structure, dim_codes),
            "category": manifest_category,
            "frequency": manifest_frequency,
            "units": "",
            "active": False,
            "source": "ecb",
            "load_type": "incremental",
            "expected_update_frequency": _expected_update_frequency(manifest_frequency),
            "vintage_enabled": True,
            "validation_profile": "lenient",
            "downstream_use_case": "ecb_candidate_review",
            "priority": 3,
            "tags": ["ecb", structure.flow_id.lower(), manifest_category],
        }
        try:
            specs.append(SeriesSpec(**spec))
            seen.add(series_id)
        except ManifestError as exc:
            skipped.append({"series_id": series_id, "reason": f"validation: {exc}"})
    return specs, skipped


def build_ecb_manifest_dict(
    name: str,
    specs: Iterable[SeriesSpec],
    *,
    description: str = "",
    version: int = 1,
) -> dict[str, Any]:
    """Assemble an ECB candidate manifest dictionary."""
    return {
        "name": name,
        "description": description,
        "version": version,
        "series": [spec.to_dict() for spec in specs],
    }


def ecb_manifest_to_yaml(manifest_dict: dict[str, Any]) -> str:
    """Serialize and validate an ECB candidate manifest."""
    Manifest.from_dict(manifest_dict)
    return yaml.safe_dump(manifest_dict, sort_keys=False, default_flow_style=False)
