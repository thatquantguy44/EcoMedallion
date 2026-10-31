"""Replay Silver (and Gold) from Bronze — no FRED calls.

Bronze retains every raw API payload verbatim (the system of record), so Silver
and Gold can always be re-derived from it. This is the tool to run after fixing
a transform/normalization bug, or to backfill a table that was dropped: it reads
archived payloads in ingestion order and re-MERGEs them, so the reconstruction
is deterministic and idempotent.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Iterable, Optional

from fred_pipeline.config import PipelineConfig
from fred_pipeline.manifest import Manifest, all_series
from fred_pipeline.silver import build_silver_rows

log = logging.getLogger("fred_pipeline.replay")


def replay_from_bronze(
    config: PipelineConfig,
    manifests: Iterable[Manifest],
    warehouse: Any,
    *,
    series_ids: Optional[list[str]] = None,
    rebuild_gold: bool = True,
) -> dict[str, Any]:
    """Rebuild Silver from archived Bronze payloads, then optionally Gold.

    ``manifests`` supplies the per-series ``vintage_enabled`` flag so replayed
    rows are normalized identically to a live run. Bronze rows are replayed in
    ingestion order; because Silver is a MERGE on the natural key, replaying the
    full history reconstructs the accumulated state without duplicates.
    """
    specs = all_series(list(manifests), active_only=False)
    vintage = {s.series_id: s.vintage_enabled for s in specs}
    source_by_id = {s.series_id: s.source for s in specs}
    bronze_rows = warehouse.read_bronze(series_ids)
    log.info("Replaying %d bronze payload(s)", len(bronze_rows))

    payloads = 0
    silver_merged = 0
    seen: set[str] = set()
    for row in bronze_rows:
        series_id = row["series_id"]
        try:
            payload = json.loads(row["response_payload"])
        except (json.JSONDecodeError, TypeError):
            log.warning("Skipping unparseable bronze payload for %s", series_id)
            continue
        # Prefer the source recorded in the bronze row (what was actually
        # fetched); fall back to the manifest, then FRED for legacy rows.
        source = row.get("source") or source_by_id.get(series_id) or "fred"
        silver_rows = build_silver_rows(
            series_id,
            payload,
            run_id=row.get("run_id") or "replay",
            ingested_at=row.get("ingested_at"),
            track_vintage=vintage.get(series_id, True),
            source=source,
        )
        silver_merged += warehouse.merge_silver(silver_rows)
        payloads += 1
        seen.add(series_id)

    result = {
        "bronze_payloads_replayed": payloads,
        "series": len(seen),
        "silver_rows_merged": silver_merged,
        "gold_rebuilt": False,
    }
    if rebuild_gold and payloads:
        warehouse.build_gold()
        result["gold_rebuilt"] = True
    log.info("Replay complete: %s", result)
    return result
