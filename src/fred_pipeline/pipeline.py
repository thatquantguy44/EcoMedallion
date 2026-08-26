"""End-to-end orchestration: manifest → Bronze → Silver → DQ → Gold → audit.

This is the module a Databricks job (or a local CLI) calls. It wires the
Spark-free core (client, transform, quality, audit) to the Spark I/O layer and
records a complete, auditable trail for every run.

The orchestrator is defensive per-series: one series failing (bad id, DQ error
under a strict profile, network exhaustion) is recorded and the run continues,
finishing as ``PARTIAL`` rather than losing all progress.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from fred_pipeline.audit import EtlRun, RunStatus
from fred_pipeline.bronze import build_bronze_row
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.manifest import LoadType, SeriesSpec, all_series, load_manifests
from fred_pipeline.quality import run_quality_checks
from fred_pipeline.sources.base import SourceClient
from fred_pipeline.sources.bea import BEAClient
from fred_pipeline.sources.bis import BISClient
from fred_pipeline.sources.bls import BLSClient
from fred_pipeline.sources.census import CensusClient
from fred_pipeline.sources.ecb import ECBClient
from fred_pipeline.sources.eia import EIAClient
from fred_pipeline.sources.fred import FredClient
from fred_pipeline.sources.ishares import ISharesClient
from fred_pipeline.sources.sec import SECClient
from fred_pipeline.sources.stooq import StooqClient
from fred_pipeline.sources.tiingo import TiingoClient
from fred_pipeline.sources.treasury import TreasuryClient
from fred_pipeline.sources.worldbank import WorldBankClient
from fred_pipeline.timing import timed
from fred_pipeline.transform import assign_revision_numbers, normalize_observations
from fred_pipeline.warehouse import SparkWarehouse, Warehouse

log = logging.getLogger("fred_pipeline")

# Full-vintage real-time window for point-in-time enabled series.
FULL_VINTAGE_START = "1776-07-04"
FULL_VINTAGE_END = "9999-12-31"


def _make_fred(config: PipelineConfig) -> SourceClient:
    return FredClient(
        api_key=config.fred_api_key,
        base_url=config.fred_base_url,
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit_per_minute=_rate_limit_for_source(config, "fred"),
    )


def _make_bls(config: PipelineConfig) -> SourceClient:
    # BLS keyless works at a lower quota; a key is used if one is configured.
    return BLSClient(
        api_key=getattr(config, "bls_api_key", "") or None,
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit_per_minute=_rate_limit_for_source(config, "bls"),
    )


def _make_eia(config: PipelineConfig) -> SourceClient:
    # EIA requires a key; EIAClient raises if one isn't configured.
    return EIAClient(
        api_key=getattr(config, "eia_api_key", "") or "",
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit_per_minute=_rate_limit_for_source(config, "eia"),
    )


def _make_ecb(config: PipelineConfig) -> SourceClient:
    return ECBClient(
        base_url=config.ecb_base_url,
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit_per_minute=_rate_limit_for_source(config, "ecb"),
    )


def _make_treasury(config: PipelineConfig) -> SourceClient:
    return TreasuryClient(
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit_per_minute=_rate_limit_for_source(config, "treasury"),
    )


def _make_worldbank(config: PipelineConfig) -> SourceClient:
    return WorldBankClient(
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit_per_minute=_rate_limit_for_source(config, "worldbank"),
    )


def _make_bis(config: PipelineConfig) -> SourceClient:
    return BISClient(
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit_per_minute=_rate_limit_for_source(config, "bis"),
    )


def _make_bea(config: PipelineConfig) -> SourceClient:
    # BEA requires a key; BEAClient raises if one isn't configured.
    return BEAClient(
        api_key=getattr(config, "bea_api_key", "") or "",
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit_per_minute=_rate_limit_for_source(config, "bea"),
    )


def _make_census(config: PipelineConfig) -> SourceClient:
    # Census works keyless at a lower quota; a key is used if configured.
    return CensusClient(
        api_key=getattr(config, "census_api_key", "") or None,
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit_per_minute=_rate_limit_for_source(config, "census"),
    )


def _make_sec(config: PipelineConfig) -> SourceClient:
    # SEC is keyless but requires a descriptive User-Agent (contact). The target
    # income-statement duration comes from SEC_PERIOD (default quarterly).
    from fred_pipeline.sources.sec import resolve_sec_period

    return SECClient(
        user_agent=getattr(config, "sec_user_agent", "") or None,
        period=resolve_sec_period(),
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit_per_minute=_rate_limit_for_source(config, "sec"),
    )


def _make_stooq(config: PipelineConfig) -> SourceClient:
    # Stooq daily OHLCV (equity price return); CSV downloads may require a key.
    return StooqClient(
        api_key=getattr(config, "stooq_api_key", "") or "",
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit_per_minute=_rate_limit_for_source(config, "stooq"),
    )


def _make_ishares(config: PipelineConfig) -> SourceClient:
    # Keyless ETF-holdings CSV (index constituents / symbol universe).
    return ISharesClient(
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit_per_minute=_rate_limit_for_source(config, "ishares"),
    )


def _normalize_tiingo_keys(value: Any) -> list[str]:
    """``tiingo_api_key`` may be a single string (the common case) or a list
    of several account keys to rotate through when one's hourly quota is
    exhausted (Tiingo's free-tier quota is tracked per key/account, not per
    IP). Accepts either shape; returns a clean list, dropping blanks."""
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, (list, tuple)):
        return [str(v) for v in value if v]
    return []


def _make_tiingo(config: PipelineConfig) -> SourceClient:
    # Tiingo requires a (free) key; TiingoClient raises if one isn't configured.
    keys = _normalize_tiingo_keys(getattr(config, "tiingo_api_key", ""))
    primary, backups = (keys[0], keys[1:]) if keys else ("", [])
    return TiingoClient(
        api_key=primary,
        backup_api_keys=backups,
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit_per_minute=_rate_limit_for_source(config, "tiingo"),
    )


# Registry of source name -> client factory. Adding a source is one entry here
# plus its client module under fred_pipeline.sources.
SOURCE_FACTORIES = {
    "fred": _make_fred,
    "bls": _make_bls,
    "eia": _make_eia,
    "ecb": _make_ecb,
    "treasury": _make_treasury,
    "worldbank": _make_worldbank,
    "bis": _make_bis,
    "bea": _make_bea,
    "census": _make_census,
    "sec": _make_sec,
    "stooq": _make_stooq,
    "ishares": _make_ishares,
    "tiingo": _make_tiingo,
}

# Sources that require an API key to call, mapped to the PipelineConfig
# attribute holding it. Sources not listed can run keyless (BLS, Census,
# Treasury, World Bank, SEC, iShares).
SOURCE_KEY_REQUIREMENTS = {
    "fred": "fred_api_key",
    "eia": "eia_api_key",
    "bea": "bea_api_key",
    "stooq": "stooq_api_key",
    "tiingo": "tiingo_api_key",
}


def missing_source_keys(
    config: PipelineConfig, sources: Iterable[str]
) -> dict[str, str]:
    """Return ``{source: config_attr}`` for sources whose required key is unset.

    Lets a caller validate that a run has the keys the *active* sources need,
    instead of demanding a FRED key unconditionally.
    """
    missing: dict[str, str] = {}
    for source in sources:
        attr = SOURCE_KEY_REQUIREMENTS.get(source)
        if attr and not getattr(config, attr, ""):
            missing[source] = attr
    return missing


def _parse_source_int_overrides(raw: str, setting: str) -> dict[str, int]:
    """Parse ``fred=16,tiingo=1`` style per-source integer overrides."""
    out: dict[str, int] = {}
    for part in (raw or "").split(","):
        item = part.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(f"{setting} entries must look like 'source=N'")
        source, value = item.split("=", 1)
        source = source.strip().lower()
        if not source:
            raise ValueError(f"{setting} contains an empty source")
        n = int(value.strip())
        if n < 1:
            raise ValueError(f"{setting} values must be >= 1")
        out[source] = n
    return out


def _parse_source_worker_overrides(raw: str) -> dict[str, int]:
    return _parse_source_int_overrides(raw, "source_extract_workers")


def _parse_source_rate_overrides(raw: str) -> dict[str, int]:
    return _parse_source_int_overrides(raw, "source_rate_limits")


def _rate_limit_for_source(config: PipelineConfig, source: str) -> int:
    """Request-per-minute cap for one source client."""
    source = source.lower()
    overrides = _parse_source_rate_overrides(config.source_rate_limits)
    if source in overrides:
        return overrides[source]
    if "*" in overrides:
        return overrides["*"]
    defaults = {
        "fred": config.rate_limit_per_minute,
        "bls": 25,
        "eia": 60,
        "ecb": 60,
        "treasury": 120,
        "worldbank": 60,
        "bis": 30,
        "bea": 100,
        "census": 30,
        "sec": 300,
        "stooq": 20,
        "ishares": 30,
        "tiingo": 10,
    }
    return defaults.get(source, 60)


def _extract_workers_for_source(config: PipelineConfig, source: str) -> int:
    """Worker count for one source-specific extraction pool.

    FRED can use the configured default because its shared rate limiter still
    gates aggregate request issuance. Tiingo gets a conservative default cap to
    reduce quota-burst 429s; callers can override any source with
    ``source_extract_workers`` / ``FRED_SOURCE_EXTRACT_WORKERS``.
    """
    total = max(1, config.extract_workers)
    overrides = _parse_source_worker_overrides(config.source_extract_workers)
    source = source.lower()
    if source in overrides:
        return overrides[source]
    if "*" in overrides:
        return overrides["*"]
    if source in {"tiingo", "stooq"}:
        return min(2, total)
    if source in {"worldbank", "bis"}:
        return min(8, total)
    return total


class FredPipeline:
    def __init__(
        self,
        config: PipelineConfig,
        *,
        client: SourceClient | None = None,
        clients: dict[str, SourceClient] | None = None,
        spark: Any = None,
        warehouse: Warehouse | None = None,
        persist_audit: bool = True,
        notify_transport: Any = None,
        alert_transport: Any = None,
    ):
        self.config = config
        self.persist_audit = persist_audit
        # Per-source client cache. ``client`` (back-compat) seeds the default
        # "fred" source; ``clients`` supplies/overrides any source explicitly.
        # Anything not provided is built lazily from SOURCE_FACTORIES.
        self._clients: dict[str, SourceClient] = dict(clients or {})
        if client is not None:
            self._clients.setdefault("fred", client)
        self._notify_transport = notify_transport
        # Injectable so tests never touch a mailbox; None means the
        # transport named in config/alerting.yml (console by default).
        self._alert_transport = alert_transport
        self._stage_tracker: Any = None
        self._alert_sent = False
        # Resolve the storage backend: explicit warehouse > Spark > None (dry run).
        if warehouse is not None:
            self.warehouse: Warehouse | None = warehouse
        elif spark is not None:
            self.warehouse = SparkWarehouse(config, spark)
        else:
            self.warehouse = None

    @property
    def client(self) -> SourceClient:
        """The default (FRED) source client. Kept for back-compat; per-series
        extraction goes through :meth:`_client_for`."""
        return self._client_for_source("fred")

    def _client_for_source(self, source: str) -> SourceClient:
        client = self._clients.get(source)
        if client is None:
            factory = SOURCE_FACTORIES.get(source)
            if factory is None:
                raise ValueError(
                    f"Unknown source {source!r}; known sources: "
                    f"{sorted(SOURCE_FACTORIES)}"
                )
            client = factory(self.config)
            self._clients[source] = client
        return client

    def _client_for(self, spec: SeriesSpec) -> SourceClient:
        return self._client_for_source(getattr(spec, "source", "fred") or "fred")

    # ---- run entrypoints ------------------------------------------------

    def run_from_manifest(
        self,
        manifest_path: str,
        *,
        triggered_by: str = "",
        build_gold_layer: bool = True,
        series: list[str] | None = None,
        sources: list[str] | None = None,
        exclude_sources: list[str] | None = None,
        force_full: bool = False,
    ) -> EtlRun:
        manifests = load_manifests(manifest_path)
        specs = all_series(manifests, active_only=True)
        if series:
            wanted = set(series)
            specs = [s for s in specs if s.series_id in wanted]
            missing = wanted - {s.series_id for s in specs}
            if missing:
                log.warning("Requested series not found/active: %s", sorted(missing))
        if sources:
            wanted_sources = {s.lower() for s in sources}
            specs = [
                s
                for s in specs
                if (getattr(s, "source", "fred") or "fred").lower() in wanted_sources
            ]
        if exclude_sources:
            blocked_sources = {s.lower() for s in exclude_sources}
            specs = [
                s
                for s in specs
                if (getattr(s, "source", "fred") or "fred").lower()
                not in blocked_sources
            ]
        log.info("Loaded %d active series from %s", len(specs), manifest_path)
        if self.warehouse is not None:
            try:
                self.warehouse.sync_meta(manifests)
                log.info("Synced %d series to meta layer", len(specs))
            except Exception:
                log.exception("Meta sync failed (continuing)")
        return self.run(
            specs,
            manifest_path=manifest_path,
            triggered_by=triggered_by,
            build_gold_layer=build_gold_layer,
            force_full=force_full,
        )

    @timed("FredPipeline.run")
    def run(
        self,
        specs: Iterable[SeriesSpec],
        *,
        manifest_path: str = "",
        triggered_by: str = "",
        build_gold_layer: bool = True,
        force_full: bool = False,
    ) -> EtlRun:
        specs = list(specs)
        run = EtlRun(
            environment=self.config.environment.value,
            manifest_path=manifest_path,
            triggered_by=triggered_by,
        )
        log.info("Starting run %s (%d series)", run.run_id, len(specs))

        # Stage tracking runs alongside the audit trail, not instead of it.
        # EtlRun.status reflects SERIES outcomes; the tracker records whether
        # each PHASE did its job, which is a different question -- a run can
        # ingest every series cleanly and still fail to rebuild Gold.
        from fred_pipeline.governance.stages import RunStageTracker

        tracker = RunStageTracker()
        self._stage_tracker = tracker

        # Phase 1 (sequential): decide each series' load window. This reads the
        # warehouse (one SQLite/Delta connection), so it must not run
        # concurrently with itself or with later writes. Series audit rows are
        # opened in manifest order so the final run object stays deterministic
        # even though extraction finishes out of order.
        with tracker.stage("plan") as stage:
            series_runs = [
                run.start_series(spec.series_id, load_type=spec.load_type.value)
                for spec in specs
            ]
            run.series_total = len(series_runs)
            plans = [self._plan_extract(spec, force_full=force_full) for spec in specs]
            work_items = list(zip(specs, plans, series_runs))
            stage.detail["series_planned"] = len(work_items)

        # Phase 2/3 (source-aware extraction, streaming finish): network-bound
        # fetches run in per-source pools, while Bronze/Silver/DQ/audit writes
        # happen immediately as each future completes on this main thread. This
        # avoids losing completed FRED/Stooq work when a low-quota source is
        # still sleeping in retry/backoff.
        # A hard extraction failure still propagates (callers rely on it),
        # but the alert goes out first -- an operator must hear about the
        # run that died, not just the ones that finished.
        try:
            with tracker.stage("extract") as extract_stage:
                grouped: dict[
                    str, list[tuple[int, SeriesSpec, tuple[str | None, str]]]
                ] = defaultdict(list)
                for idx, (spec, plan) in enumerate(zip(specs, plans)):
                    grouped[(getattr(spec, "source", "fred") or "fred").lower()].append(
                        (idx, spec, plan)
                    )
                for source in grouped:
                    if source in self._clients or source in SOURCE_FACTORIES:
                        self._client_for_source(
                            source
                        )  # build clients before threads race
                pools: list[ThreadPoolExecutor] = []
                futures = {}
                completed = 0
                try:
                    for source, items in grouped.items():
                        workers = _extract_workers_for_source(self.config, source)
                        pool = ThreadPoolExecutor(
                            max_workers=workers,
                            thread_name_prefix=f"{source}-extract",
                        )
                        pools.append(pool)
                        log.info(
                            "Submitted %d %s series (%d workers, %d rpm)",
                            len(items),
                            source,
                            workers,
                            _rate_limit_for_source(self.config, source),
                        )
                        for idx, spec, plan in items:
                            futures[pool.submit(self._safe_extract, spec, plan)] = idx

                    for fut in as_completed(futures):
                        idx = futures[fut]
                        spec, (_observation_start, load_type), sr = work_items[idx]
                        outcome = fut.result()
                        self._finish_series(
                            run, spec, load_type, outcome, series_run=sr
                        )
                        completed += 1
                        self._update_run_progress(run, total=len(specs))
                        self._persist_incremental_audit(run, sr)
                        if (
                            completed == len(futures)
                            or completed % 25 == 0
                            or sr.status == RunStatus.FAILED
                        ):
                            log.info(
                                "Run %s progress: %d/%d completed (%d ok / %d failed)",
                                run.run_id,
                                completed,
                                len(futures),
                                run.series_succeeded,
                                run.series_failed,
                            )
                except BaseException:
                    for fut in futures:
                        fut.cancel()
                    for pool in pools:
                        pool.shutdown(wait=False, cancel_futures=True)
                    raise
                else:
                    for pool in pools:
                        pool.shutdown(wait=True)
                extract_stage.detail["series_succeeded"] = run.series_succeeded
                extract_stage.detail["series_failed"] = run.series_failed
                extract_stage.detail["sources"] = ", ".join(sorted(grouped))
        except BaseException:
            self._send_run_alert(run, tracker)
            raise

        run.finalize()

        # Gold: swallow=True keeps the historical behaviour (a Gold failure does
        # not abort a run whose ingestion succeeded) but the failure is now
        # RECORDED rather than only logged, so the run summary can say so.
        if build_gold_layer and run.series_succeeded > 0 and self.warehouse is not None:
            with tracker.stage("gold", swallow=True) as stage:
                result = self.warehouse.build_gold()
                if isinstance(result, dict):
                    stage.detail["tables_built"] = len(result)
                    not_ok = {k: v for k, v in result.items() if v != "ok"}
                    if not_ok:
                        stage.detail["tables_not_ok"] = ", ".join(sorted(not_ok))
                log.info("Gold layer refreshed for run %s", run.run_id)
            gold_stage = tracker.get("gold")
            if gold_stage is not None and gold_stage.error_message:
                # log.exception() needs a *live* exception context; by this
                # point tracker.stage(swallow=True) has already caught and
                # exited, so sys.exc_info() is empty and log.exception() would
                # print "NoneType: None" instead of the real error. Log the
                # type/message the tracker already captured instead.
                log.error(
                    "Gold refresh failed for run %s: %s: %s",
                    run.run_id,
                    gold_stage.error_type,
                    gold_stage.error_message,
                )
            elif gold_stage is not None and gold_stage.detail.get("tables_not_ok"):
                tracker.warn(
                    "gold",
                    f"tables not ok: {gold_stage.detail['tables_not_ok']}",
                )
        elif not build_gold_layer:
            tracker.skip("gold", "build_gold_layer=False")
        elif self.warehouse is None:
            tracker.skip("gold", "no warehouse (dry run)")
        else:
            tracker.skip("gold", "no series succeeded")

        if build_gold_layer and self.warehouse is not None:
            with tracker.stage("release_calendar", swallow=True):
                self._refresh_release_calendar(run)
        else:
            tracker.skip("release_calendar", "gold layer not built")

        with tracker.stage("persist", swallow=True) as stage:
            self._persist_run(run)
            stage.detail["series_rows"] = len(run.series_runs)

        self._notify(run)
        self._send_run_alert(run, tracker)
        log.info(
            "Run %s finished: %s (%d ok / %d failed)",
            run.run_id,
            run.status.value,
            run.series_succeeded,
            run.series_failed,
        )
        return run

    def _send_run_alert(self, run: EtlRun, tracker: Any) -> None:
        """Email the stage summary (config/alerting.yml).

        Complements ``_notify``'s webhook rather than replacing it: the webhook
        answers "did the run fail", this answers "which stage, and was the run
        ultimately a success" — including the case where every series ingested
        but the Gold rebuild did not.

        Never raises. An alerting problem must not turn a good run into a bad
        one, and :func:`send_run_alert` already logs the summary via the console
        transport when no mailbox is configured.
        """
        if getattr(self, "_alert_sent", False):
            return
        self._alert_sent = True
        try:
            from fred_pipeline.governance.alerting import send_run_alert

            send_run_alert(
                run,
                tracker,
                environment=self.config.environment.value,
                transport=self._alert_transport,
            )
        except Exception:  # never let alerting fail a run
            log.exception("Run alert step failed for run %s", run.run_id)

    def _notify(self, run: EtlRun) -> None:
        from fred_pipeline import notify

        try:
            notify.send_notification(
                run,
                webhook_url=self.config.alert_webhook_url,
                notify_on=self.config.notify_on,
                environment=self.config.environment.value,
                transport=self._notify_transport,
            )
        except Exception:  # never let notification issues fail a run
            log.exception("Notification step failed for run %s", run.run_id)

    def _refresh_release_calendar(self, run: EtlRun) -> None:
        """Refresh ``gold.release_calendar`` (terminal module CAL).

        Unlike every Gold table built from already-warehoused Silver data,
        release *dates* are forward-looking metadata with nothing to derive
        from in storage, so this fetches live from FRED and writes directly
        — the only place in the pipeline a Gold table is populated outside
        ``build_gold()``. Failure here must not fail the run.

        Fetches **per curated release_id** (the singular ``release/dates``
        endpoint, scoped server-side) rather than one unfiltered
        ``releases/dates`` call across all ~300 FRED releases. FRED doesn't
        support filtering the plural endpoint by ``release_id`` server-side,
        so the unfiltered form has to fetch and paginate the entire global
        calendar and filter client-side — confirmed live to take minutes
        (offset-based pagination degrades sharply past the first page) even
        though only ~10 releases are ever kept. Looping per release_id is a
        few fast, independent calls instead of one slow one; a single
        release_id failing (network blip, since-removed release) is logged
        and skipped rather than losing the whole calendar.
        """
        try:
            from datetime import datetime, timedelta, timezone

            from fred_pipeline.gold_config.release_calendar_config import (
                load_release_calendar_config,
            )
            from fred_pipeline.writer.terminal_views import compute_release_calendar

            today = datetime.now(timezone.utc).date()
            fred_client = self._client_for_source("fred")
            cfg = load_release_calendar_config()

            release_dates: list[dict[str, Any]] = []
            for entry in cfg:
                try:
                    release_dates.extend(
                        fred_client.get_release_dates(
                            release_id=entry.release_id,
                            realtime_start=today.isoformat(),
                            realtime_end=(today + timedelta(days=120)).isoformat(),
                        )
                    )
                except Exception:
                    log.exception(
                        "Release dates fetch failed for release_id %s (run %s)",
                        entry.release_id,
                        run.run_id,
                    )

            rows = compute_release_calendar(release_dates, cfg, as_of=today)
            self.warehouse.write_release_calendar(rows)
            log.info(
                "Release calendar refreshed for run %s (%d rows)",
                run.run_id,
                len(rows),
            )
        except Exception:
            log.exception("Release calendar refresh failed for run %s", run.run_id)

    # ---- per-series -----------------------------------------------------

    def _safe_extract(self, spec: SeriesSpec, plan: tuple[str | None, str]) -> Any:
        """Run on the thread pool: never raises, so one series' network failure
        can't sink the whole batch or short-circuit ``pool.map``. Returns the
        raw payload, or the caught exception for phase 3 to record.
        """
        observation_start, _load_type = plan
        try:
            return self._extract(spec, observation_start=observation_start)
        except Exception as exc:  # noqa: BLE001 - isolate per-series failures
            return exc

    def _finish_series(
        self,
        run: EtlRun,
        spec: SeriesSpec,
        load_type: str,
        outcome: Any,
        *,
        series_run: Any = None,
    ) -> None:
        sr = series_run or run.start_series(
            spec.series_id, load_type=spec.load_type.value
        )
        sr.load_type = load_type
        try:
            if isinstance(outcome, Exception):
                raise outcome
            payload = outcome
            source = getattr(spec, "source", "fred") or "fred"

            silver_rows = self._normalize(spec, payload, run_id=run.run_id)
            # Count from normalized rows so the metric is source-agnostic (BLS
            # nests observations under Results.series[].data, not a top-level key).
            observations_extracted = len(silver_rows)
            bronze_row = build_bronze_row(
                spec.series_id,
                self._observations_endpoint(spec),
                payload,
                run_id=run.run_id,
                source=source,
                observation_count=observations_extracted,
            )
            report = run_quality_checks(
                spec.series_id,
                silver_rows,
                profile=spec.validation_profile,
                frequency=spec.frequency,
                min_value=spec.min_value,
                max_value=spec.max_value,
            )

            bronze_written = 0
            silver_merged = 0
            if self.warehouse is not None:
                bronze_written = self.warehouse.write_bronze([bronze_row])
                if report.passed:
                    silver_merged = self.warehouse.merge_silver(silver_rows)
                if self.persist_audit:
                    self.warehouse.persist_dq(run.run_id, report)

            if report.passed:
                sr.complete(
                    RunStatus.SUCCEEDED,
                    observations_extracted=observations_extracted,
                    rows_written_bronze=bronze_written,
                    rows_merged_silver=silver_merged,
                    dq_passed=True,
                )
            else:
                msgs = "; ".join(f.message for f in report.failures)
                sr.complete(
                    RunStatus.FAILED,
                    observations_extracted=observations_extracted,
                    rows_written_bronze=bronze_written,
                    dq_passed=False,
                    error_message=f"Data quality failed: {msgs}",
                )
                log.warning("DQ failed for %s: %s", spec.series_id, msgs)
        except Exception as exc:  # isolate per-series failures
            sr.complete(RunStatus.FAILED, error_message=str(exc))
            log.exception("Series %s failed", spec.series_id)

    def _update_run_progress(self, run: EtlRun, *, total: int) -> None:
        run.series_total = total
        run.series_succeeded = sum(
            1 for s in run.series_runs if s.status == RunStatus.SUCCEEDED
        )
        run.series_failed = sum(
            1 for s in run.series_runs if s.status == RunStatus.FAILED
        )
        run.status = RunStatus.RUNNING

    def _persist_incremental_audit(self, run: EtlRun, series_run: Any) -> None:
        if not (self.persist_audit and self.warehouse is not None):
            return
        if not getattr(self.warehouse, "supports_incremental_audit", False):
            return
        try:
            persist_run_state = self.warehouse.persist_run_state
            persist_series_run = self.warehouse.persist_series_run
            persist_run_state(run)
            persist_series_run(series_run)
        except Exception:
            log.exception(
                "Failed to persist incremental audit for run %s series %s",
                run.run_id,
                series_run.series_id,
            )

    def _plan_extract(
        self, spec: SeriesSpec, *, force_full: bool = False
    ) -> tuple[str | None, str]:
        """Decide the load window for a series.

        Returns ``(observation_start, effective_load_type)``. A series with no
        data yet (or ``load_type: full``, ``force_full``, or a dry run with no
        backend) is loaded in full (``observation_start=None``). Otherwise only
        the last N observations are re-pulled and MERGEd, restating recent
        revisions.
        """
        if force_full or spec.load_type == LoadType.FULL or self.warehouse is None:
            return None, "full"
        n = spec.restate_records or self.config.restate_last_n
        watermark_series_id = spec.series_id
        if (getattr(spec, "source", "fred") or "fred").lower() == "tiingo":
            # Tiingo manifests use the bare ticker, but Silver stores exploded
            # scalar fields. Use adjClose as the date watermark for incremental
            # reruns so an already-priced ticker does not refetch full history.
            watermark_series_id = f"{spec.series_id.partition(':')[0]}:adjClose"
        start = self.warehouse.restate_start(watermark_series_id, n)
        if start is None:
            return None, "full"  # first load: series not in the warehouse yet
        return start, f"restate_last_{n}"

    def _extract(
        self, spec: SeriesSpec, *, observation_start: str | None = None
    ) -> dict[str, Any]:
        client = self._client_for(spec)
        # Complete-history mode: batch all vintages under FRED's cap. Only
        # sources that support it (FRED) expose get_observations_all_vintages.
        if (
            spec.vintage_enabled
            and self.config.complete_vintage_history
            and hasattr(client, "get_observations_all_vintages")
        ):
            return client.get_observations_all_vintages(
                spec.series_id, observation_start=observation_start
            )
        kwargs: dict[str, Any] = {}
        if spec.vintage_enabled:
            kwargs["realtime_start"] = FULL_VINTAGE_START
            kwargs["realtime_end"] = FULL_VINTAGE_END
        if observation_start:
            kwargs["observation_start"] = observation_start
        return client.get_observations(spec.series_id, **kwargs)

    def _observations_endpoint(self, spec: SeriesSpec) -> str:
        """The upstream endpoint used for this series, for Bronze lineage.

        Clients advertise it via ``observations_endpoint``; lightweight test
        doubles that don't fall back to the FRED path.
        """
        client = self._client_for(spec)
        fn = getattr(client, "observations_endpoint", None)
        return fn(spec.series_id) if fn else "series/observations"

    def _normalize(
        self, spec: SeriesSpec, payload: dict[str, Any], *, run_id: str
    ) -> list[dict[str, Any]]:
        """Map a raw payload into revision-numbered silver rows.

        Normalization is delegated to the spec's source client (each source
        knows its own response shape); revision numbering is applied uniformly
        here so it stays source-agnostic. Clients that predate the ``normalize``
        contract (e.g. lightweight test doubles) fall back to the FRED
        normalizer.
        """
        client = self._client_for(spec)
        source = getattr(spec, "source", "fred") or "fred"
        normalize = getattr(client, "normalize", None)
        if normalize is not None:
            rows = normalize(
                spec.series_id,
                payload,
                run_id=run_id,
                track_vintage=spec.vintage_enabled,
                source=source,
            )
        else:
            rows = normalize_observations(
                spec.series_id,
                payload,
                run_id=run_id,
                track_vintage=spec.vintage_enabled,
                source=source,
            )
        return assign_revision_numbers(rows)

    # ---- audit persistence ----------------------------------------------

    def _persist_run(self, run: EtlRun) -> None:
        if not (self.persist_audit and self.warehouse is not None):
            return
        try:
            self.warehouse.persist_run(run)
        except Exception:
            log.exception("Failed to persist audit records for run %s", run.run_id)


@timed("run_pipeline")
def run_pipeline(
    environment: str = "dev",
    manifest_path: str = "manifests",
    *,
    fred_api_key: str | None = None,
    dbutils: Any = None,
    spark: Any = None,
    warehouse: Warehouse | None = None,
    triggered_by: str = "cli",
) -> EtlRun:
    """Convenience entrypoint used by the Databricks job and the CLI."""
    config = PipelineConfig.resolve(
        environment=Environment(environment),
        fred_api_key=fred_api_key,
        dbutils=dbutils,
    )
    if warehouse is None and spark is None:
        from fred_pipeline.spark_io import get_spark

        try:
            spark = get_spark()
        except Exception:  # noqa: BLE001  # pragma: no cover - allow no-Spark dry runs
            spark = None
    pipeline = FredPipeline(config, spark=spark, warehouse=warehouse)
    return pipeline.run_from_manifest(manifest_path, triggered_by=triggered_by)
