"""Library workloads for manually launched jobs; no Dagster dependency.

Configuration and credentials are resolved only inside an execution. Launch
options select a workload, never a shell command or an arbitrary config path.
"""

from contextlib import contextmanager
from datetime import datetime, timezone
import os
from pathlib import Path
import sqlite3

from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.io.warehouse_factory import (
    WarehouseConfig,
    WarehouseFactory,
    load_warehouse_config,
)
from fred_pipeline.job_execution import (
    JobReport,
    gold_result_succeeded,
    pipeline_report,
)
from fred_pipeline.manifest import all_series, load_manifests


def _close_pipeline_clients(pipeline):
    for client in pipeline._clients.values():
        session = getattr(client, "_session", None)
        if session is not None:
            session.close()


class ReadOnlyWarehouse:
    """A real read snapshot, without schema initialization or query-log writes."""

    def __init__(self, config, backend, settings):
        self._postgres = None
        if backend == "postgres":
            from psycopg import IsolationLevel
            from fred_pipeline.io.postgres_store import PostgresWarehouse

            self._postgres = PostgresWarehouse(config, initialize=False, **settings)
            self._postgres._pg_conn.read_only = True
            self._postgres._pg_conn.isolation_level = IsolationLevel.REPEATABLE_READ
            self._postgres._defer_commits = True
            self.conn = self._postgres.conn
        elif backend == "local":
            path = Path(settings.get("db_path", "fred.db")).resolve()
            self.conn = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
            self.conn.row_factory = sqlite3.Row
            self.conn.execute("PRAGMA query_only=ON")
            self.conn.execute("BEGIN")
        else:
            raise ValueError(
                "Read-only monitoring currently supports local and postgres"
            )

    def query(self, sql, params=()):
        return [dict(row) for row in self.conn.execute(sql, params).fetchall()]

    def latest_observation_dates(self, series_ids):
        latest = {}
        # Bounded statement sizes even with the complete manifest universe.
        for start in range(0, len(series_ids), 500):
            batch = series_ids[start : start + 500]
            placeholders = ",".join("?" for _ in batch)
            rows = self.query(
                "SELECT series_id, MAX(observation_date) AS latest "
                "FROM silver_fred_observation WHERE COALESCE(is_missing, 0) = 0 "
                f"AND series_id IN ({placeholders}) GROUP BY series_id",
                batch,
            )
            latest.update(
                {row["series_id"]: row["latest"] for row in rows if row["latest"]}
            )
        return latest

    def _insert(self, *args, **kwargs):
        raise RuntimeError("Read-only job cannot persist business data")

    def close(self):
        if self._postgres is not None:
            self._postgres.close()
        else:
            self.conn.close()


class JobRuntime:
    @staticmethod
    def pipeline_config(options, **overrides):
        return PipelineConfig.resolve(
            environment=Environment(options.environment), **overrides
        )

    @staticmethod
    def manifests():
        return os.environ.get("FRED_DAGSTER_MANIFESTS", "manifests")

    @contextmanager
    def warehouse(self, options, *, read_only=False, config=None, ecb=False):
        config = config or self.pipeline_config(options)
        settings = load_warehouse_config(environment=options.environment)
        backend = (
            settings.primary_backend
            if options.backend == "configured"
            else options.backend
        )
        if (
            options.environment == "prod"
            and backend != settings.primary_backend
            and not ecb
        ):
            raise ValueError("Production backend must match the configured authority")
        if backend not in settings.backends:
            raise ValueError(
                "Selected warehouse backend has no server-side configuration"
            )
        if ecb and backend != settings.ecb_assets_authoritative_backend:
            raise ValueError("Selected ECB backend is not authoritative")
        if ecb and options.environment == "prod" and backend != "postgres":
            raise ValueError("Production ECB jobs require authoritative PostgreSQL")
        if read_only:
            warehouse = ReadOnlyWarehouse(config, backend, settings.backends[backend])
        else:
            # An exact backend prevents silent fallback and writes to another destination.
            exact = WarehouseConfig(
                primary_backend=backend,
                backends=settings.backends,
                ecb_assets_authoritative_backend=settings.ecb_assets_authoritative_backend,
            )
            warehouse = WarehouseFactory(config, exact).build_exact(
                backend, initialize=not (ecb and backend == "postgres")
            )
            if warehouse is None:
                raise ValueError("Durable jobs require a warehouse")
        try:
            if ecb and not read_only:
                from fred_pipeline.ecb_writer_lock import ecb_writer_scope

                with ecb_writer_scope(warehouse):
                    # Dagster ECB writes require the existing schema; no implicit
                    # migrations or history backfill before route selection.
                    warehouse.conn.execute(
                        "SELECT release_id FROM silver_ecb_eligible_asset_release LIMIT 0"
                    )
                    yield config, warehouse, backend
            else:
                yield config, warehouse, backend
        finally:
            warehouse.close()

    def macro_pipeline(self, options):
        from contextlib import ExitStack
        from fred_pipeline.pipeline import FredPipeline, missing_source_keys

        config = self.pipeline_config(
            options,
            extract_workers=options.extract_workers,
            rate_limit_per_minute=options.rate_limit_per_minute,
        )
        specs = all_series(load_manifests(self.manifests()), active_only=True)
        if options.series:
            specs = [s for s in specs if s.series_id in options.series]
        if options.sources:
            specs = [s for s in specs if s.source.lower() in options.sources]
        specs = [s for s in specs if s.source.lower() not in options.exclude_sources]
        if not specs:
            return JobReport(
                "failure",
                {"series_total": 0, "reason": "empty_selection"},
                ("macro", "pipeline"),
            )
        if missing_source_keys(config, {s.source for s in specs}):
            raise ValueError("Selected sources require protected credentials")
        if options.environment == "prod" and options.dry_run:
            raise ValueError("Production job requires durable writes")
        pipeline = FredPipeline(config, persist_audit=not options.dry_run)
        pipeline._defer_run_alerts = True
        with ExitStack() as stack:
            stack.callback(_close_pipeline_clients, pipeline)
            with pipeline._run_network(
                specs,
                include_calendar=options.build_gold and not options.dry_run,
                refresh_fomc=options.refresh_fomc_calendar,
                warehouse_available=not options.dry_run,
            ):
                if not options.dry_run:
                    _, pipeline.warehouse, _ = stack.enter_context(
                        self.warehouse(options, config=config)
                    )
                try:
                    run = pipeline.run_from_manifest(
                        self.manifests(),
                        triggered_by="dagster",
                        series=options.series or None,
                        sources=options.sources or None,
                        exclude_sources=options.exclude_sources or None,
                        build_gold_layer=options.build_gold and not options.dry_run,
                        force_full=options.full,
                        skip_not_due=options.skip_not_due,
                        refresh_fomc_calendar=options.refresh_fomc_calendar,
                    )
                finally:
                    if pipeline._last_run is not None:
                        pipeline.finish_run_reporting(pipeline._last_run)
                return pipeline_report(pipeline, run, durable=not options.dry_run)

    @contextmanager
    def http_session(self, config, url, *, headers=None):
        from fred_pipeline.network import RunSessions, select_route
        import requests

        if config.network.mode == "env":
            session = requests.Session()
        else:
            route = select_route(config.network, url, headers=headers or {})
            session = RunSessions(route)
        try:
            yield session
        finally:
            session.close()

    def fomc_refresh(self, options):
        from fred_pipeline.catalogs.fomc_calendar import (
            DEFAULT_USER_AGENT,
            FOMC_CALENDAR_URL,
            fetch_calendar_html_requests,
        )
        from fred_pipeline.governance.fomc_calendar_check import check_fomc_calendar

        config = self.pipeline_config(options)
        if options.refresh:
            with self.http_session(
                config, FOMC_CALENDAR_URL, headers={"User-Agent": DEFAULT_USER_AGENT}
            ) as session:
                result = check_fomc_calendar(
                    refresh=True,
                    timeout_seconds=options.timeout_seconds,
                    fetch=lambda **kwargs: fetch_calendar_html_requests(
                        session=session, timeout_seconds=kwargs["timeout_seconds"]
                    ),
                )
        else:
            result = check_fomc_calendar(refresh=False)
        details = {
            key: value for key, value in result.detail().items() if key != "path"
        }
        details.update(
            refresh_attempted=result.refresh_attempted,
            refresh_outcome=result.refresh_outcome,
        )
        verdict = {"ok": "success", "warn": "partial", "fail": "failure"}[result.status]
        return JobReport(
            verdict,
            details,
            ("macro", "fomc_calendar"),
            ({"added_dates": list(result.added_dates)},) if result.refreshed else (),
        )

    def gold_rebuild(self, options):
        from fred_pipeline.benchmark_as_of import benchmark_as_of_date
        from fred_pipeline.governance.fomc_calendar_check import check_fomc_calendar

        cutoff = benchmark_as_of_date(options.benchmark_as_of)
        calendar = check_fomc_calendar(refresh=False)
        if calendar.status != "ok":
            return JobReport(
                "failure", {"calendar_status": calendar.status}, ("macro", "pipeline")
            )
        with self.warehouse(options) as (_, warehouse, backend):
            kwargs = {"benchmark_as_of": cutoff}
            if backend in {"local", "postgres"}:
                kwargs["full"] = options.full
            result = warehouse.build_gold(**kwargs)
        succeeded = gold_result_succeeded(result)
        details = {
            "backend": backend,
            "tables_built": len(result),
            "tables_failed": sum(value != "ok" for value in result.values()),
            "source_extraction": False,
        }
        return JobReport(
            "success" if succeeded else "failure",
            details,
            ("macro", "pipeline"),
            ({"gold_only": True},) if succeeded else (),
        )

    def ecb_eligible_assets(self, options):
        from contextlib import ExitStack
        from fred_pipeline.ecb_execution import ECBPollFailure, execute_ecb_poll
        from fred_pipeline.ecb_writer_lock import ECBWriterBusy
        from fred_pipeline.governance.ecb_alerting import send_ecb_assets_alert_result
        from fred_pipeline.governance.email_network import (
            plan_alert_http,
            plan_alert_smtp,
        )

        config = self.pipeline_config(options)
        report = None
        health = {}
        check_verdict = "failure"
        cleanup_failed = False
        try:
            with ExitStack() as stack:
                _, warehouse, backend = stack.enter_context(
                    self.warehouse(options, config=config, ecb=True)
                )
                session = stack.enter_context(
                    self.http_session(config, config.ecb_mid_base_url)
                )
                smtp = plan_alert_smtp(options.environment)
                alerts = None
                if config.network.mode != "env":
                    alerts = plan_alert_http(config.network, options.environment)
                    stack.callback(alerts.close)
                try:
                    report = execute_ecb_poll(
                        config,
                        warehouse=warehouse,
                        selected_backend=backend,
                        authoritative_backend=backend,
                        session=session,
                        allow_gap=options.allow_gap,
                        notify=lambda releases, status, error: (
                            send_ecb_assets_alert_result(
                                action="poll",
                                results=releases,
                                status=status,
                                error=error,
                                environment=options.environment,
                                http_client=alerts,
                                smtp_routes=smtp,
                            )
                        ),
                    )
                except ECBPollFailure as exc:
                    report = exc.report
                from fred_pipeline.ecb_health import ecb_health_snapshot
                from fred_pipeline.governance.alerting_config import (
                    load_alerting_config,
                )

                try:
                    check_verdict, health = ecb_health_snapshot(
                        warehouse, load_alerting_config().ecb_assets
                    )
                except Exception:
                    # A failed check cannot erase evidence of earlier commits.
                    health = {"checks": {"accepted_state_readable": "fail"}}
        except ECBWriterBusy:
            return JobReport(
                "success", {"outcome": "busy"}, ("ecb", "eligible_securities")
            )
        except Exception:
            if report is None:
                raise
            cleanup_failed = True
        accepted = tuple(
            {"release_id": row["release_id"]}
            for row in report.releases
            if row.get("status") == "accepted"
        )
        verdict = (
            report.health_verdict
            if report.outcome != "failed" and not cleanup_failed
            else "failure"
        )
        if check_verdict == "failure" or verdict == "failure":
            verdict = "failure"
        elif check_verdict == "partial":
            verdict = "partial"
        if verdict not in {"success", "partial", "failure"}:
            verdict = "failure"
        return JobReport(
            verdict,
            {
                "operation_id": report.operation_id,
                "outcome": report.outcome,
                "releases_processed": len(report.releases),
                "accepted_releases": len(accepted),
                "health_verdict": report.health_verdict,
                "notification_status": report.notification_status,
                "checks": health.get("checks", {}),
                "cleanup_failed": cleanup_failed,
            },
            ("ecb", "eligible_securities"),
            accepted,
        )

    def ecb_eligible_assets_health(self, options):
        from fred_pipeline.ecb_health import ecb_health_snapshot
        from fred_pipeline.governance.alerting_config import load_alerting_config

        with self.warehouse(options, read_only=True, ecb=True) as (
            _,
            warehouse,
            backend,
        ):
            verdict, details = ecb_health_snapshot(
                warehouse, load_alerting_config().ecb_assets
            )
        return JobReport(
            verdict, {"backend": backend, **details}, ("ecb", "eligible_securities")
        )

    def warehouse_health(self, options):
        from fred_pipeline.governance.reconcile import reconcile_staleness

        with self.warehouse(options, read_only=True) as (_, warehouse, backend):
            rows = reconcile_staleness(
                load_manifests(self.manifests()),
                warehouse,
                series_ids=options.series or None,
                active_only=True,
            )
            latest = warehouse.query(
                "SELECT run_id, status, ended_at FROM audit_etl_run ORDER BY started_at DESC LIMIT 1"
            )
            latest = latest[0] if latest else {}
            dq = []
            if latest:
                dq = warehouse.query(
                    "SELECT COUNT(*) AS n, SUM(CASE WHEN passed = 0 AND severity = 'error' THEN 1 ELSE 0 END) AS failed "
                    "FROM audit_data_quality_result WHERE run_id = ?",
                    (latest["run_id"],),
                )
            capture = warehouse.query(
                "SELECT MAX(ingested_at) AS latest FROM silver_fred_observation"
            )[0]["latest"]
            capture_age = None
            if capture:
                timestamp = datetime.fromisoformat(str(capture))
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                capture_age = (
                    datetime.now(timezone.utc) - timestamp
                ).total_seconds() / 86400
            gold = {}
            for relation in options.required_gold_relations:
                # Avoid schema prefixes: the PostgreSQL adapter translates
                # identifiers beginning with gold_ into relation names.
                warehouse.conn.execute("SAVEPOINT fred_health_check")
                try:
                    # Relation identifiers were allowlisted by the job's schema.
                    if relation == "gold_fred_latest_observation":
                        warehouse.query(
                            f"SELECT series_id, observation_date, value FROM {relation} LIMIT 0"
                        )
                    gold[relation] = len(
                        warehouse.query(
                            f"SELECT 1 AS present FROM {relation} LIMIT ?",
                            (options.min_gold_rows,),
                        )
                    )
                except Exception:
                    warehouse.conn.execute("ROLLBACK TO SAVEPOINT fred_health_check")
                    gold[relation] = None
                finally:
                    warehouse.conn.execute("RELEASE SAVEPOINT fred_health_check")
        missing = sum(not row["has_data"] for row in rows)
        stale = sum(row["is_stale"] for row in rows)
        failed = (
            not rows
            or not latest
            or latest.get("status") != "succeeded"
            or not dq
            or not dq[0]["n"]
            or bool(dq[0]["failed"])
            or capture_age is None
            or capture_age > options.max_capture_age_days
            or missing > options.max_missing_series
            or stale > options.max_stale_series
            or any(
                count is None or count < options.min_gold_rows
                for count in gold.values()
            )
        )
        return JobReport(
            "failure" if failed else "success",
            {
                "backend": backend,
                "series_checked": len(rows),
                "missing_series": missing,
                "stale_series": stale,
                "capture_age_days": capture_age,
                "latest_run_id": latest.get("run_id"),
                "dq_checks": dq[0]["n"] if dq else 0,
                "dq_errors": dq[0]["failed"] if dq else None,
                "gold_row_evidence": gold,
            },
            ("macro", "pipeline"),
        )

    def metadata_reconciliation(self, options):
        from fred_pipeline.fred_client import FredClient
        from fred_pipeline.governance.reconcile import (
            persist_report,
            reconcile,
            reconcile_staleness,
        )

        config = self.pipeline_config(options)
        if not config.fred_api_key:
            raise ValueError(
                "FRED metadata reconciliation requires protected FRED credentials"
            )
        with self.http_session(config, config.fred_base_url) as session:
            client = FredClient(
                api_key=config.fred_api_key,
                base_url=config.fred_base_url,
                timeout=config.request_timeout_seconds,
                max_retries=config.max_retries,
                rate_limit_per_minute=config.rate_limit_per_minute,
                session=session,
            )
            manifests = load_manifests(self.manifests())
            report = reconcile(
                manifests, client, series_ids=options.series or None, active_only=True
            )
            with self.warehouse(
                options, read_only=not options.persist, config=config
            ) as (_, warehouse, backend):
                stale = reconcile_staleness(
                    manifests,
                    warehouse,
                    series_ids=options.series or None,
                    active_only=True,
                )
                if options.persist:
                    persist_report(config, report, warehouse)
                    warehouse.write_staleness(stale)
        failed = report.has_errors or not report.series_checked
        if options.fail_on_warning and report.by_severity("warning"):
            failed = True
        return JobReport(
            "failure" if failed else "success",
            {
                **report.summary(),
                "metadata_source": "fred",
                "backend": backend,
                "stored_series_checked": len(stale),
                "stored_stale_series": sum(row["is_stale"] for row in stale),
                "stored_missing_series": sum(not row["has_data"] for row in stale),
                "persisted": options.persist,
            },
            ("macro", "metadata"),
        )
