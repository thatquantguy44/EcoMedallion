"""Command-line interface for local runs, validation, and dry runs.

Examples
--------
Validate all manifests (no network, no Spark)::

    python -m fred_pipeline validate --manifests manifests

Dry-run the pipeline against the real FRED API but without Spark writes,
printing the audit summary as JSON::

    FRED_API_KEY=... python -m fred_pipeline run --env dev --dry-run

Run fully locally, persisting to a SQLite file::

    FRED_API_KEY=... python -m fred_pipeline run --local --db-path fred_local.db

Generate a new manifest from a FRED category / release / search::

    FRED_API_KEY=... python -m fred_pipeline discover --name rates_extra \\
        --category-id 22 --frequencies d --out manifests/rates_extra.yml

Reconcile manifests against live FRED metadata (drift + lifecycle), and check
staleness across every source (FRED, Tiingo, BLS, EIA, ...)::

    FRED_API_KEY=... python -m fred_pipeline reconcile --local --fail-on-drift
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import logging
import sys

from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.manifest import all_series, load_manifests
from fred_pipeline.pipeline import FredPipeline


def _cmd_discover(args: argparse.Namespace) -> int:
    import os

    from fred_pipeline.discovery import (
        build_manifest_dict,
        discover_specs,
        manifest_to_yaml,
    )
    from fred_pipeline.fred_client import FredClient

    sources = [bool(args.category_id), bool(args.release_id), bool(args.search)]
    if sum(sources) != 1:
        print(
            "ERROR: pass exactly one of --category-id / --release-id / --search",
            file=sys.stderr,
        )
        return 2

    config = PipelineConfig.resolve(
        environment=Environment(args.env), config_file=args.config
    )
    if not config.fred_api_key:
        print(
            "ERROR: no FRED API key found (config file / FRED_API_KEY / secret).",
            file=sys.stderr,
        )
        return 2

    client = FredClient(
        api_key=config.fred_api_key,
        base_url=config.fred_base_url,
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit_per_minute=config.rate_limit_per_minute,
    )

    if args.category_id:
        metas = client.get_category_series(args.category_id, max_results=args.max)
        default_desc = f"Series discovered from FRED category {args.category_id}."
    elif args.release_id:
        metas = client.get_release_series(args.release_id, max_results=args.max)
        default_desc = f"Series discovered from FRED release {args.release_id}."
    else:
        metas = client.search_series(args.search, max_results=args.max)
        default_desc = f"Series discovered from FRED search {args.search!r}."

    exclude_ids: set[str] = set()
    if not args.include_existing and os.path.isdir(args.manifests):
        try:
            existing = load_manifests(args.manifests)
            exclude_ids = {s.series_id for s in all_series(existing, active_only=False)}
        except (OSError, ValueError):
            exclude_ids = set()

    frequencies = (
        [f.strip() for f in args.frequencies.split(",")] if args.frequencies else None
    )
    specs, skipped = discover_specs(
        metas,
        category=args.name,
        frequencies=frequencies,
        exclude_discontinued=not args.include_discontinued,
        min_popularity=args.min_popularity,
        exclude_ids=exclude_ids,
    )

    manifest = build_manifest_dict(
        args.name, specs, description=args.description or default_desc
    )
    yaml_text = manifest_to_yaml(manifest)

    print(
        f"Discovered {len(metas)} series; kept {len(specs)}, skipped {len(skipped)} "
        f"(dupes/discontinued/filtered/existing)."
    )
    if args.dry_run or not args.out:
        print("\n--- manifest (dry run, not written) ---\n")
        print(yaml_text)
    else:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(yaml_text)
        print(f"Wrote {len(specs)} series to {args.out}")
        # Prove the written file loads cleanly alongside the rest.
        load_manifests(args.out)
        print("Validated: generated manifest loads successfully.")
    return 0


def _cmd_discover_ecb(args: argparse.Namespace) -> int:
    import os

    from fred_pipeline.catalogs.ecb_discovery import (
        ECBMetadataClient,
        build_ecb_manifest_dict,
        dataflows_to_rows,
        ecb_manifest_to_yaml,
        estimate_candidate_count,
        filter_dataflows,
        generate_ecb_candidate_specs,
        parse_dimension_filters,
    )
    from fred_pipeline.pipeline import _rate_limit_for_source

    modes = [
        bool(args.list_flows),
        bool(args.inspect),
        bool(args.flow and not args.inspect),
    ]
    if sum(modes) != 1:
        print(
            "ERROR: pass exactly one ECB discovery mode: --list-flows, "
            "--flow FLOW --inspect, or --flow FLOW for candidate generation",
            file=sys.stderr,
        )
        return 2

    config = PipelineConfig.resolve(
        environment=Environment(args.env), config_file=args.config
    )
    client = ECBMetadataClient(
        base_url=config.ecb_base_url,
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit_per_minute=_rate_limit_for_source(config, "ecb"),
    )
    flows = client.list_dataflows()

    if args.list_flows:
        visible = filter_dataflows(
            flows,
            search=args.search,
            max_results=args.max,
        )

        if args.json:
            print(json.dumps(dataflows_to_rows(visible), indent=2))
            return 0

        print(f"Found {len(visible)} ECB dataflow(s).")
        if not visible:
            return 0
        print(f"{'Flow':12s} {'Agency':8s} {'Version':8s} Name")
        print(f"{'-' * 12} {'-' * 8} {'-' * 8} {'-' * 60}")
        for flow in visible:
            name = flow.name or flow.description or ""
            print(f"{flow.flow_id:12s} {flow.agency_id:8s} {flow.version:8s} {name}")
        return 0

    matches = [flow for flow in flows if flow.flow_id.upper() == args.flow.upper()]
    flow = matches[0] if matches else None
    agency_id = args.agency or (flow.agency_id if flow else "ECB")
    version = args.version or (flow.version if flow else "1.0")
    structure = client.get_dataflow_structure(
        args.flow,
        agency_id=agency_id,
        version=version,
    )

    if args.inspect:
        if args.json:
            print(
                json.dumps(structure.to_dict(sample_size=args.sample_codes), indent=2)
            )
            return 0
        print(
            f"Flow: {structure.flow_id} ({structure.agency_id} "
            f"{structure.version}) - {structure.name}"
        )
        print(
            f"Structure: {structure.structure_agency_id}:"
            f"{structure.structure_id} ({structure.structure_version})"
        )
        print(f"{'Pos':>3s} {'Dimension':24s} {'Codelist':24s} {'Codes':>7s} Sample")
        print(f"{'-' * 3} {'-' * 24} {'-' * 24} {'-' * 7} {'-' * 40}")
        for dim in structure.dimensions:
            sample = ", ".join(code.code_id for code in dim.codes[: args.sample_codes])
            print(
                f"{dim.position:3d} {dim.dimension_id:24s} "
                f"{dim.codelist_id:24s} {len(dim.codes):7d} {sample}"
            )
        return 0

    dimension_filters = parse_dimension_filters(args.dimension)
    frequencies = (
        [freq.strip() for freq in args.frequency.split(",")] if args.frequency else None
    )
    estimate = estimate_candidate_count(
        structure,
        dimension_filters=dimension_filters,
        frequencies=frequencies,
        include_code=args.include_code,
        exclude_code=args.exclude_code,
    )
    exclude_ids: set[str] = set()
    if not args.include_existing and os.path.isdir(args.manifests):
        try:
            existing = load_manifests(args.manifests)
            exclude_ids = {s.series_id for s in all_series(existing, active_only=False)}
        except (OSError, ValueError):
            exclude_ids = set()
    specs, skipped = generate_ecb_candidate_specs(
        structure,
        dimension_filters=dimension_filters,
        frequencies=frequencies,
        include_code=args.include_code,
        exclude_code=args.exclude_code,
        category=args.category,
        max_results=args.max,
        max_cartesian=args.max_cartesian,
        force=args.force,
        exclude_ids=exclude_ids,
    )
    print(
        f"ECB {structure.flow_id}: estimated {estimate} candidate combination(s); "
        f"kept {len(specs)}, skipped {len(skipped)}."
    )
    if not specs:
        print(
            "No candidates matched -- nothing to write. Sample skip reasons:",
            file=sys.stderr,
        )
        for row in skipped[:10]:
            print(f"  {row}", file=sys.stderr)
        return 1

    manifest_name = args.name or f"ecb_{structure.flow_id.lower()}_candidates"
    description = (
        args.description
        or f"Inactive ECB candidate series generated from {structure.flow_id} metadata."
    )
    manifest = build_ecb_manifest_dict(manifest_name, specs, description=description)
    yaml_text = ecb_manifest_to_yaml(manifest)
    if args.dry_run or not args.out:
        print("\n--- manifest (dry run, not written) ---\n")
        print(yaml_text)
    else:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(yaml_text)
        print(f"Wrote {len(specs)} inactive ECB candidate series to {args.out}")
        load_manifests(args.out)
        print("Validated: generated manifest loads successfully.")
    return 0


def _cmd_discover_bls(args: argparse.Namespace) -> int:
    import os

    from fred_pipeline.catalogs.bls_discovery import (
        BLSDiscoveryError,
        BLSFlatFileClient,
        bls_manifest_to_yaml,
        build_bls_manifest_dict,
        filter_series_rows,
        filter_surveys,
        generate_bls_candidate_specs,
        inspect_series_catalog,
        parse_column_filters,
        surveys_to_rows,
    )
    from fred_pipeline.pipeline import _rate_limit_for_source

    modes = [
        bool(args.list_surveys),
        bool(args.inspect),
        bool(args.survey and not args.inspect),
    ]
    if sum(modes) != 1:
        print(
            "ERROR: pass exactly one BLS discovery mode: --list-surveys, "
            "--survey SURVEY --inspect, or --survey SURVEY for candidate generation",
            file=sys.stderr,
        )
        return 2
    if modes[2] and not args.frequency:
        print(
            "ERROR: --frequency is required for candidate generation (BLS flat "
            "files don't self-describe it) -- confirm it from the survey's own "
            "documentation, e.g. --frequency m",
            file=sys.stderr,
        )
        return 2

    config = PipelineConfig.resolve(
        environment=Environment(args.env), config_file=args.config
    )
    client = BLSFlatFileClient(
        user_agent=config.bls_user_agent,
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit_per_minute=_rate_limit_for_source(config, "bls"),
    )

    if args.list_surveys:
        try:
            surveys = filter_surveys(
                client.list_surveys(), search=args.search, max_results=args.max
            )
        except BLSDiscoveryError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2

        if args.json:
            print(json.dumps(surveys_to_rows(surveys), indent=2))
            return 0
        print(f"Found {len(surveys)} BLS survey(s).")
        if not surveys:
            return 0
        print(f"{'Survey':10s} Name")
        print(f"{'-' * 10} {'-' * 60}")
        for survey in surveys:
            print(f"{survey.abbreviation:10s} {survey.name}")
        return 0

    try:
        rows = client.fetch_series_catalog(args.survey)
    except BLSDiscoveryError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.inspect:
        summary = inspect_series_catalog(rows, sample_size=args.sample_values)
        if args.json:
            print(json.dumps(summary, indent=2))
            return 0
        print(
            f"Survey: {args.survey.upper()} - {summary['row_count']} series in catalog"
        )
        print(f"{'Column':24s} {'Distinct (sample)':>17s}  Sample values")
        print(f"{'-' * 24} {'-' * 17}  {'-' * 40}")
        for col in summary["columns"]:
            sample = ", ".join(col["sample_values"])
            print(
                f"{col['column']:24s} {col['distinct_count_in_sample']:17d}  {sample}"
            )
        return 0

    column_filters = parse_column_filters(args.column)
    filtered = filter_series_rows(
        rows, column_filters=column_filters, search=args.search
    )

    exclude_ids: set[str] = set()
    if not args.include_existing and os.path.isdir(args.manifests):
        try:
            existing = load_manifests(args.manifests)
            exclude_ids = {s.series_id for s in all_series(existing, active_only=False)}
        except (OSError, ValueError):
            exclude_ids = set()

    try:
        specs, skipped = generate_bls_candidate_specs(
            args.survey,
            filtered,
            frequency=args.frequency,
            category=args.category,
            max_results=args.max,
            exclude_ids=exclude_ids,
        )
    except BLSDiscoveryError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    manifest_name = args.name or f"bls_{args.survey.lower()}_candidates"
    description = (
        args.description
        or f"Inactive BLS candidate series generated from the {args.survey.upper()} "
        "survey catalog."
    )
    manifest = build_bls_manifest_dict(manifest_name, specs, description=description)
    yaml_text = bls_manifest_to_yaml(manifest)

    print(
        f"BLS {args.survey.upper()}: {len(rows)} series in catalog, "
        f"{len(filtered)} after filters; kept {len(specs)}, skipped {len(skipped)}."
    )
    if args.dry_run or not args.out:
        print("\n--- manifest (dry run, not written) ---\n")
        print(yaml_text)
    else:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(yaml_text)
        print(f"Wrote {len(specs)} inactive BLS candidate series to {args.out}")
        load_manifests(args.out)
        print("Validated: generated manifest loads successfully.")
    return 0


def _parse_series(value):
    """Parse a --series comma list into a list of ids, or None for 'all'."""
    if not value:
        return None
    ids = [s.strip() for s in value.split(",") if s.strip()]
    return ids or None


def _open_warehouse(config, args):
    """Resolve a warehouse using the factory with config + CLI overrides.

    Precedence: CLI flags (--local, --db-path) > warehouse.yml config > defaults.
    """
    from fred_pipeline.io.warehouse_factory import (
        WarehouseConfig,
        WarehouseFactory,
        load_warehouse_config,
    )

    # Load warehouse config from file
    warehouse_config = load_warehouse_config(environment=config.environment.value)

    # CLI overrides: --local flag or --db-path specifies explicit backend
    if getattr(args, "local", False):
        # Force local backend with optional db_path override
        warehouse_config = WarehouseConfig(
            primary_backend="local",
            backends={"local": {"db_path": getattr(args, "db_path", "fred.db")}},
        )

    factory = WarehouseFactory(config, warehouse_config)
    return factory.build(force_dry_run=False)


def _cmd_backfill(args: argparse.Namespace) -> int:
    import datetime as _dt

    from fred_pipeline.backfill import ALL_TABLES, run_backfill

    try:
        from_date = _dt.date.fromisoformat(args.from_date)
        to_date = _dt.date.fromisoformat(args.to_date)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if from_date > to_date:
        print("ERROR: --from must be <= --to", file=sys.stderr)
        return 2

    tables = None
    if args.tables:
        tables = tuple(t.strip() for t in args.tables.split(",") if t.strip())
        unknown = set(tables) - set(ALL_TABLES)
        if unknown:
            print(
                f"ERROR: unknown table(s): {sorted(unknown)}. "
                f"Valid: {list(ALL_TABLES)}",
                file=sys.stderr,
            )
            return 2

    print(
        f"Backfilling {args.step} snapshots from {from_date} to {to_date} "
        f"(source: {args.db_path}, output: {args.backfill_db})"
    )
    result = run_backfill(
        db_path=args.db_path,
        backfill_db_path=args.backfill_db,
        from_date=from_date,
        to_date=to_date,
        step=args.step,
        tables=tables,
        resume=not args.no_resume,
    )
    print(json.dumps(result, indent=2))
    if result["snapshots_failed"]:
        return 1
    return 0


def _cmd_replay(args: argparse.Namespace) -> int:
    from fred_pipeline.replay import replay_from_bronze

    config = PipelineConfig.resolve(
        environment=Environment(args.env), config_file=args.config
    )
    manifests = load_manifests(args.manifests)
    series = _parse_series(args.series)
    warehouse = _open_warehouse(config, args)
    try:
        result = replay_from_bronze(
            config,
            manifests,
            warehouse,
            series_ids=series,
            rebuild_gold=not args.no_gold,
        )
    finally:
        warehouse.close()
    print(json.dumps(result, indent=2))
    return 0


def _cmd_reconcile(args: argparse.Namespace) -> int:
    from fred_pipeline.fred_client import FredClient
    from fred_pipeline.reconcile import persist_report, reconcile, reconcile_staleness

    config = PipelineConfig.resolve(
        environment=Environment(args.env), config_file=args.config
    )
    if not config.fred_api_key:
        print(
            "ERROR: no FRED API key found (config file / FRED_API_KEY / secret).",
            file=sys.stderr,
        )
        return 2

    client = FredClient(
        api_key=config.fred_api_key,
        base_url=config.fred_base_url,
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit_per_minute=config.rate_limit_per_minute,
    )
    manifests = load_manifests(args.manifests)
    series_ids = _parse_series(args.series)

    # FRED-only: diffs manifests against FRED's live /series metadata catalog,
    # so it can only cover FRED-sourced series (a Tiingo/BLS id isn't in FRED).
    report = reconcile(manifests, client, series_ids=series_ids)

    print("Reconciliation summary (FRED metadata drift):", json.dumps(report.summary()))
    for sev in ("error", "warning", "info"):
        for d in report.by_severity(sev):
            print(
                f"  [{sev:7s}] {d.series_id:12s} {d.kind}: "
                f"{d.manifest_value!r} (manifest) vs {d.fred_value!r} (FRED)"
            )
    if report.stale:
        print(
            f"  [stale-fred] {len(report.stale)} FRED series past expected "
            f"update: {', '.join(report.stale)}"
        )

    # Build a warehouse for reading regardless of --no-persist: the all-source
    # staleness check below needs it to read already-ingested Silver data.
    warehouse = None
    if args.local:
        from fred_pipeline.local_store import LocalWarehouse

        warehouse = LocalWarehouse(config, db_path=args.db_path)
    else:
        from fred_pipeline.spark_io import get_spark
        from fred_pipeline.warehouse import SparkWarehouse

        try:
            warehouse = SparkWarehouse(config, get_spark())
        except Exception:  # noqa: BLE001 - Spark may be unavailable locally.
            print(
                "(no Spark available; skipping all-source staleness check "
                "and persistence)",
                file=sys.stderr,
            )

    staleness_rows: list[dict] = []
    if warehouse is not None:
        try:
            # All sources (FRED, Tiingo, BLS, EIA, ...): compares each series'
            # manifest cadence against the latest observation already
            # ingested into Silver -- no live upstream API call needed.
            staleness_rows = reconcile_staleness(
                manifests, warehouse, series_ids=series_ids
            )
            stale_by_source: dict[str, list[str]] = {}
            no_data: list[str] = []
            for row in staleness_rows:
                if not row["has_data"]:
                    no_data.append(row["series_id"])
                elif row["is_stale"]:
                    stale_by_source.setdefault(row["source"], []).append(
                        row["series_id"]
                    )
            print(
                f"All-source staleness check: {len(staleness_rows)} series "
                f"across {len({r['source'] for r in staleness_rows})} source(s)."
            )
            for source, ids in sorted(stale_by_source.items()):
                print(
                    f"  [stale] {source}: {len(ids)} series past expected "
                    f"update: {', '.join(ids)}"
                )
            if no_data:
                print(
                    f"  [no-data] {len(no_data)} series have no ingested "
                    f"observations yet: {', '.join(no_data)}"
                )

            if not args.no_persist:
                counts = persist_report(config, report, warehouse)
                print(
                    f"Persisted {counts['lifecycle_rows']} lifecycle + "
                    f"{counts['drift_rows']} drift rows."
                )
                n_stale = warehouse.write_staleness(staleness_rows)
                print(f"Persisted {n_stale} all-source staleness rows.")
        finally:
            warehouse.close()

    if args.fail_on_drift and report.has_errors:
        print("FAIL: error-level drift detected.", file=sys.stderr)
        return 1
    return 0


def _cmd_validate(args: argparse.Namespace) -> int:
    manifests = load_manifests(args.manifests)
    specs = all_series(manifests, active_only=False)
    active = [s for s in specs if s.active]
    print(
        f"OK: {len(manifests)} manifest file(s), {len(specs)} series "
        f"({len(active)} active)."
    )
    for man in manifests:
        print(f"  - {man.name}: {len(man.series)} series ({man.source_path})")

    from fred_pipeline.governance.licensing import (
        check_commercial_use,
        check_redistribution_review,
        load_data_licensing_config,
    )

    licensing = load_data_licensing_config()
    active_sources = sorted({s.source for s in active})
    print(f"Active sources: {', '.join(active_sources) or '(none)'}")
    if args.commercial:
        violations = check_commercial_use(active, licensing)
        if violations:
            print("ERROR: commercial-use licensing check failed:", file=sys.stderr)
            for v in violations:
                print(
                    f"  - {v.source} ({v.series_count} active series): {v.reason}",
                    file=sys.stderr,
                )
            return 2
        print("Commercial-use licensing check: all active sources cleared.")

    if args.licensing_review:
        # Separate question from --commercial: not "may we sell it" but
        # "is our belief that we may redistribute it actually established".
        findings = check_redistribution_review(active, licensing)
        if findings:
            print(
                "ERROR: redistribution-review check failed -- these sources "
                "permit redistribution on unverified authority:",
                file=sys.stderr,
            )
            for v in findings:
                print(
                    f"  - {v.source} ({v.series_count} active series): {v.reason}",
                    file=sys.stderr,
                )
            print(
                "  Read each terms_url, then set review_status: verified and "
                "reviewed_by: <name> in config/data_licensing.yml.",
                file=sys.stderr,
            )
            return 2
        print("Redistribution-review check: all redistributed sources verified.")
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    from fred_pipeline.config import _ENV_OVERRIDES
    from fred_pipeline.pipeline import missing_source_keys

    config = PipelineConfig.resolve(
        environment=Environment(args.env),
        config_file=args.config,
        extract_workers=args.extract_workers,
        rate_limit_per_minute=args.rate_limit_per_minute,
        source_extract_workers=args.source_workers,
        source_rate_limits=args.source_rate_limits,
    )

    # Require only the API keys the *active* sources actually need — a
    # BLS/EIA-only run shouldn't demand a FRED key (and BLS runs keyless).
    wanted = set(_parse_series(args.series) or [])
    include_sources = {s.lower() for s in (_parse_series(args.source) or [])}
    exclude_sources = {s.lower() for s in (_parse_series(args.exclude_source) or [])}
    active = all_series(load_manifests(args.manifests), active_only=True)
    if wanted:
        active = [s for s in active if s.series_id in wanted]
    if include_sources:
        active = [s for s in active if s.source.lower() in include_sources]
    if exclude_sources:
        active = [s for s in active if s.source.lower() not in exclude_sources]
    sources = sorted({s.source for s in active})
    missing = missing_source_keys(config, sources)
    if missing:
        for src, attr in sorted(missing.items()):
            env = _ENV_OVERRIDES.get(attr, attr.upper())
            print(
                f"ERROR: source '{src}' requires an API key. Set {env}, put "
                f"{attr} in the config file, or configure a secret scope.",
                file=sys.stderr,
            )
        return 2

    spark = None
    warehouse = None
    persist = not args.dry_run

    if args.dry_run:
        # In-memory only: extract + DQ, no writes
        warehouse = None
    else:
        # Use warehouse factory with config + CLI overrides
        from fred_pipeline.io.warehouse_factory import (
            WarehouseConfig,
            WarehouseFactory,
            load_warehouse_config,
        )

        warehouse_config = load_warehouse_config(environment=config.environment.value)

        # CLI overrides: --local with optional --db-path
        if args.local:
            warehouse_config = WarehouseConfig(
                primary_backend="local",
                backends={"local": {"db_path": getattr(args, "db_path", "fred.db")}},
            )

        factory = WarehouseFactory(config, warehouse_config)
        warehouse = factory.build(force_dry_run=False)

        if warehouse is not None:
            print(f"Using warehouse: {warehouse.__class__.__name__}")
        else:
            print("No warehouse backend available; running in-memory only")

        # Spark is now initialized internally by the factory if needed
        spark = None  # Not used directly; warehouse manages its own Spark if needed

    pipeline = FredPipeline(
        config, spark=spark, warehouse=warehouse, persist_audit=persist
    )
    try:
        run = pipeline.run_from_manifest(
            args.manifests,
            triggered_by="cli-local" if args.local else "cli",
            build_gold_layer=not args.dry_run and not args.no_gold,
            series=_parse_series(args.series),
            sources=_parse_series(args.source),
            exclude_sources=_parse_series(args.exclude_source),
            force_full=args.full,
        )
    finally:
        if warehouse is not None:
            warehouse.close()

    print(json.dumps(run.to_row(), default=str, indent=2))
    if args.local and not args.dry_run:
        print(
            f"\nSaved to {args.db_path}. Inspect with e.g.:\n"
            f"  sqlite3 {args.db_path} "
            f"'SELECT series_id, observation_date, value "
            f"FROM gold_fred_latest_observation LIMIT 10;'"
        )
    return 0 if run.status.value in ("succeeded", "partial") else 1


def _cmd_gold(args: argparse.Namespace) -> int:
    config = PipelineConfig.resolve(
        environment=Environment(args.env), config_file=args.config
    )
    warehouse = None
    if args.local:
        from fred_pipeline.local_store import LocalWarehouse

        warehouse = LocalWarehouse(config, db_path=args.db_path)
        print(f"Using local SQLite backend: {args.db_path}")
    else:
        from fred_pipeline.spark_io import get_spark
        from fred_pipeline.warehouse import SparkWarehouse

        warehouse = SparkWarehouse(config, get_spark())
    try:
        result = warehouse.build_gold()
    finally:
        warehouse.close()
    print(json.dumps(result, indent=2))
    return 0


def _quota_limited(run) -> bool:
    for series_run in run.series_runs:
        message = (series_run.error_message or "").lower()
        if "http 429" in message or "hourly request allocation" in message:
            return True
        if "rate limit" in message or "quota" in message:
            return True
    return False


def _cmd_price_constituents(args: argparse.Namespace) -> int:
    from fred_pipeline.config import _ENV_OVERRIDES
    from fred_pipeline.constituent_pricing import (
        plan_tiingo_constituent_pricing,
        specs_for_tiingo_candidates,
    )
    from fred_pipeline.local_store import LocalWarehouse
    from fred_pipeline.pipeline import missing_source_keys

    config = PipelineConfig.resolve(
        environment=Environment(args.env),
        config_file=args.config,
        source_extract_workers=f"tiingo={args.workers}",
        source_rate_limits=f"tiingo={args.rate_limit_per_minute}",
    )

    as_of = None
    if args.as_of_date:
        try:
            as_of = _dt.date.fromisoformat(args.as_of_date)
        except ValueError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2

    warehouse = LocalWarehouse(config, db_path=args.db_path)
    try:
        constituent_rows = warehouse.query(
            """
            SELECT constituent AS ticker, weight_rank, weight_pct
            FROM gold_index_constituents
            WHERE index_etf = ? AND is_latest_snapshot = 1
            ORDER BY weight_rank, constituent
            """,
            (args.index_etf.upper(),),
        )
        latest_price_rows = warehouse.query(
            """
            SELECT
                substr(series_id, 1, instr(series_id, ':') - 1) AS ticker,
                MAX(observation_date) AS latest_price_date
            FROM silver_fred_observation
            WHERE source = 'tiingo' AND series_id LIKE '%:adjClose'
            GROUP BY ticker
            """
        )
        plan = plan_tiingo_constituent_pricing(
            constituent_rows,
            latest_price_rows,
            index_etf=args.index_etf,
            as_of_date=as_of,
            stale_days=args.stale_days,
            limit=args.max_symbols,
        )

        summary = {
            "index_etf": plan.index_etf,
            "as_of_date": plan.as_of_date,
            "stale_days": plan.stale_days,
            "total_constituents": plan.total_constituents,
            "already_fresh": plan.already_fresh,
            "skipped_unpriceable": list(plan.skipped_unpriceable),
            "candidates_total": len(plan.candidates),
            "batch_size": len(plan.batch),
            "batch": [c.__dict__ for c in plan.batch],
        }
        print(json.dumps(summary, indent=2))

        if args.dry_run or not plan.batch:
            return 0

        missing = missing_source_keys(config, ["tiingo"])
        if missing:
            attr = missing["tiingo"]
            env = _ENV_OVERRIDES.get(attr, attr.upper())
            print(
                f"ERROR: source 'tiingo' requires an API key. Set {env}, put "
                f"{attr} in the config file, or configure a secret scope.",
                file=sys.stderr,
            )
            return 2

        pipeline = FredPipeline(
            config, warehouse=warehouse, persist_audit=not args.no_audit
        )
        succeeded = 0
        failed = 0
        stopped_for_quota = False
        run_rows = []
        for spec in specs_for_tiingo_candidates(plan.batch):
            run = pipeline.run(
                [spec],
                manifest_path=f"dynamic:{plan.index_etf}:tiingo_constituents",
                triggered_by="cli-local-constituent-pricing",
                build_gold_layer=False,
            )
            run_rows.append(run.to_row())
            succeeded += run.series_succeeded
            failed += run.series_failed
            if _quota_limited(run):
                stopped_for_quota = True
                print(
                    f"Stopping after {spec.series_id}: Tiingo quota/rate limit hit.",
                    file=sys.stderr,
                )
                break

        result = {
            "series_attempted": succeeded + failed,
            "series_succeeded": succeeded,
            "series_failed": failed,
            "stopped_for_quota": stopped_for_quota,
            "runs": run_rows,
        }
        if args.rebuild_gold and succeeded:
            result["gold"] = warehouse.build_gold()
        print(json.dumps(result, default=str, indent=2))
        if failed and not succeeded:
            return 1
        return 0
    finally:
        warehouse.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fred_pipeline", description=__doc__)
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="enable debug logging"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    v = sub.add_parser("validate", help="validate manifests only")
    v.add_argument("--manifests", default="manifests")
    v.add_argument(
        "--commercial",
        action="store_true",
        help="fail if any active source's license doesn't clear commercial "
        "use (see config/data_licensing.yml)",
    )
    v.add_argument(
        "--licensing-review",
        action="store_true",
        help="fail if any active source permits redistribution but nobody has "
        "verified its terms (review_status != verified). Run this before "
        "publishing anything derived from the data outside the org.",
    )
    v.set_defaults(func=_cmd_validate)

    r = sub.add_parser("run", help="run the pipeline")
    r.add_argument("--env", default="dev", choices=[e.value for e in Environment])
    r.add_argument("--manifests", default="manifests")
    r.add_argument(
        "--config",
        default=None,
        help="path to a YAML config file "
        "(default: $FRED_CONFIG_FILE or config/config.yaml)",
    )
    r.add_argument(
        "--dry-run",
        action="store_true",
        help="extract + validate in memory, no writes (no Spark, no local db)",
    )
    r.add_argument(
        "--local",
        action="store_true",
        help="persist to a local SQLite file instead of Databricks/Delta",
    )
    r.add_argument(
        "--db-path",
        default="fred_local.db",
        help="SQLite file path for --local runs (default: fred_local.db)",
    )
    r.add_argument(
        "--series",
        default=None,
        help="comma-separated series ids to run (default: all active)",
    )
    r.add_argument(
        "--full",
        action="store_true",
        help="force a full re-pull, ignoring the restate watermark",
    )
    r.add_argument(
        "--no-gold",
        action="store_true",
        help="skip the Gold refresh after extracting/writing Silver",
    )
    r.add_argument(
        "--source",
        default=None,
        help="comma-separated source names to include, e.g. fred,stooq",
    )
    r.add_argument(
        "--exclude-source",
        default=None,
        help="comma-separated source names to skip, e.g. tiingo",
    )
    r.add_argument(
        "--extract-workers",
        type=int,
        default=None,
        help="override concurrent extraction workers for this run",
    )
    r.add_argument(
        "--rate-limit-per-minute",
        type=int,
        default=None,
        help="override FRED aggregate request rate for this run",
    )
    r.add_argument(
        "--source-workers",
        default=None,
        help="per-source worker overrides, e.g. fred=16,tiingo=1",
    )
    r.add_argument(
        "--source-rate-limits",
        default=None,
        help="per-source request-rate overrides, e.g. fred=60,tiingo=5",
    )
    r.set_defaults(func=_cmd_run)

    g = sub.add_parser(
        "gold",
        help="rebuild Gold from already persisted Bronze/Silver data",
    )
    g.add_argument("--env", default="dev", choices=[e.value for e in Environment])
    g.add_argument(
        "--config",
        default=None,
        help="path to a YAML config file "
        "(default: $FRED_CONFIG_FILE or config/config.yaml)",
    )
    g.add_argument("--local", action="store_true", help="use a local SQLite backend")
    g.add_argument("--db-path", default="fred_local.db")
    g.set_defaults(func=_cmd_gold)

    pc = sub.add_parser(
        "price-constituents",
        help=(
            "derive Tiingo pricing batches from latest ETF constituents "
            "and pull only missing/stale tickers"
        ),
    )
    pc.add_argument("--env", default="dev", choices=[e.value for e in Environment])
    pc.add_argument(
        "--config",
        default=None,
        help="path to a YAML config file "
        "(default: $FRED_CONFIG_FILE or config/config.yaml)",
    )
    pc.add_argument("--db-path", default="fred_local.db")
    pc.add_argument("--index-etf", default="IVV")
    pc.add_argument("--as-of-date", default=None, help="YYYY-MM-DD; default today")
    pc.add_argument("--stale-days", type=int, default=7)
    pc.add_argument("--max-symbols", type=int, default=25)
    pc.add_argument("--workers", type=int, default=1)
    pc.add_argument("--rate-limit-per-minute", type=int, default=5)
    pc.add_argument("--dry-run", action="store_true")
    pc.add_argument(
        "--rebuild-gold",
        action="store_true",
        help="rebuild Gold after successful constituent price pulls",
    )
    pc.add_argument(
        "--no-audit",
        action="store_true",
        help="skip audit persistence for the per-symbol dynamic runs",
    )
    pc.set_defaults(func=_cmd_price_constituents)

    d = sub.add_parser(
        "discover",
        help="generate a manifest from a FRED category / release / search",
    )
    d.add_argument(
        "--name",
        required=True,
        help="manifest name + category label for the generated series",
    )
    src = d.add_mutually_exclusive_group(required=True)
    src.add_argument("--category-id", type=int, help="FRED category id")
    src.add_argument("--release-id", type=int, help="FRED release id")
    src.add_argument("--search", help="full-text search string")
    d.add_argument(
        "--out",
        default=None,
        help="output YAML path (omit or use --dry-run to print instead)",
    )
    d.add_argument("--env", default="dev", choices=[e.value for e in Environment])
    d.add_argument("--config", default=None, help="YAML config path for the API key")
    d.add_argument(
        "--manifests",
        default="manifests",
        help="existing manifests dir to dedupe against (default: manifests)",
    )
    d.add_argument(
        "--frequencies",
        default=None,
        help="comma-separated frequency filter, e.g. 'd,m,q'",
    )
    d.add_argument(
        "--max", type=int, default=100, help="max series to keep (default: 100)"
    )
    d.add_argument(
        "--min-popularity",
        type=float,
        default=0.0,
        help="drop series below this FRED popularity (0-100)",
    )
    d.add_argument(
        "--include-discontinued",
        action="store_true",
        help="keep series whose title is marked DISCONTINUED",
    )
    d.add_argument(
        "--include-existing",
        action="store_true",
        help="do not dedupe against series already in --manifests",
    )
    d.add_argument("--description", default=None, help="manifest description")
    d.add_argument("--dry-run", action="store_true", help="print instead of writing")
    d.set_defaults(func=_cmd_discover)

    de = sub.add_parser(
        "discover-ecb",
        help="inspect ECB SDMX metadata and generate candidate manifests",
    )
    de.add_argument("--list-flows", action="store_true", help="list ECB dataflows")
    de.add_argument(
        "--flow",
        default=None,
        help="ECB dataflow id to inspect or expand, e.g. EXR",
    )
    de.add_argument(
        "--agency",
        default=None,
        help="override dataflow agency id; defaults to the listed ECB agency",
    )
    de.add_argument(
        "--version",
        default=None,
        help="override dataflow version; defaults to the listed ECB version",
    )
    de.add_argument(
        "--inspect",
        action="store_true",
        help="inspect one --flow's ordered dimensions and code lists",
    )
    de.add_argument(
        "--search",
        default=None,
        help="case-insensitive filter on flow id/name/description",
    )
    de.add_argument(
        "--max",
        type=int,
        default=100,
        help="maximum dataflows or generated candidates to print/write",
    )
    de.add_argument(
        "--sample-codes",
        type=int,
        default=8,
        help="codes to show per dimension when inspecting a flow",
    )
    de.add_argument(
        "--dimension",
        action="append",
        default=[],
        help="dimension filter KEY=VALUE[,VALUE]; repeatable",
    )
    de.add_argument(
        "--frequency",
        default=None,
        help="manifest frequency filter, e.g. d,m,q,a",
    )
    de.add_argument(
        "--include-code",
        action="append",
        default=[],
        help="keep only codes whose id/name contains this text; repeatable",
    )
    de.add_argument(
        "--exclude-code",
        action="append",
        default=[],
        help="drop codes whose id/name contains this text; repeatable",
    )
    de.add_argument(
        "--max-cartesian",
        type=int,
        default=10000,
        help="refuse candidate expansion above this count unless --force is set",
    )
    de.add_argument(
        "--force",
        action="store_true",
        help="allow candidate expansion above --max-cartesian",
    )
    de.add_argument(
        "--include-existing",
        action="store_true",
        help="do not dedupe against series already in --manifests",
    )
    de.add_argument(
        "--manifests",
        default="manifests",
        help="manifest directory used for duplicate exclusion",
    )
    de.add_argument("--name", default=None, help="generated manifest name")
    de.add_argument("--category", default=None, help="generated manifest category")
    de.add_argument(
        "--description", default=None, help="generated manifest description"
    )
    de.add_argument("--dry-run", action="store_true", help="print instead of writing")
    de.add_argument("--out", default=None, help="path for generated manifest YAML")
    de.add_argument(
        "--json", action="store_true", help="print machine-readable metadata"
    )
    de.add_argument("--env", default="dev", choices=[e.value for e in Environment])
    de.add_argument(
        "--config",
        default=None,
        help="YAML config path for ECB_BASE_URL/proxy overrides",
    )
    de.set_defaults(func=_cmd_discover_ecb)

    db_ = sub.add_parser(
        "discover-bls",
        help="inspect BLS survey flat-file catalogs and generate candidate manifests",
    )
    db_.add_argument("--list-surveys", action="store_true", help="list BLS surveys")
    db_.add_argument(
        "--survey",
        default=None,
        help="BLS survey abbreviation to inspect or expand, e.g. CU, CE, LN",
    )
    db_.add_argument(
        "--inspect",
        action="store_true",
        help="inspect one --survey's flat-file columns and value samples",
    )
    db_.add_argument(
        "--search",
        default=None,
        help="case-insensitive filter on survey name, or on series id/title",
    )
    db_.add_argument(
        "--max",
        type=int,
        default=100,
        help="maximum surveys or generated candidates to print/write",
    )
    db_.add_argument(
        "--sample-values",
        type=int,
        default=8,
        help="distinct sample values to show per column when inspecting",
    )
    db_.add_argument(
        "--column",
        action="append",
        default=[],
        help="flat-file column filter COLUMN=VALUE[,VALUE]; repeatable",
    )
    db_.add_argument(
        "--frequency",
        default=None,
        help=(
            "manifest frequency for generated candidates (d/w/m/q/sa/a) -- "
            "required; BLS flat files don't self-describe this the way ECB's "
            "SDMX FREQ dimension does"
        ),
    )
    db_.add_argument(
        "--include-existing",
        action="store_true",
        help="do not dedupe against series already in --manifests",
    )
    db_.add_argument(
        "--manifests",
        default="manifests",
        help="manifest directory used for duplicate exclusion",
    )
    db_.add_argument("--name", default=None, help="generated manifest name")
    db_.add_argument("--category", default=None, help="generated manifest category")
    db_.add_argument(
        "--description", default=None, help="generated manifest description"
    )
    db_.add_argument("--dry-run", action="store_true", help="print instead of writing")
    db_.add_argument("--out", default=None, help="path for generated manifest YAML")
    db_.add_argument(
        "--json", action="store_true", help="print machine-readable metadata"
    )
    db_.add_argument("--env", default="dev", choices=[e.value for e in Environment])
    db_.add_argument(
        "--config",
        default=None,
        help="YAML config path for BLS_USER_AGENT overrides",
    )
    db_.set_defaults(func=_cmd_discover_bls)

    rc = sub.add_parser(
        "reconcile",
        help="FRED metadata drift/lifecycle + all-source staleness check",
    )
    rc.add_argument("--manifests", default="manifests")
    rc.add_argument("--env", default="dev", choices=[e.value for e in Environment])
    rc.add_argument("--config", default=None, help="YAML config path for the API key")
    rc.add_argument(
        "--series",
        default=None,
        help="comma-separated series ids to reconcile (default: all)",
    )
    rc.add_argument(
        "--local",
        action="store_true",
        help="persist lifecycle/drift to a local SQLite file",
    )
    rc.add_argument("--db-path", default="fred_local.db")
    rc.add_argument(
        "--no-persist",
        action="store_true",
        help="report only; do not write to any backend",
    )
    rc.add_argument(
        "--fail-on-drift",
        action="store_true",
        help="exit non-zero if any error-level drift is found (for CI)",
    )
    rc.set_defaults(func=_cmd_reconcile)

    bf = sub.add_parser(
        "backfill",
        help="generate point-in-time Gold snapshots over a date range",
    )
    bf.add_argument(
        "--db-path",
        default="fred_local.db",
        help="SQLite source database containing Silver data (default: fred_local.db)",
    )
    bf.add_argument(
        "--backfill-db",
        default="fred_backfill.db",
        help="SQLite output database for pit_* tables (default: fred_backfill.db)",
    )
    bf.add_argument(
        "--from",
        dest="from_date",
        required=True,
        metavar="DATE",
        help="start date for snapshots (YYYY-MM-DD)",
    )
    bf.add_argument(
        "--to",
        dest="to_date",
        required=True,
        metavar="DATE",
        help="end date for snapshots (YYYY-MM-DD)",
    )
    bf.add_argument(
        "--step",
        default="monthly",
        choices=["monthly", "weekly", "daily"],
        help="snapshot cadence: monthly (month-end), weekly (Sunday), daily "
        "(default: monthly)",
    )
    bf.add_argument(
        "--tables",
        default=None,
        help=(
            "comma-separated subset of tables to build "
            "(default: all). Valid names: feature_transforms, ml_feature_matrix, "
            "macro_factor_scores, macro_factor_loadings, macro_anomaly_scores, "
            "macro_regime_daily, recession_probability_daily"
        ),
    )
    bf.add_argument(
        "--no-resume",
        action="store_true",
        help="recompute already-finished snapshot dates instead of skipping them",
    )
    bf.set_defaults(func=_cmd_backfill)

    rp = sub.add_parser(
        "replay",
        help="rebuild Silver/Gold from archived Bronze payloads (no FRED calls)",
    )
    rp.add_argument("--manifests", default="manifests")
    rp.add_argument("--env", default="dev", choices=[e.value for e in Environment])
    rp.add_argument("--config", default=None)
    rp.add_argument(
        "--series",
        default=None,
        help="comma-separated series ids to replay (default: all)",
    )
    rp.add_argument("--local", action="store_true", help="use a local SQLite backend")
    rp.add_argument("--db-path", default="fred_local.db")
    rp.add_argument(
        "--no-gold",
        action="store_true",
        help="rebuild Silver only, skip the Gold refresh",
    )
    rp.set_defaults(func=_cmd_replay)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
