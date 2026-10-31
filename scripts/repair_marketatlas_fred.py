"""Explicit, staged FRED window repair. No writes occur until the apply command.

Uses the upstream credential/configuration and localhost warehouse only. All old
rows are retained in audit.fred_repair_backup before a transactional replacement.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import uuid
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.sources.fred import FredClient
from fred_pipeline.transform import normalize_observations, assign_revision_numbers

TABLES = (
    "silver.fred_observation",
    "gold.fred_latest_observation",
    "gold.fred_revision_stats",
)
COLUMNS = (
    "source",
    "series_id",
    "observation_date",
    "realtime_start",
    "realtime_end",
    "value",
    "raw_value",
    "is_missing",
    "row_hash",
    "revision_number",
    "ingested_at",
    "run_id",
)


def encode(value):
    return json.dumps(value, sort_keys=True, allow_nan=False).encode()


def validate(rows, sid, start, end, max_rows=100000, *, current_only=False):
    if not re.fullmatch(r"[A-Za-z0-9_]+", sid) or not rows or len(rows) > max_rows:
        raise ValueError("invalid series or observation row limit")
    periods = defaultdict(list)
    for row in rows:
        if row["series_id"] != sid or row["source"] != "fred":
            raise ValueError("source identity mismatch")
        period = date.fromisoformat(row["observation_date"])
        if current_only:
            if row["realtime_start"] or row["realtime_end"] or periods[period]:
                raise ValueError(
                    "current-only capture must have unknown dates and unique periods"
                )
            if not date.fromisoformat(start) <= period <= date.fromisoformat(end):
                raise ValueError("current-only period outside window")
            if row["is_missing"] != (row["value"] is None) or (
                row["value"] is not None and not math.isfinite(row["value"])
            ):
                raise ValueError("invalid current-only value state")
            periods[period].append(None)
            continue
        available = date.fromisoformat(row["realtime_start"])
        through = date.fromisoformat(row["realtime_end"])
        if (
            not date.fromisoformat(start) <= period <= date.fromisoformat(end)
            or available > date.fromisoformat(end)
            or through < available
        ):
            raise ValueError("invalid source interval or window")
        if row["is_missing"] != (row["value"] is None) or (
            row["value"] is not None and not math.isfinite(row["value"])
        ):
            raise ValueError("invalid value state")
        periods[period].append((available, through))
    if current_only:
        return len(rows)
    for intervals in periods.values():
        intervals.sort()
        if any(b[0] <= a[1] for a, b in zip(intervals, intervals[1:])):
            raise ValueError("overlapping source vintages")
        if intervals[-1][1] != date.max:
            raise ValueError("latest source interval is clipped")
    return len(rows)


def stage(args):
    root = Path(args.output).resolve()
    root.mkdir(parents=True, exist_ok=True)
    specs = {}
    for path in args.manifests:
        for spec in json.loads(Path(path).read_text())["series"]:
            specs[spec["series_id"]] = spec
    ids = args.series.split(",") if args.series else sorted(specs)
    if (
        not set(ids) <= set(specs)
        or len(ids) > 268
        or date.fromisoformat(args.start) > date.fromisoformat(args.end)
    ):
        raise ValueError("invalid selected series or window")
    config = PipelineConfig.resolve(
        environment=Environment.DEV, config_file=args.config
    )
    client = FredClient(
        config.fred_api_key, rate_limit_per_minute=60, max_retries=1, timeout=30
    )
    current_only = getattr(args, "current_only", False)
    result = {
        "current_only": current_only,
        "version": 1,
        "start": args.start,
        "end": args.end,
        "series": {},
        "failures": {},
        "source": "fred",
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }
    for sid in ids:
        path = root / f"{sid}.json"
        try:
            if path.exists():
                document = json.loads(path.read_text())
                if document["start"] != args.start or document["end"] != args.end:
                    raise ValueError("existing stage window differs")
            else:
                method = (
                    client.get_observations
                    if current_only
                    else client.get_observations_all_vintages
                )
                payload = method(
                    sid, observation_start=args.start, observation_end=args.end
                )
                rows = assign_revision_numbers(
                    normalize_observations(sid, payload, track_vintage=not current_only)
                )
                document = {
                    "series_id": sid,
                    "current_only": current_only,
                    "start": args.start,
                    "end": args.end,
                    "captured_at": datetime.now(timezone.utc).isoformat(),
                    "rows": rows,
                    "payload": payload,
                }
                validate(rows, sid, args.start, args.end, current_only=current_only)
                path.write_bytes(encode(document))
            if document.get("current_only", False) != current_only:
                raise ValueError("stage mode differs; use a separate output directory")
            count = validate(
                document["rows"], sid, args.start, args.end, current_only=current_only
            )
            result["series"][sid] = {
                "file": path.name,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "rows": count,
                "latest_period": max(r["observation_date"] for r in document["rows"]),
            }
            print(
                json.dumps({"series_id": sid, "status": "staged", "rows": count}),
                flush=True,
            )
        except Exception as error:
            result["failures"][sid] = type(error).__name__
            print(
                json.dumps(
                    {
                        "series_id": sid,
                        "status": "quarantined",
                        "error_type": type(error).__name__,
                    }
                ),
                flush=True,
            )
        (root / "manifest.json").write_bytes(encode(result))
    print(
        json.dumps(
            {
                "status": "staged",
                "series": len(result["series"]),
                "failures": len(result["failures"]),
                "rows": sum(r["rows"] for r in result["series"].values()),
            }
        ),
        flush=True,
    )


def connect():
    import psycopg
    from urllib.parse import urlsplit
    from fred_pipeline.io.postgres_config import resolve_postgres_settings
    from fred_pipeline.io.warehouse_factory import load_warehouse_config

    settings = resolve_postgres_settings(
        load_warehouse_config(environment="dev").backends.get("postgres", {})
    )
    target = urlsplit(settings.dsn)
    if (
        target.hostname not in ("localhost", "127.0.0.1")
        or target.port != 55432
        or target.path != "/macro_medallion"
    ):
        raise ValueError("repair is restricted to the approved localhost warehouse")
    return psycopg.connect(settings.dsn)


def ensure_audit(connection):
    # Bounded per-series repair needs this index on the large revision table.
    # Concurrent creation preserves normal readers and writers during setup.
    connection.autocommit = True
    with connection.cursor() as q:
        q.execute("SET statement_timeout='5min'")
        q.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_gold_revision_sid_date ON gold.fred_revision_stats(series_id,observation_date)"
        )
        q.execute("RESET statement_timeout")
    connection.autocommit = False
    with connection.transaction(), connection.cursor() as q:
        q.execute("""CREATE TABLE IF NOT EXISTS audit.fred_repair_batch (
            batch_id text PRIMARY KEY,series_id text NOT NULL,stage_sha256 text NOT NULL,
            window_start text NOT NULL,window_end text NOT NULL,status text NOT NULL,
            metadata_before jsonb NOT NULL,applied_at timestamptz DEFAULT now(),rows_inserted bigint NOT NULL)""")
        q.execute("""CREATE TABLE IF NOT EXISTS audit.fred_repair_backup (
            batch_id text NOT NULL REFERENCES audit.fred_repair_batch(batch_id),
            object_name text NOT NULL,row_data jsonb NOT NULL)""")


REVISION_SQL = """INSERT INTO gold.fred_revision_stats
(series_id,observation_date,revision_count,first_value,first_realtime_start,latest_value,latest_realtime_start,revision_delta,revision_pct)
WITH grouped AS (
 SELECT series_id,observation_date,count(*) AS n,
 (array_agg(value ORDER BY realtime_start))[1] AS first_value,
 min(realtime_start) AS first_start,
 (array_agg(value ORDER BY realtime_start DESC))[1] AS latest_value,
 max(realtime_start) AS latest_start
 FROM silver.fred_observation WHERE source='fred' AND series_id=%s
 AND observation_date BETWEEN %s AND %s AND is_missing=0 AND value IS NOT NULL
 GROUP BY series_id,observation_date)
SELECT series_id,observation_date,n,first_value,first_start,latest_value,latest_start,
 latest_value-first_value,CASE WHEN first_value<>0 THEN (latest_value-first_value)/first_value ELSE NULL END FROM grouped"""


def apply(args):
    from psycopg import sql

    root = Path(args.output).resolve()
    manifest = json.loads((root / "manifest.json").read_text())
    ids = args.series.split(",") if args.series else sorted(manifest["series"])
    if not set(ids) <= set(manifest["series"]):
        raise ValueError("series not successfully staged")
    connection = connect()
    ensure_audit(connection)
    report = []
    for sid in ids:
        spec = manifest["series"][sid]
        path = root / spec["file"]
        if (
            path.name != f"{sid}.json"
            or path.parent != root
            or hashlib.sha256(path.read_bytes()).hexdigest() != spec["sha256"]
        ):
            raise ValueError("stage checksum or file identity mismatch")
        doc = json.loads(path.read_text())
        rows = doc["rows"]
        validate(
            rows,
            sid,
            manifest["start"],
            manifest["end"],
            current_only=doc.get("current_only", False),
        )
        batch = uuid.uuid4().hex
        params = (sid, manifest["start"], manifest["end"])
        with connection.transaction(), connection.cursor() as q:
            q.execute("SET LOCAL lock_timeout='5s'")
            q.execute("SET LOCAL statement_timeout='60s'")
            q.execute("SELECT pg_advisory_xact_lock(194700122)")
            q.execute(
                "SELECT batch_id FROM audit.fred_repair_batch WHERE series_id=%s AND stage_sha256=%s AND status=%s",
                (sid, spec["sha256"], "applied"),
            )
            existing = q.fetchone()
            if existing:
                report.append(
                    {
                        "series_id": sid,
                        "status": "already_applied",
                        "batch_id": existing[0],
                    }
                )
                continue
            q.execute("LOCK TABLE silver.fred_observation IN SHARE ROW EXCLUSIVE MODE")
            q.execute(
                "SELECT to_jsonb(m) FROM meta.fred_series m WHERE series_id=%s FOR UPDATE",
                (sid,),
            )
            meta = q.fetchone()
            if not meta:
                raise ValueError("source metadata absent")
            q.execute(
                "SELECT max(ingested_at) FROM silver.fred_observation WHERE source=%s AND series_id=%s AND observation_date BETWEEN %s AND %s",
                ("fred", *params),
            )
            last = q.fetchone()[0]
            if last and datetime.fromisoformat(str(last)) > datetime.fromisoformat(
                doc["captured_at"]
            ):
                raise ValueError(
                    "source changed after capture; restage before replacing"
                )
            q.execute(
                "INSERT INTO audit.fred_repair_batch(batch_id,series_id,stage_sha256,window_start,window_end,status,metadata_before,rows_inserted) VALUES(%s,%s,%s,%s,%s,%s,%s::jsonb,%s)",
                (
                    batch,
                    sid,
                    spec["sha256"],
                    manifest["start"],
                    manifest["end"],
                    "applied",
                    json.dumps(meta[0]),
                    len(rows),
                ),
            )
            for table in TABLES:
                extra = " AND source='fred'" if table == TABLES[0] else ""
                qualified = sql.SQL(".").join(
                    sql.Identifier(p) for p in table.split(".")
                )
                q.execute(
                    sql.SQL(
                        "INSERT INTO audit.fred_repair_backup SELECT %s,%s,to_jsonb(t) FROM {} t WHERE series_id=%s AND observation_date BETWEEN %s AND %s"
                        + extra
                    ).format(qualified),
                    (batch, table, *params),
                )
                q.execute(
                    sql.SQL(
                        "DELETE FROM {} WHERE series_id=%s AND observation_date BETWEEN %s AND %s"
                        + extra
                    ).format(qualified),
                    params,
                )
            for row in rows:
                row["run_id"] = batch
                row["is_missing"] = int(row["is_missing"])
            with q.copy(
                "COPY silver.fred_observation (" + ",".join(COLUMNS) + ") FROM STDIN"
            ) as copy:
                for row in rows:
                    copy.write_row(tuple(row[key] for key in COLUMNS))
            q.execute(
                "UPDATE meta.fred_series SET vintage_enabled=%s,updated_at=%s WHERE series_id=%s",
                (int(not doc.get("current_only", False)), doc["captured_at"], sid),
            )
            q.execute(
                """INSERT INTO gold.fred_latest_observation(series_id,observation_date,value,realtime_start,realtime_end,is_missing,revision_number,ingested_at)
                SELECT DISTINCT ON(observation_date) series_id,observation_date,value,realtime_start,realtime_end,is_missing,revision_number,ingested_at
                FROM silver.fred_observation WHERE source='fred' AND series_id=%s AND observation_date BETWEEN %s AND %s ORDER BY observation_date,realtime_start DESC""",
                params,
            )
            q.execute(REVISION_SQL, params)
            now = datetime.now(timezone.utc).isoformat()
            q.execute(
                """INSERT INTO audit.etl_run(run_id,environment,manifest_path,triggered_by,status,started_at,ended_at,duration_seconds,series_total,series_succeeded,series_failed,error_message,series_skipped_not_due)
                VALUES(%s,'dev',%s,'authorized-source-repair','succeeded',%s,%s,0,1,1,0,'',0)""",
                (batch, str(root / "manifest.json"), doc["captured_at"], now),
            )
            q.execute(
                """INSERT INTO audit.etl_series_run(run_id,series_id,status,load_type,started_at,ended_at,duration_seconds,observations_extracted,rows_written_bronze,rows_merged_silver,dq_passed,error_message)
                VALUES(%s,%s,'succeeded','bounded-historical-repair',%s,%s,0,%s,0,%s,1,'')""",
                (batch, sid, doc["captured_at"], now, len(rows), len(rows)),
            )
            q.execute(
                "SELECT count(*) FROM silver.fred_observation WHERE source='fred' AND series_id=%s AND observation_date BETWEEN %s AND %s",
                params,
            )
            if q.fetchone()[0] != len(rows):
                raise ValueError("post-write count mismatch")
            q.execute(
                "SELECT count(*) FROM audit.fred_repair_backup WHERE batch_id=%s",
                (batch,),
            )
            backups = q.fetchone()[0]
            report.append(
                {
                    "series_id": sid,
                    "status": "applied",
                    "batch_id": batch,
                    "rows": len(rows),
                    "backed_up_rows": backups,
                }
            )
        (root / "apply-result.json").write_bytes(encode(report))
        print(json.dumps(report[-1]), flush=True)
    connection.close()
    (root / "apply-result.json").write_bytes(encode(report))


def restore(args):
    from psycopg import sql

    connection = connect()
    with connection.transaction(), connection.cursor() as q:
        q.execute("SET LOCAL lock_timeout='5s'")
        q.execute("SET LOCAL statement_timeout='60s'")
        q.execute("SELECT pg_advisory_xact_lock(194700122)")
        q.execute(
            "SELECT series_id,window_start,window_end,metadata_before,status FROM audit.fred_repair_batch WHERE batch_id=%s FOR UPDATE",
            (args.batch_id,),
        )
        row = q.fetchone()
        if not row or row[4] != "applied":
            raise ValueError("batch is absent or already restored")
        params = row[:3]
        q.execute("LOCK TABLE silver.fred_observation IN SHARE ROW EXCLUSIVE MODE")
        q.execute(
            "SELECT 1 FROM silver.fred_observation WHERE source='fred' AND series_id=%s AND observation_date BETWEEN %s AND %s AND run_id IS DISTINCT FROM %s LIMIT 1",
            (*params, args.batch_id),
        )
        if q.fetchone():
            raise ValueError(
                "source changed after repair; refusing to restore over newer records"
            )
        for table in TABLES:
            qualified = sql.SQL(".").join(sql.Identifier(p) for p in table.split("."))
            extra = " AND source='fred'" if table == TABLES[0] else ""
            q.execute(
                sql.SQL(
                    "DELETE FROM {} WHERE series_id=%s AND observation_date BETWEEN %s AND %s"
                    + extra
                ).format(qualified),
                params,
            )
            q.execute(
                sql.SQL(
                    "INSERT INTO {} SELECT (jsonb_populate_record(NULL::{},row_data)).* FROM audit.fred_repair_backup WHERE batch_id=%s AND object_name=%s"
                ).format(qualified, qualified),
                (args.batch_id, table),
            )
        q.execute(
            "UPDATE meta.fred_series SET vintage_enabled=%s,updated_at=%s WHERE series_id=%s",
            (row[3]["vintage_enabled"], row[3]["updated_at"], row[0]),
        )
        q.execute(
            "UPDATE audit.fred_repair_batch SET status='restored' WHERE batch_id=%s",
            (args.batch_id,),
        )
    connection.close()
    print(json.dumps({"status": "restored", "batch_id": args.batch_id}))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)
    s = sub.add_parser("stage")
    s.add_argument("--manifests", nargs="+", required=True)
    s.add_argument("--output", required=True)
    s.add_argument("--config", default="config/config.yaml")
    s.add_argument("--start", default="2024-01-01")
    s.add_argument("--end", required=True)
    s.add_argument("--series")
    s.add_argument(
        "--current-only",
        action="store_true",
        help="Explicit latest-only refresh; keep availability unknown and vintage tracking disabled",
    )
    a = sub.add_parser("apply")
    a.add_argument("--output", required=True)
    a.add_argument("--series")
    r = sub.add_parser("restore")
    r.add_argument("--batch-id", required=True)
    args = p.parse_args()
    try:
        {"stage": stage, "apply": apply, "restore": restore}[args.command](args)
    except Exception as error:
        p.exit(
            1,
            f"Source repair stopped ({type(error).__name__}); inspect retained stage and audit backups.\n",
        )


if __name__ == "__main__":
    main()
