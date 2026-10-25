# Spec 012 Task Checklist

Status: Draft for review. All implementation tasks are pending; T1 published
package metadata research is complete, but runtime verification is not.

**Next task: complete T1**, following the [canonical handoff](../docs/handoffs/dagster_orchestration.md#next-task-t1).

Requirements: [spec](../specs/spec012/README.md). Sequence: [plan](plan.md).
Each task includes code/docs and focused tests for its behavior; preserve the
repository's existing suites. Listed new files are proposals, not existing code.

## Phase 1: Execution foundation

- [ ] **T1 — Resolve the optional runtime** (S/M; no dependency)
  - Established: candidate pins are `dagster==1.13.26`,
    `dagster-webserver==1.13.26`, `dagster-postgres==0.29.26`; PyPI declares
    matching dependencies and Python 3.12 support. No clean install recorded.
  - Acceptance: Python 3.12 resolves a compatible exact Dagster/webserver/Postgres
    set; core installation still works without Dagster; resolved versions and
    commands are recorded (AC-01).
  - Verify: clean core and orchestration virtual environments; `pip check`;
    CLI help in both environments; dependency import smoke.
  - Files: `pyproject.toml`, `requirements-dagster.lock.txt`, planned
    `docs/deployment/dagster_runbook.md`,
    `tests/test_dagster_optional_dependencies.py`, README install section.
  - Handoff: record actual two-environment results; keep T1 unchecked until
    they pass, then update all pickup pointers to T2. Definitions import safety
    is the later T4 portion of AC-01, not a missing module to implement in T1.

- [ ] **T2 — Share the ECB poll service** (M; no dependency)
  - Acceptance: CLI and main-run hook delegate through one service; report has
    status/correlation/notification fields; existing defaults, JSON and exit
    behavior remain compatible (AC-02/03).
  - Verify: recorded accepted/no-op/quarantine/multi-release fixtures, cleanup
    on failure, authority rejection; existing CLI/ECB/alert tests.
  - Files: `src/fred_pipeline/ecb_execution.py`, `src/fred_pipeline/cli.py`,
    `tests/test_ecb_execution.py`, `tests/test_cli_ecb_assets_default.py`.

- [ ] **T3 — Serialize every live PostgreSQL writer** (M; T2)
  - Acceptance: writer-session lock covers poll, ingest-release, main hook
    and live promotion before mutation; contention reports busy; lock loss
    halts writes and exit releases ownership (AC-04).
  - Verify: independent PostgreSQL sessions/processes contend, crash, reconnect
    and retry; no duplicate intervals or source calls by a contender.
  - Files: `io/ecb_writer_lock.py`, `ecb_execution.py`, `cli.py`,
    `ecb_archive_promotion.py` under `src/fred_pipeline/`,
    `tests/test_ecb_writer_lock.py`. First characterize warehouse initialization;
    split a T3a factory/session-lifecycle slice (factory, PostgreSQL adapter,
    lock helper and focused tests) if changes there are needed, then T3b entry
    point integration. Neither slice may exceed five primary files.

Checkpoint A: core compatibility, shared results and real PostgreSQL lock tests
pass. Do not proceed with a Dagster writer if a lock entry point is unprotected.

## Phase 2: Manual Dagster execution and health

- [ ] **T4 — Run one manual ECB poll through Dagster** (M; T1–T3)
  - Acceptance: definitions load without side effects; one asset/poll job uses
    configured resource; accepted commits materialize and no-op/busy observe;
    partial commits and post-commit event failure stay truthful (AC-01–05).
  - Verify: fixture job executions plus PostgreSQL CLI/Dagster parity against
    separate databases; inspect persisted events and closed resources.
  - Files: `orchestration/__init__.py`, `definitions.py`, `resources.py`,
    `ecb_jobs.py` under `src/fred_pipeline/`, `tests/test_dagster_ecb_jobs.py`.

- [ ] **T5 — Execute bounded ECB asset checks** (M; T4)
  - Acceptance: spec's seven checks and checks-only job use a consistent
    accepted frontier; ERROR fails execution, WARN remains policy-visible;
    publication freshness survives no-op days (AC-06/12).
  - Verify: every failing rule, no accepted release, starts-here/gapped coverage,
    source-no-op audit and concurrent accepted-frontier transition; no full
    security payloads in Dagster events.
  - Files: `orchestration/ecb_checks.py`, `ecb_jobs.py`, `definitions.py`,
    `tests/test_dagster_ecb_checks.py`, shared health query helper if necessary.

- [ ] **T6 — Mirror schedules with missed-tick recovery** (M; T5)
  - Acceptance: exact three stopped schedules in Europe/Berlin; stale queued
    runs skip explicitly; documented missed-tick recovery uses current poll
    and existing TARGET/grace rules (AC-07).
  - Verify: winter/summer UTC ticks, DST weeks, TARGET closure, weekend, grace,
    stale queue and daemon restart scenarios against pinned version.
  - Files: `orchestration/ecb_schedules.py`, `definitions.py`, `ecb_jobs.py`,
    `tests/test_dagster_ecb_schedules.py`, Dagster runbook.

- [ ] **T7 — Prove notification ownership and fallback** (M; T5)
  - Acceptance: normal failures send one domain email, initialization/crash
    failures use fallback, receipts persist sanitized; failed delivery cannot
    masquerade as proof or roll back accepted data (AC-08).
  - Verify: fake SMTP/Graph acceptance and failure, sensor reevaluation,
    failure before report, provider/event persistence crash window.
  - Files: `orchestration/ecb_notifications.py`, `definitions.py`,
    `ecb_execution.py`, `tests/test_dagster_ecb_notifications.py`,
    shared email helper only if needed; split transport refactor if larger.

Checkpoint B: manual Dagster UI shows truthful accepted/no-op/quarantine state,
checks and alert outcomes. All schedules/sensors remain stopped by default.

## Phase 3: Deployment proof

- [ ] **T8 — Add isolated orchestration CI coverage** (S; T4–T7)
  - Acceptance: Python 3.12 Dagster job and dedicated PostgreSQL tests run with
    pinned dependencies; core Python matrix still omits Dagster; no live API,
    production DSN or live email required (AC-01–08).
  - Verify: CI jobs execute (skips are reported), fixture parity and lock tests
    pass; existing core/Spark jobs retain behavior.
  - Files: `.github/workflows/ci.yml`, test fixtures/helper, test configuration
    only as needed.

- [ ] **T9 — Provide private Linux deployment templates** (M; T8)
  - Acceptance: example instance/workspace config and service definitions use
    separate metadata DB/role, protected environment, loopback UI, one daemon,
    bounded concurrency and versioned release (AC-09).
  - Verify: load configs with dummy secrets; `systemd-analyze verify`; document
    real install/upgrade/start/stop and stopped-automation restore commands.
  - Files: split into T9a instance/workspace/env examples (≤4 files), then T9b
    webserver/daemon/code-location services plus runbook (≤4 files), all under
    `resources/dagster/` and `docs/deployment/`.

- [ ] **T10 — Rehearse on isolated Linux staging** (M; T9)
  - Acceptance: restarts, worker crash, post-commit recovery, independent
    daemon-outage alert and metadata restore succeed with automation stopped;
    overhead/resource measurements meet spec (AC-09/11/12).
  - Verify: five warm runs per CLI/Dagster scenario; ledger/hash parity; RPO/RTO
    proof; monitor and transport evidence; recheck schedule states after restore.
  - Files: runbook, redacted validation record, handoff, independent watchdog
    script/service in separate small slice if the host lacks one.

Checkpoint C: actual host inputs, runtime compatibility, rollback commands,
monitor delivery and restore/performance evidence are recorded. No skipped
host gate is counted as complete.

- [ ] **T11 — Cut over production and record the operating-day proof** (M; T10)
  - Acceptance: manual production poll/verification SQL pass; old triggers are
    disabled/drained before Dagster activation; one complete scheduled cycle,
    new accepted release, real mailbox receipt and rollback drill pass (AC-10/11).
  - Verify: systemd/Dagster schedule state, main-run `--no-ecb-assets`, database
    frontier/count/hash/history/no-observation-write evidence and monitor route.
  - Files: Dagster runbook, Dagster handoff, ECB handoff, Spec 012 status,
    redacted production validation record. Requires actual production host access.

Checkpoint D: all AC-01–AC-12 evidence is linked. Mark first release complete
only after live gates pass. Deferred RSS/archive/macro modules remain deferred.
