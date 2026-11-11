# Implementation Plan: Spec 013 Dagster Orchestration

## Active additional work: Spec 017 performance reporting

The approved [performance design](../specs/spec017/README.md) is implemented on
`feature/pipeline-performance-reporting`. Build order: collector → audit/file
storage → instrumentation → complete CLI/library lifecycle → report visuals.
Use tests with fake clocks and transports; verify SQLite and PostgreSQL against
isolated test databases. Preserve the existing Dagster plan below.

Risks: overlapping workers (unique spans and copied context), extra I/O
(bounded queues flushed by the owner thread), failed telemetry (file fallback),
and incomplete alerts (defer combined reporting until ECB completes).

Status: T1 optional runtime verification complete; execution, writer locking,
and Dagster integration remain pending.

The current combined initiative follows [Spec 016](../specs/spec016/README.md),
which supersedes the earlier ECB-only branch and job scope.

This is the active implementation plan for [Spec 013](../specs/spec013/README.md).
Dagster was renumbered after the geography Spec 012 was added.
Tasks are tracked in [todo.md](todo.md); acceptance IDs refer to the spec.
This plan does not replace the remaining Spec 011 production-host proof.

**Next task: T2 shared ECB execution**, followed by T3 writer exclusion. T1
evidence is in the [Dagster runbook](../docs/deployment/dagster_runbook.md).

## Decisions

- Start with ECB forward polling on PostgreSQL and self-hosted Dagster OSS.
- Reuse one shared execution service from all entry points; retain core CLI
  compatibility and optional Dagster dependencies.
- Serialize all live ECB writers through a PostgreSQL session advisory lock on
  the writer connection, including
  CLI and historical promotion; Dagster queue limits provide additional control.
- Model the coupled accepted dataset as one logical asset initially. Record
  accepted releases as materializations and no-op polls as observations.
- Separate checks-only execution from polling and preserve calendar-aware
  health/SMTP/Graph policies, with a fallback failure sensor.
- Use private systemd-managed webserver/daemon/code-location services and a
  separate PostgreSQL metadata database. Ship all automation stopped.

## Dependency graph and delivery slices

```text
T1 dependency compatibility ───────────┐
T2 shared execution → T3 writer lock ─┴→ T4 manual Dagster poll
                                         ↓
                               T5 health checks → T6 schedules
                                         ↓             ↓
                                    T7 notifications ──┘
                                         ↓
                                  T8 CI → T9 deployment
                                         ↓
                                  T10 staging rehearsal
                                         ↓
                                  T11 production cutover
```

T1 and T2 are independently investigable; shared source-file changes and all
database/deployment operations are sequential. No parallel agents are required.
Each task should touch at most five primary files; split a task rather than
silently expanding it. Scope estimates in the checklist are review aids, not
delivery-time promises.

Checkpoint A (T1–T3): package compatibility, CLI characterization and independent
session/process lock proof pass before adding the Dagster write consumer.

Checkpoint B (T4–T7): accepted/no-op/quarantine/partial-commit/crash scenarios
are truthful in the UI; checks, DST/holiday schedules and failure delivery are
proven with fixtures. All automation remains stopped.

Checkpoint C (T8–T10): isolated CI and Linux staging prove process restarts,
metadata backup/restore, independent outage detection and bounded overhead.
Record actual deployment inputs and commands; unresolved host access is not a
reason to claim deployment complete.

Checkpoint D (T11): manual production proof, scheduler handover, full weekday
cycle, new release, real mailbox receipt and rollback evidence satisfy every
AC-01–AC-12 gate. Completion is evidence-based, not inferred from a green CI run.

## Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Dagster installed on a different Python than the pipeline supports | Separate Python 3.12 orchestration runtime; exact constraints; core-only CI |
| Two schedulers or main-run hook write concurrently | Cross-process lock first, disable/drain old triggers, `--no-ecb-assets` in other schedules |
| Empty poll masks missing release | Check after every poll and provide independent checks-only job |
| Email or event failure after database commit | Preserve committed releases; report receipt/event limits and recover from ledger |
| Daemon outage defeats Dagster alerts | Independent service/poll watchdog and live outage drill |
| Restored metadata revives old schedules | Restore with automation stopped and inspect actual persisted states |
| Historical partitions corrupt chronological SCD2 | Defer archive automation; require ordered rebuild and exclusive promotion |
| Spec scope expands into all macro sources | Separate later-module addenda with acceptance and activation decisions |

## Next action

Complete T2 shared execution and T3 writer exclusion before adding a Dagster
writer. T1 clean installation and core-only verification have passed.
No host credentials or running production scheduler are needed for these slices.
Production hostname, access, protected paths, monitor route, capacity and
backup target must be resolved before T9–T11 production execution.
