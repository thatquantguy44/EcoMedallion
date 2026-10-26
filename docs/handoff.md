# Active Work Pickup

Last verified: 2026-10-25 (documentation and repository state only).

**Continue working on the existing branch `spec012-dagster-orchestration`**,
tracking `origin/spec012-dagster-orchestration`. Keep subsequent Spec 013
implementation commits on this branch. The active work is
[Spec 013: Dagster Orchestration](../specs/spec013/README.md).

Dagster was renumbered to Spec 013 after syncing main's
[Spec 012: Series Geography Metadata and Reviewed Country Map](../specs/spec012/README.md).
The branch name stays `spec012-dagster-orchestration`.

**Next task: complete T1 — optional Dagster runtime installation and
verification.** Start at the [canonical Dagster handoff](handoffs/dagster_orchestration.md#next-task-t1).
It records the candidate versions, what was actually verified, remaining
files/checks, and when to advance to T2. The [plan](../tasks/plan.md) and
[checklist](../tasks/todo.md) define the subsequent order.

The specification and README bootstrap commands are committed at `72703d5`;
runtime installation and integration remain unproven. Check Git status and
the current head before continuing; do not infer completion from documented commands.

For the existing ECB pipeline's separate production-host proof, use the
[Spec 011 handoff](handoffs/ecb_eligible_assets.md). Spec 013 T1 does not need
production access and must not activate a scheduler or perform a live poll.
