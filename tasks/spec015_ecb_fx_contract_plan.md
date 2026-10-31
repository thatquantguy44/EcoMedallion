# Implementation Plan: Spec 015 ECB FX contract

Status: In progress, 2026-10-30. Tasks are recorded in
[`spec015_ecb_fx_contract_todo.md`](spec015_ecb_fx_contract_todo.md) so the
unrelated Spec 013 plan and checklist remain authoritative for Dagster work.

## Delivery order

```text
ECB schema + normalisation contract
             |
             v
append-only history + replay capture time
             |
             v
captured-as-of selection
             |
             v
Decimal FX Gold native / inverse / cross output
```

The temporal ledger comes first because both captured-as-of selection and FX
lineage depend on an immutable capture identity. Exact values/metadata are
implemented with that base; FX Gold is last and is source-owned rather than a
change to the generic forward-filled macro matrix.

## Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Existing Silver natural key is compact/mutable | Add a separate append-only history relation; retain compact table/API compatibility. |
| Legacy data lacks full provider timestamps/metadata | Treat fields as nullable and document a separately authorised backfill. |
| Capture time ties | Persist a deterministic event identity that includes run/content information. |
| Float output loses economic precision | Use Decimal for FX-specific transforms; retain existing floats only for legacy consumers. |
| Date alignment accidentally fills a missing FX leg | Require exact same observation date and emit a quality reason instead. |

## Verification checkpoints

After the temporal slice: run the ECB, replay, local-store and feature tests.
After FX Gold: run the focused suite plus PostgreSQL parity tests when the
configured local test service is available. Complete with repository lint,
format and diff checks. No live data fetch, migration or schedule activation
is part of these checkpoints.
