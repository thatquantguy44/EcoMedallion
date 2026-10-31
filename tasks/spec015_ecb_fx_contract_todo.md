# Spec 015 Task Checklist

- [x] **T1 — Characterise and persist observed ECB states**
  - Acceptance: A -> B -> A, same-day valid times and delete actions are
    retained in append-only history while compact Silver remains compatible.
  - Verify: offline ECB and local-store tests.
  - Files: ECB normaliser, Silver model, local/PostgreSQL/Delta storage, tests.

- [x] **T2 — Enforce captured-as-of and replay capture time**
  - Acceptance: as-of rejects future capture/observation data; replay uses
    Bronze capture time; provider-vintage views remain distinct.
  - Verify: feature/replay tests and supported-backend parity tests.
  - Depends on: T1.

- [x] **T3 — Preserve FX Decimal values and metadata**
  - Acceptance: exact value, orientation/scale/status and provider timing are
    retained and semantic-only changes are observable.
  - Verify: ECB/transform fixtures.
  - Depends on: T1.

- [x] **T4 — Build canonical ECB FX Gold rates**
  - Acceptance: nine-currency native, inverse and same-date cross rates have
    Decimal calculation, lineage, policy version and no forward fill.
  - Verify: dedicated FX feature/warehouse tests.
  - Depends on: T1–T3.

- [x] **Checkpoint — Verify and document**
  - Acceptance: focused/full applicable tests, lint/format and migration
    documentation pass; no live warehouse mutation.
  - Depends on: T1–T4.
  - Completed: focused offline/PostgreSQL regression suite and targeted Ruff
    lint/format pass. No live warehouse was opened or migrated.
