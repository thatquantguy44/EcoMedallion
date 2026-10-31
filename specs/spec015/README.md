# Spec 015: ECB FX temporal contract and Gold rates

Status: **In progress.** Created 2026-10-30.

## Objective

Harden the existing ECB exchange-rate ingestion owned by EcoMedallion. The
existing ECB series remain the source of record; this specification does not
add a duplicate connector or activate a schedule. It makes a captured-as-of
reconstruction possible, keeps ECB FX values and semantics lossless, and
publishes a canonical FX Gold relation for the nine Spec 004 currencies:
USD, GBP, JPY, CHF, CAD, AUD, NZD, NOK and SEK.

## Scope and contract

### Temporal history

* Keep the current compact `silver.fred_observation` relation for existing
  consumers, but append every changed observed state to an immutable
  observation-history relation. An A -> B -> A sequence under one provider
  vintage key therefore has three observable events instead of one overwritten
  row.
* The history identity includes the source natural key, capture timestamp,
  run identifier and content hash. Capture ordering is deterministic even if
  two source events arrive in the same clock tick.
* Persist full ECB `VALID_FROM` / `VALID_TO` timestamps separately from the
  legacy date fields. A same-day source update must not collide merely because
  its date portion matches another update.
* Model ECB `ACTION=Delete` as an observed tombstone. It must remove the
  affected version from current/as-of analytical output without destroying its
  history.
* Captured-as-of selects only observations captured at or before the requested
  instant and whose observation date is not in the future. Provider-vintage
  dates remain a separate eligibility condition, not a substitute for capture
  time.
* Bronze replay passes each archived Bronze row's original `ingested_at` into
  normalisation. A replay reconstructs capture ordering instead of assigning
  the current clock time.

### Exact ECB semantics

* ECB records retain the provider lexical `raw_value` and a canonical
  `exact_value` Decimal string. Legacy float `value` stays populated for
  unchanged generic macro consumers; it is not authoritative for FX math.
* ECB uses `detail=full`, as its API documents that `dataonly` excludes
  attributes. For `ECB:EXR` records the normaliser records and checks the EUR
  base, quoted currency, unit multiplier, observation status, action and
  provider validity timestamps. A conflicting supplied currency or a non-zero
  multiplier is rejected; absent optional attributes remain explicit nulls.
* The row hash covers the retained FX semantics, so a status/action/scale
  change is observable even when the numeric text is unchanged.

### FX Gold

* Build `gold.ecb_fx_reference_rate` from the nine existing daily ECB EXR
  series. It emits native `QUOTE per EUR`, inverse `EUR per QUOTE`, and the
  configured same-date cross rates. It never forward fills, uses Decimal
  arithmetic, rejects zero denominators, and records a versioned rounding
  policy.
* Gold rows carry base/quote currency, orientation, native input series ids,
  input capture identities, source observation date, availability state and
  a null-quality reason. They are absent when a required native observation is
  unavailable; there is no silent calendar fill.

## Commands

```bash
.venv/bin/python -m pytest tests/test_ecb_client.py tests/test_transform.py \
  tests/test_features.py tests/test_replay.py tests/test_local_store.py
.venv/bin/ruff check src tests
.venv/bin/ruff format --check src tests
```

PostgreSQL parity tests run only against their explicitly configured test
service; this implementation does not run a migration against `macro_medallion`.

## Project structure

* `src/fred_pipeline/sources/ecb.py` — retrieval and ECB normalisation.
* `src/fred_pipeline/data/transform.py` / `data/silver.py` — generic Silver
  shape and Delta history write path.
* `src/fred_pipeline/io/local_store.py` / `io/postgres_store.py` — local and
  PostgreSQL history storage and Gold persistence.
* `src/fred_pipeline/writer/features.py` — captured-as-of and pure Decimal FX
  feature computation.
* `tests/` — offline fixtures for revisions, tombstones, exact values, replay
  and native/inverse/cross rates.

## Testing strategy

All acceptance cases use synthetic ECB CSV/JSON fixtures and SQLite by
default. The cases include A -> B -> A observations, same-day provider
timestamps, deletion, a future-captured row queried at an earlier cutoff,
high-precision Decimal input, metadata-only changes, replay preservation,
same-date cross rates and mismatched-date non-emission. PostgreSQL parity is
added where the repository test service is available; no provider request is
made in tests.

## Boundaries

* Always preserve Bronze payloads, identifiers, provider times and observed
  versions; run focused tests before expanding scope.
* Ask first before a live database migration, backfill, schedule activation,
  dependency addition, or changing unrelated generic Gold semantics.
* Never mutate the existing `macro_medallion` database, fetch live ECB data as
  part of tests, or remove legacy macro tables/columns in this change.

## Success criteria

1. Every supported backend retains and can query observed ECB correction and
   withdrawal events without changing generic compact-Silver consumers.
2. Captured-as-of never selects a future-captured or future-observation row;
   provider-vintage output remains separately available.
3. ECB EXR inputs retain Decimal precision and validated semantic metadata.
4. The FX Gold output has correct native, inverse and same-date cross rates,
   explicit lineage/rounding/quality fields, and no forward fill.
5. Offline focused tests, lint and formatting pass; operational docs explain
   the additive migration and deferred live backfill.

## Sources

* ECB data API: <https://data.ecb.europa.eu/help/api/data>
* ECB exchange-rate methodology: <https://data.ecb.europa.eu/methodology/exchange-rates>
