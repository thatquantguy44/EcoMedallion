# Source Catalog

This folder is the human-readable catalog companion to `manifests/*.yml` and
`config/series_catalog.yml`.

- `manifests/*.yml` decides what the pipeline can ingest and which series are
  active.
- `config/series_catalog.yml` decides which active series get terminal/dashboard
  presentation semantics.
- The files here explain source-level coverage, current curated series, and
  candidate areas to explore.

## Sources

| Source | Catalog page | Status |
|---|---|---|
| BEA | [bea.md](bea.md) | populated |
| BIS | [bis.md](bis.md) | populated |
| BLS | [bls.md](bls.md) | populated |
| Census | [census.md](census.md) | populated |
| ECB | [ecb.md](ecb.md) | populated |
| ECB candidates | [ecb_candidate_flows.md](ecb_candidate_flows.md) | discovery backlog |
| EIA | [eia.md](eia.md) | populated |
| FRED | [fred.md](fred.md) | populated starter (too large for full per-series enumeration — see manifests/*.yml) |
| iShares | [ishares.md](ishares.md) | populated |
| SEC | [sec.md](sec.md) | populated |
| Stooq | [stooq.md](stooq.md) | populated |
| Tiingo | [tiingo.md](tiingo.md) | populated |
| Treasury | [treasury.md](treasury.md) | populated |
| World Bank | [worldbank.md](worldbank.md) | populated |

## Next Catalog Work

- Every non-FRED source page now documents: upstream API + series id
  convention, auth requirements, current active coverage (full enumeration
  for small sources, grouped summary for large ones — Tiingo/Stooq/BIS/World
  Bank), whether a `discover-*` tool exists, and licensing caveats from
  `config/data_licensing.yml`. Last verified 2026-08-25 against
  `manifests/*.yml` and live source client code.
- Keep ECB discovery notes in sync with `specs/spec002`.
- Keep active-series counts synchronized with
  `PYTHONPATH=src python -m fred_pipeline validate --manifests manifests` —
  each page's counts will drift as manifests change; re-verify against the
  actual YAML rather than trusting the doc, the same way these were written.
  **This is enforced, not just a reminder**: `tests/test_catalog_docs.py`
  runs in CI on every push/PR and fails if a page's `## Current Series (N
  active)` / `## Current Coverage (N active ...)` heading count doesn't
  match the real manifest count, or if an active source has no catalog page
  at all. Keep that heading format when editing a page's count, or the check
  can't find it.
