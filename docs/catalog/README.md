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
| BEA | [bea.md](bea.md) | placeholder |
| BIS | [bis.md](bis.md) | placeholder |
| BLS | [bls.md](bls.md) | placeholder |
| Census | [census.md](census.md) | placeholder |
| ECB | [ecb.md](ecb.md) | populated |
| EIA | [eia.md](eia.md) | placeholder |
| FRED | [fred.md](fred.md) | populated starter |
| iShares | [ishares.md](ishares.md) | placeholder |
| SEC | [sec.md](sec.md) | placeholder |
| Stooq | [stooq.md](stooq.md) | placeholder |
| Tiingo | [tiingo.md](tiingo.md) | placeholder |
| Treasury | [treasury.md](treasury.md) | placeholder |
| World Bank | [worldbank.md](worldbank.md) | placeholder |

## Next Catalog Work

- Fill each placeholder with source purpose, active manifests, inactive or
  candidate coverage, licensing notes, and known caveats.
- Keep ECB discovery notes in sync with `specs/spec002`.
- Keep active-series counts synchronized with
  `PYTHONPATH=src python -m fred_pipeline validate --manifests manifests`.
