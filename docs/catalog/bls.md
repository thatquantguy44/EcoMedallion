# BLS Catalog

Source key: `bls`

Upstream: two separate BLS surfaces, not one API:

- `https://api.bls.gov/publicAPI/v2` — the JSON API used for observations
  (`fred_pipeline.sources.bls.BLSClient`) and for `/surveys` (the coarse
  survey abbreviation/name list). No series-search or metadata-enumeration
  endpoint exists here.
- `https://download.bls.gov/pub/time.series/<survey>/<survey>.series` — a
  tab-delimited flat file per survey; the only place BLS publishes the full
  list of series that actually exist. Used only by `discover-bls`
  (`fred_pipeline.catalogs.bls_discovery`), never by ingestion.

Authentication: keyless works at a lower daily quota; a `bls_api_key` (BLS
registration key) raises it. The flat-file server separately requires a
User-Agent that looks like real contact info (an `@`-containing string) or it
returns HTTP 403 regardless of content — live-verified. `bls_user_agent`
defaults to a placeholder that satisfies this; set `BLS_USER_AGENT` to your
own contact for polite real use.

Series id convention: raw BLS series IDs, unprefixed (e.g. `CES0000000001`,
`CUSR0000SA0`) — unlike ECB's `SOURCE:flow:key` convention, BLS series IDs are
already globally unique on their own.

## Current Coverage (79 active series)

| Manifest | Active series | What |
|---|---:|---|
| `manifests/bls_cpi_basket.yml` | 30 | CPI-U, NSA item hierarchy (headline, core, major groups, sub-strata) |
| `manifests/bls_cpi_basket_sa.yml` | 29 | CPI-U, SA mirror of the same item hierarchy |
| `manifests/bls_productivity.yml` | 7 | Productivity and cost indexes (2017=100), for business, nonfarm business, and manufacturing |
| `manifests/bls_jt_candidates.yml` | 5 | JOLTS national headline levels — job openings, hires, quits, layoffs & discharges, total separations |
| `manifests/bls_ppi.yml` | 3 | Final-demand construction and intermediate-demand goods/services |
| `manifests/bls_wp_candidates.yml` | 1 | Final-demand services (`WPSFD42`) |
| `manifests/bls_ce_candidates.yml` | 3 | Nonfarm + total-private payrolls, average hourly earnings |
| `manifests/bls_labor.yml` | 1 | `LNS14000000` — unemployment rate (SA), kept as a deliberate cross-source validation pair against FRED's `UNRATE` |

The CPI item trees feed `config/inflation_items.yml` (weights + waterfall
decomposition for `gold.inflation_explorer`/`gold.inflation_contribution`) —
see that file's header comment for the weight-refresh caveat.

On 2026-11-10, live provider metadata corrected the productivity definitions to
duration `3` (index levels) and manufacturing sector `3000`. The previous
duration `2` codes returned growth rates despite being labeled as indexes;
sector `8800` identified nonfinancial corporations. Final-demand services and
construction now use the verified `WPSFD42` and `WPSFD43` keys.

## Discovery Tooling

`discover-bls` (added alongside this doc) mirrors `discover-ecb`'s workflow
but has to work differently: BLS flat-file rows already name real, currently
published series, so candidate generation is filtering + capping, not
combinatorial expansion. Column layout differs per survey (CPI dimensions by
area/item code; Employment by industry/data-type code; JOLTS has no
`series_title` column at all — its BLS-published code-definition files
`jt.dataelement`/`jt.industry`/`jt.area`/`jt.sizeclass` are the real source of
truth for what its codes mean, live-verified). Workflow:

```bash
python -m fred_pipeline discover-bls --list-surveys
python -m fred_pipeline discover-bls --survey CE --inspect
python -m fred_pipeline discover-bls --survey CE \
  --column seasonal=S --search "total nonfarm" \
  --frequency m --dry-run
```

## Generated Candidate Manifests

First real output, live-verified through `BLSClient.get_observations` during
generation (not just matched in the flat-file catalog):

- `manifests/bls_ce_candidates.yml` — nonfarm + total-private payrolls,
  average hourly earnings (production & nonsupervisory, total private)
- `manifests/bls_jt_candidates.yml` — JOLTS national headline levels (hires,
  job openings, layoffs & discharges, quits, total separations)
- `manifests/bls_wp_candidates.yml` — final-demand services is active;
  headline, core, and goods remain inactive.

## Caveats

- `restate_last_n` (default 90) is a count of trailing observations, not
  calendar days or a frequency-aware window — for a monthly BLS series that's
  ~7.5 years of trailing history re-pulled every incremental run. Harmless
  (idempotent MERGE) but worth knowing.
- BLS is U.S. federal government work, public domain by statute — exempt from
  the redistribution-review governance check that flags sources like `ecb`
  and `bis` (see `config/data_licensing.yml`).
