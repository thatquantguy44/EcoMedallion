# Census Catalog

Source key: `census`

Upstream: US Census Bureau time-series datasets API
(`https://api.census.gov/data`).

Authentication: optional `key` — keyless works at a lower daily quota.
`CENSUS_API_KEY` / `census_api_key`.

Series id convention: Census datasets are queried with dataset-specific
predicates rather than a flat series list, so a manifest `series_id` encodes
the dataset path plus the predicate set that pins down one series, separated
by the first colon:

```text
timeseries/eits/marts:category_code=44X72,data_type_code=SM,seasonally_adj=yes
└──── dataset path ───┘ └──────────────── predicates (k=v,...) ─────────────┘
```

The response shape is also non-standard: a 2-D array (`[[header...],
[row...], ...]`) rather than an object — handled in
`fred_pipeline/sources/census.py`, invisible past normalization.

## Current Series (1 active)

| Series ID | Frequency | Category | Description |
|---|---:|---|---|
| `timeseries/eits/marts:category_code=44X72,data_type_code=SM,seasonally_adj=yes` | m | growth | Advance Retail Sales — Retail & Food Services (SA). |

Manifest: `manifests/census_indicators.yml`.

## Discovery

No `discover-census` tool exists. Census publishes many other Economic
Indicators Time Series (EITS) datasets under the same
`timeseries/eits/<program>` path with their own predicate vocabularies (e.g.
`bfs` business formation statistics, `hv` housing vacancies) — extending
coverage means reading the specific program's API documentation and
hand-writing the predicate set, not enumerating a code list the way ECB/BLS
discovery does.

## Caveats

- Public domain (17 U.S.C. 105, U.S. federal government work) — no
  attribution or commercial-use restriction; see `config/data_licensing.yml`.
- Predicate correctness is entirely on the manifest author — a wrong
  `category_code`/`data_type_code` combination fails at request time with no
  earlier validation, since there's no shared schema across EITS programs.
