# BEA Catalog

Source key: `bea`

Upstream: Bureau of Economic Analysis API (single `/data` endpoint).

Authentication: `UserID` query param, **required**. `BEA_API_KEY` /
`bea_api_key`. Any `bea_*` manifest active means a key must be configured.

Series id convention: a manifest `series_id` encodes the table coordinates
that pin down one line series:

```text
NIPA:T10101:1:Q
│    │      │ └ frequency (A / Q / M)
│    │      └── LineNumber within the table
│    └───────── TableName
└────────────── datasetname
```

Errors are carried in a `BEAAPI.Error` block, often on HTTP 200 rather than a
4xx/5xx status — surfaced via `_error_detail` in
`fred_pipeline/sources/bea.py`, invisible past normalization.

## Current Series (23 active)

| Manifest | Active series | What |
|---|---:|---|
| `manifests/bea_national_accounts.yml` | 2 | NIPA Table T10101 lines (headline GDP components) |
| `manifests/bea_pce_items.yml` | 21 | NIPA Table T20404 lines — PCE price index item-level detail feeding `config/inflation_items.yml`'s PCE waterfall (goods/services/durables/nondurables sub-items) |

`config/inflation_items.yml` documents which `NIPA:T20404:*` lines map to
which PCE contribution-waterfall category and their approximate weights (see
that file's own header caveat about refreshing weights from BEA Section 2
Underlying Detail).

## Discovery

No `discover-bea` tool exists. BEA's table/line structure is documented per
dataset at [apps.bea.gov/API](https://apps.bea.gov/api/signup/) — extending
coverage means picking a `TableName`/`LineNumber` from BEA's own table
documentation (e.g. the NIPA Table 2.4.4 reference linked in
`config/inflation_items.yml`) and hand-writing the manifest row; there's no
enumerable code list to discover against the way ECB/BLS have.

## Caveats

- Public domain (17 U.S.C. 105, U.S. federal government work) — no
  attribution or commercial-use restriction; see `config/data_licensing.yml`.
- BEA's API has had live reliability issues independent of this pipeline —
  the `NIPA:T20404:*` PCE item series returned `Error retrieving NIPA data.`
  from BEA itself for every active line during a routine refresh on
  2026-08-21/22 while every other source succeeded; not a pipeline bug, worth
  a retry rather than investigation when it recurs.
