# EIA Catalog

Source key: `eia`

Upstream: US Energy Information Administration API v2
(`https://api.eia.gov/v2`).

Authentication: `api_key` query param, **required** — EIA has no keyless
tier. `EIA_API_KEY` / `eia_api_key`. Any `eia_*` manifest active means a key
must be configured.

Series id convention: the raw EIA v2 series id, used directly as the
manifest `series_id` (e.g. `PET.RWTC.M`). The client hits the
`seriesid/{id}` compatibility route, which returns one series' full history
in the v2 envelope. Observations are dated by a `period` string whose
granularity follows the series frequency (`YYYY`, `YYYY-MM`, `YYYY-MM-DD`,
`YYYY-Qn`).

## Current Series (2 active)

| Series ID | Frequency | Category | Description |
|---|---:|---|---|
| `PET.RWTC.M` | m | energy | Cushing OK WTI Spot Price FOB (Monthly). |
| `ELEC.PRICE.US-ALL.M` | m | energy | Average Retail Price of Electricity, All Sectors, US (Monthly). |

Manifest: `manifests/eia_energy.yml`.

## Discovery

No `discover-eia` tool exists. EIA's own [API browser](https://www.eia.gov/opendata/browser/)
is the practical way to find `seriesid`-compatible series across its many
categories (petroleum, electricity, natural gas, coal, renewables,
international, ...) — this pipeline only mirrors two starter series today.

## Caveats

- Public domain (17 U.S.C. 105, U.S. federal government work) — no
  attribution or commercial-use restriction; see `config/data_licensing.yml`.
- No key means `eia_*` manifests can't run at all (unlike BLS/Census, which
  degrade to a lower quota keyless) — `validate` won't catch a missing key,
  only a live `run` will.
