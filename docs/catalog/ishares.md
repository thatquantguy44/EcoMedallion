# iShares Catalog

Source key: `ishares`

Upstream: iShares / State Street daily holdings CSV (per-fund, keyless).
Index *membership* is licensed data with no good free API; the daily holdings
CSV each provider publishes is the free, defensible workaround this client
uses.

Authentication: keyless.

Series id convention: a manifest `series_id` is the ETF ticker (e.g. `IVV`,
`SPY`); the download URL is resolved from `HOLDINGS_URLS` in
`fred_pipeline/sources/ishares.py`. One fetch explodes into one scalar Silver
series **per constituent** — `series_id = <ETF>:<constituent>`, `value =
weight %`, `observation_date` = the file's own "as of" date — so
membership/weight history accumulates through the normal incremental path
with no schema change. The holdings CSV has a preamble; the parser locates
the "as of" date and header row tolerantly rather than assuming fixed
offsets.

⚠️ `HOLDINGS_URLS` entries are product-specific and change — verify each
against the fund's own page before activating a new ticker.

## Current Series (1 active)

| Series ID | Frequency | Category | Description |
|---|---:|---|---|
| `IVV` | d | equity | iShares Core S&P 500 ETF — holdings. |

Manifest: `manifests/etf_holdings.yml`. This is also the seed for the equity
symbol universe (`build_equity_manifest`) — `gold.index_constituents` and the
Tiingo/Stooq active-ticker lists trace back to whichever ETFs are active
here.

## Discovery

No `discover-ishares` tool exists. Growing coverage means finding another
fund's public holdings CSV URL (iShares and State Street both publish these
per-product) and adding it to `HOLDINGS_URLS` plus a manifest row — see the
README's "Grow ETF constituent coverage" workflow for the follow-up sequence
(`price-constituents` → `gold`).

## Caveats

- `redistribution_allowed: false`, `commercial_use_allowed: false` in
  `config/data_licensing.yml` — no published terms cover reuse of the daily
  holdings file for anything beyond this kind of internal/defensible use;
  conservative defaults until someone reviews the actual per-provider terms.
- Membership/weight data only — no price. Constituent prices come from
  `price-constituents` (Tiingo), a separate pull keyed off whatever this
  source's holdings currently list.
