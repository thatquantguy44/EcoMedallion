# FRED Catalog

Source key: `fred`

Upstream: Federal Reserve Economic Data API.

Authentication: `FRED_API_KEY` is required for pipeline pulls.

Current active FRED series: 2,580

FRED is the broadest source in this project. The manifests ingest a large
research universe, while `config/series_catalog.yml` selects a smaller set of
series for terminal-facing Gold dimensions and dashboard aggregates.

## MarketIndicators additions

| Order | Series ID | Manifest | Gold category | Description |
|---|---|---|---|---|
| 1 | `VXVCLS` | `market_indices.yml` | CREDIT | Cboe three-month VIX; pair with `VIXCLS` on matching observation dates for a volatility term-structure feature. Cboe reuse rights need review before redistribution. |
| 2 | `COMPOUT` | `money_banking.yml` | MONEY | Weekly commercial paper outstanding in billions of dollars; use growth rather than interpreting the stock as a spread. |
| 3 | `DFII5` | `rates.yml` | RATES | Daily five-year TIPS real yield; complements `DFII10` for a real-yield curve slope. |
| 4 | `DRBLACBN` | `money_banking.yml` | CREDIT | Quarterly business-loan delinquency rate, not seasonally adjusted; preserve release and revision timing in research features. |
| 5 | `DRCCLACBS` | `money_banking.yml` | CREDIT | Quarterly credit-card delinquency rate, seasonally adjusted; preserve release and revision timing in research features. |
| 6 | `DRSFRMACBS` | `money_banking.yml` | CREDIT | Quarterly single-family mortgage delinquency rate, seasonally adjusted; preserve release and revision timing. |
| 7 | `MICH` | `inflation.yml` | INFLATION | Monthly University of Michigan survey inflation expectation; confirm survey reuse rights before redistribution. |
| 8 | `SAHMREALTIME` | `macro_flags.yml` | GROWTH | Monthly real-time Sahm recession indicator in percentage points; use its release vintage, not observation month, in backtests. |
| 9 | `T30YIEM` | `inflation.yml` | INFLATION | Monthly 30-year breakeven inflation rate; corrects the legacy `T30YIE` identifier. |

## Recently Added To Terminal Catalog

These active FRED series were present in manifests and local base Gold latest
observations, but were missing terminal presentation semantics.

| Series ID | Manifest | Gold category | Description |
|---|---|---|---|
| `STICKCPIM159SFRBATL` | `prices_extra.yml` | INFLATION | Atlanta Fed sticky headline CPI, year-over-year rate. |
| `GDP` | `growth.yml` | GROWTH | Nominal GDP, complementing the existing real GDP catalog row. |
| `DCOILWTICO` | `market_indices.yml` | ACTIVITY | WTI crude oil spot price, exposed as a neutral macro/commodity activity signal. |
| `DGS3` | `rates.yml` | RATES | 3-year Treasury constant maturity rate. |
| `DFII10` | `rates.yml` | RATES | 10-year TIPS real yield. |
| `MORTGAGE15US` | `rates.yml` | RATES | 15-year fixed mortgage rate. |
| `DTB4WK` | `rates.yml` | RATES | 4-week Treasury bill discount rate. |
| `DTB3` | `rates.yml` | RATES | 3-month Treasury bill discount rate. |
| `DTB6` | `rates.yml` | RATES | 6-month Treasury bill discount rate. |
| `DFF` | `fed_funding.yml` | RATES | Daily effective federal funds rate. |
| `SOFR30DAYAVG` | `fed_funding.yml` | RATES | 30-day average SOFR. |
| `TGCRRATE` | `fed_funding.yml` | RATES | Tri-party general collateral rate. |
| `RRPONTSYAWARD` | `fed_funding.yml` | RATES | ON RRP award rate. |
| `DCPF3M` | `fed_funding.yml` | RATES | 90-day AA financial commercial paper rate. |
| `BAMLH0A1HYBB` | `ice_credit.yml` | CREDIT | BB high-yield corporate option-adjusted spread. |
| `BAMLH0A2HYB` | `ice_credit.yml` | CREDIT | B high-yield corporate option-adjusted spread. |

## Sticky CPI Notes

- `STICKCPIM159SFRB` is not a valid FRED series id and is not declared in any
  manifest.
- `STICKCPIM159SFRBATL` is the valid Atlanta Fed headline sticky CPI YoY series.
- The core YoY companion, `CORESTICKM159SFRBATL`, was already cataloged.
- Other active Sticky CPI transforms remain ingestion-only until a dashboard or
  model explicitly needs them.

## Remaining Catalog Work

- Add richer per-manifest coverage notes for the broad FRED universe.
- Decide whether the full BLS CPI baskets, international CPI families, and
  regional aggregate series should get separate dashboard surfaces rather than
  being folded into the national macro dashboard.
