# MarketIndicators opportunities for EcoMedallion

## Scope and evidence

This inventory compares EcoMedallion `main` at `6c78771a7d5c935b7116b7df936105a1f4bd1309` with the MarketIndicators default branch (`claude/market-data-pipeline-gQNmH`) at `7268c476c1e80fc04c21fd9daeffc35be99b790b`. It is a research backlog, not an ingestion change.

I compared the 119 entries in MarketIndicators' `src/marketindicators/resources/default.yaml` universe against every `series_id` in EcoMedallion's `manifests/*.yml`. The exact-ID overlap is 60 of 81 FRED entries and 12 of 18 yfinance entries. All 20 `sample` aliases are absent as raw manifest IDs, as expected for derived or publisher-supplied series. Exact-ID absence does **not** establish absence of an equivalent signal. Existing Gold configuration, equity manifests, and constituent-pricing support were checked separately. Manifest presence also does not establish current warehouse coverage or source health.

MarketIndicators' `scripts/generate_samples.py` explicitly creates deterministic synthetic data, including securities-finance, sentiment, breadth, and valuation aliases. Its `default.yaml` says the `sample` provider expects precomputed files. Do not migrate those CSVs as observations. The legacy `docs/indicator_frequencies.md` still describes 35 indicators, while its README describes 112; the default configuration and source modules were used for this inventory.

## Exact raw-series gaps

The following 21 FRED-designated IDs appear in the legacy universe but in no EcoMedallion manifest. They are **candidates**, not 21 confirmed usable FRED series: several legacy identifiers or labels are wrong. See [the feasibility review](marketindicators_feasibility.md) for corrections, source checks, and ranking.

| Theme | Missing FRED IDs | Suggested first use |
| --- | --- | --- |
| Money and funding | `MTSDS133FMS`, `M2V`, `COMPOUT` | Monthly federal balance, velocity, wholesale funding growth |
| Rates and inflation | `DFII5`, `T30YIE`, `MICH`, `CPIHOSSL`, `PCETRIM12M656SFRBDAL`, `CPIFOOD` | Real-rate and inflation breadth; confirm the food CPI identifier |
| Labor and recession | `SAHMREALTIME`, `FRBATLWGT12MMM`, `RECPROUSM156N` | Wage and recession context; verify the published definition and lag |
| Bank and household credit | `DRBLACBN`, `DRCCLACBS`, `DRSFRMACBS`, `DRAACBS`, `TDSP` | Delinquency and debt-service cycle |
| Federal debt and fiscal | `FDHBFRBN`, `FYFSGDA188S`, `GFDEGDQ188S` | Federal Reserve Bank Treasury holdings, deficit and debt ratios |
| Housing | `CSUSHPISA` | National home-price growth |

The six yfinance symbols without an exact EcoMedallion manifest ID are `IEI`, `LUMBER=F`, `HG=F`, `^VIX`, `^VIX3M`, and `BTC-USD`. EcoMedallion already ingests FRED `VIXCLS`, so spot VIX has an alternate path. The futures and crypto symbols need source and continuity review. FRED hosts the three-month VIX equivalent `VXVCLS`, absent from EcoMedallion's manifests. `IEI` is a relatively simple ETF candidate through the existing Tiingo path; check its use against existing `IEF` before adding it.

## Feature engineering opportunities

| Priority | Signal and recipe | Existing inputs / gap | Validation before promotion |
| --- | --- | --- | --- |
| 1 | Repo funding pressure: `SOFR - IORB` in basis points | Both in `manifests/rates.yml` and `manifests/fed_funding.yml`; add a derived feature through `config/spreads.yml` or `config/cross_series.yml` | Same-date observations, percent-to-basis-point conversion, holiday gaps |
| 1 | Primary mortgage spread proxy: `MORTGAGE30US - DGS10` in percentage points | Both in `manifests/rates.yml`; align weekly mortgage print to an available 10-year yield | Publication timing and the distinction from actual agency MBS OAS |
| 1 | Equal-weight participation: trailing total return of `RSP` minus `SPY` | Both ETFs in `manifests/equity_tiingo.yml` | Use adjusted or total returns consistently, not raw price levels |
| 2 | Credit deterioration panel: level and trailing change for C&I, credit-card, mortgage, and auto delinquency rates | Add four `DR*` series above | Quarterly publication lags, revisions, and no daily forward-fill masquerading as new data |
| 2 | Market breadth: share of current constituents above trailing 200-session average, plus net 52-week highs | EcoMedallion has `src/fred_pipeline/constituent_pricing.py`; the legacy aliases `PCT_ABOVE_200DMA` and `NET_NEW_HIGHS` are supplied as samples | Point-in-time membership, delistings, full warm-up, adjusted prices, coverage threshold |
| 2 | Volatility risk premium: `(VIXCLS/100)^2 - 252 * trailing_mean(daily SPY log_return^2)` | `VIXCLS` and `SPY` already have source paths | Align 30-calendar-day VIX horizon with realized horizon; decide annualization and missing-day policy |
| 3 | Survey and positioning stress: AAII bull-bear, CBOE put/call and SKEW, CFTC COT, FINRA short interest, SEC fails to deliver | Legacy `AAII_SPREAD`, `PUT_CALL_RATIO`, `SKEW`, `COT_SP500_NETLONG`, `SHORT_INTEREST`, `FAILS_TO_DELIVER` are `sample` inputs | Build licensed, documented source feeds; define aggregation denominator and release timestamps |

For the first two spreads, `config/spreads.yml` currently supports same-frequency subtraction or ratio. Mortgage rate is weekly and Treasury yield daily, so `config/cross_series.yml` is the more natural starting point after checking its as-of behavior. Neither feature should be formed by subtracting unaligned raw rows. Relative return and volatility signals require a return or window transform beyond the existing spread operators.

MarketIndicators' `vix_implied_realized.py` contains a forward 21-bar SPY return, explicitly unobservable for the latest 21 bars. That column is useful as a **label** in a point-in-time backtest, never as a contemporaneous predictor. Its current-bar trailing realized variance is a separate, usable feature. The legacy boom-bust barometer uses copper futures divided by smoothed jobless claims; treat its copper proxy and cross-frequency alignment as research choices, not a ready-made production series.

## Recommended sequence

1. Add source metadata checks for the 21 FRED candidates, reconcile alternate definitions, and rank by current availability. Start with the verified delinquency/debt-service IDs, `M2V`, `DFII5`, and FRED's `VXVCLS`. Resolve the legacy `T30YIE` ID before considering a 30-year breakeven.
2. Prototype the three signals from existing inputs: SOFR–IORB, mortgage–Treasury, and RSP–SPY. Inspect actual warehouse coverage and publication timing first.
3. Prototype constituent breadth using point-in-time membership and adjusted price history. Reject a breadth history built from today's constituent list.
4. Treat publisher-driven securities-finance and sentiment series as separate ingestion projects. The legacy synthetic samples provide formulas and interface examples, not reusable source data.

This branch records the comparison and decision points. No new series has been activated, no legacy sample data has been copied, and no signal has been claimed as validated.
