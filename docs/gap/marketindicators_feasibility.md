# MarketIndicators source feasibility and candidate ranking

Reviewed 2026-11-14. This follows [the inventory](marketindicators_opportunities.md) on `research/marketindicators-gap-analysis`. Scores are implementation judgments, not measured predictive value. “Low effort” means existing EcoMedallion source and schema paths; all candidates still need a warehouse coverage check and a point-in-time research test. The legacy sample CSVs are synthetic (see MarketIndicators `scripts/generate_samples.py`), so they are not source data.

## Best candidates

| Rank | Candidate | Why it leads | Source and work | Main caveat |
| --- | --- | --- | --- | --- |
| 1 | VIX term structure: `VIXCLS / VXVCLS` or spread | Distinct daily volatility regime measure; one missing series, no new connector | FRED `VIXCLS` already ingested; add [`VXVCLS`](https://fred.stlouisfed.org/series/VXVCLS) | Cboe index reuse terms; align same close and calendar |
| 2 | SOFR minus IORB | Direct repo/funding pressure measure from two existing active series | Existing FRED `SOFR`, `IORB`; derived feature | Units are percentage points; convert to basis points and same-day join |
| 3 | Bank credit deterioration: `DRBLACBN`, `DRCCLACBS`, `DRSFRMACBS` | Adds distinct household and business credit stress beyond loan balances | Existing FRED connector; three manifest entries and release-aware features | Quarterly and revised; report known-at timestamps rather than daily “new” readings |
| 4 | Commercial paper outstanding growth (`COMPOUT`) | Adds a weekly wholesale-funding quantity | Existing FRED connector; one manifest entry | Weekly stock, not a spread; smooth and compare with context |
| 5 | RSP versus SPY trailing total return | Market participation signal with no new source | Both already in Tiingo manifest and equity return layer | ETF launch/history and relative return window; no claim of broad constituent breadth |
| 6 | Real 5-year yield (`DFII5`) | Complements ingested 10-year real yield, gives real-curve slope | Existing FRED connector; one manifest entry | Check any extra information beyond nominal and breakeven components |

The VIX term structure is a fast feature prototype. SOFR minus IORB is already configured as `SOFR_IORB` in `config/funding.yml`; the benchmark board already configures `MORTGAGE30US` versus `DGS10` in `config/benchmark_rates.yml`. Credit deterioration and commercial paper were the best raw-series additions. This ranking prioritizes distinct information and a tractable data path; it is not a backtested alpha ranking.

## Engineered feature status

The inputs below are active on this branch. Priorities 1–5 are configured or built in Gold; no live warehouse rows or predictive value have been verified.

| Priority | Feature | Definition and implementation path | Main check |
| --- | --- | --- | --- |
| 1 | VIX term inversion | `vix_term_inversion` in `gold.fred_cross_series_feature`: same-date `VIXCLS - VXVCLS` in index points. Positive values mean spot VIX is above three-month VIX. | Common market close/calendar, missing days, and Cboe reuse rights. |
| 2 | Real-yield curve slope | `real_yield_10y_5y` in `gold.fred_cross_series_feature`: same-date `DFII10 - DFII5` in percentage points. | Real-yield coverage and incremental value beyond nominal yield and breakeven slopes. |
| 3 | Bank-credit deterioration | `business_loan_delinquency_4q_change`, `credit_card_delinquency_4q_change`, and `mortgage_delinquency_4q_change` in `gold.fred_cross_series_feature`, with first-report variants in `gold.fred_cross_series_feature_pit`. Values are four-quarter changes in percentage points. | PIT rows are quarter-labelled; align to Silver release dates before a backtest. Do not blend the NSA business and SA consumer levels without normalization. |
| 4 | Equal-weight participation | `gold.equal_weight_participation_daily`: trailing 63 matched-session total return of `RSP` minus `SPY`, in percentage points, using `gold.equity_total_return_index`. | Dividend-adjusted continuity, ETF history, and matching sessions. |
| 5 | Implied versus trailing realized volatility | `gold.implied_realized_vol_gap_daily`: same-date `VIXCLS` minus 21-session annualized SPY realized volatility from `gold.realized_volatility`, in volatility percentage points. | VIX is forward-looking while the realized input is trailing; label it an implied/realized gap, not a realized future risk premium. |

`COMPOUT` already has a year-over-year percent-change presentation transform in `config/series_catalog.yml`. A separate four- or thirteen-week funding pulse is only useful after checking that the shorter window adds value beyond that generic transform.

## All 21 FRED-designated legacy IDs

| Legacy ID(s) | Feasibility | Source check / action |
| --- | --- | --- |
| `MTSDS133FMS` | Conditional | [FRED](https://fred.stlouisfed.org/series/MTSDS133FMS) calls it **Federal Surplus or Deficit**, monthly, not Treasury bill supply. Add only for a fiscal balance use case. For bill stock use a separately defined series such as [`BOGZ1FL313161110Q`](https://fred.stlouisfed.org/series/BOGZ1FL313161110Q), then assess its quarterly lag. |
| `M2V` | Easy, lower priority | [FRED](https://fred.stlouisfed.org/series/M2V) publishes quarterly nominal GDP / average M2. Valid but partly redundant with EcoMedallion's GDP and M2; treat as a slow macro feature. |
| `COMPOUT` | Easy, high priority | [FRED](https://fred.stlouisfed.org/series/COMPOUT) publishes weekly commercial paper outstanding. |
| `DFII5` | Easy, high priority | [FRED](https://fred.stlouisfed.org/series/DFII5) publishes daily 5-year TIPS real yield. |
| `T30YIE` | Wrong legacy ID | The verified FRED 30-year breakeven is [`T30YIEM`](https://fred.stlouisfed.org/series/T30YIEM), **monthly**. Do not label it daily. |
| `MICH` | Easy, moderate priority | [FRED](https://fred.stlouisfed.org/series/MICH) publishes monthly Michigan inflation expectations. Survey result timing and revisions matter. |
| `CPIHOSSL` | Easy, moderate priority | [FRED](https://fred.stlouisfed.org/series/CPIHOSSL) publishes monthly shelter CPI. EcoMedallion already has a wider CPI basket, so check overlap first. |
| `PCETRIM12M656SFRBDAL` | Wrong legacy ID | Verified Dallas Fed trimmed-mean PCE YoY ID is [`PCETRIM12M159SFRBDAL`](https://fred.stlouisfed.org/series/PCETRIM12M159SFRBDAL). The `159` series is already a percent change from year ago; do not apply another YoY transform. |
| `CPIFOOD` | Wrong or unverified ID; duplicate concept | Verified food CPI is [`CPIUFDSL`](https://fred.stlouisfed.org/series/CPIUFDSL), already in `manifests/prices_extra.yml`. No new ingest needed. |
| `SAHMREALTIME` | Easy, moderate priority | [FRED](https://fred.stlouisfed.org/series/SAHMREALTIME) publishes monthly real-time Sahm indicator. Compare to EcoMedallion's existing recession model before adding another signal. |
| `FRBATLWGT12MMM` | ID needs verification | FRED has an [Atlanta Fed Wage Growth Tracker release](https://fred.stlouisfed.org/release?rid=637), but the exact legacy ID did not resolve in this review. Select and document the desired weighted/unweighted and smoothing variant before ingest. |
| `RECPROUSM156N` | Easy, moderate priority | [FRED](https://fred.stlouisfed.org/series/RECPROUSM156N) publishes the New York Fed recession-probability series, monthly; it is model output from the yield curve, so test incremental value against existing curve features. |
| `DRBLACBN`, `DRCCLACBS`, `DRSFRMACBS` | Easy, high priority | FRED pages for [business loans](https://fred.stlouisfed.org/series/DRBLACBN), [credit cards](https://fred.stlouisfed.org/series/DRCCLACBS), and [mortgages](https://fred.stlouisfed.org/series/DRSFRMACBS) show quarterly delinquency rates. Check seasonal-adjustment consistency. |
| `DRAACBS` | Unverified | No matching FRED auto-loan delinquency page was established. Do not add as a presumed valid ID; research an official auto-specific replacement or omit. |
| `TDSP` | Easy, low incremental value | [FRED](https://fred.stlouisfed.org/series/TDSP) publishes quarterly household debt-service ratio, but EcoMedallion already ingests [`BOGZ1FL010000346Q`](https://fred.stlouisfed.org/series/BOGZ1FL010000346Q), a near-identical household debt-service percentage. Across 25 shared quarters from 2020 Q1 through 2026 Q1, public FRED graph CSV values have correlation 0.999998 and mean absolute difference 0.000306 percentage points. As of 2026-11-14, `TDSP` has 2026 Q2 while the existing series ends at 2026 Q1; that is a freshness argument rather than a new signal. |
| `FDHBFRBN` | Valid ID, wrong legacy label | [FRED](https://fred.stlouisfed.org/series/FDHBFRBN) is debt held by **Federal Reserve Banks**, not foreign holdings. For foreign/international holdings consider [`FDHBFIN`](https://fred.stlouisfed.org/series/FDHBFIN). Lower priority than credit/funding signals. |
| `FYFSGDA188S`, `GFDEGDQ188S` | Easy, lower priority | [FRED deficit/GDP](https://fred.stlouisfed.org/series/FYFSGDA188S) is annual; [debt/GDP](https://fred.stlouisfed.org/series/GFDEGDQ188S) is quarterly. Assess overlap with existing Treasury/BEA debt-to-GDP feature. |
| `CSUSHPISA` | Easy, low incremental value | [FRED](https://fred.stlouisfed.org/series/CSUSHPISA) is seasonally adjusted Case-Shiller national index. EcoMedallion already ingests `CSUSHPINSA`; only add for a deliberate SA/NSA comparison. |

## Market symbols and engineered aliases

| Legacy opportunity | Feasibility and source | Decision |
| --- | --- | --- |
| `IEI` | Existing Tiingo ETF path; `IEF` already ingested | Low effort, lower incremental value. Add if a 3–7-year duration bucket matters. |
| `^VIX`, `^VIX3M` | Existing `VIXCLS`; FRED [`VXVCLS`](https://fred.stlouisfed.org/series/VXVCLS) for 3-month VIX | Prefer FRED path; validate redistribution rights for Cboe-derived data. |
| `HG=F`, `LUMBER=F` | EcoMedallion has no yfinance source; continuous futures require roll methodology and a reliable licensed feed | Defer copper/gold and lumber/gold until vendor, contract roll, and price-adjustment rules are specified. |
| `BTC-USD` | Separate crypto price vendor or new source path | Defer unless a crypto research use case is approved. `IBIT` already offers an ETF proxy, with a shorter history and ETF basis. |
| `SOFR_TBILL_BPS`, `SOFR_IORB_SPREAD`, `PRIME_FF_SPREAD`, `MORTGAGE_TREASURY_SPREAD` | FRED legs already in manifests | Low effort. Do same-date/frequency and unit checks; prioritize SOFR–IORB and mortgage–Treasury. |
| `BUFFETT_INDICATOR`, `SHILLER_CAPE`, `EQUITY_RISK_PREMIUM`, `DIV_YIELD_SPREAD` | Buffett needs a suitable broad-market capitalization numerator plus GDP; [Shiller's dataset](https://shillerdata.com/) has CAPE; ERP also needs `DFII10`; dividend yield can use Tiingo dividend cash and FRED `DTB3` | Medium effort, lower priority. Specify numerator, frequency, licensing and as-of date first. |
| `MARGIN_DEBT_YOY` | [FINRA margin statistics](https://www.finra.org/rules-guidance/key-topics/margin-accounts/margin-statistics) plus monthly YoY transform | Medium effort; publication lag and possible historical-definition changes matter. |
| `SHORT_INTEREST` | [FINRA equity short interest](https://otce.finra.org/otce/equityShortInterest) plus point-in-time universe and float shares | High effort. Do not substitute daily short-sale volume for short interest; they measure different things. |
| `FAILS_TO_DELIVER` | [SEC FTD files](https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data); existing SEC XBRL connector does not read them | Medium effort, good securities-finance candidate. Files have delayed half-month publication; quantities are outstanding balances, not daily new fails. |
| `REG_SHO_COUNT` | [NYSE threshold lists](https://www.nyse.com/regulation/regulation-sho) and other listing venues | High effort due to multi-exchange coverage and history. |
| `COT_SP500_NETLONG` | [CFTC historical compressed reports](https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm) | Medium effort. Choose report family and S&P contract mapping; use report publication date, not Tuesday position date, in backtests. |
| `PUT_CALL_RATIO` | [Cboe historical options statistics](https://www.cboe.com/us/options/market_statistics/historical_data/) | Medium effort. Select equity vs index vs total ratio before building a signal. |
| `SKEW`, `VVIX` | Cboe index data; [VVIX history](https://www.cboe.com/tradable-products/vix/vix-historical-data) is posted | Medium effort with Cboe usage-rights check. |
| `AAII_SPREAD` | [AAII sentiment history](https://www.aaii.com/sentimentsurvey/sent_results) | Lower priority: full history is membership-gated; confirm permitted use before connector work. |
| `PCT_ABOVE_200DMA`, `NET_NEW_HIGHS` | Existing constituent pricing plus historical membership | Medium-to-high research effort. Strong candidate only if survivorship-safe history and adequate price coverage exist. |
| `AD_LINE` | Exchange-level advances/declines history, or a clearly named constituent-universe approximation | Defer until universe and market-coverage definition are explicit. |

## Next implementation slice

1. Verify warehouse rows and source entitlements for `SOFR`, `IORB`, `VIXCLS`, `VXVCLS`, `DFII5`, `DFII10`, `RSP`, and `SPY`. Review the newly built Gold features and the existing SOFR–IORB funding spread.
2. `VXVCLS`, `COMPOUT`, `DFII5`, `DRBLACBN`, `DRCCLACBS`, `DRSFRMACBS`, `MICH`, `SAHMREALTIME`, and `T30YIEM` are active in EcoMedallion manifests and the terminal catalog. `TDSP` is a low-priority freshness variant of an existing series. Check warehouse coverage and run a source-key dry run before using any newly ingested series in research.
3. Measure incremental value with walk-forward, point-in-time tests against existing EcoMedallion curve, funding, and recession features. Report coverage, staleness, correlation and out-of-sample stability; do not rank by in-sample fit alone.
4. For securities-finance research, evaluate a read-only exchange from MarketMedallion's existing SEC FTD and Reg SHO publications before building another provider collector. CFTC COT remains a separate new-connector candidate.

Sources linked above establish availability and definitions as of review date. They do not establish permission to redistribute third-party index or survey data.

## MarketMedallion overlap (checked 2026-11-14)

MarketMedallion is the separate `market-medallion` repository, inspected at main tree `ac4d52094fefa2e9f5f55cb7c82bfee804aa0492`. Its [master pipeline register](https://github.com/thatquantguy44/market-medallion/blob/main/docs/pipelines.md) distinguishes implemented pipelines from proposed source contracts.

| Candidate from this review | MarketMedallion status | Implication |
| --- | --- | --- |
| AAII bull–bear spread | Implemented in [AAII Gold](https://github.com/thatquantguy44/market-medallion/blob/main/src/market_data_pipeline/pipelines/aaii/data/gold.py) as `bull_bear_spread_pp`; authorized local data exists, recurring acquisition remains gated | Prefer its attributed output over a second AAII collector. |
| SEC fails to deliver | Implemented in [SEC FTD pipeline](https://github.com/thatquantguy44/market-medallion/tree/main/src/market_data_pipeline/pipelines/sec_fails_to_deliver), with bounded real-source acceptance and a [research export](https://github.com/thatquantguy44/market-medallion/blob/main/src/market_data_pipeline/pipelines/sec_fails_to_deliver/research_export.py) | Prefer the exported observations and coverage, with publication basis intact. Broader history and scheduled refresh remain gated. |
| Reg SHO threshold membership | Implemented for Nasdaq/NYSE in [threshold pipeline](https://github.com/thatquantguy44/market-medallion/tree/main/src/market_data_pipeline/pipelines/reg_sho_thresholds), with [membership/coverage/episode export](https://github.com/thatquantguy44/market-medallion/blob/main/src/market_data_pipeline/pipelines/reg_sho_thresholds/research_export.py) | A scope-labelled count can be derived after checking complete-date and market coverage. |
| FINRA daily short-sale volume | Local source/parser, SQLite research and typed Parquet publication implemented; acquisition/replay/export/jobs remain open in [Spec012](https://github.com/thatquantguy44/market-medallion/blob/main/specs/spec012/README.md) | This is **trading flow**, not the legacy `SHORT_INTEREST` outstanding-position series. Do not mark short interest covered. |
| CFTC COT, Cboe put/call, Nasdaq breadth | Draft contracts only; connectors absent per [register](https://github.com/thatquantguy44/market-medallion/blob/main/docs/pipelines.md) | Do not treat a spec or proposed package as data coverage. |
| FRED macro candidates, VIX term structure, Tiingo ETF return features | No corresponding pipeline or manifest in MarketMedallion's main tree | EcoMedallion remains the appropriate owner of these source series and transforms. MarketMedallion need not duplicate the FRED/Tiingo ingestion merely to serve a downstream comparison. |

This comparison checks repository code, manifests, acceptance records and the stated local-data status; it does not query either live warehouse. If EcoMedallion consumes a MarketMedallion research export, preserve dataset scope, source dates, capture/publication knowledge and coverage. The repositories' current contracts keep their runtime state and history independent.
