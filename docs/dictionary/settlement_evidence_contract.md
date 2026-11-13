# Settlement evidence contract

This is a narrow, offline Python research contract, separate from
`gold.market_calendar`. It is not a settlement-calendar feed, a complete date
range, proof that a particular trade settled, point-in-time evidence, or an
approved model input. It performs no network request and makes no inference
from trading days, Fedwire days, weekend rules or other sources. The catalog
contains only the historical Nasdaq notices explicitly listed below. It is not
an automatic fallback when a provider or database is unavailable.

## Callers and states

`fred_pipeline.settlement_evidence` provides:

- `lookup_settlement_day(market=..., day=...)`: whether an exact date is
  explicitly reported as a valid settlement day in the catalog.
- `lookup_reported_settlement(market=..., convention=..., trade_date=...)`:
  the **reported scheduled** settlement date and standard cycle for an exact
  trade date in a reviewed notice. `execution_verified` remains `false`.

Both require an explicit `market="NASDAQ_US_EQUITIES"`; the trade-date lookup
also requires `convention="REGULAR_WAY"`. Other scopes are unavailable; do
not infer applicability to NYSE, other assets, negotiated terms or individual
executions. Dates must be valid canonical `YYYY-MM-DD` strings.

| Status | Meaning | Value and evidence |
| --- | --- | --- |
| `REPORTED` | Exact scope and date were explicitly stated in a reviewed notice. | Reported value and cited notice; not execution proof or admission. |
| `UNAVAILABLE` | Scope is unsupported or no reviewed notice covers the requested date. | Value, cycle and evidence are null; reason identifies the gap. |
| `INVALID_INPUT` | Required scope or date is malformed. | Value, cycle and evidence are null. |

Every result includes `contract_version: settlement-evidence/1` and
`catalog_version: nasdaq-mourning-notices/1`. `source_mode` is always
`CURATED_HISTORICAL_NOTICES` and `knowledge_basis` is
`CURRENT_VINTAGE`; this does not claim a provider connection or point-in-time
availability. Notice evidence contains the source, URL, publication **date**
and review date. The publication time and first-seen timestamp are unknown and
are null. A publication date must not be treated as a decision-time timestamp.
`model_admission` is `NOT_ADMITTED` for every result. `asdict(result)` is
JSON-serializable, including invalid inputs; untrusted non-string inputs are not
echoed as objects.

The settlement-day lookup covers only the two explicitly stated valid settlement
dates; it does not infer that other dates are open or closed. The trade-date
lookup reports only these notice-backed cases:

| Trade date | Reported regular-way settlement | Standard cycle | Source |
| --- | --- | --- | --- |
| 2018-12-03 | 2018-12-05 | T+2 | [Nasdaq 2018 notice](https://www.nasdaqtrader.com/TraderNews.aspx?id=ETA2018-99) |
| 2018-12-04 | 2018-12-06 | T+2 | [Nasdaq 2018 notice](https://www.nasdaqtrader.com/TraderNews.aspx?id=ETA2018-99) |
| 2018-12-06 | 2018-12-10 | T+2 | [Nasdaq 2018 notice](https://www.nasdaqtrader.com/TraderNews.aspx?id=ETA2018-99) |
| 2025-01-08 | 2025-01-09 | T+1 | [Nasdaq 2024 notice](https://www.nasdaqtrader.com/TraderNews.aspx?id=ECA2024-632) |
| 2025-01-10 | 2025-01-13 | T+1 | [Nasdaq 2024 notice](https://www.nasdaqtrader.com/TraderNews.aspx?id=ECA2024-632) |

Nasdaq's notices also report that 2018-12-05 and 2025-01-09 were valid
settlement dates despite the trading closures. The standard-cycle labels are
separately supported by SEC notices on the [2017 T+2 transition](https://www.sec.gov/newsroom/press-releases/2017-68-0)
and [2024 T+1 transition](https://www.sec.gov/newsroom/press-releases/2024-62).
These statements do not establish the terms or settlement of an individual
trade. Do not extrapolate to unsourced dates.

## Consumer inventory and legacy field

A search of the local EcoMedallion and QuantMeridian source, tests, SQL, docs
and configuration found `t2_settle_date` in the raw Gold schema and calendar
writer, schema/behavior tests, catalog metadata, PORA evidence notes, and the
Power BI build plan's `qMarketCalendar` query. No direct PORA application
consumer of that field was found in the searched source. External or deployed
Power BI artifacts and consumers outside the searched repositories were not
inspected and remain unknown.

The Power BI plan no longer selects the legacy field, and the Gold catalog
metadata calls it unverified calendar arithmetic. The raw column remains for
schema compatibility; existing readers can still query it, and their use is
not automatically controlled by this change. It counts two business days of
the selected calendar, which may differ from the applicable settlement
calendar and current standard cycle. The corrected catalog metadata requires a
Gold rebuild to appear in a materialized `powerbi_catalog`; no shared warehouse
refresh or external report deployment is performed by this change.

## Extending the contract

A production calendar requires an identified source, rights, market and
instrument scope, effective dates, holiday and ad-hoc coverage, publication and
first-seen timestamps, revision policy, independent reconciliation, and explicit
failure behavior. Admit any new notice with its source and tests; do not infer
unreported adjacent days or convert missing evidence into an open day. Any
future point-in-time consumer needs actual availability timestamps, not a
publication date alone. Until those requirements are met, the generic calendar
and these historical notices do not qualify settlement calculations for models
or operational use.
