# Spec 004: FRED Ingestion for a Retail-Positioning / Dealer-Hedging Model

Status: proposed build plan
Last verified: 2026-09-03 (repo-local only — FRED egress is blocked in the
authoring environment, so no candidate series id below was live-verified; see §9)
Primary owner: TBD
Target source key: `fred`
Depends on: existing FRED source, `manifests/market_indices.yml`,
`config/cross_series.yml`, `config/spreads.yml`, `config/series_catalog.yml`

## 1. Goal

Land the FRED-sourced portion of the data a downstream model needs to test one
hypothesis: that retail options positioning, and the dealer hedging it forces,
predicts index direction over a 3–5 day horizon.

The hypothesis, as stated by its author:

> when retail is net long >68% on SPY options, market makers are holding the
> offsetting short gamma [...] that hedge pressure moves price against retail
> positioning in 71% of cases on a 3-5 day window

This spec does **not** claim FRED can produce that signal. It cannot. What
FRED can do is supply the volatility-regime, dealer-capacity, and
retail-participation covariates that any such model needs in order to be
testable at all — and to be falsified cheaply, before anyone pays for
positioning data. §2 states the correction plainly; §4 onward builds the FRED
layer anyway, scoped to what it can honestly carry.

## 2. Scope Correction (read before §5)

Three claims in the premise do not survive contact with the sources.

### 2.1 FRED has no order-flow data, and neither does EDGAR

- FRED is an aggregator of published macro and market **statistics**. It
  carries no order flow, no payment-for-order-flow figures, no options open
  interest, no put/call ratios, and no positioning of any kind. Nothing in
  §5 changes that.
- SEC Rule 606(a)(1) order-routing reports are published **quarterly by each
  broker-dealer on its own website**, not filed to EDGAR. There is no EDGAR
  full-text or structured path to them. The repo's existing `sec` source
  (XBRL company facts, `config/sec_concepts.yml`) reaches company financial
  facts, which is a different corpus entirely.
- What EDGAR does carry for this problem: Form 13F (quarterly institutional
  long equity/options holdings, 45-day lag), N-PORT (fund holdings), and
  X-17A-5 (broker-dealer FOCUS annual audited reports). None is retail, none
  is daily.

### 2.2 A 606 report cannot produce "retail net long 68%"

A 606(a)(1) report discloses, per venue, the **share of orders routed** and
the **net payment received per hundred shares**, split by order type (market,
marketable limit, non-marketable limit, other) and by S&P 500 stocks / other
stocks / options. It does not disclose:

- direction (buy vs sell),
- open vs close,
- strike, expiry, or underlying within the options bucket,
- anything at a grain finer than the quarter.

So the input the model is specified on does not exist in the cited source. A
quarterly, direction-free, venue-level routing statistic cannot be turned into
a daily net-long percentage on SPY options. It also gives roughly 4
observations per broker per year — about 8 over the two years the premise
describes — which cannot support a 71% hit-rate claim on 3–5 day windows at
any conventional significance level.

### 2.3 The hedging mechanism as described has the wrong sign

If retail is net long calls, dealers are net short calls, which is **short
gamma**. Short-gamma delta hedging means buying as price rises and selling as
it falls. That amplifies moves in whichever direction they occur — it adds
momentum and realized volatility. It does not systematically push price
*against* retail. The forces that do bleed long-option retail positions are
theta and vanna/charm decay, which are a different mechanism with a different
signature.

The practical consequence for this spec: the tradable quantity is the **sign
and size of aggregate dealer gamma**, not retail's direction. Dealer gamma
sign requires open interest by strike (OCC/CBOE), which FRED does not have.
Everything in §5 is therefore a conditioner or a confirmation series, and §8
defines the slot where the real positioning input must plug in.

### 2.4 Treat the stated numbers as an untested hypothesis

The `>68%` threshold and `71%` hit rate come from an unattributed social post
reporting an unpublished backtest. They are a hypothesis to pre-register and
test, not a calibrated prior to build toward. §8 fixes the falsification
protocol before any data is bought.

## 3. Non-Goals

- **Not** building a PFOF / Rule 606 source. It is not FRED and not EDGAR; it
  is per-broker website scraping with no common schema. That is its own spec,
  and §2.2 plus §8.1 argue it should be the last source attempted, not the
  first.
- **Not** building the downstream model. This spec delivers the feature
  substrate and the handoff contract only.
- **Not** adding a source client or new Bronze/Silver schemas. The `fred`
  source already exists and is the broadest in the repo (2,570 active series).
- **Not** ingesting SPY prices. `manifests/equity_tiingo.yml` already carries
  `SPY` as an active Tiingo series with dividend-adjusted history; FRED's
  `SP500` is already active for the index level.
- **Not** activating any new series before the live verification in §9. Every
  candidate ships `active: false`, matching the `*_candidates.yml` convention.

## 4. What FRED Can Actually Contribute

Four roles, ordered by how much the model depends on them.

| Role | Why the model needs it | Grain available |
|---|---|---|
| Volatility level and term structure | Dealer hedging intensity scales with vol; VIX/VIX3M backwardation is the observable regime marker for when hedging flow is destabilizing rather than dampening | Daily |
| Dealer / intermediary balance-sheet capacity | Whether dealers can warehouse the risk at all; a capacity-constrained dealer hedges more aggressively | Weekly (NFCI) and quarterly (Z.1) |
| Retail participation proxies | Confirms the regime the premise assumes (elevated retail share); cannot time anything | Monthly and quarterly |
| Risk-free rate for any options-implied computation | Discounting in any downstream greeks or variance-premium calculation | Daily, already active |

The grain column is the honest constraint. The hypothesis is a 3–5 day
signal. **Only the daily volatility complex can participate in a trigger.**
The weekly and quarterly series are regime conditioners and sample-splitters
and must never be used as a timing input — the repo's
`config/regime.yml` already encodes exactly this discipline via
`max_staleness_days` and as-of carrying, and that pattern should be reused
rather than reinvented.

Already active and reusable with no new ingestion:

| Series | Manifest | Role here |
|---|---|---|
| `VIXCLS` | `market_indices.yml` | Volatility level; the anchor of the term-structure ratio |
| `SP500` | `market_indices.yml` | Index level for realized-vol and label construction |
| `NFCILEVERAGE`, `NFCIRISK`, `NFCI`, `ANFCI` | `production_housing.yml` | Dealer leverage and financial conditions |
| `BOGZ1FL663067003Q` | `money_banking.yml` | Broker-dealer receivables due from customers (margin loans) — the closest public margin-debt proxy in the repo |
| `BOGZ1FL153064486Q` | `money_banking.yml` | Household equities as a share of financial assets — slow retail participation proxy |
| `WRMFNS`, `MMMFFAQ027S` | `money_banking.yml` | Retail money-fund assets — cash on the sidelines |
| `UMCSENT` | `labor_extra.yml` | Consumer sentiment |
| `DGS3MO`, `DTB3`, `SOFR` | `rates.yml` | Risk-free discounting |
| `SPY` (Tiingo) | `equity_tiingo.yml` | The actual traded instrument |

## 5. Series To Add

One new manifest, `manifests/volatility_complex.yml`, all entries
`active: false` until §9 clears them. Follow `market_indices.yml`'s framing:
these are quoted market levels, not revised statistics, so
`vintage_enabled: false` and `validation_profile: standard`.

| Candidate id | Title | Freq | Priority | Role / risk |
|---|---|---|---|---|
| `VXVCLS` | CBOE S&P 500 3-Month Volatility Index | d | 1 | The single most valuable addition. Pairs with `VIXCLS` for the term-structure ratio (§6). CBOE renamed this index VIX3M; confirm FRED's id and that it is still updating, not discontinued. |
| `VXNCLS` | CBOE Nasdaq-100 Volatility Index | d | 2 | Retail options activity skews to high-beta tech; VXN/VIX is a cheap proxy for that tilt. |
| `RVXCLS` | CBOE Russell 2000 Volatility Index | d | 2 | Small-cap vol; second leg of the same dispersion read. |
| `VXDCLS` | CBOE DJIA Volatility Index | d | 3 | Low marginal value; include only if it verifies cleanly. |
| `OVXCLS` | CBOE Crude Oil ETF Volatility Index | d | 3 | Cross-asset hedging spillover; conditioner only. |
| `GVZCLS` | CBOE Gold ETF Volatility Index | d | 3 | As above. |
| `EVZCLS` | CBOE EuroCurrency ETF Volatility Index | d | 3 | As above. |
| `VXOCLS` | CBOE S&P 100 Volatility Index | d | 4 | Believed discontinued. Value is pre-1990 history for regime work, not live signal. Ingest only if history matters to the downstream backtest window. |

Two additions that need id discovery before they can be written into a
manifest at all, rather than a guessed id:

- **Broker-dealer customer credit balances.** The Z.1 counterpart to the
  already-ingested `BOGZ1FL663067003Q`, giving the liability side of the
  margin relationship. Resolve the exact `BOGZ1FL...Q` id through FRED search
  before adding; do not guess a Financial Accounts id.
- **A Nasdaq-100 index level** to pair with `VXNCLS`. `NASDAQCOM` (composite)
  is already active and is a near substitute; add a 100-specific level only if
  verification shows one exists and the composite proves inadequate.

### 5.1 History truncation — a live risk to the entire volatility layer

FRED's CBOE series carry a licensing constraint that undercuts the plan above,
and it affects the **already-active** `VIXCLS`, not only the candidates.

Public FRED pages for `VIXCLS` and `VXVCLS` state that **starting in April
2026 these series include only a rolling three years of observations**, with
users directed to the source for anything longer. That date has already
passed. `VXOCLS` is separately confirmed as discontinued.

Consequences, in order of severity:

- A rolling three-year window cannot support a backtest that needs to span
  more than one volatility regime. Three years from today reaches back to
  2023 — no 2020, no 2018, no 2008. A dealer-hedging model calibrated only on
  that window has never seen the conditions it exists to trade.
- Any existing Gold history the repo already holds for `VIXCLS` becomes more
  valuable than the upstream series, because the pipeline's Bronze archive and
  point-in-time tables retain what FRED itself has now dropped. **Do not
  replay or full-refresh `VIXCLS` from FRED without first confirming what the
  API currently returns** — a `--full` reload could truncate locally held
  history to match the shortened upstream window.
- It moves the volatility complex from "ingest from FRED" toward "ingest from
  CBOE directly." CBOE publishes full VIX-family history free on its own CDN
  (§8.2). FRED's advantage was convenience and a single client, not coverage,
  and that advantage is now largely gone for this family.

Verify the current state in §9 step 1 before writing the manifest. If the
truncation is confirmed, the recommended shape of this spec changes: the
volatility complex becomes a small CBOE source addition, and FRED's role
narrows to the macro conditioners it is genuinely best at.

### 5.2 What is deliberately absent from FRED

State this in `docs/catalog/fred.md` so the next person does not go looking:

- CBOE equity and index **put/call ratios** — the best public daily
  retail-tilt proxy that exists. Not on FRED. Sourced from CBOE directly.
- **SKEW** and **VVIX** — tail-pricing and vol-of-vol, both directly relevant
  to a gamma-regime model. Not on FRED.
- **OCC open interest and volume** by strike — the input dealer gamma sign
  actually requires. Not on FRED.
- Exchange or wholesaler **volume**, off-exchange share, retail liquidity
  taker share. Not on FRED.

That list is the honest boundary of this spec, and it is the argument for §8's
interface rather than for a bigger FRED pull.

## 6. Derived Features — Config Only, No New Engine

The repo already has the machinery. Every feature below is a config entry, not
Python, with one flagged exception.

**`config/cross_series.yml`** (`op: ratio`, `frequency: d`) expresses the
term-structure signal directly:

```yaml
  - name: vix_term_structure
    op: ratio
    frequency: d
    legs: [VIXCLS, VXVCLS]
    description: >
      VIX / VIX3M. Above 1.0 = backwardation: near-term implied vol exceeds
      3-month. This is the observable regime marker for stressed, short-gamma
      hedging conditions; below ~0.9 is the complacent contango regime.
```

**`config/spreads.yml`** carries the same pair as a level difference
(`VIXCLS` − `VXVCLS`) for anyone who wants the spread rather than the ratio,
and `VXNCLS` − `VIXCLS` as the tech-tilt leg.

**`config/stats_pairs.yml`** is where the hypothesis first meets evidence at
near-zero cost. Adding `{series_a: VIXCLS, series_b: SP500}` and
`{series_a: vix_term_structure, series_b: SP500}` gives rolling correlation,
cross-correlation at lags ±12, and a Granger F-test in both directions from
existing Gold. A 3–5 day predictive relationship, if one exists in the
volatility regime alone, shows up here before any model is written.

**`config/regime.yml`** gains a `volatility` pillar (or extends `liquidity`)
with `VIXCLS` and the term-structure ratio, so regime labels can split the
downstream backtest sample. Reuse the existing expanding, point-in-time-safe
z-score path — never a full-sample z-score.

**`config/ml_features.yml`** adds the daily vol features so they reach the
macro PCA and anomaly surfaces already built on that config.

### 6.1 The one genuine code decision: realized volatility

The variance risk premium (implied minus realized) is the most informative
single feature for a hedging-pressure model, and it cannot be expressed today.
`ml_features.py`'s `_TRANSFORM_COL` supports `level`, `mom`, `diff`, `yoy`,
and `zscore` only — there is no rolling standard deviation of log returns.

Two options, to be decided before implementation starts, not during:

1. Add a `realized_vol` transform (windowed stdev of log returns, annualized)
   to `gold_fred_feature_transforms` and its config allow-list. Reusable
   across every price series in the repo, including the Tiingo equities.
2. Compute it only inside the downstream model module.

Option 1 is preferred: realized vol on a price series is general-purpose, and
keeping it in Gold keeps the point-in-time guarantees the transform layer
already enforces. Option 2 duplicates it into every consumer.

## 7. Point-in-Time and Leakage Discipline

The repo's existing guarantees carry most of this, but three hazards are
specific to a 3–5 day equity signal and must be written into the downstream
model's tests.

- **Close-time misalignment.** VIX and SPY do not stop trading at the same
  instant; index options settle after the equity close. A signal formed from
  day *t*'s VIX close cannot be traded at day *t*'s SPY close. Require signal
  at *t*, execution at *t+1* open or close, and assert the offset in a test.
  A large share of published 3–5 day equity results are this bug.
- **Publication lag on the conditioners.** NFCI is weekly with a release lag;
  Z.1 quarterly series land roughly ten weeks after quarter-end. As-of joins
  with `max_staleness_days`, per `config/regime.yml`, are mandatory; a naive
  join on observation date leaks.
- **Vintage flags.** Market quotes are `vintage_enabled: false` (they are not
  revised). Financial Accounts series stay `vintage_enabled: true` and must be
  read through `gold_fred_point_in_time`, never
  `gold_fred_latest_observation`, in any backtest.

## 8. Handoff Contract To The Downstream Model

The downstream model consumes one daily frame keyed on
`(observation_date)`, assembled from Gold:

| Column | Source | Status |
|---|---|---|
| `spx_close` | `SP500` / Tiingo `SPY` | Available |
| `vix` | `VIXCLS` | Available |
| `vix3m`, `vix_term_structure` | `VXVCLS`, `config/cross_series.yml` | This spec |
| `realized_vol_20d`, `variance_risk_premium` | §6.1 | This spec, pending the §6.1 decision |
| `vxn`, `rvx` and their ratios to `vix` | This spec | This spec |
| `nfci_leverage_z`, `regime_composite` | Existing configs | Available |
| `dealer_margin_receivables`, `household_equity_share` | Existing `money_banking.yml` | Available, quarterly, conditioner only |
| `small_trader_net_long` | CFTC Commitments of Traders, nonreportable line | **Unfilled**, free, weekly — the closest public directional retail proxy (§8.1) |
| `put_call_ratio` | CBOE daily archives | **Unfilled**, free, daily with history (§8.1) |
| `dealer_gamma_sign`, `gamma_notional` | CBOE delayed-quotes JSON (greeks and open interest per strike) | **Unfilled**, free daily snapshot, no history — must be accumulated (§8.1) |
| `retail_net_long_pct` as literally specified | CBOE Open-Close Volume Summary | **Unfilled and not free** (§8.1) |

The unfilled rows are the hypothesis's actual content. Ranked candidates for
filling them:

see §8.1. None is FRED, and the best of them is not free.

### 8.1 Free non-FRED sources that can fill the unfilled rows

Reachability could not be tested here (§9 blocks every one of these hosts too),
so treat every endpoint shape below as **needing live confirmation before it is
written into a source client**. Ranked by how directly each one answers the
question the model is actually asking.

| Source | What it gives | Grain | Cost | Verdict |
|---|---|---|---|---|
| **Cboe delayed-quotes JSON** (`cdn.cboe.com/api/global/delayed_quotes/options/_SPX.json`, and the `SPY` equivalent) | Full option chain: every strike × expiry with bid/ask, volume, **open interest, implied vol, and per-contract delta and gamma** | Snapshot, ~15-min delayed intraday, end-of-day values after the close | Free, no key | **Best free option.** This is the direct input to a dealer gamma-exposure calculation — no pricing model needed, since Cboe publishes the greeks. Its limitation is decisive: it is a *snapshot*, with no history. You must capture it daily going forward and accumulate your own archive. That is exactly what this repo's Bronze layer already does for every other source. |
| **CFTC Commitments of Traders** (`publicreporting.cftc.gov`, Socrata API) | Long/short/spread positions by trader class on E-mini S&P 500 futures, including the **"nonreportable positions"** line — small traders, i.e. the closest thing to an official retail long/short series | Weekly (Tuesday positions, Friday release, ~3-day lag) | Free, no key | **Best free source with actual direction.** It is futures rather than options, weekly rather than daily, and small-trader is a proxy for retail rather than a measurement of it. But it is directional, long-history, revision-free, and government-published — the only candidate here with all four. Weekly grain makes it a conditioner for a 3–5 day signal, not a trigger. |
| **OCC volume and open interest** (`theocc.com` market data reports), notably **Volume by Account Type** | Daily volume and open interest, split **customer / firm / market maker** | Daily | Free | Strong second input. The customer-versus-market-maker split is a direct read on which side of aggregate volume the customer base sits, and OCC open interest by series is the alternative gamma input if the Cboe endpoint proves unusable. No buy/sell direction. |
| **Cboe put/call ratio archives** (`cdn.cboe.com/resources/options/volume_and_call_put_ratios/`) | Daily equity, index, and total put/call ratios, with a downloadable historical archive | Daily, with history | Free | The cheapest first attempt, and it has the history the FRED volatility series no longer do (§5.1). A ratio is a crude tilt proxy — it conflates opening and closing trades and hedging with speculation — but it is the classic sentiment input and costs almost nothing to add. |
| **Cboe VIX-family history** (`cdn.cboe.com`, e.g. the VIX history CSV) | Full history for VIX, VIX3M, VIX9D and the rest of the family | Daily, full history | Free | The replacement for the FRED volatility complex if §5.1's truncation is confirmed. |
| **FINRA daily short sale volume** (`cdn.finra.org` / `regsho.finra.org`, plus a free Query API at `api.finra.org`) | Per-ticker short volume, including a **consolidated off-exchange** file across the trade reporting facilities | Daily, posted by 6pm ET same day | Free | Useful and underused. Retail marketable flow is internalized off-exchange, so off-exchange share and its short component are a genuine retail-activity proxy — arguably closer to the premise's actual intent than anything on EDGAR. Short volume is not short interest and is widely misread; document that distinction wherever it lands. |
| **FINRA margin statistics** | Aggregate customer margin debt and credit balances | Monthly | Free | The series most people mean by "margin debt." Slower and cleaner than the Z.1 broker-dealer receivables already ingested; a leverage conditioner, not a signal. |
| **SEC Rule 606 reports** | Quarterly routed-order share and payment per hundred shares, per broker, per venue | Quarterly | Free, but per-broker website scraping with no common schema | **Last.** Per §2.2 it is structurally incapable of producing the stated signal, despite being the premise's headline claim. |

And the dataset that would actually answer the question, for completeness:

- **Cboe Open-Close Volume Summary** classifies every trade by participant type
  (customer, professional customer, broker-dealer, market maker), by buy versus
  sell, by **open versus close**, and breaks customer activity into contract-size
  tiers (under 100, 100–199, 200+). Small-tier customer *opening buys* is, as
  close as any public dataset gets, the literal quantity the premise describes,
  with history for one exchange back to 2011.
- It is a paid Cboe DataShop product. That is the honest bottom line: the exact
  data exists, it is well-documented, and it is neither free nor on EDGAR. The
  free stack above approximates it; it does not replace it.

### 8.2 Falsification protocol, pre-registered

Fix these before any of the above is built:

- The cheap baseline runs first: can the volatility term structure alone
  reproduce any part of a 3–5 day directional edge? `config/stats_pairs.yml`
  (§6) answers this from existing Gold at effectively zero cost. If the answer
  is no, that is informative; if the answer is yes, the positioning data must
  beat it, not merely work.
- Thresholds (`>68%`) and horizon (3–5 days) are declared in config before
  fitting, and a threshold sweep is reported alongside the headline number.
  A result that survives only at 68% is a result about 68%.
- Report the count of independent observations, not just the hit rate. A 71%
  hit rate on overlapping 3–5 day windows across a few hundred effective
  observations is well inside what noise produces after a threshold search.
- Costs are in the backtest from the first run: SPY spread, options spread if
  the trade is expressed in options, and the *t+1* execution lag from §7.

## 9. Verification Procedure — Not Done, And Why

`specs/spec001` and `specs/spec002` were written against live API checks.
This one could not be. In the authoring environment the network egress policy
rejects every relevant host at the proxy — the FRED hosts, and every
alternative source evaluated in §8.1:

```text
fred.stlouisfed.org:443     gateway answered 403 to CONNECT (policy denial)
api.stlouisfed.org:443      gateway answered 403 to CONNECT (policy denial)
cdn.cboe.com:443            gateway answered 403 to CONNECT (policy denial)
www.cboe.com:443            gateway answered 403 to CONNECT (policy denial)
publicreporting.cftc.gov:443  gateway answered 403 to CONNECT (policy denial)
cdn.finra.org:443           gateway answered 403 to CONNECT (policy denial)
api.finra.org:443           gateway answered 403 to CONNECT (policy denial)
www.theocc.com:443          gateway answered 403 to CONNECT (policy denial)
```

Everything in §8.1 therefore rests on published documentation rather than a
live call, and every endpoint shape there needs the same confirmation step as
the §5 series ids.

So **every candidate id in §5 is unverified**, and the manifest must ship
inactive until someone with FRED reachability runs the following. This is the
same discipline `market_indices.yml` records in its own description block,
where `GOLDPMGBD228NLBM` was dropped rather than guessed.

```bash
# 1. Resolve each candidate id against the live API (requires FRED_API_KEY).
for id in VXVCLS VXNCLS RVXCLS VXDCLS OVXCLS GVZCLS EVZCLS VXOCLS; do
  curl -s "https://api.stlouisfed.org/fred/series?series_id=$id&api_key=$FRED_API_KEY&file_type=json" \
    | python -c "import json,sys; d=json.load(sys.stdin); s=d.get('seriess',[{}])[0]; print(s.get('id'), '|', s.get('last_updated'), '|', s.get('title'))"
done

# 2. Validate manifest structure with no network.
PYTHONPATH=src python -m fred_pipeline validate

# 3. Flip verified ids to active: true, then a scoped run.
PYTHONPATH=src python -m fred_pipeline run --local
```

Record the result the way the ECB specs do: ids that resolve get activated and
noted in `docs/catalog/fred.md`; ids that do not resolve get **removed with a
note**, not renamed to a guess. Pay particular attention to `last_updated` —
several CBOE indices on FRED are discontinued and will resolve happily while
being years stale, which is worse than absent because it silently poisons a
daily feature.

## 10. Suggested Implementation Slices

1. `manifests/volatility_complex.yml` with all §5 candidates inactive, plus
   the §5.1 absence note in `docs/catalog/fred.md`. No behavior change.
2. Run §9 verification. Activate what resolves and is current; delete the
   rest. Update the FRED active-series count in `docs/catalog/fred.md`.
3. Config-only features (§6): `cross_series.yml`, `spreads.yml`,
   `stats_pairs.yml`, `regime.yml`, `ml_features.yml`. No Python.
4. Read the §6 stats-pairs output. This is the first real evidence checkpoint
   and it comes before any modeling work.
5. Decide §6.1 (realized-vol transform in Gold vs in the model), implement it,
   and add the variance-risk-premium feature.
6. Write the handoff doc under `docs/handoffs/` describing the §8 frame.
7. Open a follow-on spec for a `cboe` source covering, in order: the VIX-family
   history (which §5.1 may make mandatory rather than optional), the put/call
   archives, and the delayed-quotes chain snapshot that feeds a gamma estimate.
   Start the chain snapshot capture early even if nothing consumes it yet — it
   has no history, so every day not captured is permanently lost.
8. Open a second follow-on for the CFTC Commitments of Traders source. It is a
   clean Socrata API with long history and no snapshot urgency, so it can wait,
   but it is the only free source here that carries actual position direction.

## 11. Open Questions

- **Partly answered, and it is bad news.** `VXVCLS` does still exist on FRED
  under that id, but public FRED pages state that both it and `VIXCLS` carry
  only a rolling three years of observations from April 2026 (§5.1). If §9
  confirms that, the volatility complex should come from CBOE's own free CDN
  rather than FRED, which turns slice 3 into a small source addition and
  narrows this spec's FRED footprint to the macro conditioners. Settle this
  first; more of the plan depends on it than on anything else here.
- What does the pipeline currently hold for `VIXCLS`, and would a `--full`
  refresh destroy history FRED has already dropped upstream? Check before any
  replay touches that series.
- Is a `realized_vol` transform in `gold_fred_feature_transforms` the right
  home, given that most FRED series are macro levels where realized vol is
  meaningless? Possibly it belongs to the equity/price path only.
- The premise's `$3 billion a year` PFOF figure is not verifiable from
  anything this pipeline ingests, and nothing in the design depends on it. It
  should not appear in any handoff doc as a fact.
- Should the downstream model live in `src/fred_pipeline/ml/` alongside
  `recession_model.py` and `macro_pca.py`, or outside the pipeline entirely?
  Those existing models consume macro Gold on monthly/quarterly grain; a daily
  equity trading signal has a different testing burden (§8.1) and may not
  belong in the same module namespace.
