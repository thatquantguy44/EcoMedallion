# ECB Candidate Dataflows

Source key: `ecb`

Purpose: backlog of ECB dataflows to revisit with `spec002` metadata discovery.
These are not active ingestion series yet. Use `discover-ecb --flow FLOW
--inspect` first, then generate inactive candidate manifests with explicit
dimension filters.

## Recommended Workflow

```bash
PYTHONPATH=src python -m fred_pipeline discover-ecb --flow EXR --inspect
PYTHONPATH=src python -m fred_pipeline discover-ecb --flow EXR \
  --frequency d \
  --dimension CURRENCY=CZK,HUF \
  --dimension CURRENCY_DENOM=EUR \
  --dimension EXR_TYPE=SP00 \
  --dimension EXR_SUFFIX=A \
  --dry-run
```

## Progress

- **`EST` — done.** `manifests/ecb_est_candidates.yml` (inactive, pending
  review): the euro short-term rate (€STR), volume-weighted trimmed mean
  (`BENCHMARK_ITEM=EU000A2X2A25`, `DATA_TYPE_EST=WT`). Live-verified end to
  end through `ECBClient.get_observations`. Note for anyone extending this:
  EST publishes under `FREQ=B` (business-daily), not `D` -- a `D`-keyed
  query 404s. `discover-ecb` now maps `B` to manifest frequency `d`.
- **`ICP_PUB` — done.** `manifests/ecb_icp_candidates.yml` (inactive, pending
  review): euro area HICP headline (`ICP_ITEM=000000`) and core ex
  energy/food (`ICP_ITEM=XEF000`), annual rate of change (`ICP_SUFFIX=ANR`),
  NSA (`ADJUSTMENT=N`), Eurostat-published (`STS_INSTITUTION=4`) -- the
  ECB's own inflation-target gauge, previously entirely uncovered by this
  source. Live-verified end to end; both the headline/core item codes and
  the adjustment/institution combination were confirmed against real
  published rows (a wildcard flow query), not assumed from the codelist --
  `ICP_ITEM`/`ADJUSTMENT`/`STS_INSTITUTION` have far more *structurally
  valid* combinations than *actually published* ones, the same trap EST's
  `LEV` guess fell into.
- **`EON` — done.** `manifests/ecb_eon_candidates.yml` (inactive, pending
  review): EONIA (Euro OverNight Index Average, `EONIA_BANK=EONIA_TO`,
  `EONIA_ITEM=RATE`), the overnight-rate benchmark €STR replaced. Only 3
  dimensions total, one with a single code -- about as simple as ECB
  discovery gets. Live-verified: publishes under `FREQ=D` (not `B`, unlike
  EST -- don't assume the two share a convention just because they're both
  overnight rates), and data stops exactly at 2021-12-31 (EONIA's real
  discontinuation date), so this is fixed historical backfill, not a live
  feed -- useful for pre-€STR backtests, not for ongoing ingestion.
  (`OMO`, the flow next to `EON` in the same category, was checked and
  rejected: it's per-operation data keyed by 22,923 individual tender IDs,
  not a clean aggregate time series.)
- **`MOBILE_KEY_*` flows checked — mostly not narrower, despite this doc's
  own note below.** `MOBILE_KEY_1/2/3/5/7/8` all turned out to share the
  *exact same* full dimension structure as their parent statistical cube
  (`MOBILE_KEY_1` uses `ICP_PUB`'s own `ECB_ICP1` structure end to end;
  `MOBILE_KEY_2` (BSI) has **11 dimensions**, more than `FM_PUB`'s 7;
  `MOBILE_KEY_3`/`MOBILE_KEY_7` use Eurostat's national-accounts structure
  with 939-code area lists on both `REF_AREA` and `COUNTERPART_AREA`;
  `MOBILE_KEY_8` shares `FM_PUB`'s 103,676-entry ticker codelist). Whatever
  "narrower" means for these, it isn't a smaller declared dimension space --
  don't assume a `MOBILE_KEY_*` flow is easy just because of the name.
- **The "pin most dimensions, then wildcard-query the real API for what's
  actually published" technique (used for EST/ICP_PUB) cracked both `BSI_PUB`
  and `YC_PUB`.** Neither needed guessing across the full dimension space --
  a `curl` against the flow with only the known-relevant dimensions pinned
  and the rest left blank returns every combination that's actually live,
  usually a handful of rows, which then get decoded by name and re-verified
  through `ECBClient` before shipping. Both `_PUB` and `MOBILE_KEY_2` share
  the *identical* 11-dimension `BSI` structure -- the win here wasn't a
  simpler structure, it was not needing to reason about the structure at all.
  - **`BSI_PUB` — done.** `manifests/ecb_bsi_candidates.yml`: euro area M1
    (narrow money), M2 (intermediate), M3 (broad, ECB's traditional
    monetary-analysis reference value) annual growth rates. Reporting sector
    `BS_REP_SECTOR=V` (MFIs + central government + post office giro
    institutions) held by `BS_COUNT_SECTOR=2300` (non-MFIs excluding central
    government) -- the standard official-statistics sector pair, confirmed
    by finding all three in one wildcard query rather than assumed.
  - **`YC_PUB` — done.** `manifests/ecb_yc_pub_candidates.yml`: two spot
    tenors ecb_rates.yml doesn't have (3M, 1Y), four instantaneous forward
    rates (1Y/2Y/5Y/10Y), and the 10Y-1Y curve spread. Confirmed narrower
    than raw `YC` in a way that actually holds: `SR_30Y` live-404s under
    `YC_PUB` even though it's active via `YC` -- this `_PUB` variant
    genuinely doesn't carry the full tenor set, unlike `BSI_PUB` which
    turned out to be the same cube as `BSI`. Don't generalize "`_PUB` =
    narrower" or "`_PUB` = same" across flows; it's decided per flow family,
    check each one.
- **`FM_PUB` — a real finding, but it's a scope question, not a rates gap.**
  Wildcard-querying it (`REF_AREA=U2`, no currency pin, `FREQ=M` -- it
  publishes monthly, not daily like `FM`/`YC`) surfaced European equity
  indices (`DJES50I` = EURO STOXX 50, plus several STOXX sector
  sub-indices) and a €STR-adjacent overnight-rate summary that's redundant
  with the already-active `EST` flow. This isn't more rates coverage --
  it's a first European-equity-index data point, a different kind of
  addition than everything else in this backlog (equity coverage today is
  entirely Tiingo/Stooq, both US-focused). Worth a deliberate decision
  before pursuing, not a default yes.
- **`LFSI_PUB` — done.** `manifests/ecb_lfsi_candidates.yml`: the euro area
  unemployment rate (monthly, SA, ages 15-74, both sexes) -- the classic
  headline labour-market print. **Real gotcha worth remembering**: this
  flow's euro-area reference-area code is `I9` ("Euro area 20, fixed
  composition"), *not* `U2` ("changing composition") used everywhere else
  in this catalog -- assuming `U2` here wildcard-queries to nothing.
  Different Eurostat-sourced flows (this one runs on `EUROSTAT_LFS1`, not
  an ECB-native structure) don't necessarily share ECB's own area-code
  convention; check per flow rather than assuming.
- **`JVC_PUB` (job vacancy rate) — attempted, not completed.** Every
  wildcard combination tried came back empty or 404, including one the API
  itself echoed back in an error's `<SeriesKey>` as if it might be valid
  (it wasn't, live-checked). Possibly genuinely sparse/discontinued data
  for this dataflow, or it needs a country-level key rather than a euro-area
  aggregate -- unclear without more digging. Lower priority than it looked:
  the unemployment rate is the more important headline metric and that one
  worked.
- **`BP6_PUB` (balance of payments) — not yet attempted to completion.**
  Structurally similar to `BSI_PUB`/`YC_PUB` (IMF `BOP1_15` structure, 11
  dimensions) so the wildcard technique should apply; `INT_ACC_ITEM=CA`
  (current account) is a promising lead but wasn't confirmed with real data.
  **The specific wide-open wildcard key tried for it
  (`..U2......CA..`) got a repeat WAF block on retry** (narrow single-series
  lookups through the normal client kept working fine throughout, confirmed
  before and after) -- unlike EST's transient block earlier, this one didn't
  clear on its own within the session. Worth trying a *narrower* first
  wildcard next time (a few more dimensions pinned) rather than the
  widest-open version, both to get a usable result faster and because ECB's
  WAF seems to specifically flag broad multi-wildcard keys, not query
  volume alone.
- `discover-ecb` had two real bugs found and fixed while doing this work:
  `--include-code`/`--exclude-code` used to also filter dimensions already
  pinned by `--dimension`/`--frequency` (silently zeroing results), and
  generating zero candidates crashed instead of printing a clean message.

## Candidate Areas

| Area | Candidate flows | Why revisit |
|---|---|---|
| ECB/Eurosystem policy and exchange rates | `EXR`, `EXR_PUB`, `FM`, `FM_PUB`, `EST`, `EON`, `OMO`, `YC`, `YC_PUB`, `MOBILE_EXR`, `MOBILE_KEY_6`, `MOBILE_KEY_8` | Extend FX, policy rates, money-market rates, euro short-term rate, open-market operations, and yield curve coverage. |
| Money, credit and banking | `BSI`, `BSI_PUB`, `BLS`, `BKN`, `BKN_PUB`, `BNT`, `EMMS`, `JDF_BSI_MFI_BALANCE_SHEET`, `JDF_BSI_MFI_DOMESTIC_CROSS_BORDER`, `JDF_BSI_MFI_GROWTH_RATES`, `JDF_MFI_MFI_LIST`, `JDF_MIR_MFI_INTEREST_RATES`, `MFI`, `MIR`, `MIR_PUB`, `MMS`, `MMSR`, `MOBILE_BSI`, `MOBILE_KEY_2`, `RIR` | Monetary aggregates, MFI balance sheets, loan/deposit rates, bank lending survey data, banknote statistics, and money-market reporting. |
| Non-bank financial corporations | `FVC`, `FVC_PUB`, `ICB`, `ICB_PUB`, `ICO`, `ICO_PUB`, `ICPF`, `ICPF_PUB`, `IVF`, `IVF_PUB`, `JDF_ICPF_PENSION_FUNDS`, `JDF_IVF_ASSETS_LIABILITIES`, `JDF_IVF_SHARES`, `LIG`, `OFI`, `PFB`, `PFB_PUB`, `PFBM`, `PFBR`, `PFBR_PUB` | Investment funds, financial vehicle corporations, insurers, pension funds, and other financial intermediaries. |
| Financial markets and interest rates | `CISS`, `CLIFS`, `EMMS`, `FM`, `FM_PUB`, `IRS`, `IRS_PUB`, `JDF_MIR_MFI_INTEREST_RATES`, `JDF_SEC_OAT_DEBT_SECURITIES`, `MIR`, `MIR_PUB`, `MMS`, `MMSR`, `MOBILE_MIR`, `RIR`, `SEC`, `SEC_PUB`, `SEE`, `SESFOD`, `SHS`, `SST`, `YC`, `YC_PUB` | Financial stress, securities, interest-rate statistics, money-market reporting, and yield-curve expansion. |
| Macroeconomic and sectoral statistics | `AME`, `DD`, `ENA`, `ENA_PUB`, `ESA`, `IDCM`, `IDCM_PUB`, `IDCS`, `MNA`, `MNA_PUB`, `MOBILE_KEY_3`, `MOBILE_KEY_5`, `MPD`, `RTD`, `STS`, `STS_PUB` | Euro area macro aggregates, national accounts, projections, real-time vintages, and short-term statistics. |
| Inflation and consumer prices | `HICP`, `ICP`, `ICP_PUB`, `JDF_EXR_HCI_CPI`, `JDF_ICP_COICOP_ANR`, `JDF_ICP_COICOP_INW`, `JDF_ICP_COICOP_INX`, `JDF_ICP_ECONOMIC_ACTIVITIES_ANR`, `JDF_ICP_ECONOMIC_ACTIVITIES_INW`, `JDF_ICP_ECONOMIC_ACTIVITIES_INX`, `MOBILE_ICP`, `MOBILE_KEY_1` | HICP indices, annual inflation rates, expenditure weights, and competitiveness indicators. |
| Other prices and costs | `CPP`, `CPP_PUB`, `EWT`, `INW`, `JDF_EXR_HCI_ULCT`, `LCI`, `LCI_PUB`, `RESC`, `RESC_PUB`, `RESH`, `RESR`, `RESR_PUB`, `RESV`, `RPP`, `RPP_PUB`, `RPV` | Commercial and residential property prices, labor costs, wage trackers, negotiated wages, and unit labor cost indicators. |
| GDP, output, demand and income | `ESA`, `IDCM`, `IDCM_PUB`, `JDF_EXR_HCI_GDP`, `JDF_MNA_A_GDP_GROWTH_QOQ`, `JDF_MNA_B_GDP_GROWTH_YOY`, `JDF_MNA_C_GDP_CONTRIBUTIONS_QOQ`, `JDF_MNA_D_GDP_CONTRIBUTIONS_YOY`, `MNA`, `MNA_PUB` | GDP growth, expenditure components, GDP contributions, output, demand, and income aggregates. |
| Sector accounts | `DWA`, `IDCS`, `IEAF`, `IEAQ`, `IEAQ_PUB`, `QSA`, `QSA_PUB` | Quarterly sector accounts, euro area accounts, institutional-sector accounts, and distributional wealth accounts. |
| Government finance | `E09`, `E11`, `EDP`, `EDP_PUB`, `GFS`, `GFS_PUB`, `GST`, `MOBILE_GST`, `MOBILE_KEY_7` | Government finance, tax/social contributions, deficit/debt procedure tables, and government function classifications. |
| Labour market | `ENA`, `ENA_PUB`, `EWT`, `IESS`, `IESS_PUB`, `INW`, `JVC`, `JVC_PUB`, `JVS`, `JVS_PUB`, `LCI`, `LCI_PUB`, `LFSI`, `LFSI_PUB` | Employment, labor force, job vacancies, labor costs, wage tracker, and negotiated wage rates. |
| Balance of payments and external statistics | `BOP`, `BP6`, `BP6_PUB`, `BPS`, `BPS_PUB`, `ESB`, `RA`, `RA6`, `RA6_PUB`, `RAS`, `RAS_PUB`, `ST1`, `ST3`, `TRD`, `TRD_PUB`, `WTS` | Balance of payments, international investment position, reserves, external trade, and trade weights. |
| Supervisory and prudential statistics | `CBD`, `CBD2`, `KRI`, `RAI`, `RDE`, `RDF`, `SUP` | Consolidated banking data, supervisory banking statistics, key risk indicators, and risk dashboard data. |
| Payment statistics | `JDF_PSS_PAYMENTS_N`, `JDF_PSS_PAYMENTS_N_NEA`, `JDF_PSS_PAYMENTS_P`, `JDF_PSS_PAYMENTS_P_NEA`, `JDF_PSS_PAYMENTS_V`, `JDF_PSS_PAYMENTS_V_NEA`, `PAY`, `PCP`, `PCT`, `PIS`, `PLB`, `PSS`, `SSP`, `SST` | Payment transactions, payment values, relative payment service use, settlement systems, fraud losses, and structural payment indicators. |
| ECB surveys | `BLS`, `CES`, `MMS`, `SAFE`, `SESFOD`, `SPF`, `SUR`, `SUR_PUB` | Bank lending, consumer expectations, SME finance, professional forecasters, money-market surveys, and other opinion surveys. |

## Notes

- Prefer `*_PUB` (published) flows for first-pass discovery when they exist --
  `ICP_PUB` worked well this way. **Correction from live probing** (see
  Progress above): `MOBILE_KEY_*` flows are *not* reliably narrower than
  their parent statistical cube -- several share the parent's exact
  dimension structure, including the sprawling ones. Don't assume "mobile"
  or "key indicator" in a flow's name implies a small dimension space;
  check with `--inspect` first regardless.
- Keep generated rows inactive until a live smoke test confirms observations
  exist for the exact `ECB:<flow_ref>:<key>` id.
- Large flows such as `BSI`, `ICP`, `MNA`, `QSA`, `BOP`, `PSS`, and `SUP`
  should always use explicit dimension filters and the `--max-cartesian` guard.
