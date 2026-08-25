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
- **`FM_PUB` / `YC_PUB` — attempted, not completed.** Both are 7-dimension
  flows sharing a 103,676-entry generic ticker codelist
  (`PROVIDER_FM_ID`/`BENCHMARK_ITEM`) across `PROVIDER_FM`, `INSTRUMENT_FM`,
  and `DATA_TYPE_FM`. `--include-code` only narrows a dimension it's
  applied to (by design, after the fix below) -- with this many independent
  unpinned dimensions, a single search term essentially never matches all of
  them simultaneously, so useful candidates require pinning most dimensions
  by exact code first, which in turn requires knowing what's in them.
  Revisit with a specific instrument/currency already in mind rather than
  open-ended browsing.
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

- Prefer published or mobile/key-indicator flows for first-pass discovery when
  they exist; they are often narrower than the full statistical cubes.
- Keep generated rows inactive until a live smoke test confirms observations
  exist for the exact `ECB:<flow_ref>:<key>` id.
- Large flows such as `BSI`, `ICP`, `MNA`, `QSA`, `BOP`, `PSS`, and `SUP`
  should always use explicit dimension filters and the `--max-cartesian` guard.
