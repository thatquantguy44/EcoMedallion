# ECB Catalog

Source key: `ecb`

Upstream: European Central Bank Data Portal SDMX API.

Authentication: keyless.

Series id convention:

```text
ECB:<flow_ref>:<key>
```

Current manifest: `manifests/ecb_rates.yml`

## Current Series (20 active)

| Series ID | Flow | Frequency | Units | Gold category | Description |
|---|---|---:|---|---|---|
| `ECB:EXR:D.USD.EUR.SP00.A` | `EXR` | d | USD per EUR | FX | ECB daily euro foreign exchange reference rate for the US dollar. |
| `ECB:EXR:D.GBP.EUR.SP00.A` | `EXR` | d | GBP per EUR | FX | ECB daily euro foreign exchange reference rate for pound sterling. |
| `ECB:EXR:D.JPY.EUR.SP00.A` | `EXR` | d | JPY per EUR | FX | ECB daily euro foreign exchange reference rate for the Japanese yen. |
| `ECB:EXR:D.CHF.EUR.SP00.A` | `EXR` | d | CHF per EUR | FX | ECB daily euro foreign exchange reference rate for the Swiss franc. |
| `ECB:EXR:D.CNY.EUR.SP00.A` | `EXR` | d | CNY per EUR | FX | ECB daily euro foreign exchange reference rate for the Chinese yuan renminbi. |
| `ECB:EXR:D.CAD.EUR.SP00.A` | `EXR` | d | CAD per EUR | FX | ECB daily euro foreign exchange reference rate for the Canadian dollar. |
| `ECB:EXR:D.AUD.EUR.SP00.A` | `EXR` | d | AUD per EUR | FX | ECB daily euro foreign exchange reference rate for the Australian dollar. |
| `ECB:EXR:D.NOK.EUR.SP00.A` | `EXR` | d | NOK per EUR | FX | ECB daily euro foreign exchange reference rate for the Norwegian krone. |
| `ECB:EXR:D.SEK.EUR.SP00.A` | `EXR` | d | SEK per EUR | FX | ECB daily euro foreign exchange reference rate for the Swedish krona. |
| `ECB:EXR:D.DKK.EUR.SP00.A` | `EXR` | d | DKK per EUR | FX | ECB daily euro foreign exchange reference rate for the Danish krone. |
| `ECB:EXR:D.NZD.EUR.SP00.A` | `EXR` | d | NZD per EUR | FX | ECB daily euro foreign exchange reference rate for the New Zealand dollar. |
| `ECB:EXR:D.PLN.EUR.SP00.A` | `EXR` | d | PLN per EUR | FX | ECB daily euro foreign exchange reference rate for the Polish zloty. |
| `ECB:FM:M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA` | `FM` | m | Percent | RATES | Monthly 3-month EURIBOR money-market rate for the euro area. |
| `ECB:FM:D.U2.EUR.4F.KR.DFR.LEV` | `FM` | d | Percent | RATES | ECB deposit facility rate. |
| `ECB:FM:D.U2.EUR.4F.KR.MRR_FR.LEV` | `FM` | d | Percent | RATES | ECB main refinancing operations fixed rate. |
| `ECB:FM:D.U2.EUR.4F.KR.MLFR.LEV` | `FM` | d | Percent | RATES | ECB marginal lending facility rate. |
| `ECB:YC:B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y` | `YC` | d | Percent | RATES | ECB euro area 2-year yield curve spot rate. |
| `ECB:YC:B.U2.EUR.4F.G_N_A.SV_C_YM.SR_5Y` | `YC` | d | Percent | RATES | ECB euro area 5-year yield curve spot rate. |
| `ECB:YC:B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y` | `YC` | d | Percent | RATES | ECB euro area 10-year yield curve spot rate. |
| `ECB:YC:B.U2.EUR.4F.G_N_A.SV_C_YM.SR_30Y` | `YC` | d | Percent | RATES | ECB euro area 30-year yield curve spot rate. |

## Why These Series

- The FX rows extend the initial USD per EUR starter to a small set of major
  euro reference rates that were live-probed successfully.
- The EURIBOR row adds a euro money-market rate with a compact monthly cadence.
- The policy-rate rows add the ECB deposit facility, main refinancing, and
  marginal lending facility rates.
- The yield-curve rows add daily euro area 2-year, 5-year, 10-year, and 30-year
  spot rates for cross-market rate comparisons.

## Discovery Areas

`specs/spec002` tracks the buildout for ECB metadata discovery. The broader
revisit backlog is maintained in [ecb_candidate_flows.md](ecb_candidate_flows.md).

## Caveats

- Do not generate broad active manifests from SDMX dimensions without bounded
  candidate generation and live smoke tests.
- ECB rows are keyless, but requests still use the shared retry and rate-limit
  transport.
