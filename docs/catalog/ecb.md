# ECB Catalog

Source key: `ecb`

Upstream: European Central Bank Data Portal SDMX API.

Authentication: keyless.

Series id convention:

```text
ECB:<flow_ref>:<key>
```

## Current Series (46 active)

Coverage now spans four manifests, not just `ecb_rates.yml`:

| Manifest | Active series | What |
|---|---:|---|
| `manifests/ecb_rates.yml` | 34 | FX reference rates, policy/money-market rates, yield curve — the table below covers the original 20 of these |
| `manifests/ecb_yc_pub_candidates.yml` | 7 | Yield curve beyond `ecb_rates.yml` — 3M/1Y spot, 1Y/2Y/5Y/10Y instantaneous forwards, 10Y-1Y spread |
| `manifests/ecb_bsi_candidates.yml` | 3 | Monetary aggregates — M1/M2/M3 annual growth rates |
| `manifests/ecb_icp_candidates.yml` | 2 | HICP — headline and core (ex energy/food) annual rates |

Still shipped **inactive**: `ecb_est_candidates.yml` (€STR),
`ecb_eon_candidates.yml` (EONIA, discontinued 2021-12-31),
`ecb_lfsi_candidates.yml` (euro area unemployment rate).

⚠️ The per-series table below is **no longer exhaustive** — it documents the
original 20 `ecb_rates.yml` series. Three entries in that manifest are known
to fail live (`ECB:FM:M.U2.EUR.RT.MM.EURIBOR12MD_.HSTA`,
`ECB:ICP:M.U2.N.000000.4`, `ECB:FM:D.U2.EUR.4F.KR.ESTR.LEV`) — unverified
guesses from commit `c6a7bbb` that 404 on every run. The `ICP` one is
superseded by the verified `ECB:ICP_PUB:M.U2.N.000000.4.ANR` in
`ecb_icp_candidates.yml`.

### Original `ecb_rates.yml` series

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
