# ECB Catalog

Source key: `ecb`

Upstream: European Central Bank Data Portal SDMX API.

Authentication: keyless.

Series id convention:

```text
ECB:<flow_ref>:<key>
```

Current manifest: `manifests/ecb_rates.yml`

Current active ECB series: 9

## Current Series

| Series ID | Flow | Frequency | Units | Gold category | Description |
|---|---|---:|---|---|---|
| `ECB:EXR:D.USD.EUR.SP00.A` | `EXR` | d | USD per EUR | FX | ECB daily euro foreign exchange reference rate for the US dollar. |
| `ECB:EXR:D.GBP.EUR.SP00.A` | `EXR` | d | GBP per EUR | FX | ECB daily euro foreign exchange reference rate for pound sterling. |
| `ECB:EXR:D.JPY.EUR.SP00.A` | `EXR` | d | JPY per EUR | FX | ECB daily euro foreign exchange reference rate for the Japanese yen. |
| `ECB:EXR:D.CHF.EUR.SP00.A` | `EXR` | d | CHF per EUR | FX | ECB daily euro foreign exchange reference rate for the Swiss franc. |
| `ECB:EXR:D.CNY.EUR.SP00.A` | `EXR` | d | CNY per EUR | FX | ECB daily euro foreign exchange reference rate for the Chinese yuan renminbi. |
| `ECB:EXR:D.CAD.EUR.SP00.A` | `EXR` | d | CAD per EUR | FX | ECB daily euro foreign exchange reference rate for the Canadian dollar. |
| `ECB:EXR:D.AUD.EUR.SP00.A` | `EXR` | d | AUD per EUR | FX | ECB daily euro foreign exchange reference rate for the Australian dollar. |
| `ECB:FM:M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA` | `FM` | m | Percent | RATES | Monthly 3-month EURIBOR money-market rate for the euro area. |
| `ECB:YC:B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y` | `YC` | d | Percent | RATES | ECB euro area 10-year yield curve spot rate. |

## Why These Series

- The FX rows extend the initial USD per EUR starter to a small set of major
  euro reference rates that were live-probed successfully.
- The EURIBOR row adds a euro money-market rate with a compact monthly cadence.
- The yield-curve row adds a daily euro area 10-year rate for cross-market rate
  comparisons.

## Discovery Areas

`specs/spec002` tracks the buildout for ECB metadata discovery. Promising flows:

- `EXR` and `EXR_PUB`: exchange rates.
- `YC` and `YC_PUB`: euro area yield curve.
- `RIR`: retail interest rates.
- `JDF_MIR_MFI_INTEREST_RATES`: MFI and bank interest rates.
- `JDF_BSI_MFI_BALANCE_SHEET`: MFI balance sheet levels.
- `JDF_BSI_MFI_GROWTH_RATES`: MFI balance-sheet growth rates.
- `MOBILE_BSI`: euro area monetary aggregates.

## Caveats

- Do not generate broad active manifests from SDMX dimensions without bounded
  candidate generation and live smoke tests.
- ECB rows are keyless, but requests still use the shared retry and rate-limit
  transport.
