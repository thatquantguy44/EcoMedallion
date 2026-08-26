# Tiingo Catalog

Source key: `tiingo`

Upstream: Tiingo daily-prices endpoint (free tier). The only free tier that
returns dividend cash amounts per call, which is why it's the pipeline's
**total-return** equity source (`gold.equity_total_return_index`) —
`Stooq` is the price-return counterpart, no dividends.

Authentication: `api_key` (free account), required. `TIINGO_API_KEY` /
`tiingo_api_key` — accepts a single string or a list (primary + backup keys)
for quota rotation, see `_normalize_tiingo_keys` in `fred_pipeline/pipeline.py`.

Series id convention: a manifest `series_id` is the **bare ticker** (e.g.
`AAPL`) — unlike Stooq's one-field-per-series-id convention. One fetch
explodes into several scalar Silver series: `<ticker>:close`,
`<ticker>:divCash`, `<ticker>:splitFactor`, `<ticker>:adjClose`
(`EXPLODE_FIELDS` in `fred_pipeline/sources/tiingo.py`) — `close`/`divCash`/
`splitFactor` are the raw inputs the total-return engine reconstructs from;
`adjClose` is Tiingo's own split+dividend-adjusted close, kept for
reconciliation. A ~500-name core list is ~500 requests/day, not 500×fields,
since one fetch covers all four.

⚠️ Tiingo's daily endpoint does **not** return full history when `startDate`
is omitted — it silently defaults to only the most recent trading day, unlike
FRED. The pipeline's full-load contract handles this explicitly (see the
module docstring in `sources/tiingo.py`).

## Current Series (85 active tickers, all `category: equity`)

Manifest: `manifests/equity_tiingo.yml`. Also the seed list for
`price-constituents` (dynamic ETF-holdings pricing) — see the README's "Grow
ETF constituent coverage" workflow.

| Group | Tickers |
|---|---|
| Broad US market | SPY, VTI, QQQ, IWM, DIA, RSP, MDY |
| US style/factor | VTV, VUG, MTUM, VLUE, QUAL, USMV, SPLV, VYM, SCHD |
| US sectors & thematic | XLK, XLF, XLE, XLY, XLV, XLI, XLP, XLU, XLB, XLRE, XLC, SMH, XBI, KRE, KBE, ITA, XME, XRT, ARKK |
| Fixed income | AGG, TLT, IEF, SHY, TIP, BIL, LQD, EMB, JNK, BKLN, MBB, MUB, BSV, HYD, FLOT, HYG |
| Commodities & metals | SLV, USO, DBC, DBA, CPER, GLD, GDX, GDXJ |
| Currency & volatility | VIXY, UUP, FXE, FXY |
| International & regional | VGK, EWJ, ACWI, EWC, EWU, INDA, FXI, EWZ, EWG, EEM, EFA, VXUS, EWY, EWW, EWT, EWA, EWL, EWQ |
| Real estate | VNQ, VNQI |
| Preferred | PFF |
| Crypto | IBIT |

## Discovery

No `discover-tiingo` tool exists. Growing this list is either manual curation
(any US-listed ticker Tiingo covers works) or driven by `IVV`'s live holdings
via `price-constituents --dry-run` (see `docs/catalog/ishares.md`) — the two
paths converge in `gold.index_constituents`.

## Caveats

- **Personal/non-commercial use only.** `redistribution_allowed: false`,
  `commercial_use_allowed: false` per the free tier's terms of service
  (decided in `docs/handoffs/completed/handoff.md`) — a paid plan would be
  needed for commercial use. See `config/data_licensing.yml`.
- Metered free tier — the daily-request quota (not the unique-symbol cap) is
  the binding constraint under per-ticker fetching; `--rate-limit-per-minute`
  and multi-key rotation (`tiingo_api_key` as a list) exist for this.
