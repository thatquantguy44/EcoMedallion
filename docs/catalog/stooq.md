# Stooq Catalog

Source key: `stooq`

Upstream: Stooq per-ticker daily CSV (`https://stooq.com/q/d/l/?s=<symbol>&i=d`
→ `Date,Open,High,Low,Close,Volume`). Free, keyless, no account. This is the
pipeline's **price-return** equity source — split-adjusted close, **no
dividends** (total return needs the Tiingo half). Coverage is broad
(thousands of US tickers) at essentially no rate cost.

Authentication: keyless.

Series id convention: ticker plus which OHLCV field the series carries —
unlike Tiingo's bare-ticker convention, Stooq explodes at the manifest level,
not the fetch level, the same composite-id pattern Treasury
(`<dataset>:<field>`) and World Bank (`<country>:<indicator>`) use:

```text
AAPL:close     SPY:close     AAPL:volume
└tk┘ └field┘
```

`field` defaults to `close` (`AAPL` alone means `AAPL:close`). US tickers get
the `.us` market suffix Stooq expects appended automatically
(`fred_pipeline/sources/stooq.py`).

## Current Series (89 total, 0 active)

Manifest: `manifests/equity_stooq.yml`, shipped **entirely inactive** by
design — it's the price-return counterpart to Tiingo's total-return series,
activated only when doing the cross-source price reconciliation
(`gold.equity_price_reconciliation`). Its composition is **not** a 1:1
mirror of `equity_tiingo.yml`, despite both being "the equity universe":

- **61 tickers overlap** with Tiingo's active list (mostly the ETF coverage —
  see `docs/catalog/tiingo.md` for the group breakdown).
- **28 tickers are Stooq-only**: individual large-cap stocks Tiingo's
  manifest doesn't carry at all — AAPL, MSFT, GOOGL, AMZN, META, NVDA, TSLA,
  JPM, BAC, GS, XOM, CVX, UNH, NFLX, and a set of newer/higher-beta names
  (COIN, MSTR, PLTR, RIVN, LCID, SMCI, UPST, AFRM, HOOD, CVNA, AMC, GME,
  BBBY, DJT).
- **24 tickers are Tiingo-only**: mostly niche/regional ETFs (EWA, EWL, EWQ,
  EWT, EWW, EWY, GDX, GDXJ, PFF, HYD, ITA, MDY, QUAL, SCHD, SPLV, USMV,
  VLUE, VNQI, VXUS, VYM, XME, XRT, FLOT, BSV) that have no Stooq counterpart
  in this manifest.

## Discovery

No `discover-stooq` tool exists. Given the composition above, coverage isn't
simply "track Tiingo's list" — the individual-stock set here looks like it
was curated independently (large caps + higher-beta/momentum names), while
the ETF set does track Tiingo's core list. Growing either side means picking
tickers Stooq covers (broad — thousands of US names) and hand-writing a
manifest row; there's no discovery client to automate it.

## Caveats

- `redistribution_allowed: false`, `commercial_use_allowed: false` in
  `config/data_licensing.yml` — no published API terms of use were found for
  programmatic/bulk access; conservative defaults until someone reviews
  Stooq's actual terms directly.
- No dividends in this feed — don't use Stooq series for total-return
  calculations; that's what the Tiingo half of the pair is for.
