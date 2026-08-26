# SEC Catalog

Source key: `sec`

Upstream: SEC EDGAR XBRL `companyconcept` API (`data.sec.gov`).

Authentication: no key — a **required descriptive User-Agent** header
instead (SEC returns HTTP 403 without one, live-verified). `sec_user_agent`
defaults to an email-shaped placeholder that satisfies the check out of the
box; set `SEC_USER_AGENT` to your own real contact for polite real use (see
`config/config.example.yaml`).

Series id convention: one XBRL concept for one company:

```text
CIK0000320193:us-gaap/Assets:USD
│             │              └ unit
│             └──────────────── taxonomy/tag
└────────────────────────────── zero-padded CIK (CIK##########)
```

Each filing's `filed` date becomes `realtime_start`, so restatements and
amendments are captured as genuine point-in-time vintages
(`vintage_enabled: true`) — SEC is the one source that exercises the
pipeline's point-in-time machinery for reasons other than FRED-style data
revisions.

Duration handling: income-statement concepts are *duration* facts, and a
single 10-Q reports both the ~3-month quarterly and ~9-month YTD figure for
the same period end. `normalize_sec_observations` keeps only facts matching
the target duration (`SEC_PERIOD`, default `quarterly`) so they don't collide
on the natural key; balance-sheet **instant** concepts (`Assets`,
`StockholdersEquity`, ...) have no `start` and are always kept.
Standardization into canonical statements + ratios lives in
`fred_pipeline.sec_standardization`.

## Current Series (3 active, one company)

| Series ID | Frequency | Category | Description |
|---|---:|---|---|
| `CIK0000320193:us-gaap/Assets:USD` | q | company_financials | Apple Inc. — Total Assets. |
| `CIK0000320193:us-gaap/StockholdersEquity:USD` | q | company_financials | Apple Inc. — Stockholders Equity. |
| `CIK0000320193:us-gaap/NetIncomeLoss:USD` | q | company_financials | Apple Inc. — Net Income (quarterly). |

Manifest: `manifests/sec_financials.yml`.

## Discovery

No `discover-sec` tool exists. Adding a company means finding its CIK (SEC's
[EDGAR company search](https://www.sec.gov/cgi-bin/browse-edgar)) and picking
US-GAAP taxonomy tags from its own filings; adding a concept for an
already-covered company is just a new manifest row with the same CIK.

## Caveats

- Public domain (17 U.S.C. 105, U.S. federal government work) — EDGAR's
  fair-access policy governs bulk-fetch rate/User-Agent requirements, not
  reuse of the disclosed financial data itself; see
  `config/data_licensing.yml`.
- Not every company reports every tag every period (voluntary disclosure,
  taxonomy changes across years) — a gap in the series is often real absence
  of that fact for that period, not a pipeline bug.
