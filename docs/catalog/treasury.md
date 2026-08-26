# Treasury Catalog

Source key: `treasury`

Upstream: US Treasury Fiscal Data API (`https://fiscaldata.treasury.gov`).

Authentication: keyless (no key, no account).

Series id convention: the Fiscal Data API exposes *datasets* (e.g.
`debt_to_penny`) keyed by `record_date`, not a flat series catalog, so a
manifest `series_id` encodes both the dataset path and the value column,
separated by the **last** colon:

```text
v2/accounting/od/debt_to_penny:tot_pub_debt_out_amt
└────────── dataset path ─────────┘ └──── value field ────┘
```

## Current Series (2 active)

| Series ID | Frequency | Category | Description |
|---|---:|---|---|
| `v2/accounting/od/debt_to_penny:tot_pub_debt_out_amt` | d | fiscal | Total Public Debt Outstanding (Debt to the Penny). |
| `v2/accounting/od/debt_to_penny:debt_held_public_amt` | d | fiscal | Debt Held by the Public (Debt to the Penny). |

Manifest: `manifests/treasury_fiscal.yml`.

## Discovery

No `discover-treasury` tool exists. Fiscal Data publishes dozens of other
datasets (e.g. auction results, interest expense, exchange rates) under the
same `v2/...` path convention; adding one is a matter of picking the dataset
path + value column and hand-writing a manifest entry — there's nothing to
programmatically enumerate beyond browsing
[fiscaldata.treasury.gov's API docs](https://fiscaldata.treasury.gov/api-documentation/).

## Caveats

- Public domain (17 U.S.C. 105, U.S. federal government work) — no
  attribution or commercial-use restriction; see `config/data_licensing.yml`.
- The API paginates at 10,000 rows/request (`PAGE_SIZE` in
  `fred_pipeline/sources/treasury.py`); the client handles this transparently.
- No vintage/point-in-time tracking — Treasury data doesn't carry a comparable
  revision concept the way FRED/SEC do.
