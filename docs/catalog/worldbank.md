# World Bank Catalog

Source key: `worldbank`

Upstream: World Bank Indicators API (`https://api.worldbank.org`).

Authentication: keyless.

Series id convention: a (country, indicator) pair, separated by the first
colon:

```text
USA:NY.GDP.MKTP.CD      WLD:SP.POP.TOTL
└cty┘ └── indicator ──┘
```

Two response quirks handled in `fred_pipeline/sources/worldbank.py`: the
response is a **top-level JSON array** `[meta, data]`, not an object; and a
bad request returns **HTTP 200** with a single-element `[{"message": ...}]`
body, surfaced as an error rather than a normal HTTP failure.

## Current Series (37 active)

All annual (`a`), `category: international`. Manifest:
`manifests/worldbank_global.yml`.

| Indicator | Countries covered |
|---|---|
| `NY.GDP.MKTP.CD` — GDP, current US$ | United States only |
| `SP.POP.TOTL` — Population, total | World only |
| `FP.CPI.TOTL.ZG` — CPI inflation YoY % | 35 countries/areas: `EMU` (euro area), GBR, JPN, CAN, MEX, BRA, CHN, IND, KOR, AUS, DEU, CHE, ESP, TUR, IDN, ZAF, SAU, ARG, CHL, COL, PER, POL, RUS, SWE, NOR, NZL, VNM, THA, MYS, PHL, SGP, HKG, ISR, EGY, NGA — note the US itself isn't in this list; FRED already covers US CPI directly |

The CPI inflation indicator is the bulk of this source's coverage — a
country-by-country cross-check set that complements ECB/FRED's own inflation
series rather than duplicating them (World Bank's figure is World
Bank-computed, not each country's own statistical office).

## Discovery

No `discover-worldbank` tool exists. The [World Bank Indicators
catalog](https://data.worldbank.org/indicator) lists thousands of indicator
codes across dozens of topics (trade, health, education, environment, ...);
extending coverage means picking an indicator code from there and a country
(or aggregate) code from the API's own country list, then hand-writing a
manifest row — there's no discovery client here to automate it.

## Caveats

- **Attribution required on redistribution** — CC BY 4.0 per the World Bank
  Open Data terms, unlike the U.S. federal sources in this catalog where
  attribution is a preference, not a legal requirement. See
  `config/data_licensing.yml`.
- `review_status: provisional` — the CC BY 4.0 determination was taken from a
  widely-published summary, not a primary read of the terms page; a human
  should confirm `terms_url` before relying on this for external
  redistribution.
- Flagged by the governance sentinel (`tests/test_governance.py`) alongside
  `bis` and `ecb` as an unverified-but-redistributable source — expected, not
  a bug.
- Annual frequency only in current coverage — don't expect intra-year
  updates; these move once a year at most.
