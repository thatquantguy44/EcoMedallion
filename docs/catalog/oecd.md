# OECD Catalog

Source key: `oecd`

Upstream: OECD public SDMX REST API (`https://sdmx.oecd.org/public/rest`).

Authentication: keyless.

Series id convention: OECD's REST path needs three coordinates — the owning
agency, the dataflow, and the series key — so the manifest id carries all
three:

```text
OECD:OECD.SDD.STES:DSD_STES@DF_CLI:USA.M.LI...AA...H
└sr┘ └── agency ──┘ └─ dataflow ──┘ └──── key ─────┘
```

The agency is part of the id because it is **not** always `OECD` — the same
catalogue serves flows owned by `ESTAT`, `IAEG-SDGs`, and per-directorate
agencies like `OECD.SDD.STES`. Keys use dots, so the colon split is
unambiguous.

## Current Series (10 active)

Manifest: `manifests/oecd_cli.yml` — 10 Composite Leading Indicator series
(G7, G20, US, China, Germany, Japan, UK, France, India, Brazil). Shipped
inactive 2026-09-12 per `specs/spec006`'s acceptance criteria, then activated
2026-09-14 in a separate, deliberate commit as that spec required.

CLI is amplitude-adjusted and normalised so **100 = long-run trend** — above
100 signals above-trend momentum, below 100 below-trend. It's genuinely
additive: the pipeline has no cross-country leading-indicator panel today.

## Discovery

No dedicated `discover-oecd` command exists, but **one isn't needed to
browse the catalogue** — OECD serves SDMX 2.1, the same dialect the ECB does,
so `ecb_discovery.py`'s structure parser reads it directly:

```python
from fred_pipeline.catalogs.ecb_discovery import parse_dataflows_xml
import requests
xml = requests.get("https://sdmx.oecd.org/public/rest/dataflow",
                   headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"}).text
flows = parse_dataflows_xml(xml)   # 1,546 dataflows as of 2026-09-12
```

To find which keys actually publish for a dataflow, query with an empty
dimension slot rather than guessing — the same technique that worked for ECB:

```bash
# every REF_AREA that publishes CLI, for one month
curl -H "Accept: application/vnd.sdmx.data+csv;version=1.0.0" \
  "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI,/.M.LI...AA...H?startPeriod=2025-06&endPeriod=2025-06"
```

That returned exactly 22 areas — including the aggregates `G7`, `G20`,
`G4E` (four big European), `A5M` (major five Asia), and `NAFTA`. Note that
plausible-looking codes like `OECD` and `EA20` are **not** among them and
404; this is the same "structurally plausible ≠ actually published" trap
documented for ECB in `docs/catalog/ecb_candidate_flows.md`.

## Caveats

- **No vintages.** OECD's SDMX CSV carries no `VALID_FROM`/`VALID_TO`, so
  `realtime_start`/`realtime_end` are always empty and manifests ship
  `vintage_enabled: false`. Point-in-time queries over OECD data resolve to
  latest-revised only — unlike FRED/SEC, which carry real vintages.
- **Dataflow versions roll.** The client deliberately sends an empty version
  segment (`agency,dataflow,`) so OECD serves the current version;
  `DSD_STES@DF_CLI` answered as `4.1` when the client was written. Pinning a
  version would silently 404 on the next bump.
- **The legacy endpoint is dead.** `stats.oecd.org/restsdmx` 404s — anything
  written against older OECD documentation will not work.
- `redistribution_allowed: false` in `config/data_licensing.yml`. That's a
  deliberate project posture (internal use only, `specs/spec006` decision #3),
  not a finding that OECD forbids redistribution. Revisit with a primary terms
  read before anything OECD-derived goes outside.
