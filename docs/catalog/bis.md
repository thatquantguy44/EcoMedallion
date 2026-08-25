# BIS Catalog

Source key: `bis`

Upstream: BIS Central Bank Policy Rates dataflow (`WS_CBPOL`), served as
SDMX CSV. A single-purpose dataset keyed by monthly frequency and reference
area — not a general BIS statistics client.

Authentication: keyless.

Series id convention: the BIS reference area with a stable source prefix:

```text
BIS:GB      BIS:JP      BIS:XM
└src┘└ref_area┘
```

Bronze keeps the raw CSV body in a small JSON envelope; Silver normalization
reads `TIME_PERIOD`/`OBS_VALUE`. The feed carries current values only — no
vintages.

## Current Series (36 active — central bank policy rates)

All monthly, `category: rates`, one series per country/area, title pattern
`Policy Rate (<Country>)`. Manifest: `manifests/bis_policy_rates.yml`.

Americas: `BIS:CA` `BIS:MX` `BIS:BR` `BIS:AR` `BIS:CL` `BIS:CO` `BIS:PE`

Europe: `BIS:GB` `BIS:CH` `BIS:CZ` `BIS:DK` `BIS:HU` `BIS:IS` `BIS:MK`
`BIS:SE` `BIS:NO` `BIS:PL` `BIS:RO` `BIS:RS` `BIS:TR` `BIS:RU`

Middle East / Africa: `BIS:KW` `BIS:ZA` `BIS:SA` `BIS:IL`

Asia-Pacific: `BIS:JP` `BIS:CN` `BIS:IN` `BIS:KR` `BIS:AU` `BIS:NZ` `BIS:ID`
`BIS:TH` `BIS:MY` `BIS:PH` `BIS:HK`

(36 codes total; run `python -m fred_pipeline validate` for the exact current
list if this drifts.)

5 further reference areas are inactive in the same manifest — `BIS:NG`
(Nigeria), `BIS:EG` (Egypt), `BIS:VN` (Vietnam), `BIS:TW` (Taiwan), `BIS:SG`
(Singapore) — all live-verified 404 in `WS_CBPOL` as of 2026-07-31, kept
cataloged so they can be rechecked without failing normal source runs.

## Discovery

No `discover-bis` tool exists, and BIS publishes many other dataflows beyond
`WS_CBPOL` (credit statistics, exchange rates, effective exchange rates,
property prices, ...) that this client doesn't touch at all — it's
purpose-built for one dataflow. Extending to another BIS dataflow means a new
client method (or a new source), not a manifest-only change.

## Caveats

- **Commercial use is NOT covered.** `license_type: attribution-noncommercial`
  in `config/data_licensing.yml` — BIS permits redistribution for
  non-commercial purposes with attribution, but commercial use needs BIS's
  own permission. `validate --commercial` flags this source for exactly that
  reason.
- `review_status: provisional` — the primary terms page couldn't be fetched
  from the environment this entry was written in (egress to bis.org was
  blocked); a human should re-read `terms_url` and bump `review_status` to
  `verified` before relying on this for a real compliance decision.
- Flagged by the governance sentinel (`tests/test_governance.py`) alongside
  `ecb` and `worldbank` as an unverified-but-redistributable source — this is
  the check doing its job, not a bug to silence.
