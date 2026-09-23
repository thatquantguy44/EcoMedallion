# IMF Catalog

Source key: `imf`

Upstream: IMF's public SDMX 3.0 REST API (`https://api.imf.org/external/sdmx/3.0`).

Authentication: keyless (unconfirmed — see caveats below).

## ⚠️ Status: client built, UNVERIFIED against a live response

This is the one source client in this pipeline built without ever seeing
real output from its own upstream. Every other source (`ecb`, `oecd`,
`bis`, even `french`'s decades-stable CSV) was either live-verified during
construction or built against a format old and public enough that the risk
was low. Neither is true here: `api.imf.org` has been blocked in every
environment this repo has been worked from — the exact same class of block
recorded on `bis.org` and `mba.tuck.dartmouth.edu` elsewhere in this
catalogue.

`src/fred_pipeline/sources/imf.py` is built against the **public SDMX-JSON
2.0.0 data-message specification** — a real, versioned, documented
interchange format, not a blind guess — but IMF's own dialect (`structures`
vs. `structure`, the index-key separator, which agency actually maintains a
given dataflow) has known real variation across SDMX-JSON providers and has
never been checked against IMF specifically.

**No manifest ships with this source, inactive or otherwise.** Every other
new source in this pipeline ships a manifest (even fully `active: false`)
as its second step. This one doesn't, deliberately: a plausible-looking
`series_id` here would look vetted when the underlying decoding logic
isn't — the same "structurally plausible ≠ actually published" trap this
catalogue already documents for ECB and OECD, one level further out (there
it's about which series is real; here it's about whether the *format
itself* is real).

## Current Series (0 active)

No manifest exists for this source yet — see "What actually needs to happen
before this is trustworthy" below for why.

## Series id convention (once one is used)

Mirrors OECD's own multi-part-id worked example (`specs/spec006`,
`docs/instructions/adding_a_source.md`): IMF's REST addressing needs an
agency, a dataflow, and a key, so a manifest `series_id` would be:

```text
IMF:IMF.STA:COFER:USA.Q.RES_USD
└sr┘ └── agency ──┘ └flow┘ └── key ──┘
```

The agency segment exists because it is not always literally `IMF` — the
same reasoning `oecd.py`'s series id carries an agency for exactly this
reason.

## What actually needs to happen before this is trustworthy

1. Run `scripts/probe_imf_dataflows.py --sample-dataflow <id> --out-dir ...`
   from an environment with real network access to `api.imf.org`. This
   captures a real dataflow-list response *and* attempts a real data query
   (extended for exactly this purpose — see the script's own docstring).
2. Compare the captured response against what `sources/imf.py`'s
   `_structures` / `_observation_period_values` / `_data_series` assume.
   Fix whatever doesn't match — dialect differences are the expected
   outcome here, not a surprise.
3. Only then write a manifest with real dataflow/key values, shipped
   `active: false` per this repo's standard convention, and update
   `config/data_licensing.yml`'s `imf` entry with a real terms-page read.

## Caveats

- **Unverified data-query decoding.** See above. `normalize_imf_observations`
  raises loudly (`IMFAPIError`) rather than guessing when a response doesn't
  match a known shape — including when there's more than one observation
  dimension, or the query resolves to more than one series — so a real,
  wrong response fails visibly instead of silently mis-decoding.
- **Unverified structure-endpoint dialect.** `scripts/probe_imf_dataflows.py`
  recorded a live finding on 2026-09-12 (191 dataflows, 77 stable / 114
  vintage-rotating), but the raw response was never saved — so even that
  finding isn't independently reproducible from this repo alone yet.
- **`redistribution_allowed: false` and `commercial_use_allowed: false`** in
  `config/data_licensing.yml`. Internal-use-only default posture for an
  unverified source (`specs/spec006` decision #3), not a finding about
  IMF's actual terms.
- **No vintages assumed.** Like OECD/BIS/most non-`_PUB` ECB flows,
  `realtime_start`/`realtime_end` are always empty in this client's decoding
  — unconfirmed, like everything else here, but consistent with every other
  SDMX-JSON/CSV source in this pipeline that isn't FRED or SEC.
