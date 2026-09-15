# Kenneth French Data Library Catalog

Source key: `french`

Upstream: the Kenneth French Data Library
(`mba.tuck.dartmouth.edu/pages/faculty/ken.french`) — the standard academic
source for Fama-French factor returns. Distributed as ZIP archives, each
containing one CSV.

Authentication: keyless.

Series id convention: a manifest `series_id` is the **bare dataset name**
(e.g. `F-F_Research_Data_Factors`) — the same "fetch key, not the output
series id" shape as Tiingo's bare ticker. One fetch downloads that dataset's
ZIP once; `normalize` explodes it into one scalar series per factor column:

```text
F-F_Research_Data_Factors:Mkt-RF
└──────── dataset ────────┘└factor┘
```

## Current Series (0 active)

Manifest: `manifests/french_factors.yml` — 3 datasets, **shipped inactive**:

| Dataset | Exploded factors |
|---|---|
| `F-F_Research_Data_Factors` | `Mkt-RF`, `SMB`, `HML`, `RF` (the classic 3-factor set) |
| `F-F_Research_Data_5_Factors_2x3` | `Mkt-RF`, `SMB`, `HML`, `RMW`, `CMA`, `RF` (its own 2x3-sort construction, not the same series as the 3-factor set's) |
| `F-F_Momentum_Factor` | `Mom` |

Values are percentages as published (e.g. `2.96` means +2.96%), matching
this pipeline's existing convention of storing raw source units rather than
converting to a decimal fraction.

This is genuinely additive: `gold.equity_factor_attribution` and
`gold.equity_factor_implied_return` exist today with no authoritative factor
input (`specs/spec006` §2) — this is that input. Activation, and wiring
either Gold table to actually consume it, is a separate, deliberate step
this manifest does not take.

## File layout this client parses

Each dataset's CSV is: a one-line text preamble, a **monthly** table
(header `,Mkt-RF,SMB,HML,RF`, then rows keyed by a 6-digit `YYYYMM`), a
blank line, then an "Annual Factors" table keyed by 4-digit years. The
client (`src/fred_pipeline/sources/french.py`) reads only the monthly table
— the annual table and the library's separate daily-CSV files are a
follow-up, not this first build slice (`specs/spec006` §5.3/§6.3/§7).

## Caveats

- **⚠️ Not live-verified.** Unlike `oecd_cli.yml`, this manifest and client
  were built without a live fetch against the real upstream: this session's
  environment blocks egress to `mba.tuck.dartmouth.edu` (recorded in
  `config/data_licensing.yml`'s `french` entry, the same class of block
  already recorded there for `bis.org`). Confirm the parser against a real
  downloaded ZIP before activating anything here.
- **No vintages.** The library carries no revision history for a given
  print, so `realtime_start`/`realtime_end` are always empty and manifests
  ship `vintage_enabled: false` — same as OECD/BIS/World Bank.
- **`redistribution_allowed: false` and `commercial_use_allowed: false`** in
  `config/data_licensing.yml`. That's the project's internal-use-only
  default posture for an unverified source (`specs/spec006` decision #3),
  not a finding about the library's actual terms — nobody has done a
  primary terms read yet (see the entry's `source_of_truth`).
- **5-factor vs. 3-factor overlap is intentional.** `F-F_Research_Data_5_Factors_2x3`'s
  own `Mkt-RF`/`SMB`/`HML` use a different portfolio construction (2x3 sorts)
  and a shorter sample (1963-07 onward) than `F-F_Research_Data_Factors`'s —
  they are two real, distinct series per factor name, not a duplicate to
  dedupe away.
