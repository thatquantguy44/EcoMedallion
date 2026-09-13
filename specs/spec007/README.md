# Spec 007: Due-Date Gating on Extraction

Status: design decided (§5, §10), implementation not started
Last verified: 2026-09-13
Primary owner: TBD
Target: `Pipeline.run()` (`src/fred_pipeline/pipeline.py`) — every `run`
invocation, local and Spark alike.

## 1. Goal

A routine `run --local` currently spends 42.6 minutes extracting 2,821
series, every single time, regardless of whether any of them are actually
due for a new observation. `Pipeline.run(specs, ...)` restates every spec
passed to it on every invocation — nothing consults each series' own
publish cadence. Most FRED-family series are not daily: monthly and
quarterly economic indicators are the majority of a typical manifest set
(§2 below has the real counts from this repo's manifests), so on any given
day the overwhelming majority of a routine run's extraction work is
re-pulling a series whose upstream source has not published anything new
since the last successful pull.

The goal is to skip a series' extraction entirely when it is not yet due,
using data that mostly already exists (`expected_update_frequency`, a
metadata field already populated for most manifest entries but never read
for this purpose), cutting routine extraction time roughly in proportion to
the fraction of series that are monthly/quarterly/annual rather than daily.

This was found while researching
[`specs/spec003`](../spec003/README.md) Phase 3 (Incremental Gold): the
same "nothing gates which series get touched" fact that ruled out
whole-series skipping for Gold rebuilds applies just as much upstream, at
the extraction stage — except here nothing has replaced it with a working
alternative yet. Fixing this compounds with spec003: fewer series touched
per run also means more of Phase 3's per-entity Gold checkpoints get
skipped rather than appended-to, and reopens two tables spec003 left
deferred specifically because "most series get touched most runs" made
per-entity skipping there low-value (see `docs/handoffs/pipeline_performance.md`,
"Phase 3: what's left, and why it's parked").

## 2. Current State (Evidence)

- `Pipeline.run(specs, ...)` (`pipeline.py:433`) takes the full list of
  specs and processes every one, unconditionally:
  `plans = [self._plan_extract(spec, force_full=force_full) for spec in specs]`
  (`pipeline.py:470`, inside the `with tracker.stage("plan")` block,
  `pipeline.py:464-472`). No filtering happens before this.
- `_plan_extract` (`pipeline.py:828-850`) decides the *load window* for a
  series already known to run (full vs. `restate_last_n`-scoped) — it has no
  concept of "should this series run at all this time."
- `expected_update_frequency` exists as a real, mostly-populated field, not
  a theoretical one:
  - Schema: `meta_fred_series.expected_update_frequency TEXT`
    (`local_store.py:155`); `SeriesSpec.expected_update_frequency: str = ""`
    (`catalogs/manifest.py:80`).
  - Populated by discovery-generated manifests too, not just hand-written
    ones: `bls_discovery.py:316-324` and `ecb_discovery.py:567+` both map a
    source's native frequency code (`d`/`w`/`m`/`q`/`sa`/`a`) to
    `expected_update_frequency` when generating candidate manifests.
  - Real coverage, counted directly from `manifests/*.yml` in this repo:
    3,003 `series_id:` entries total, 2,513 have `expected_update_frequency:`
    set (including some explicitly set to `''`); the taxonomy actually in
    use is `{daily, business_daily, weekly, monthly, quarterly, annual}`
    (`semiannual` is supported by the BLS-discovery mapping but not
    currently used by any manifest). The remaining ~490 entries with no
    field at all get the dataclass default `""`.
  - It is genuinely dead code for gating today — confirmed by exhaustive
    grep across `src/fred_pipeline/`: the field is written by the three
    call sites above and read nowhere except passthrough into
    `meta_fred_series` rows (`catalogs/meta.py:23`).
- No "last successful pull" timestamp is tracked per series today outside
  of what's implicit in Silver itself. `silver_fred_observation.ingested_at`
  is set at merge time (`data/transform.py`'s `normalize_observations`,
  `ingested_at = ingested_at or _utc_now_iso()`), so `MAX(ingested_at)`
  grouped by `series_id` is a real, already-correct proxy for "when was this
  series last actually written to" — and (per spec003's Phase 3 work)
  `merge_silver`'s upsert now only advances `ingested_at` on a genuine
  `row_hash` content change, not a byte-identical re-pull, so this
  timestamp will not already be flooded with "attempted but unchanged" noise
  the way it might have been before that fix landed.
- No index exists on `silver_fred_observation(series_id, ingested_at)` or
  `(ingested_at)` scoped for a `MAX(...) GROUP BY series_id` aggregate over
  all series at once — `ix_silver_obs_ingested_at` (added for spec003 Phase
  3) covers `WHERE ingested_at > ?` well, but a `GROUP BY series_id`
  aggregate over 34M rows would still be a real scan without a composite
  `(series_id, ingested_at)` index. See §5.

## 3. Non-Goals

- **Not** about shrinking `restate_last_n` (the trailing-observation window
  pulled *once a series is judged due*) — separate, already-flagged
  spec003 follow-up ("Revisit whether `config.restate_last_n` ... is
  inflating per-run Silver churn more than necessary for daily series").
  This spec is about whether a series gets attempted *at all* this run, not
  how much history it pulls once it does.
- **Not** about per-source worker counts or rate limits
  (`--source-workers`/`--source-rate-limits`) — those already exist and are
  orthogonal.
- **Not** about backfill/replay semantics (`backfill.py`, `replay`) — both
  are explicit, deliberate full-history operations, not routine runs.
- **Not** a redesign of the manifest schema or the discovery tooling that
  populates `expected_update_frequency` — this spec consumes that field,
  it doesn't change how it's produced.
- **Not** extending gating to Gold (spec003 already covers that, via a
  different mechanism — row-range checkpointing, not skip/run gating,
  because Gold's problem is "how much history to recompute," not "should
  this series run at all").

## 4. Design Facts

- **A cadence-to-interval mapping is unavoidable and is the one piece of
  real domain judgment this spec requires.** `expected_update_frequency`
  names a *publication* cadence, not an *extraction* cadence — a monthly
  series doesn't need pulling exactly once a month, it needs pulling
  *at least* often enough that a new print is never missed for long, with
  slack for: the source's own publication being early/late, weekends and
  holidays (`daily` series don't publish on weekends — FRED and most
  sources simply don't produce a new observation those days, so a rigid
  "24 hours since last pull" rule would call a normal Friday-to-Monday gap
  "overdue" incorrectly), and clock/timezone skew between this pipeline's
  run schedule and the source's publish schedule.
- **A missing or unrecognized `expected_update_frequency` must default to
  "always due."** ~490 manifest entries have no value set at all today: a
  bug or gap in this mapping must never silently stop refreshing a series
  that would have refreshed correctly before this change existed. The
  existing behavior (always attempt) is the safe fallback, not an error.
- **A failed extraction must not count as a successful pull for gating
  purposes.** Since the due-date check is keyed off `MAX(ingested_at)` in
  Silver (only ever set on a successful merge), this falls out naturally —
  a series whose last attempt failed has no more recent `ingested_at` than
  its last real success, so it stays "due" and gets retried next run
  without any special-case logic.
- **`run --full` already exists and already means "ignore the restate
  watermark, re-pull everything"** (`cli.py:1019-1023`,
  `pipeline.py:392,429,440,470`, `_plan_extract`'s `force_full` branch).
  The natural, minimal-surface-area choice is for `--full` to *also* bypass
  due-date gating — one flag, one well-understood meaning ("stop being
  clever, just pull everything"), rather than a second flag with an
  overlapping but not-identical meaning. See the next bullet for the case
  against this default.
- **This changes run output/observability, not just timing.** A run that
  silently extracts 421 of 2,821 series with no explanation of the other
  2,400 would look broken to an operator used to today's "every series gets
  attempted" behavior. The skip decision needs to be visible in run output
  (a count, and ideally which series/why, at least at higher log
  verbosity), the same way spec003's `_STAGE_TIMING_ENABLED` env var and
  per-run JSON summaries make its own behavior legible.

## 5. Design Decisions

- ✅ **DECIDED (2026-09-13) — rollout: opt-in flag first.** Ship behind an
  explicit flag (`run --skip-not-due`) for at least one observation cycle
  across real manifests before flipping the default. Rationale: this changes
  behavior for every existing caller of `run` the moment it ships, and its
  correctness depends entirely on the cadence-to-interval mapping in §4
  being right for every series in every manifest — a mapping bug could
  under-refresh a series (stale data silently served). Validate against real
  data before it becomes the assumed path, not after.
- ✅ **DECIDED (2026-09-13) — `--full` bypasses gating; no dedicated flag.**
  Reuse the existing `--full` flag rather than introducing
  `--ignore-due-date` or similar. One flag, one well-understood meaning
  ("stop being clever, just pull everything"), minimal surface area. The
  case for a dedicated flag (expressing "pull full history for the 40 series
  I know changed, skip the other 2,800 that haven't") was considered and
  rejected as not worth the extra surface area for the first slice —
  revisit only if real usage shows that combination is actually needed.
- **Cadence → minimum re-check interval** — ✅ **DECIDED (2026-09-13):
  validate against real publish patterns before writing gating logic**, per
  §6 step 1 (don't assume the intervals below are correct from the label
  alone; cross-check a sample of daily/weekly/monthly manifest series
  against actual FRED/source publish history first — e.g. confirm "daily"
  series really are business-day-only, and that no "monthly" series
  publishes on an irregular day-of-month that a 27-day check would miss).
  The values below are the starting point for that validation, generous
  rather than tight, since a false "not due" is worse than a wasted pull —
  `daily`/`business_daily` → ~20 hours (catches a series that publishes
  once every business day without re-pulling mid-day); `weekly` → ~6 days;
  `monthly` → ~27 days; `quarterly` → ~85 days; `annual` → ~360 days;
  anything else (empty string, unrecognized value, `semiannual` since no
  manifest uses it yet) → always due. These are deliberately looser than
  the nominal cadence, not equal to it — being a day early costs nothing;
  being a day late misses a print until the next run.
- **Where the check lives**: inside `Pipeline.run()`'s existing "plan"
  stage (`pipeline.py:464-472`), as a filter applied to `specs` before
  `series_runs`/`plans` are built — not inside `_plan_extract` itself,
  which already has a different job (deciding the load window for a series
  known to run). A skipped series still gets a lightweight audit record
  (status "skipped — not due", not "succeeded" or "failed") so `run`'s own
  output and the audit trail both show *why* 2,400 series don't appear in
  the extraction summary, rather than looking like they were silently
  dropped.
- **Per-series last-pull source**: `MAX(ingested_at)` from
  `silver_fred_observation`, grouped by `series_id`, computed once per run
  (mirrors spec003's `_touched_series_since_watermark` shape: one query,
  not one per series). Needs a new composite index —
  `ix_silver_obs_sid_ingested` on `(series_id, ingested_at)` — since the
  existing `ix_silver_obs_ingested_at` (spec003) is shaped for "all series
  touched after a global watermark," not "last touch per series," and
  neither existing index (`ix_silver_obs_sid_rt`, `ix_silver_obs_sid_date`)
  has `ingested_at` in it at all.

## 6. Proposed Approach

1. **Confirm the cadence taxonomy and interval mapping against real data**
   before writing the gating logic — don't assume §5's starting intervals
   are correct. Cross-check a sample of `daily`/`weekly`/`monthly`
   manifest series against their actual FRED/source publish history (are
   "daily" series really business-day-only? do any "monthly" series
   publish on an irregular day-of-month that a 27-day check would miss?).
2. **Build `_series_is_due(expected_update_frequency, last_ingested_at,
   now) -> bool`** as a small, pure, independently unit-tested function —
   no `Pipeline`/warehouse dependency, so its date-math edge cases (missing
   value, unrecognized value, `last_ingested_at is None` meaning never
   pulled, weekend/holiday slack) can be tested directly without a full
   pipeline fixture.
3. **Wire into `Pipeline.run()`'s plan stage**: one `MAX(ingested_at) GROUP
   BY series_id` query (needs the new index from §5), filter `specs` before
   building `series_runs`, emit a per-run count
   (`"series_attempted": N, "series_skipped_not_due": M`) in the same
   place `result = {...}` JSON summaries already get built
   (`cli.py:949-955`'s shape is a good model — mirror it for the main `run`
   command's own summary, not just `price-constituents`'). Gate behind the
   rollout flag from §5's first open decision.
4. **Re-baseline extraction time** against real manifests with gating
   enabled, on a day where the mix of due/not-due series reflects normal
   operation (not immediately after a `--full` run, which would make
   everything look "not due" and understate the steady-state benefit).
5. **Revisit spec003's deferred tables** once real "how often is a series
   actually touched" data exists:
   `gold_series_lead_lag`/`gold_series_structural_breaks` and the
   recession-probability/equity-total-return-index tables were left
   full-rebuild specifically because "most series get touched most runs"
   made per-entity skipping low-value; if this spec changes that fact,
   those become worth a second look, and should reuse the per-entity
   skip/full-recompute split already designed for them rather than
   whole-entity Gold checkpointing.

## 7. Acceptance Criteria

- `_series_is_due` unit tests cover: no prior pull (always due), exactly at
  the boundary, just inside/outside the interval for each cadence value,
  missing/empty/unrecognized `expected_update_frequency` (always due), and
  the weekend-adjacent daily case.
- An integration test proves a series with a fresh `ingested_at` and a
  `monthly` cadence is skipped on a same-day re-run, and that a failed
  extraction (no `ingested_at` advance) leaves it due on the next run.
- `run`'s output/audit trail clearly distinguishes skipped-not-due from
  succeeded/failed — verified by a test asserting the summary counts add up
  (`attempted + skipped == total specs`).
- `run --full` (and, if built, the dedicated bypass flag from §5) is proven
  to touch every series regardless of due-date, via a test with a
  deliberately-fresh `ingested_at` that would otherwise be skipped.
- A real extraction timing comparison (gating on vs. off, same manifest
  set, same day) — this is the number that actually justifies the spec;
  don't close it out on unit tests alone.

## 8. Suggested First Implementation Slice

1. `_series_is_due` as a standalone, fully-tested pure function (step 2
   above) — land this alone first, proves the date-math is right before
   anything depends on it.
2. The new `(series_id, ingested_at)` index + the `MAX(...) GROUP BY`
   query as a standalone `LocalWarehouse` method, tested against a small
   fixture db, independent of wiring it into `run`.
3. Wire into `Pipeline.run()` behind the rollout flag from §5, with the
   audit-trail/summary changes from step 3 above. Stop here for a first
   PR — don't flip the default or touch spec003's deferred tables (step 5
   above) until real timing data exists.

## 9. Follow-Ups

- Once real due/not-due data exists, revisit spec003's deferred tables
  (§6 step 5 above already covers this — repeated here as a pointer since
  it's the main reason this spec was written from spec003's research in
  the first place, not a standalone concern).
- ✅ **DECIDED (2026-09-13):** backfilling `expected_update_frequency` for
  the ~490 manifest entries that don't have it set is a **separate,
  independent follow-up**, not part of this spec's first slice. Today an
  omission is harmless (defaults to "always due"); once gating ships it's
  still harmless correctness-wise, it just means those ~490 series get none
  of the benefit until backfilled later.
- `manifests/*.yml`'s `expected_update_frequency` values were counted by
  grep against the manifest files' declared, not-yet-generated state at
  the time this spec was written — re-count before relying on exact
  figures if manifests have changed meaningfully since 2026-09-13.

## 10. Open Decisions

All four decisions below were resolved 2026-09-13; see §5/§9 for full
rationale on each. Recorded here so implementation can start without
re-litigating them:

1. ✅ Opt-in flag first (`run --skip-not-due`), not default-on (§5).
2. ✅ Reuse `--full` for gating bypass; no dedicated flag (§5).
3. ✅ Validate the cadence → interval mapping against real publish patterns
   *before* writing gating logic (§5, §6 step 1) — the values in §5 are a
   starting point for that validation, not a conclusion to build on
   directly.
4. ✅ Back-filling `expected_update_frequency` for manifest entries missing
   it is separate, follow-up work — not blocking this spec's first slice
   (§9).

The only decision still genuinely open is the *outcome* of #3 (the actual
validated intervals) — that requires cross-checking real source publish
history per §6 step 1, not a judgment call to make from this doc alone.
