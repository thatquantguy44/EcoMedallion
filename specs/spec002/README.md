# Spec 002: ECB Metadata Discovery and Candidate Manifest Generation

Status: implemented
Last verified: 2026-09-09
Primary owner: TBD
Target source key: `ecb`
Depends on: `specs/spec001`

## 1. Goal

Add an ECB metadata/discovery helper that uses the ECB Data Portal SDMX metadata
endpoints to discover dataflows, inspect each flow's dimensions/code lists, and
generate reviewable candidate manifests for additional `source: ecb` series.

The finished implementation should let a user run commands such as:

```bash
PYTHONPATH=src python -m fred_pipeline discover-ecb --list-flows --search "exchange"
PYTHONPATH=src python -m fred_pipeline discover-ecb --flow EXR --dry-run --max 25
PYTHONPATH=src python -m fred_pipeline discover-ecb --flow FM --frequency m \
  --out manifests/ecb_money_market_candidates.yml
```

Generated entries should be valid pipeline manifests using the existing ECB
series id shape:

```yaml
series_id: "ECB:<flow_ref>:<key>"
source: ecb
```

## 2. Current ECB Footprint

The repository currently has a reviewed ECB footprint across the hand-curated
`manifests/ecb_rates.yml` file and several generated candidate manifests. As of
the last verification:

- `manifests/ecb_rates.yml` contains 34 active ECB rates/FX/yield-curve series.
- Reviewed generated candidate manifests activate additional ECB money, HICP,
  and `YC_PUB` yield-curve rows.
- Other generated ECB candidate manifests remain inactive until deliberately
  reviewed and activated.

Generated entries from `discover-ecb` still default to `active: false`; active
candidate manifests represent follow-on review work after the discovery helper
landed.

## 3. Non-Goals

- Do not ingest broad ECB wildcard queries directly into production manifests.
- Do not make generated candidate series active by default.
- Do not add new Bronze/Silver/Gold schemas; discovery is metadata authoring,
  not observation ingestion.
- Do not require an API key. ECB remains keyless.
- Do not try to exhaustively materialize every possible Cartesian product for
  large flows without explicit limits.

## 4. ECB Metadata API Facts To Build Against

Use the same base URL as the ECB source client:

```text
https://data-api.ecb.europa.eu/service
```

Discovery should use SDMX metadata endpoints:

- `/dataflow`
- `/dataflow/{agencyID}/{resourceID}/{version}?references=all`
- `/datastructure/{agencyID}/{resourceID}/{version}`
- Code-list references returned from the data structure response.

Implementation notes:

- Use SDMX XML for the implemented slice. ECB returns dimensions and embedded
  code lists from the dataflow reference endpoint when `references=all`.
- Preserve the simple flow id used by ingestion when possible, such as `EXR`.
- Retain the full ECB agency/resource/version metadata in diagnostics so a user
  can trace generated ids back to the exact metadata source.
- Treat endpoint formats and headers as source-specific and keep them inside the
  ECB metadata helper rather than the generic FRED discovery module.

## 5. User-Facing Workflow

### 5.1 List Dataflows

Command:

```bash
PYTHONPATH=src python -m fred_pipeline discover-ecb --list-flows
```

Output:

- flow id
- agency
- version
- name/title
- description when available

Options:

- `--search TEXT`: case-insensitive filter on id/title/description.
- `--max N`: cap printed rows.
- `--json`: print machine-readable records.

### 5.2 Inspect One Dataflow

Command:

```bash
PYTHONPATH=src python -m fred_pipeline discover-ecb --flow EXR --inspect
```

Output:

- flow metadata
- dimension order
- code-list id for each dimension
- sample codes for each dimension
- whether the flow appears safe for bounded candidate generation

### 5.3 Generate Candidate Manifest

Command:

```bash
PYTHONPATH=src python -m fred_pipeline discover-ecb --flow EXR \
  --frequency d --max 25 --dry-run
```

Generated manifest defaults:

- `active: false`
- `source: ecb`
- `load_type: incremental`
- `validation_profile: lenient`
- `vintage_enabled: true`
- `category`: supplied by `--category`, else inferred from flow id/name
- `tags`: `["ecb", <flow id>, <category>]`
- `priority`: `3`

Candidate rows should be written only when `--out` is supplied. Dry run should
print the YAML and a summary of kept/skipped candidates.

## 6. Candidate Key Generation Strategy

ECB dataflow structures can have many dimensions, and naive Cartesian products
can explode. Candidate generation must be bounded and explainable.

Generate keys in this order:

1. Read the dataflow's dimension order from the data structure.
2. Load code lists for those dimensions.
3. Apply explicit user filters before expanding candidates:
   - `--frequency d,m,q,a`
   - `--dimension KEY=VALUE[,VALUE]`
   - `--include-code TEXT`
   - `--exclude-code TEXT`
4. Estimate candidate count before generating.
5. Refuse expansion if the estimate is greater than `--max-cartesian` unless
   the user passes `--force`.
6. Generate dot-separated keys in ECB dimension order.
7. Format manifest ids as `ECB:<flow_ref>:<key>`.
8. Exclude any ids already present in existing manifests unless
   `--include-existing` is passed.

For flows where a reliable complete key set is available only through data
queries rather than structures, add an optional sample mode:

```bash
PYTHONPATH=src python -m fred_pipeline discover-ecb --flow EXR \
  --sample-data --last-n-observations 1
```

Sample mode may query observations to discover keys, but it must be opt-in,
rate-limited, and capped.

## 7. Proposed Code Changes

### Phase 1: Metadata Client

Add `src/fred_pipeline/catalogs/ecb_discovery.py`.

Core classes/functions:

- `ECBDataflow`
- `ECBDimension`
- `ECBCode`
- `ECBDiscoveryError`
- `ECBMetadataClient`
- `list_dataflows(client, search=None, max_results=None)`
- `get_dataflow_structure(client, flow_ref)`
- `estimate_candidate_count(structure, filters)`
- `generate_ecb_candidate_specs(...)`
- `build_ecb_manifest_dict(...)`
- `ecb_manifest_to_yaml(...)`

`ECBMetadataClient` should reuse the same HTTP design as `ECBClient` where
reasonable:

- base URL default: `https://data-api.ecb.europa.eu/service`
- timeout/retries/rate limit from `PipelineConfig`
- clear error messages with endpoint, status, and abbreviated response body

### Phase 2: Pure Metadata Normalization

Add pure parser/normalizer functions for metadata responses. Unit tests should
use local fixtures and should not perform network calls.

Required parser outputs:

- dataflow id, agency, version, name, description
- data structure reference for a flow
- ordered dimensions
- attached code-list ids
- code id/name pairs for each dimension

### Phase 3: Candidate Spec Mapping

Map generated keys to `SeriesSpec` dictionaries.

Title rules:

- Prefer a concise title assembled from flow name plus selected code names.
- Keep code ids in the title only when names are missing or ambiguous.
- Include flow id in tags, not necessarily in the title.

Frequency rules:

- If a frequency dimension exists, map ECB values to manifest frequencies:
  - `D` -> `d`
  - `W` -> `w`
  - `M` -> `m`
  - `Q` -> `q`
  - `S` or `H` -> `sa`
  - `A` -> `a`
- Skip candidates whose frequency cannot be mapped, with a skipped reason.

Validation rules:

- Every generated row must instantiate `SeriesSpec`.
- Every generated manifest must pass `Manifest.from_dict`.
- Generated entries default to inactive.

### Phase 4: CLI

Add a new CLI subcommand, separate from FRED `discover`, to avoid overloading
FRED-specific flags:

```bash
python -m fred_pipeline discover-ecb
```

Required flags:

- `--list-flows`
- `--flow FLOW_REF`
- `--inspect`
- `--search TEXT`
- `--dimension KEY=VALUE[,VALUE]` repeatable
- `--frequency d,m,q,a`
- `--max N`
- `--max-cartesian N`
- `--include-existing`
- `--dry-run`
- `--out PATH`
- `--json`

Behavior:

- Require exactly one mode: `--list-flows`, `--inspect`, or manifest generation.
- Use `ECB_BASE_URL` / `ecb_base_url` for approved proxy/mirror routes.
- Load existing manifests to avoid duplicate ids by default.
- Never write a file unless `--out` is present and `--dry-run` is false.

### Phase 5: Docs

Update:

- `README.md`
  - Add `discover-ecb` to the command table.
  - Mention ECB discovery under data sources.
- `docs/instructions/running_multi_source.md`
  - Add a short ECB candidate-generation recipe.
- `specs/spec001/README.md`
  - Mark ECB discovery as split into this spec.

### Phase 6: Tests

Add `tests/test_ecb_discovery.py`.

Minimum tests:

- parses sample dataflow metadata
- parses sample data structure with ordered dimensions
- maps ECB frequency codes to manifest frequencies
- estimates Cartesian size and refuses unsafe expansion
- applies dimension filters before expansion
- generates valid `ECB:<flow_ref>:<key>` ids
- excludes existing manifest ids
- generated YAML round-trips through `Manifest.from_dict`
- CLI argument validation for mutually exclusive modes

Use fixtures under `tests/fixtures/ecb/` if the metadata samples are large.

## 8. Acceptance Criteria

- Implemented: `python -m fred_pipeline discover-ecb --list-flows` can list ECB
  flows with no API key.
- Implemented: `python -m fred_pipeline discover-ecb --flow EXR --inspect`
  prints dimension order and sample code-list values.
- Implemented: `python -m fred_pipeline discover-ecb --flow EXR --frequency d
  --max 25 --dry-run` produces a valid inactive candidate manifest.
- Implemented: generated manifests validate through
  `PYTHONPATH=src python -m fred_pipeline validate`.
- Implemented: generated ECB manifests default inactive; later reviewed
  candidate manifests may be deliberately activated.
- Implemented: focused tests pass without network access
  (`PYTHONPATH=src pytest -q tests/test_ecb_discovery.py`).

## 9. Suggested First Implementation Slice

Build the smallest useful version first:

1. Add `ECBMetadataClient.list_dataflows()`. Status: implemented.
2. Add `discover-ecb --list-flows --search --json`. Status: implemented.
3. Add parser tests from a stored fixture. Status: implemented.
4. Add docs for listing flows. Status: implemented.
5. Add `--flow FLOW --inspect` structure inspection. Status: implemented.
6. Add bounded inactive candidate manifest generation. Status: implemented.

Verification on 2026-09-09:

- `PYTHONPATH=src pytest -q tests/test_ecb_discovery.py` -> 13 passed.
- `PYTHONPATH=src python -m fred_pipeline validate --manifests manifests` -> OK.

## 10. Candidate Dataflow Backlog

The broad ECB flow backlog is recorded in
`docs/catalog/ecb_candidate_flows.md`. First-pass expansion should prioritize:

- `EST`, `FM_PUB`, and `YC_PUB` for policy, money-market, and curve coverage.
- `BSI_PUB`, `MIR_PUB`, and `MOBILE_BSI` for money, credit, and banking.
- `ICP_PUB`, `HICP`, and `MOBILE_ICP` for inflation.
- `MNA_PUB` and the `JDF_MNA_*` GDP growth/contribution flows for macro.
- `QSA_PUB`, `GFS_PUB`, `BP6_PUB`, `PSS`, `SUP`, `CES`, `SPF`, and `SAFE`
  for sector accounts, government finance, external statistics, payments,
  supervision, and surveys.

Then add live smoke-test assistance for generated candidates.

## 11. Follow-Ups

- Optional sample-data key discovery (`--sample-data --last-n-observations N`)
  remains unimplemented; use explicit dimension filters and bounded metadata
  expansion for now.
- Add an interactive review report that groups candidates by flow, frequency,
  geography, and unit.
- Add curated ECB flow presets for high-value domains:
  - exchange rates (`EXR`)
  - money market rates (`FM`)
  - yield curves (`YC`)
  - monetary aggregates
  - balance sheet/statistical items
- Add cross-source reconciliation candidates where ECB overlaps with FRED/BIS
  rates and FX series.
- Consider extracting a generic SDMX discovery layer if BIS or another SDMX
  source needs the same metadata machinery.
