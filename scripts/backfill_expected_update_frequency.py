"""One-time backfill: set expected_update_frequency from the existing
frequency code on manifest entries missing it (spec007 sec9 / the
performance handoff's due-date-gating follow-up).

Mirrors the exact mapping bls_discovery.py / ecb_discovery.py already use
(_expected_update_frequency): d -> daily, w -> weekly, m -> monthly,
q -> quarterly, a -> annual, sa -> semiannual.

Edits files as plain text, not via a YAML round-trip: a full ruamel.yaml
load/dump was tried first and silently dropped every blank line and inline
comment across the file (confirmed on manifests/rates.yml) -- unacceptable
for hand-maintained manifests. This script only ever inserts or replaces
the single line/segment holding expected_update_frequency, leaving every
other byte untouched.

Usage: python scripts/backfill_expected_update_frequency.py [--dry-run]
"""

from __future__ import annotations

import glob
import re
import sys

FREQ_MAP = {
    "d": "daily",
    "w": "weekly",
    "m": "monthly",
    "q": "quarterly",
    "a": "annual",
    "sa": "semiannual",
}

_BLOCK_START_RE = re.compile(r"^(\s*)-\s*series_id:")
_ENTRY_BOUNDARY_RE = re.compile(r"^\s*-\s*(series_id:|\{)")
_FLOW_LINE_RE = re.compile(r"^\s*-\s*\{.*\}\s*$")
_BLOCK_FREQ_RE = re.compile(r"^(\s*)frequency:\s*(\S+)\s*$")
_BLOCK_EUF_EMPTY_RE = re.compile(r"^(\s*)expected_update_frequency:\s*(''|\"\"|)\s*$")
_FLOW_FREQ_RE = re.compile(r"\bfrequency:\s*([a-zA-Z]+)\s*,")
_FLOW_EUF_EMPTY_RE = re.compile(r"expected_update_frequency:\s*(''|\"\")")


def fix_flow_line(line: str) -> tuple[str, bool]:
    if "expected_update_frequency" in line:
        if not _FLOW_EUF_EMPTY_RE.search(line):
            return line, False  # already has a real value
        freq_m = _FLOW_FREQ_RE.search(line)
        if not freq_m:
            return line, False
        mapped = FREQ_MAP.get(freq_m.group(1).lower())
        if not mapped:
            return line, False
        new_line = _FLOW_EUF_EMPTY_RE.sub(
            f"expected_update_frequency: {mapped}", line, count=1
        )
        return new_line, True

    freq_m = _FLOW_FREQ_RE.search(line)
    if not freq_m:
        return line, False
    mapped = FREQ_MAP.get(freq_m.group(1).lower())
    if not mapped:
        return line, False
    new_line = _FLOW_FREQ_RE.sub(
        lambda m: f"{m.group(0)} expected_update_frequency: {mapped},", line, count=1
    )
    return new_line, True


def fix_block(block_lines: list[str]) -> tuple[list[str], bool]:
    has_key = any("expected_update_frequency:" in l for l in block_lines)
    freq_val = None
    for l in block_lines:
        m = _BLOCK_FREQ_RE.match(l)
        if m:
            freq_val = m.group(2).strip("\"'")
            break
    if freq_val is None:
        return block_lines, False
    mapped = FREQ_MAP.get(freq_val.lower())
    if not mapped:
        return block_lines, False

    if has_key:
        new_lines = []
        changed = False
        for l in block_lines:
            m = _BLOCK_EUF_EMPTY_RE.match(l)
            if m and not changed:
                new_lines.append(f"{m.group(1)}expected_update_frequency: {mapped}\n")
                changed = True
            else:
                new_lines.append(l)
        return new_lines, changed

    new_lines = []
    changed = False
    for l in block_lines:
        new_lines.append(l)
        m = _BLOCK_FREQ_RE.match(l)
        if m and not changed:
            new_lines.append(f"{m.group(1)}expected_update_frequency: {mapped}\n")
            changed = True
    return new_lines, changed


def process_file(path: str, *, dry_run: bool) -> int:
    with open(path) as f:
        lines = f.readlines()

    out: list[str] = []
    i = 0
    n = len(lines)
    changed_count = 0
    while i < n:
        line = lines[i]
        if _FLOW_LINE_RE.match(line) and "series_id" in line:
            new_line, changed = fix_flow_line(line)
            out.append(new_line)
            changed_count += changed
            i += 1
            continue
        if _BLOCK_START_RE.match(line):
            block_lines = [line]
            j = i + 1
            while j < n and not _ENTRY_BOUNDARY_RE.match(lines[j]):
                block_lines.append(lines[j])
                j += 1
            new_block, changed = fix_block(block_lines)
            out.extend(new_block)
            changed_count += changed
            i = j
            continue
        out.append(line)
        i += 1

    if changed_count and not dry_run:
        with open(path, "w") as f:
            f.writelines(out)
    return changed_count


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    total = 0
    for path in sorted(glob.glob("manifests/*.yml")):
        n = process_file(path, dry_run=dry_run)
        if n:
            print(f"{'[dry-run] ' if dry_run else ''}{path}: {n} entries")
        total += n
    print(f"\nTotal entries {'would be ' if dry_run else ''}backfilled: {total}")


if __name__ == "__main__":
    main()
