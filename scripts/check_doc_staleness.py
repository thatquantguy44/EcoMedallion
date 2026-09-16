#!/usr/bin/env python3
"""Pre-commit hook: check if README.md, data_dictionary.md, and handoff docs are stale.

Uses a ratcheting model: establishes a baseline of current gaps, then fails
only if NEW gaps are introduced (e.g. a new CLI command added without being
documented). Existing debt is tracked but permitted.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# git root
GIT_ROOT = Path(__file__).parent.parent


def get_cli_commands() -> set[str]:
    """Extract registered subcommand names from cli.py."""
    cli_path = GIT_ROOT / "src" / "fred_pipeline" / "cli.py"
    src = cli_path.read_text()
    matches = re.findall(r'add_parser\(\s*"([a-z][a-z0-9-]*)"', src)
    return set(matches)


def get_documented_commands() -> set[str]:
    """Extract CLI command names from README.md table."""
    readme_path = GIT_ROOT / "README.md"
    text = readme_path.read_text()
    # Look for table cells like | `run` | or | `price-constituents` |
    matches = re.findall(r"^\| `([a-z][a-z0-9-]*)` \|", text, re.MULTILINE)
    return set(matches)


def get_schema_tables() -> set[str]:
    """Extract table/view names from _SCHEMA in local_store.py."""
    sys.path.insert(0, str(GIT_ROOT / "src"))
    from fred_pipeline.io.local_store import _SCHEMA

    tables = set(
        re.findall(r"CREATE TABLE IF NOT EXISTS\s+([a-z0-9_]+)", _SCHEMA, re.IGNORECASE)
    )
    views = set(
        re.findall(
            r"CREATE VIEW(?:\s+IF NOT EXISTS)?\s+([a-z0-9_]+)", _SCHEMA, re.IGNORECASE
        )
    )
    return tables | views


def get_documented_tables() -> set[str]:
    """Extract table names from data_dictionary.md."""
    dict_path = GIT_ROOT / "docs" / "dictionary" / "data_dictionary.md"
    text = dict_path.read_text()
    # Look for headers like ### `meta.fred_series`
    matches = re.findall(r"^### `([a-z_]+\.[a-z0-9_]+)`", text, re.MULTILINE)
    return set(matches)


def check_handoff_dates() -> list[str]:
    """Check if handoff docs have Last verified: stamps within 90 days."""
    handoff_dir = GIT_ROOT / "docs" / "handoffs"
    issues = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=90)

    for doc in sorted(handoff_dir.glob("*.md")):
        text = doc.read_text()
        # Look for "Last verified: YYYY-MM-DD" or similar
        match = re.search(r"Last verified:\s*(\d{4}-\d{2}-\d{2})", text)
        if not match:
            issues.append(f"{doc.name}: missing 'Last verified: YYYY-MM-DD' stamp")
        else:
            try:
                verified_date = datetime.strptime(match.group(1), "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
                if verified_date < cutoff:
                    issues.append(
                        f"{doc.name}: Last verified {match.group(1)} is >90 days old"
                    )
            except ValueError:
                issues.append(f"{doc.name}: unparseable date in Last verified stamp")

    return issues


def load_baseline() -> dict:
    """Load the baseline gaps file, or create empty if missing."""
    baseline_path = GIT_ROOT / ".doc-staleness-baseline.json"
    if baseline_path.exists():
        return json.loads(baseline_path.read_text())
    return {
        "undocumented_commands": [],
        "undocumented_tables": [],
        "stale_handoff_docs": [],
    }


def save_baseline(baseline: dict) -> None:
    """Save the baseline gaps file."""
    baseline_path = GIT_ROOT / ".doc-staleness-baseline.json"
    baseline_path.write_text(json.dumps(baseline, indent=2, sort_keys=True) + "\n")


def main() -> int:
    """Main hook entrypoint."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--update-baseline",
        action="store_true",
        help="Update baseline gaps file (commit this to accept current state)",
    )
    args = parser.parse_args()

    # Collect current state
    cli_commands = get_cli_commands()
    documented_commands = get_documented_commands()
    schema_tables = get_schema_tables()
    documented_tables = get_documented_tables()
    handoff_issues = check_handoff_dates()

    # Find gaps
    undocumented_commands = sorted(cli_commands - documented_commands)
    undocumented_tables = sorted(schema_tables - documented_tables)

    current_state = {
        "undocumented_commands": undocumented_commands,
        "undocumented_tables": undocumented_tables,
        "stale_handoff_docs": handoff_issues,
    }

    if args.update_baseline:
        save_baseline(current_state)
        print("Updated .doc-staleness-baseline.json")
        return 0

    # Load baseline and check for NEW gaps
    baseline = load_baseline()
    new_commands = set(undocumented_commands) - set(baseline["undocumented_commands"])
    new_tables = set(undocumented_tables) - set(baseline["undocumented_tables"])
    new_handoff_issues = [
        i for i in handoff_issues if i not in baseline["stale_handoff_docs"]
    ]

    if not (new_commands or new_tables or new_handoff_issues):
        return 0

    # Report new gaps only
    print("\n❌ Doc staleness check failed (new gaps detected):\n")
    if new_commands:
        print(f"  New undocumented CLI commands: {', '.join(sorted(new_commands))}")
        print("  → Add to README.md Operations reference table\n")
    if new_tables:
        print(f"  New undocumented tables/views: {', '.join(sorted(new_tables))}")
        print("  → Add to docs/dictionary/data_dictionary.md\n")
    if new_handoff_issues:
        print("  New stale handoff docs:")
        for issue in new_handoff_issues:
            print(f"    - {issue}")
        print("  → Add 'Last verified: YYYY-MM-DD' stamp or update its date\n")

    print("Known existing gaps (not blocking):")
    print(json.dumps(baseline, indent=2))
    print("\nTo accept the current state and update the baseline, run:")
    print("  python scripts/check_doc_staleness.py --update-baseline")
    print("  git add .doc-staleness-baseline.json")

    return 1


if __name__ == "__main__":
    sys.exit(main())
