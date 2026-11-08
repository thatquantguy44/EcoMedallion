"""Core pipeline imports must not require the optional orchestration runtime."""

import subprocess
import sys
from pathlib import Path

try:
    import tomllib
except ImportError:  # Python 3.10, where pytest supplies tomli
    import tomli as tomllib


ROOT = Path(__file__).resolve().parents[1]


def test_dagster_runtime_is_an_explicit_optional_extra():
    with (ROOT / "pyproject.toml").open("rb") as source:
        project = tomllib.load(source)["project"]
    assert set(project["optional-dependencies"]["dagster"]) == {
        "dagster==1.13.26",
        "dagster-webserver==1.13.26",
        "dagster-postgres==0.29.26",
    }
    assert all(
        "dagster" not in dependency.lower() for dependency in project["dependencies"]
    )
    constraints = {
        line.strip()
        for line in (ROOT / "requirements-dagster.lock.txt").read_text().splitlines()
        if line.strip() and not line.startswith("#")
    }
    assert set(project["optional-dependencies"]["dagster"]) <= constraints
    assert all("==" in entry and " @ " not in entry for entry in constraints)


def test_core_imports_and_cli_help_do_not_import_dagster():
    script = """
import importlib.abc
import sys
class BlockDagster(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'dagster' or fullname.startswith(('dagster.', 'dagster_')):
            raise ImportError('Dagster is intentionally unavailable')
sys.meta_path.insert(0, BlockDagster())
import fred_pipeline.cli
import fred_pipeline.pipeline
import fred_pipeline.ecb_eligible_assets
parser = fred_pipeline.cli.build_parser()
assert 'run' in parser.format_help()
assert not any(name == 'dagster' or name.startswith('dagster.') for name in sys.modules)
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
