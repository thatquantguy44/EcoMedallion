from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from benchmark_pipeline import run_fixture_benchmark  # noqa: E402


FIXTURE = ROOT / "tests" / "fixtures" / "benchmark_pipeline.json"


def test_fixture_benchmark_reports_distinct_unchanged_runs():
    report = run_fixture_benchmark(FIXTURE, workload="unchanged", iterations=2)

    assert report["summary"]["iterations_completed"] == 2
    first, second = report["runs"]
    assert first["run_id"] != second["run_id"]
    for run in report["runs"]:
        assert run["status"] == "succeeded"
        assert run["selected_series"] == 2
        assert run["due_series"] == 2
        assert run["succeeded_series"] == 2
        assert run["rows"]["silver_rows_changed"] == 0
        assert run["rows"]["silver_rows_read"] > 0
        assert run["rows"]["latest_rows_read"] > 0
        assert run["stages"]["plan"]["seconds"] is not None
        assert run["stages"]["extract"]["seconds"] is not None
        assert run["stages"]["gold"]["seconds"] is not None
        assert run["gold_table_seconds"]
        assert run["database_size_bytes"] > 0
        assert run["source_calls"]["fred"]["get_observations"] == 2


def test_fixture_restatement_reports_changed_silver_rows():
    report = run_fixture_benchmark(FIXTURE, workload="restatement", iterations=1)
    run = report["runs"][0]

    assert run["status"] == "succeeded"
    assert run["rows"]["silver_rows_changed"] == 1


def test_fixture_full_workload_marks_full_series_loads():
    report = run_fixture_benchmark(FIXTURE, workload="full", iterations=1)
    run = report["runs"][0]

    assert run["status"] == "succeeded"
    assert run["workload"] == "full"
    assert run["rows"]["observations_extracted"] == 8
