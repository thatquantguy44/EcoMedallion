"""``gold.dim_series`` is declared in four places; they must agree.

1. :func:`fred_pipeline.writer.terminal_views.build_dim_series` — the row dicts
2. ``_SCHEMA`` in :mod:`fred_pipeline.io.local_store` — the SQLite mirror
3. the ``StructType`` in :func:`fred_pipeline.writer.gold._build_terminal_views`
   — the Delta/Databricks write
4. ``sql/50_gold.sql`` — the DDL shipped for SQL-first engineers

Adding ``geo`` and ``metric`` to the catalog updated (1), (2) and (4) but missed
(3), and nothing caught it: the local backend was fine, and the Spark tests
auto-skip wherever PySpark is absent — which is CI's unit matrix and most
laptops. The Databricks ``dim_series`` would simply have lost both columns,
taking Report 12's map key with it.

These tests read all four declarations and compare them. (3) is read from source
rather than imported, because importing it requires PySpark; a source scan is
inelegant but it is hermetic, and it is exactly the drift that bit us.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

from fred_pipeline.gold_config.catalog_config import CatalogEntry
from fred_pipeline.writer.terminal_views import build_dim_series

REPO_ROOT = Path(__file__).resolve().parents[1]


def _row_keys() -> list[str]:
    """(1) the keys build_dim_series actually emits."""
    (row,) = build_dim_series([CatalogEntry("X", "RATES")], [])
    return list(row)


def _sqlite_columns() -> list[str]:
    """(2) the SQLite mirror's gold_dim_series column list."""
    from fred_pipeline.io import local_store

    match = re.search(
        r"CREATE TABLE IF NOT EXISTS gold_dim_series \((.*?)\);",
        local_store._SCHEMA,
        re.DOTALL,
    )
    assert match, "gold_dim_series not found in local_store._SCHEMA"
    return [
        part.strip().split()[0] for part in match.group(1).split(",") if part.strip()
    ]


def _spark_struct_fields(source: str | None = None) -> list[str]:
    """(3) the StructField names in the Spark writer, read from source."""
    if source is None:
        source = (REPO_ROOT / "src" / "fred_pipeline" / "writer" / "gold.py").read_text(
            encoding="utf-8"
        )
    writes = [
        node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_write"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and node.args[0].value == "dim_series"
    ]
    assert len(writes) == 1, "expected one dim_series write in the Spark writer"
    schema = writes[0].args[2]
    assert isinstance(schema, ast.Call) and isinstance(schema.func, ast.Name)
    assert schema.func.id == "StructType"
    fields = schema.args[0]
    assert isinstance(fields, ast.List)
    names = []
    for field in fields.elts:
        assert isinstance(field, ast.Call) and isinstance(field.func, ast.Name)
        assert field.func.id == "StructField"
        assert isinstance(field.args[0], ast.Constant)
        names.append(field.args[0].value)
    return names


def _sql_ddl_columns() -> list[str]:
    """(4) the shipped Unity Catalog DDL."""
    sql = (REPO_ROOT / "sql" / "50_gold.sql").read_text(encoding="utf-8")
    match = re.search(
        r"CREATE TABLE IF NOT EXISTS gold\.dim_series \((.*?)\)\s*USING DELTA;",
        sql,
        re.DOTALL,
    )
    assert match, "gold.dim_series not found in sql/50_gold.sql"
    columns = []
    for line in match.group(1).splitlines():
        line = line.strip()
        if not line or line.startswith("--"):
            continue
        columns.append(line.split()[0].rstrip(","))
    return columns


def test_spark_struct_matches_the_emitted_rows():
    """The one that was broken: a row key with no StructField is dropped or
    raises on the Databricks write."""
    assert set(_spark_struct_fields()) == set(_row_keys())


def test_spark_schema_lookup_is_independent_of_formatter_and_quote_style():
    source = """
_write('unrelated', rows, StructType([StructField('not_a_dimension', StringType())]), ['*'])
_write(
    'dim_series', rows,
    StructType([
        StructField('series_id', StringType()),
        StructField('geo', StringType()),
        StructField('metric', StringType()),
    ]),
    ['*'],
)
"""
    assert _spark_struct_fields(source) == ["series_id", "geo", "metric"]
    assert _spark_struct_fields(ast.unparse(ast.parse(source))) == [
        "series_id",
        "geo",
        "metric",
    ]


def test_sqlite_schema_matches_the_emitted_rows():
    assert set(_sqlite_columns()) == set(_row_keys())


def test_sql_ddl_matches_the_emitted_rows():
    assert set(_sql_ddl_columns()) == set(_row_keys())


def test_all_four_declarations_agree():
    """Single assertion over all four, so a failure names every disagreement
    at once rather than one per run."""
    declarations = {
        "build_dim_series rows": set(_row_keys()),
        "local_store _SCHEMA": set(_sqlite_columns()),
        "gold.py StructType": set(_spark_struct_fields()),
        "sql/50_gold.sql": set(_sql_ddl_columns()),
    }
    reference = declarations["build_dim_series rows"]
    disagreements = {
        name: {"missing": sorted(reference - cols), "extra": sorted(cols - reference)}
        for name, cols in declarations.items()
        if cols != reference
    }
    assert not disagreements, f"dim_series declarations disagree: {disagreements}"


def test_geo_and_metric_are_present_everywhere():
    """Explicit because these two are what Report 12's choropleth binds to."""
    for name, columns in (
        ("build_dim_series", _row_keys()),
        ("local_store", _sqlite_columns()),
        ("gold.py StructType", _spark_struct_fields()),
        ("sql/50_gold.sql", _sql_ddl_columns()),
    ):
        assert "geo" in columns, f"{name} is missing geo"
        assert "metric" in columns, f"{name} is missing metric"
