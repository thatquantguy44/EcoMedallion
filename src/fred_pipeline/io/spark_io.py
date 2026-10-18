"""Spark/Delta helpers, isolated so PySpark is imported lazily.

The rest of the package's business logic is Spark-free and unit-testable. This
module is the single place that touches a ``SparkSession`` and Delta Lake, so
in a non-Databricks context (tests, local dev) importing the package never
requires PySpark to be installed.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional


def get_spark(spark: Any = None) -> Any:
    """Return the active SparkSession, creating/getting one if not supplied."""
    if spark is not None:
        return spark
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "PySpark is required for Spark I/O but is not installed. "
            "Install it or run inside Databricks."
        ) from exc
    return SparkSession.builder.getOrCreate()


def table_exists(spark: Any, table: str) -> bool:
    try:
        return spark.catalog.tableExists(table)
    except Exception:  # pragma: no cover - older runtimes
        try:
            spark.sql(f"DESCRIBE TABLE {table}")
            return True
        except Exception:
            return False


def merge_delta(
    spark: Any,
    source_df: Any,
    target_table: str,
    merge_keys: Iterable[str],
    *,
    update: bool = True,
    insert: bool = True,
    match_update_condition: Optional[str] = None,
) -> dict[str, int]:
    """Idempotent upsert of ``source_df`` into ``target_table`` via Delta MERGE.

    Returns a small metrics dict. Requires that ``target_table`` already exists
    (created from the SQL DDL). Uses the DeltaTable API when available and falls
    back to a temp-view SQL ``MERGE`` otherwise.
    """
    keys = list(merge_keys)
    on_clause = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
    src_count = source_df.count()

    try:
        from delta.tables import DeltaTable

        dt = DeltaTable.forName(spark, target_table)
        builder = dt.alias("t").merge(source_df.alias("s"), on_clause)
        if update:
            builder = builder.whenMatchedUpdateAll(condition=match_update_condition)
        if insert:
            builder = builder.whenNotMatchedInsertAll()
        builder.execute()
    except ImportError:  # pragma: no cover - non-Delta fallback
        view = "__merge_src"
        source_df.createOrReplaceTempView(view)
        set_clause = ", ".join(f"t.{c} = s.{c}" for c in source_df.columns)
        cols = ", ".join(source_df.columns)
        vals = ", ".join(f"s.{c}" for c in source_df.columns)
        matched_clause = ""
        if update:
            condition = f" AND {match_update_condition}" if match_update_condition else ""
            matched_clause = f"WHEN MATCHED{condition} THEN UPDATE SET {set_clause}"
        sql = f"""
            MERGE INTO {target_table} t
            USING {view} s ON {on_clause}
            {matched_clause}
            {"WHEN NOT MATCHED THEN INSERT (" + cols + ") VALUES (" + vals + ")" if insert else ""}
        """
        spark.sql(sql)

    return {"source_rows": src_count}


def append_rows(spark: Any, rows: list[dict[str, Any]], target_table: str) -> int:
    """Append plain dict rows to a Delta table. Returns rows written."""
    if not rows:
        return 0
    df = spark.createDataFrame(rows)
    df.write.format("delta").mode("append").saveAsTable(target_table)
    return len(rows)
