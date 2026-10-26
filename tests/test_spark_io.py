from fred_pipeline.io.spark_io import estimated_rows_bytes, target_partitions_for_rows


def test_target_partitions_uses_explicit_value_without_estimation():
    assert (
        target_partitions_for_rows(
            [{"value": "x"}], target_file_size_mb=1, target_partitions="3"
        )
        == 3
    )


def test_target_partitions_auto_uses_serialized_buffer_size():
    rows = [{"value": "x" * 2_000_000}]
    estimated = estimated_rows_bytes(rows)

    assert estimated > 1_024 * 1_024
    assert (
        target_partitions_for_rows(
            rows, target_file_size_mb=1, target_partitions="auto"
        )
        == 2
    )
