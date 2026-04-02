from typing import Any

import narwhals as nw
import pyarrow as pa
import pytest
from pytest_cases import parametrize_with_cases

import narwhals_map  # noqa: F401 - triggers monkey-patching

BACKENDS = ["pyarrow", "polars_eager", "polars_lazy", "ibis"]


class MapCases:
    def case_string_keys(self) -> tuple[pa.Table, Any, list[Any]]:
        map_array = pa.array(
            [
                [("key1", 1), ("key2", 2)],
                [("key1", 3), ("key2", 4)],
                [("key1", 5)],
            ],
            type=pa.map_(pa.string(), pa.int64()),
        )
        return pa.table({"map_col": map_array}), "key1", [1, 3, 5]

    def case_int_keys(self) -> tuple[pa.Table, Any, list[Any]]:
        map_array = pa.array(
            [
                [(10, "a"), (20, "b")],
                [(10, "c"), (20, "d")],
                [(10, "e")],
            ],
            type=pa.map_(pa.int64(), pa.string()),
        )
        return pa.table({"map_col": map_array}), 10, ["a", "c", "e"]


def to_backend(pa_table: pa.Table, backend: str) -> Any:
    if backend == "pyarrow":
        return pa_table
    if backend == "ibis":
        import ibis

        return ibis.memtable(pa_table)
    import polars as pl

    df = pl.from_arrow(pa_table)
    if backend == "polars_lazy":
        return df.lazy()  # pyrefly: ignore [missing-attribute]
    return df


@parametrize_with_cases("pa_table, key, expected", cases=MapCases)
@pytest.mark.parametrize("backend", BACKENDS)
def test_expr_map_get(pa_table: pa.Table, key: Any, expected: list[Any], backend: str) -> None:
    native_df = to_backend(pa_table, backend)
    df = nw.from_native(native_df)
    result = df.select(narwhals_map.col("map_col").map.get(key))  # pyrefly: ignore [missing-attribute]
    if isinstance(result, nw.LazyFrame):
        result = result.collect()
    pa_result = result.to_arrow()
    assert pa_result[str(key)].to_pylist() == expected


@parametrize_with_cases("pa_table, key, expected", cases=MapCases)
def test_series_map_get_pyarrow(pa_table: pa.Table, key: Any, expected: list[Any]) -> None:
    df = nw.from_native(pa_table)
    result = df["map_col"].map.get(key)
    assert result.to_list() == expected
    assert result.name == str(key)


@parametrize_with_cases("pa_table, key, expected", cases=MapCases)
def test_series_map_get_polars(pa_table: pa.Table, key: Any, expected: list[Any]) -> None:
    import polars as pl

    pl_series = pl.from_arrow(pa_table)["map_col"]  # pyrefly: ignore [bad-index]
    series = nw.from_native(pl_series, series_only=True)
    result = series.map.get(key)  # pyrefly: ignore [missing-attribute]
    assert result.to_list() == expected
    assert result.name == str(key)
