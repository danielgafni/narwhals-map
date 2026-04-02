from typing import Any, NamedTuple

import narwhals as nw
import polars as pl
import polars_map  # noqa: F401 - triggers plugin registration
import pyarrow as pa
from pytest_cases import fixture, parametrize, parametrize_with_cases

import narwhals_map  # noqa: F401 - triggers monkey-patching
from narwhals_map import Map

BACKENDS = ["pyarrow", "polars_eager", "polars_lazy", "polars_map_eager", "polars_map_lazy", "ibis"]
SERIES_BACKENDS = ["pyarrow", "polars", "polars_map"]
MAP_DTYPE_BACKENDS = [b for b in BACKENDS if b not in {"polars_eager", "polars_lazy"}]


class MapTestData(NamedTuple):
    df: nw.DataFrame | nw.LazyFrame
    key: Any
    expected: list[Any]


class SeriesTestData(NamedTuple):
    series: nw.Series
    key: Any
    expected: list[Any]


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


def _pa_table_to_polars_map(pa_table: pa.Table) -> pl.DataFrame:
    """Convert a PyArrow table with map columns to a Polars DataFrame using polars_map.Map dtype."""
    df = pl.from_arrow(pa_table)
    map_cols = [name for name in pa_table.column_names if isinstance(pa_table.schema.field(name).type, pa.MapType)]
    if map_cols:
        df = df.with_columns(pl.col(c).map.from_entries() for c in map_cols)  # pyrefly: ignore [missing-attribute]
    return df  # pyrefly: ignore [bad-return]


def _to_native(pa_table: pa.Table, backend: str) -> Any:
    if backend == "pyarrow":
        return pa_table
    if backend == "ibis":
        import ibis

        return ibis.memtable(pa_table)
    if backend.startswith("polars_map"):
        df = _pa_table_to_polars_map(pa_table)
        if backend == "polars_map_lazy":
            return df.lazy()  # pyrefly: ignore [missing-attribute]
        return df
    df = pl.from_arrow(pa_table)
    if backend == "polars_lazy":
        return df.lazy()  # pyrefly: ignore [missing-attribute]
    return df


@fixture
@parametrize("backend", BACKENDS)
@parametrize_with_cases("pa_table, key, expected", cases=MapCases)
def map_test_data(pa_table: pa.Table, key: Any, expected: list[Any], backend: str) -> MapTestData:
    return MapTestData(df=nw.from_native(_to_native(pa_table, backend)), key=key, expected=expected)


@fixture
@parametrize("backend", SERIES_BACKENDS)
@parametrize_with_cases("pa_table, key, expected", cases=MapCases)
def series_test_data(pa_table: pa.Table, key: Any, expected: list[Any], backend: str) -> SeriesTestData:
    if backend == "pyarrow":
        series = nw.from_native(pa_table)["map_col"]
    elif backend == "polars_map":
        pl_series = _pa_table_to_polars_map(pa_table)["map_col"]  # pyrefly: ignore [bad-index]
        series = nw.from_native(pl_series, series_only=True)
    else:
        pl_series = pl.from_arrow(pa_table)["map_col"]  # pyrefly: ignore [bad-index]
        series = nw.from_native(pl_series, series_only=True)
    return SeriesTestData(series=series, key=key, expected=expected)


def test_expr_map_get(map_test_data: MapTestData) -> None:
    df, key, expected = map_test_data
    result = df.select(nw.col("map_col").map.get(key))  # pyrefly: ignore [missing-attribute]
    if isinstance(result, nw.LazyFrame):
        result = result.collect()
    pa_result = result.to_arrow()
    assert pa_result[str(key)].to_pylist() == expected


def test_series_map_get(series_test_data: SeriesTestData) -> None:
    series, key, expected = series_test_data
    result = series.map.get(key)  # pyrefly: ignore [missing-attribute]
    assert result.to_list() == expected
    assert result.name == str(key)


_MAP_DTYPE_PA_TABLE = pa.table({"map_col": pa.array([[("a", 1)]], type=pa.map_(pa.string(), pa.int64()))})


@fixture
@parametrize("backend", MAP_DTYPE_BACKENDS)
def map_dtype_df(backend: str) -> nw.DataFrame | nw.LazyFrame:
    return nw.from_native(_to_native(_MAP_DTYPE_PA_TABLE, backend))


def test_schema_map_dtype(map_dtype_df: nw.DataFrame | nw.LazyFrame) -> None:
    schema = map_dtype_df.collect_schema() if isinstance(map_dtype_df, nw.LazyFrame) else map_dtype_df.schema
    assert isinstance(schema["map_col"], Map)
    assert schema["map_col"] == Map(nw.String(), nw.Int64())
    assert repr(schema["map_col"]) == "Map(String, Int64)"
