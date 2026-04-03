from typing import Any, NamedTuple

import ibis.expr.datatypes.core
import narwhals as nw
import polars as pl
import polars_map  # noqa: F401 - triggers plugin registration
import pyarrow as pa
from narwhals.typing import Frame
from pytest_cases import fixture, parametrize, parametrize_with_cases

import narwhals_map  # noqa: F401 - triggers monkey-patching

Impl = nw.Implementation

BACKENDS: list[tuple[Impl, bool]] = [
    (Impl.PYARROW, False),
    (Impl.POLARS, False),
    (Impl.POLARS, True),
    (Impl.IBIS, True),
]

SERIES_BACKENDS: list[Impl] = [Impl.PYARROW, Impl.POLARS]

EXPECTED_NATIVE_MAP_DTYPE: dict[Impl, type] = {
    Impl.PYARROW: pa.MapType,
    Impl.POLARS: polars_map.Map,
    Impl.IBIS: ibis.expr.datatypes.core.Map,
}


def _get_native_col_dtype(
    native: pa.Table | pl.DataFrame | pl.LazyFrame | ibis.Table,
    col: str,
    impl: Impl,
) -> pa.DataType | pl.DataType | ibis.expr.datatypes.core.DataType:
    """Extract a column's dtype from a native frame."""
    if impl == Impl.PYARROW:
        return native.schema.field(col).type  # type: ignore[union-attr]
    elif impl == Impl.IBIS:
        return native.schema()[col]  # type: ignore[union-attr]
    elif impl == Impl.POLARS:
        schema = native.collect_schema() if isinstance(native, pl.LazyFrame) else native.schema  # type: ignore[union-attr]
        return schema[col]
    raise NotImplementedError(f"Implementation {impl} is not supported")


class FrameTestData(NamedTuple):
    df: Frame
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


def _pa_table_to_polars_map(pa_table: pa.Table) -> "pl.DataFrame":
    """Convert a PyArrow table with map columns to a Polars DataFrame using polars_map.Map dtype."""
    import polars as pl

    df = pl.from_arrow(pa_table)
    map_cols = [name for name in pa_table.column_names if isinstance(pa_table.schema.field(name).type, pa.MapType)]
    if map_cols:
        df = df.with_columns(pl.col(c).map.from_entries() for c in map_cols)  # pyrefly: ignore [missing-attribute]
    return df  # pyrefly: ignore [bad-return]


def _make_native_df(pa_table: pa.Table, impl: Impl, lazy: bool) -> "pl.DataFrame | pl.LazyFrame | pa.Table | ibis.Table":
    if impl == Impl.PYARROW:
        return pa_table
    elif impl == Impl.IBIS:
        import ibis

        return ibis.memtable(pa_table)
    elif impl == Impl.POLARS:
        df = _pa_table_to_polars_map(pa_table)

        if lazy:
            return df.lazy()
        else:
            return df
    raise NotImplementedError(f"Implementation {impl} is not supported")


@fixture
@parametrize("impl, lazy", BACKENDS)
@parametrize_with_cases("pa_table, key, expected", cases=MapCases)
def map_test_data(pa_table: pa.Table, key: Any, expected: list[Any], impl: Impl, lazy: bool) -> FrameTestData:
    return FrameTestData(df=nw.from_native(_make_native_df(pa_table, impl, lazy)), key=key, expected=expected)


@fixture
@parametrize("impl", SERIES_BACKENDS)
@parametrize_with_cases("pa_table, key, expected", cases=MapCases)
def series_test_data(pa_table: pa.Table, key: Any, expected: list[Any], impl: Impl) -> SeriesTestData:
    if impl == Impl.PYARROW:
        series = nw.from_native(pa_table)["map_col"]
    else:
        pl_series = _pa_table_to_polars_map(pa_table)["map_col"]  # pyrefly: ignore [bad-index]
        series = nw.from_native(pl_series, series_only=True)
    return SeriesTestData(series=series, key=key, expected=expected)


def test_expr_map_get(map_test_data: FrameTestData) -> None:
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


class NestedMapCases:
    def case_list_values(self) -> tuple[pa.Table, Any, list[Any]]:
        map_array = pa.array(
            [[(1, [10, 20]), (2, [30])], [(1, [40])]],
            type=pa.map_(pa.int64(), pa.list_(pa.int64())),
        )
        return pa.table({"map_col": map_array}), 1, [[10, 20], [40]]

    def case_struct_values(self) -> tuple[pa.Table, Any, list[Any]]:
        map_array = pa.array(
            [
                [(1, {"x": 10, "y": "a"}), (2, {"x": 20, "y": "b"})],
                [(1, {"x": 30, "y": "c"})],
            ],
            type=pa.map_(pa.int64(), pa.struct([("x", pa.int64()), ("y", pa.string())])),
        )
        return pa.table({"map_col": map_array}), 1, [{"x": 10, "y": "a"}, {"x": 30, "y": "c"}]

    def case_nested_map_values(self) -> tuple[pa.Table, Any, list[Any]]:
        map_array = pa.array(
            [
                [(1, [("a", 100)]), (2, [("b", 200)])],
                [(1, [("c", 300)])],
            ],
            type=pa.map_(pa.int64(), pa.map_(pa.string(), pa.int64())),
        )
        # Inner maps may appear as list of tuples (PyArrow/Ibis) or list of dicts (Polars)
        return pa.table({"map_col": map_array}), 1, [{"a": 100}, {"c": 300}]


@fixture
@parametrize("impl, lazy", BACKENDS)
@parametrize_with_cases("pa_table, key, expected", cases=NestedMapCases)
def nested_map_test_data(pa_table: pa.Table, key: Any, expected: list[Any], impl: Impl, lazy: bool) -> tuple[Frame, Any, list[Any]]:
    return nw.from_native(_make_native_df(pa_table, impl, lazy)), key, expected


def _normalize_map_pylist(values: list[Any]) -> list[Any]:
    """Normalize map-like values: convert list of tuples/dicts to dict.

    This is required because of the current limitation of `.to_arrow()` with `polars-map`-backed Narwhals frames:
        it produces `list<struct<key,value>>` columns instead of Arrow `map` types.
    """
    result = []
    for v in values:
        if isinstance(v, list) and v and isinstance(v[0], (tuple, dict)):
            if isinstance(v[0], tuple):
                result.append(dict(v))
            else:
                result.append({d["key"]: d["value"] for d in v})
        else:
            result.append(v)
    return result


def test_nested_map_get(nested_map_test_data: tuple[Frame, Any, list[Any]]) -> None:
    df, key, expected = nested_map_test_data
    schema = df.collect_schema() if isinstance(df, nw.LazyFrame) else df.schema
    assert isinstance(schema["map_col"], narwhals_map.Map)

    result = df.select(nw.col("map_col").map.get(key))  # pyrefly: ignore [missing-attribute]
    if isinstance(result, nw.LazyFrame):
        result = result.collect()
    pa_result = result.to_arrow()
    assert _normalize_map_pylist(pa_result[str(key)].to_pylist()) == expected


FROM_DICT_BACKENDS = [Impl.PYARROW, Impl.POLARS]


@fixture
@parametrize("impl", FROM_DICT_BACKENDS)
@parametrize_with_cases("pa_table, key, expected", cases=MapCases)
def from_dict_test_data(pa_table: pa.Table, key: Any, expected: list[Any], impl: Impl) -> tuple[Impl, nw.DataFrame]:
    source_df = nw.from_native(_make_native_df(pa_table, impl, lazy=False))
    map_dtype = source_df.schema["map_col"]
    df = nw.from_dict(
        {"map_col": source_df["map_col"]},
        schema={"map_col": map_dtype},
        backend=impl,  # pyrefly: ignore [bad-argument-type]
    )
    return impl, df


def test_from_dict_with_map_schema(from_dict_test_data: tuple[Impl, nw.DataFrame]) -> None:
    impl, df = from_dict_test_data
    assert isinstance(df.schema["map_col"], narwhals_map.Map)

    native = df.to_native()
    native_dtype = _get_native_col_dtype(native, "map_col", impl)
    assert isinstance(native_dtype, EXPECTED_NATIVE_MAP_DTYPE[impl])


_TO_NATIVE_PA_TABLE = pa.table(
    {
        "map_col": pa.array(
            [[(1, 10), (2, 20)], [(1, 30), (2, 40)], [(1, 50)]],
            type=pa.map_(pa.int64(), pa.int64()),
        )
    }
)


@fixture
@parametrize("impl, lazy", BACKENDS)
def to_native_test_data(impl: Impl, lazy: bool) -> tuple[Impl, Frame]:
    nw_df = nw.from_native(_make_native_df(_TO_NATIVE_PA_TABLE, impl, lazy))
    return impl, nw_df


def test_to_native_keeps_map(to_native_test_data: tuple[Impl, Frame]) -> None:
    impl, nw_df = to_native_test_data
    result = nw_df.with_columns(nw.col("map_col").map.get(1))  # pyrefly: ignore [missing-attribute]
    assert result.schema["map_col"] == narwhals_map.Map
    assert result.schema["1"].is_integer()
    native = result.to_native()
    map_dtype = _get_native_col_dtype(native, "map_col", impl)
    assert isinstance(map_dtype, EXPECTED_NATIVE_MAP_DTYPE[impl])


_MAP_DTYPE_PA_TABLE = pa.table({"map_col": pa.array([[("a", 1)]], type=pa.map_(pa.string(), pa.int64()))})


@fixture
@parametrize("impl, lazy", BACKENDS)
def map_dtype_native(impl: Impl, lazy: bool) -> tuple[Impl, Any]:
    native = _make_native_df(_MAP_DTYPE_PA_TABLE, impl, lazy)
    nw_df = nw.from_native(native)
    return impl, nw_df.to_native()


def test_schema_map_dtype(map_dtype_native: tuple[Impl, Any]) -> None:
    impl, native = map_dtype_native
    map_dtype = _get_native_col_dtype(native, "map_col", impl)
    assert isinstance(map_dtype, EXPECTED_NATIVE_MAP_DTYPE[impl])
