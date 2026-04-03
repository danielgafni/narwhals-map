"""Monkey-patch narwhals internals to add Map dtype and .map namespace support."""

from narwhals.expr import Expr
from narwhals.series import Series

from narwhals_map._dtype import Map
from narwhals_map.expr_map import ExprMapNamespace
from narwhals_map.series_map import SeriesMapNamespace

# --- Public API namespace patches (always available) ---

Expr.map = property(lambda self: ExprMapNamespace(self))  # type: ignore[attr-defined]
Series.map = property(lambda self: SeriesMapNamespace(self))  # type: ignore[attr-defined]

# --- PyArrow backend (optional) ---

try:
    import pyarrow as pa
    from narwhals._arrow import utils as _arrow_utils
    from narwhals._arrow.series import ArrowSeries
    from narwhals._compliant.expr import EagerExpr

    from narwhals_map._arrow.expr_map import ArrowExprMapNamespace
    from narwhals_map._arrow.series_map import ArrowSeriesMapNamespace

    ArrowSeries.map = property(lambda self: ArrowSeriesMapNamespace(self))  # type: ignore[attr-defined]
    EagerExpr.map = property(lambda self: ArrowExprMapNamespace(self))  # type: ignore[attr-defined]

    _orig_arrow_native_to_narwhals_dtype = _arrow_utils.native_to_narwhals_dtype

    def _patched_arrow_native_to_narwhals_dtype(dtype, version):  # type: ignore[no-untyped-def]
        if isinstance(dtype, pa.MapType):
            return Map(
                _patched_arrow_native_to_narwhals_dtype(dtype.key_type, version),
                _patched_arrow_native_to_narwhals_dtype(dtype.item_type, version),
            )
        return _orig_arrow_native_to_narwhals_dtype(dtype, version)

    _arrow_utils.native_to_narwhals_dtype = _patched_arrow_native_to_narwhals_dtype  # type: ignore[assignment]

    _orig_arrow_narwhals_to_native_dtype = _arrow_utils.narwhals_to_native_dtype

    def _patched_arrow_narwhals_to_native_dtype(dtype, version):  # type: ignore[no-untyped-def]
        if isinstance(dtype, Map):
            return pa.map_(
                _orig_arrow_narwhals_to_native_dtype(dtype.key, version),
                _orig_arrow_narwhals_to_native_dtype(dtype.value, version),
            )
        return _orig_arrow_narwhals_to_native_dtype(dtype, version)

    _arrow_utils.narwhals_to_native_dtype = _patched_arrow_narwhals_to_native_dtype  # type: ignore[assignment]
except ImportError:
    pass

# --- Polars backend (optional) ---

try:
    import polars_map as _polars_map
    from narwhals._polars import utils as _polars_utils
    from narwhals._polars.expr import PolarsExpr
    from narwhals._polars.series import PolarsSeries

    from narwhals_map._polars.expr_map import PolarsExprMapNamespace
    from narwhals_map._polars.series_map import PolarsSeriesMapNamespace

    PolarsExpr.map = property(lambda self: PolarsExprMapNamespace(self))  # type: ignore[attr-defined]
    PolarsSeries.map = property(lambda self: PolarsSeriesMapNamespace(self))  # type: ignore[attr-defined]

    _orig_polars_native_to_narwhals_dtype = _polars_utils.native_to_narwhals_dtype

    def _patched_polars_native_to_narwhals_dtype(dtype, version):  # type: ignore[no-untyped-def]
        if isinstance(dtype, _polars_map.Map):
            return Map(
                _patched_polars_native_to_narwhals_dtype(dtype.key, version),
                _patched_polars_native_to_narwhals_dtype(dtype.value, version),
            )
        return _orig_polars_native_to_narwhals_dtype(dtype, version)

    _polars_utils.native_to_narwhals_dtype = _patched_polars_native_to_narwhals_dtype  # type: ignore[assignment]
except ImportError:
    pass

# --- Ibis backend (optional) ---

try:
    from narwhals._ibis import utils as _ibis_utils
    from narwhals._ibis.expr import IbisExpr

    from narwhals_map._ibis.expr_map import IbisExprMapNamespace

    IbisExpr.map = property(lambda self: IbisExprMapNamespace(self))  # type: ignore[attr-defined]

    _orig_ibis_native_to_narwhals_dtype = _ibis_utils.native_to_narwhals_dtype

    def _patched_ibis_native_to_narwhals_dtype(ibis_dtype, version):  # type: ignore[no-untyped-def]
        if ibis_dtype.is_map():
            return Map(
                _patched_ibis_native_to_narwhals_dtype(ibis_dtype.key_type, version),
                _patched_ibis_native_to_narwhals_dtype(ibis_dtype.value_type, version),
            )
        return _orig_ibis_native_to_narwhals_dtype(ibis_dtype, version)

    _ibis_utils.native_to_narwhals_dtype = _patched_ibis_native_to_narwhals_dtype  # type: ignore[assignment]
except ImportError:
    pass
