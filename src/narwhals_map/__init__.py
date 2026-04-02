"""narwhals-map: Map dtype support for narwhals."""

import narwhals as nw
from narwhals._arrow.series import ArrowSeries
from narwhals._compliant.expr import EagerExpr
from narwhals._polars.expr import PolarsExpr
from narwhals._polars.series import PolarsSeries
from narwhals.expr import Expr
from narwhals.series import Series

from narwhals_map._arrow.expr_map import ArrowExprMapNamespace
from narwhals_map._arrow.series_map import ArrowSeriesMapNamespace
from narwhals_map._polars.expr_map import PolarsExprMapNamespace
from narwhals_map._polars.series_map import PolarsSeriesMapNamespace
from narwhals_map.expr_map import ExprMapNamespace
from narwhals_map.series_map import SeriesMapNamespace

# Monkey-patch narwhals classes to add .map namespace
# this is necessary until Narwhals actually adds accessors for `narwhals-map`

# Public API: narwhals.Expr.map
Expr.map = property(lambda self: ExprMapNamespace(self))  # type: ignore[attr-defined]

# Public API: narwhals.Series.map
Series.map = property(lambda self: SeriesMapNamespace(self))  # type: ignore[attr-defined]

# Compliant layer: Arrow series
ArrowSeries.map = property(lambda self: ArrowSeriesMapNamespace(self))  # type: ignore[attr-defined]

# Compliant layer: Arrow expression (EagerExpr)
EagerExpr.map = property(lambda self: ArrowExprMapNamespace(self))  # type: ignore[attr-defined]

# Compliant layer: Polars expression
PolarsExpr.map = property(lambda self: PolarsExprMapNamespace(self))  # type: ignore[attr-defined]

# Compliant layer: Polars series
PolarsSeries.map = property(lambda self: PolarsSeriesMapNamespace(self))  # type: ignore[attr-defined]

# Compliant layer: Ibis expression (optional)
try:
    from narwhals._ibis.expr import IbisExpr

    from narwhals_map._ibis.expr_map import IbisExprMapNamespace

    IbisExpr.map = property(lambda self: IbisExprMapNamespace(self))  # type: ignore[attr-defined]
except ImportError:
    pass

# Re-export col for convenience
col = nw.col

__all__ = ["col"]
