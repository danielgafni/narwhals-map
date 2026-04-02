from typing import TYPE_CHECKING, Any

import polars_map  # noqa: F401 - registers .map on pl.Expr
from narwhals._polars.expr import PolarsExprNamespace

from narwhals_map._compliant.namespace import MapNamespace

if TYPE_CHECKING:
    from narwhals._polars.expr import PolarsExpr


class PolarsExprMapNamespace(PolarsExprNamespace, MapNamespace["PolarsExpr"]):
    def get(self, key: Any) -> "PolarsExpr":
        return self.compliant._with_native(self.native.map.get(key))  # pyrefly: ignore [missing-attribute]
