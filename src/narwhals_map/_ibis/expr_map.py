from __future__ import annotations

from typing import TYPE_CHECKING, Any

from narwhals._compliant import LazyExprNamespace

from narwhals_map._compliant.namespace import MapNamespace

if TYPE_CHECKING:
    from narwhals._ibis.expr import IbisExpr


class IbisExprMapNamespace(LazyExprNamespace["IbisExpr"], MapNamespace["IbisExpr"]):
    def get(self, key: Any) -> IbisExpr:
        return self.compliant._with_callable(lambda expr: expr.get(key).name(str(key)))
