
from typing import Any

from narwhals._compliant.expr import EagerExprNamespace


class ArrowExprMapNamespace(EagerExprNamespace):  # type: ignore[type-arg]
    def get(self, key: Any) -> Any:
        compliant = self._compliant_expr

        def inner(df):  # type: ignore[no-untyped-def]
            return [series.map.get(key=key) for series in compliant(df)]  # type: ignore[attr-defined]

        return compliant._from_callable(
            inner,
            evaluate_output_names=compliant._evaluate_output_names,
            alias_output_names=compliant._alias_output_names,
            context=compliant,
        )
