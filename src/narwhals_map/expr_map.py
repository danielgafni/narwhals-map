from typing import TYPE_CHECKING, Any, Generic, TypeVar

from narwhals._expression_parsing import ExprKind, ExprNode

if TYPE_CHECKING:
    from narwhals.expr import Expr

ExprT = TypeVar("ExprT", bound="Expr")


class ExprMapNamespace(Generic[ExprT]):
    def __init__(self, expr: ExprT) -> None:
        self._expr = expr

    def get(self, key: Any) -> ExprT:
        return self._expr._append_node(ExprNode(ExprKind.ELEMENTWISE, "map.get", key=key))._append_node(
            ExprNode(ExprKind.ELEMENTWISE, "alias", name=str(key))
        )
