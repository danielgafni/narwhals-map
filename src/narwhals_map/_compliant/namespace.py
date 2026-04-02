from typing import TYPE_CHECKING, Any, ClassVar, Protocol, TypeVar

from narwhals._utils import CompliantT_co, _StoresCompliant

if TYPE_CHECKING:
    from narwhals._compliant.typing import Accessor

T = TypeVar("T")


class MapNamespace(_StoresCompliant[CompliantT_co], Protocol[CompliantT_co]):
    _accessor: ClassVar["Accessor"] = "map"  # type: ignore[assignment]

    def get(self, key: Any) -> CompliantT_co: ...
