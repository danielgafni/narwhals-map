from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING

from narwhals.dtypes import DType, DTypeClass, NestedType

if TYPE_CHECKING:
    from narwhals.typing import IntoDType


class Map(NestedType):
    """Map composite type.

    Arguments:
        key: The key data type of the map. Must be hashable.
        value: The value data type of the map.

    Examples:
       >>> import pyarrow as pa
       >>> import narwhals as nw
       >>> s_native = pa.map_(pa.string(), pa.int64())
       ...     [[("key1", 1), ("key2", 2)]]
       ... )
       >>> nw.from_native(s_native, series_only=True).dtype
       Map(String, Int64)
    """

    __slots__ = (
        "key",
        "value",
    )
    key: IntoDType
    value: IntoDType
    """The key and value data types of the map."""

    def __init__(self, key: IntoDType, value: IntoDType) -> None:
        self.key = key
        self.value = value

    def __eq__(self, other: DType | type[DType]) -> bool:  # type: ignore[override]
        """Check if this Map is equivalent to another DType.

        Examples:
            >>> import narwhals as nw
            >>> nw.Map(nw.Int64, nw.Int64) == nw.Map(nw.Int64, nw.Int64)
            True
            >>> nw.Map(nw.Int64, nw.Int64) == nw.Map(nw.Int64, nw.Boolean)
            False

            If a parent type is not specific about its inner type, we infer it as equal

            >>> nw.Map(nw.Int64, nw.Int64) == nw.Map
            True
        """
        if type(other) is DTypeClass and issubclass(other, self.__class__):
            return True
        if isinstance(other, self.__class__):
            return (self.key, self.value) == (other.key, other.value)
        return False

    def __hash__(self) -> int:
        return hash((self.__class__, (self.key, self.value)))

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return f"{class_name}({self.key}, {self.value})"

    def to_schema(self) -> OrderedDict[str, IntoDType]:
        """Return Map dtype as a schema dict."""
        return OrderedDict({"key": self.key, "value": self.value})
