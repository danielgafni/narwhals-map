from typing import TYPE_CHECKING, Any, Generic, TypeVar

if TYPE_CHECKING:
    from narwhals.series import Series

SeriesT = TypeVar("SeriesT", bound="Series")


class SeriesMapNamespace(Generic[SeriesT]):
    def __init__(self, series: SeriesT) -> None:
        self._narwhals_series = series

    def get(self, key: Any) -> SeriesT:
        return self._narwhals_series._with_compliant(self._narwhals_series._compliant_series.map.get(key))  # pyrefly: ignore [missing-attribute]
