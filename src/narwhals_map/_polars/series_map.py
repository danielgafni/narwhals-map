

from typing import TYPE_CHECKING, Any

from narwhals._polars.series import PolarsSeriesNamespace

from narwhals_map._compliant.namespace import MapNamespace

if TYPE_CHECKING:
    from narwhals._polars.series import PolarsSeries


class PolarsSeriesMapNamespace(PolarsSeriesNamespace, MapNamespace["PolarsSeries"]):
    def get(self, key: Any) -> "PolarsSeries":
        ns = self.__narwhals_namespace__()
        return self.to_frame().select(ns.col(self.name).map.get(key)).get_column(str(key))
