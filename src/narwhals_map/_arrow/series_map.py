from typing import TYPE_CHECKING, Any

import pyarrow.compute as pc
from narwhals._arrow.utils import ArrowSeriesNamespace

from narwhals_map._compliant.namespace import MapNamespace

if TYPE_CHECKING:
    from narwhals._arrow.series import ArrowSeries


class ArrowSeriesMapNamespace(ArrowSeriesNamespace, MapNamespace["ArrowSeries"]):
    def get(self, key: Any) -> "ArrowSeries":
        return self.with_native(pc.map_lookup(self.native, key, "first")).alias(str(key))  # pyrefly: ignore [missing-attribute]
