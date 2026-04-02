"""narwhals-map: Map dtype support for narwhals."""

import narwhals_map._narwhals_patch  # noqa: F401 - applies monkey-patches at import time
from narwhals_map._dtype import Map

__all__ = ["Map"]
