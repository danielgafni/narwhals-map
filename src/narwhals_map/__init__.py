import narwhals_map._narwhals_patch  # noqa: F401 - applies monkey-patches at import time
from narwhals_map._dtype import Map
from narwhals_map._version import __version__

__all__ = ["Map", "__version__"]
