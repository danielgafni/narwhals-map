# narwhals-map

[![PyPI](https://img.shields.io/pypi/v/narwhals-map)](https://pypi.org/project/narwhals-map/)

An experimental Narwhals plugin adding `Map` datatype.

> [!TIP]
> See [`narwhals-dev/narwhals#3525`](https://github.com/narwhals-dev/narwhals/issues/3525) issue for more info.

`Map` is natively supported across all backends except for Polars.

Supported Narwhals backends:
  - Arrow
  - Ibis
  - Polars (via [`polars-map`](https://pypi.org/project/polars-map/))

Currently monkey-patches Narwhals to expose a new `nw.col.map` namespace.

## Installation

```console
$ uv add narwhals-map
```

## Usage

```python
import narwhals as nw
import narwhals_map  # registers the Map dtype and .map namespace

df = nw.from_native(native_df)  # use polars_map.Map for Polars-backed frames
result = df.select(nw.col("map_col").map.get("key1"))
```
