# narwhals-map

An experimental Narwhals plugin adding `Map` datatype.

> [!TIP]
> See [`narwhals-dev/narwhals#3525`](https://github.com/narwhals-dev/narwhals/issues/3525) issue for more info.

## Summary

`Map` is natively supported across all backends except for Polars.

Supported Narwhals backends:
  - Arrow
  - Ibis
  - Polars (via [`polars-map`](https://github.com/hafaio/polars-map))

Currently monkey-patches Narwhals to expose a new `nw.col.map` namespace. Methods implemented:
  - `nw.col.map.get`
