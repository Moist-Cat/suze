"""Data profiling for list[dict] payloads.

Call from a `transform` stage or a manual debug hook. For richer
profiling, pipe into `ydata-profiling` on the resulting polars frame.
"""

from collections import Counter
from typing import Any


def _infer_type(values: list) -> str:
    types = {type(v).__name__ for v in values if v is not None}
    if not types:
        return "null"
    if len(types) == 1:
        return types.pop()
    return "mixed"


def _column_stats(values: list) -> dict:
    total = len(values)
    non_null = [v for v in values if v is not None]
    stats = {
        "count": total,
        "non_null": len(non_null),
        "null": total - len(non_null),
        "null_pct": (total - len(non_null)) / total if total else 0.0,
        "dtype": _infer_type(values),
        "unique": len({str(v) for v in non_null}),
    }
    if non_null and all(isinstance(v, (int, float)) for v in non_null):
        stats["min"] = min(non_null)
        stats["max"] = max(non_null)
        stats["mean"] = sum(non_null) / len(non_null)
    return stats


def profile(data: Any, *, top_k: int = 5) -> dict:
    if not isinstance(data, list):
        data = [data]
    if not data or not isinstance(data[0], dict):
        return {"rows": len(data), "columns": {}, "note": "non-tabular data"}

    columns: dict[str, list] = {}
    for row in data:
        for k in row:
            columns.setdefault(k, []).append(row.get(k))

    report = {
        "rows": len(data),
        "columns": {k: _column_stats(v) for k, v in columns.items()},
    }
    for k, vals in columns.items():
        col = report["columns"][k]
        if col["dtype"] == "str" and col["unique"] <= 100:
            col["top"] = Counter(v for v in vals if v is not None).most_common(top_k)
    return report


def to_dataframe(data):
    """Convert to polars DataFrame for downstream profiling / plotting."""
    import polars as pl
    return pl.DataFrame(data)
