# ---------------------------------------------------------------------------
# Rudimentary per-column statistics over the merged table.
# Pure stdlib — no numpy / pandas.
# ---------------------------------------------------------------------------

import math
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Math helpers
# ---------------------------------------------------------------------------

def _percentile(sorted_vals: List[float], p: float) -> float:
    """
    Compute percentile p (0-100) of a pre-sorted list using
    linear interpolation (method 'inclusive').
    """
    n = len(sorted_vals)
    if n == 1:
        return sorted_vals[0]
    idx   = (p / 100) * (n - 1)
    lower = int(idx)
    upper = min(lower + 1, n - 1)
    frac  = idx - lower
    return sorted_vals[lower] + frac * (sorted_vals[upper] - sorted_vals[lower])


def _stddev(vals: List[float], mean: float) -> float:
    if len(vals) < 2:
        return 0.0
    variance = sum((v - mean) ** 2 for v in vals) / (len(vals) - 1)
    return math.sqrt(variance)


# ---------------------------------------------------------------------------
# Core statistics
# ---------------------------------------------------------------------------

def compute_column_stats(
    table:       List[Dict[str, Any]],
    column:      str,
    percentiles: List[int] = None,
) -> Dict[str, Any]:
    """
    Compute statistics for a single column of the merged table.

    Returns
    -------
    {
        "column":       str,
        "count":        int,   ← total rows
        "valid":        int,   ← non-None values
        "missing":      int,
        "missing_pct":  float,
        "min":          float | None,
        "max":          float | None,
        "mean":         float | None,
        "median":       float | None,
        "std_dev":      float | None,
        "percentiles":  {str: float} | None,
    }
    """
    if percentiles is None:
        percentiles = [25, 50, 75, 95]

    total  = len(table)
    vals   = [row[column] for row in table if row.get(column) is not None]
    valid  = len(vals)
    miss   = total - valid

    if not vals:
        return {
            "column":      column,
            "count":       total,
            "valid":       0,
            "missing":     miss,
            "missing_pct": 100.0,
            "min":         None,
            "max":         None,
            "mean":        None,
            "median":      None,
            "std_dev":     None,
            "percentiles": None,
        }

    sorted_vals = sorted(vals)
    mean        = sum(vals) / valid

    return {
        "column":      column,
        "count":       total,
        "valid":       valid,
        "missing":     miss,
        "missing_pct": round(miss / total * 100, 2) if total else 0.0,
        "min":         round(sorted_vals[0],  6),
        "max":         round(sorted_vals[-1], 6),
        "mean":        round(mean, 6),
        "median":      round(_percentile(sorted_vals, 50), 6),
        "std_dev":     round(_stddev(vals, mean), 6),
        "percentiles": {
            f"p{p}": round(_percentile(sorted_vals, p), 6)
            for p in percentiles
        },
    }


def compute_all_stats(
    table:       List[Dict[str, Any]],
    columns:     List[str],
    percentiles: List[int] = None,
) -> Dict[str, Any]:
    """
    Compute statistics for every data column in the merged table.

    Returns
    -------
    {
        "total_rows":       int,
        "time_start":       str | None,
        "time_end":         str | None,
        "column_stats":     [per-column stat dicts]
    }
    """
    if percentiles is None:
        percentiles = [25, 50, 75, 95]

    timestamps = [row["timestamp"] for row in table if row.get("timestamp")]
    time_start = min(timestamps) if timestamps else None
    time_end   = max(timestamps) if timestamps else None

    col_stats = [
        compute_column_stats(table, col, percentiles)
        for col in columns
    ]

    return {
        "total_rows":   len(table),
        "time_start":   time_start,
        "time_end":     time_end,
        "column_stats": col_stats,
    }


# ---------------------------------------------------------------------------
# Pretty print
# ---------------------------------------------------------------------------

def print_stats(stats: Dict[str, Any]) -> None:
    """Print statistics to stdout in a readable table."""
    print(f"\n{'=' * 62}")
    print(f"  MERGED DATA STATISTICS")
    print(f"{'=' * 62}")
    print(f"  Total rows  : {stats['total_rows']}")
    print(f"  Time start  : {stats['time_start']}")
    print(f"  Time end    : {stats['time_end']}")

    for cs in stats.get("column_stats", []):
        print(f"\n  {'─' * 58}")
        print(f"  Column      : {cs['column']}")
        print(f"  Valid       : {cs['valid']} / {cs['count']}  "
              f"(missing: {cs['missing_pct']}%)")

        if cs["min"] is None:
            print("  (no valid values)")
            continue

        print(f"  Min         : {cs['min']}")
        print(f"  Max         : {cs['max']}")
        print(f"  Mean        : {cs['mean']}")
        print(f"  Median      : {cs['median']}")
        print(f"  Std-Dev     : {cs['std_dev']}")

        if cs["percentiles"]:
            pct_str = "  ".join(
                f"{k}={v}" for k, v in cs["percentiles"].items()
            )
            print(f"  Percentiles : {pct_str}")

    print(f"{'=' * 62}\n")
