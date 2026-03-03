# ---------------------------------------------------------------------------
# Merges multiple tagged data series into a single time-indexed table.
#
# Each series is assigned to a normalised 500 ms timestamp slot.
# If two values from the same series fall into the same slot, the
# last one wins (series are assumed to be time-ordered).
# ---------------------------------------------------------------------------

import csv
import json
import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.app_data_merger.logic.ts_normalizer import normalise_ts


# ---------------------------------------------------------------------------
# Core merge logic
# ---------------------------------------------------------------------------

def build_merged_table(
    series:         List[Dict[str, Any]],
    resolution_ms:  int = 500,
) -> List[Dict[str, Any]]:
    """
    Merge multiple named series into one sorted table.

    Parameters
    ----------
    series : list of dicts, each with:
        {
            "column": str,                         ← column name in output
            "rows":   [{"source_timestamp": str,
                        "value": float|None}, ...]
        }
    resolution_ms : normalisation grid in milliseconds

    Returns
    -------
    Sorted list of row dicts:
        {"timestamp": str, "<col1>": val, "<col2>": val, ...}
    """
    columns: List[str] = [s["column"] for s in series]

    # slot_map: normalised_ts -> {col: value}
    slot_map: Dict[str, Dict[str, Optional[float]]] = {}

    for s in series:
        col  = s["column"]
        rows = s["rows"]
        for row in rows:
            raw_ts = row.get("source_timestamp", "")
            norm   = normalise_ts(raw_ts, resolution_ms)
            if norm is None:
                continue
            if norm not in slot_map:
                slot_map[norm] = {}
            slot_map[norm][col] = row.get("value")

    # Build sorted output rows — missing column values stay None.
    merged: List[Dict[str, Any]] = []
    for ts in sorted(slot_map.keys()):
        slot = slot_map[ts]
        row: Dict[str, Any] = {"timestamp": ts}
        for col in columns:
            row[col] = slot.get(col)
        merged.append(row)

    return merged


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def _export_ts() -> str:
    """Filesystem-safe export timestamp."""
    now = dt.datetime.now(dt.timezone.utc)
    ms2 = f"{now.microsecond // 10_000:02d}"
    return now.strftime(f"%Y-%m-%dT%H_%M_%S.{ms2}")


def write_merged_csv(
    path:    Path,
    table:   List[Dict[str, Any]],
    columns: List[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["timestamp"] + columns)
        for row in table:
            w.writerow([row["timestamp"]] + [row.get(c) for c in columns])


def write_merged_json(
    path:    Path,
    table:   List[Dict[str, Any]],
    columns: List[str],
    meta:    Dict[str, Any],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "meta":    meta,
        "columns": ["timestamp"] + columns,
        "count":   len(table),
        "rows":    table,
    }
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Public export entry point
# ---------------------------------------------------------------------------

def export_merged(
    series:         List[Dict[str, Any]],
    output_dir:     str | Path,
    output_basename: str,
    resolution_ms:  int  = 500,
    write_csv:      bool = True,
    write_json:     bool = True,
    extra_meta:     Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build merged table and write CSV / JSON to output_dir.

    Returns dict with file paths and row count.
    """
    output_dir = Path(output_dir).resolve()
    columns    = [s["column"] for s in series]
    table      = build_merged_table(series, resolution_ms)
    exp_ts     = _export_ts()
    basename   = f"{output_basename}_{exp_ts}"
    csv_path   = output_dir / f"{basename}.csv"
    json_path  = output_dir / f"{basename}.json"

    meta: Dict[str, Any] = {
        "exported_utc":   dt.datetime.now(dt.timezone.utc).isoformat(),
        "resolution_ms":  resolution_ms,
        "columns":        columns,
        "row_count":      len(table),
    }
    if extra_meta:
        meta.update(extra_meta)

    if write_csv:
        write_merged_csv(csv_path, table, columns)
    if write_json:
        write_merged_json(json_path, table, columns, meta)

    return {
        "csv_path":  str(csv_path)  if write_csv  else None,
        "json_path": str(json_path) if write_json else None,
        "row_count": len(table),
        "columns":   columns,
    }
