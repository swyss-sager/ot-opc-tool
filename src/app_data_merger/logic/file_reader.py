# ---------------------------------------------------------------------------
# Reads CSV or JSON historian export files into a unified list of dicts:
#   [{"source_timestamp": str, "value": float|None}, ...]
# ---------------------------------------------------------------------------

import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _coerce_value(raw: Any) -> Optional[float]:
    """Try to cast a raw cell value to float; return None on failure."""
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return float(str(raw).replace(",", "."))
    except (ValueError, TypeError):
        return None


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    """
    Parse a historian CSV export.
    Expected columns: source_timestamp, server_timestamp, value, status_code
    """
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = row.get("source_timestamp", "").strip()
            if not ts:
                continue
            rows.append({
                "source_timestamp": ts,
                "value":            _coerce_value(row.get("value")),
            })
    return rows


def _read_json(path: Path) -> List[Dict[str, Any]]:
    """
    Parse a historian JSON export.
    Expected structure: { "values": [{ "source_timestamp": ..., "value": ... }] }
    """
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows: List[Dict[str, Any]] = []
    for entry in payload.get("values", []):
        ts = (entry.get("source_timestamp") or "").strip()
        if not ts:
            continue
        rows.append({
            "source_timestamp": ts,
            "value":            _coerce_value(entry.get("value")),
        })
    return rows


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def read_file(path: str | Path, fmt: str = "csv") -> List[Dict[str, Any]]:
    """
    Read a single historian export file.

    Parameters
    ----------
    path : path to the file
    fmt  : 'csv' or 'json'

    Returns
    -------
    List of {"source_timestamp": str, "value": float|None}
    """
    p = Path(path).resolve()
    if not p.exists():
        raise FileNotFoundError(f"Input file not found: {p}")

    fmt = fmt.lower().strip()
    if fmt == "csv":
        return _read_csv(p)
    if fmt == "json":
        return _read_json(p)
    raise ValueError(f"Unsupported format: {fmt!r}  (allowed: 'csv', 'json')")


def read_in_memory(
    values: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Accept already-loaded HistoryValue dicts (from a live OPC export)
    and normalise them to the same schema as read_file().
    """
    rows: List[Dict[str, Any]] = []
    for v in values:
        ts = v.get("source_timestamp") or ""
        if hasattr(ts, "isoformat"):
            ts = ts.isoformat()
        ts = str(ts).strip()
        if not ts:
            continue
        rows.append({
            "source_timestamp": ts,
            "value":            _coerce_value(v.get("value")),
        })
    return rows
