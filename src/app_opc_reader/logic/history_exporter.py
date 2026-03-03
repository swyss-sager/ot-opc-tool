# ---------------------------------------------------------------------------
# Writes historian data to CSV and / or JSON.
# Filename schema: <Tag>_<dd.mm.yy>-<dd.mm.yy>_<ISO-export-ts>
# ---------------------------------------------------------------------------

import csv
import datetime as dt
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.app_opc_reader.logic.helper import HistoryValue, repo_root


# ---------------------------------------------------------------------------
# Internal formatting helpers
# ---------------------------------------------------------------------------

def _sanitize(s: str) -> str:
    """Strip / replace characters that are unsafe in file names."""
    s = s.strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_\-]+", "_", s)
    return s[:120]


def _fmt_date(ts: Optional[dt.datetime]) -> str:
    """Format datetime as dd.mm.yy (used for range segment in filename)."""
    return ts.strftime("%d.%m.%y") if ts else "None"


def _fmt_export_ts(ts: Optional[dt.datetime]) -> str:
    """
    Format export timestamp as filesystem-safe ISO string.
    Colons replaced by underscores; microseconds truncated to 2 digits.
    Example: 2026-03-03T14_37_22.05
    """
    if ts is None:
        return "None"
    ms2 = f"{ts.microsecond // 10_000:02d}"
    return ts.strftime(f"%Y-%m-%dT%H_%M_%S.{ms2}")


def _fmt_iso(ts: Optional[dt.datetime]) -> Optional[str]:
    """Return full ISO 8601 string for JSON / CSV cell content."""
    return ts.isoformat() if ts else None


def _to_local(ts: Optional[dt.datetime], utc_offset_hours: float) -> Optional[str]:
    """Convert a UTC-aware (or naive-UTC) datetime to local ISO string."""
    if ts is None:
        return None
    tz     = dt.timezone(dt.timedelta(hours=utc_offset_hours))
    ts_utc = ts if ts.tzinfo else ts.replace(tzinfo=dt.timezone.utc)
    return ts_utc.astimezone(tz).isoformat()


# ---------------------------------------------------------------------------
# Exporter
# ---------------------------------------------------------------------------

class HistoryDataExporter:
    """Serialises HistoryValue lists to CSV and JSON files."""

    def __init__(self) -> None:
        self.data_dir = (repo_root() / "data").resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)

    # -- Filename ------------------------------------------------------------

    @staticmethod
    def build_basename(
        tag_description: str,
        start_utc:       Optional[dt.datetime],
        end_utc:         Optional[dt.datetime],
        export_utc:      Optional[dt.datetime] = None,
    ) -> str:
        """
        Build the base filename (without extension).
        Schema: <Tag>_<dd.mm.yy>-<dd.mm.yy>_<ISO-export>
        """
        if export_utc is None:
            export_utc = dt.datetime.now(dt.timezone.utc)

        return (
            f"{_sanitize(tag_description)}"
            f"_{_fmt_date(start_utc)}-{_fmt_date(end_utc)}"
            f"_{_fmt_export_ts(export_utc)}"
        )

    # -- Writers -------------------------------------------------------------

    @staticmethod
    def write_csv(
        path: Path,
        values: List[HistoryValue],
        utc_offset_hours: float = 0.0,
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["source_timestamp", "server_timestamp", "value", "status_code"])
            for hv in values:
                w.writerow([
                    _to_local(hv.source_timestamp, utc_offset_hours),
                    _to_local(hv.server_timestamp, utc_offset_hours),
                    hv.value,
                    hv.status_code,
                ])

    @staticmethod
    def write_json(
        path: Path,
        values: List[HistoryValue],
        meta: Dict[str, Any],
        utc_offset_hours: float = 0.0,
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "meta":   meta,
            "count":  len(values),
            "values": [
                {
                    "source_timestamp": _to_local(v.source_timestamp, utc_offset_hours),
                    "server_timestamp": _to_local(v.server_timestamp, utc_offset_hours),
                    "value":            v.value,
                    "status_code":      v.status_code,
                }
                for v in values
            ],
        }
        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    # -- Public API ----------------------------------------------------------

    def export(
        self,
        tag_description:  str,
        start_utc:        Optional[dt.datetime],
        end_utc:          Optional[dt.datetime],
        values:           List[HistoryValue],
        write_csv:        bool  = True,
        write_json:       bool  = True,
        utc_offset_hours: float = 0.0,
        extra_meta:       Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        timestamp_format: str = "%Y-%m-%d_%H-%M-%S"
        ts = dt.datetime.now().strftime(timestamp_format)
        basename   = self.build_basename(tag_description, start_utc, end_utc)

        csv_path  = self.data_dir / f"{basename}__{ts}.csv"
        json_path = self.data_dir / f"{basename}__{ts}.json"

        meta: Dict[str, Any] = {
            "tag_description": tag_description,
            "start_utc":       _fmt_iso(start_utc),
            "end_utc":         _fmt_iso(end_utc),
            "exported_utc":    ts,
            "utc_offset_hours": utc_offset_hours,
        }
        if extra_meta:
            meta.update(extra_meta)

        if write_csv:
            self.write_csv(csv_path, values, utc_offset_hours)
        if write_json:
            self.write_json(json_path, values, meta, utc_offset_hours)

        return {
            "tag":       tag_description,
            "csv_path":  str(csv_path)  if write_csv  else None,
            "json_path": str(json_path) if write_json else None,
            "count":     len(values),
        }
