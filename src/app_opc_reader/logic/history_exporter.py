import csv
import json
import re
from dataclasses import asdict
from pathlib import Path
from typing import List, Optional

from src.app.logic.helper import project_root, HistoryValue


def _sanitize_filename(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_.-]+", "_", s)
    return s[:180] if len(s) > 180 else s


def _format_utc(ts) -> Optional[str]:
    if ts is None:
        return None
    # ts kann aware oder naive sein; wir schreiben ISO
    return ts.isoformat()


class HistoryDataExporter:
    def __init__(self, data_dir: Optional[Path] = None) -> None:
        root = project_root()
        self.data_dir = (data_dir or (root / "data")).resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def build_basename(tag_description: str, start_utc, end_utc) -> str:
        tag = _sanitize_filename(tag_description)
        start_s = _sanitize_filename(_format_utc(start_utc) or "None")
        end_s = _sanitize_filename(_format_utc(end_utc) or "None")
        return f"{tag}__{start_s}__{end_s}"

    @staticmethod
    def write_csv(path: Path, values: List[HistoryValue]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["source_timestamp", "server_timestamp", "value", "status_code"])
            for hv in values:
                w.writerow(
                    [
                        _format_utc(hv.source_timestamp),
                        _format_utc(hv.server_timestamp),
                        hv.value,
                        hv.status_code,
                    ]
                )

    @staticmethod
    def write_json(path: Path, values: List[HistoryValue], meta: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)

        payload = {
            "meta": meta,
            "count": len(values),
            "values": [
                {
                    "source_timestamp": _format_utc(v.source_timestamp),
                    "server_timestamp": _format_utc(v.server_timestamp),
                    "value": v.value,
                    "status_code": v.status_code,
                }
                for v in values
            ],
        }

        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    def export(
        self,
        tag_description: str,
        start_utc,
        end_utc,
        values: List[HistoryValue],
        write_csv: bool = True,
        write_json: bool = True,
        extra_meta: Optional[dict] = None,
    ) -> dict:
        basename = self.build_basename(tag_description, start_utc, end_utc)

        csv_path = self.data_dir / f"{basename}.csv"
        json_path = self.data_dir / f"{basename}.json"

        meta = {"tag_description": tag_description, "start_utc": _format_utc(start_utc), "end_utc": _format_utc(end_utc)}
        if extra_meta:
            meta.update(extra_meta)

        if write_csv:
            self.write_csv(csv_path, values)

        if write_json:
            self.write_json(json_path, values, meta=meta)

        return {
            "csv_path": str(csv_path) if write_csv else None,
            "json_path": str(json_path) if write_json else None,
            "count": len(values),
        }
