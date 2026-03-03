import csv
import datetime as dt
import json
import re
from pathlib import Path
from typing import List, Optional

from src.app_opc_reader.logic.helper import repo_root, HistoryValue


def _sanitize_filename(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_\-]+", "_", s)
    return s[:120] if len(s) > 120 else s


def _fmt_date(ts: Optional[dt.datetime]) -> str:
    """datetime → dd.mm.yy  (Bereichs-Datum im Dateinamen)"""
    if ts is None:
        return "None"
    return ts.strftime("%d.%m.%y")


def _fmt_export_ts(ts: Optional[dt.datetime]) -> str:
    """
    datetime → ISO mit filesystem-sicheren Zeichen
    Beispiel: 2026-03-03T23_59_59.99
              Kolons  →  Unterstriche
              Mikrosekunden → 2-stellige Millisekunden (gerundet)
    """
    if ts is None:
        return "None"
    # Millisekunden 2-stellig (erste 2 Stellen der Mikrosekunden)
    ms2 = f"{ts.microsecond // 10000:02d}"
    return ts.strftime(f"%Y-%m-%dT%H_%M_%S.{ms2}")


def _format_utc(ts) -> Optional[str]:
    """datetime → ISO-String (für JSON/CSV-Inhalt, unverändert)"""
    if ts is None:
        return None
    return ts.isoformat()

def _format_local(ts, utc_offset_hours: float = 0.0) -> Optional[str]:
    """Konvertiert UTC-Timestamp → Lokalzeit ISO-String."""
    if ts is None:
        return None
    tz     = dt.timezone(dt.timedelta(hours=utc_offset_hours))
    ts_utc = ts if ts.tzinfo else ts.replace(tzinfo=dt.timezone.utc)
    return ts_utc.astimezone(tz).isoformat()



class HistoryDataExporter:
    def __init__(self) -> None:
        root = repo_root()
        self.data_dir = (root / "data").resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)



    @staticmethod
    def build_basename(
        tag_description: str,
        start_utc: Optional[dt.datetime],
        end_utc: Optional[dt.datetime],
        export_utc: Optional[dt.datetime] = None,
    ) -> str:
        """
        Schema: <TagName>_<dd.mm.yy>-<dd.mm.yy>_<ISO-Export>
        Beispiel:
          Kessel-Temperatur_01.02.26-03.03.26_2026-03-03T23_59_59.99
        """
        if export_utc is None:
            export_utc = dt.datetime.now(dt.timezone.utc)

        tag    = _sanitize_filename(tag_description)
        start  = _fmt_date(start_utc)
        end    = _fmt_date(end_utc)
        exp    = _fmt_export_ts(export_utc)

        return f"{tag}_{start}-{end}_{exp}"

    @staticmethod
    def write_csv(path: Path, values: List[HistoryValue]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["source_timestamp", "server_timestamp", "value", "status_code"])
            for hv in values:
                w.writerow([
                    _format_utc(hv.source_timestamp),
                    _format_utc(hv.server_timestamp),
                    hv.value,
                    hv.status_code,
                ])

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
            start_utc: Optional[dt.datetime],
            end_utc: Optional[dt.datetime],
            values: List[HistoryValue],
            write_csv: bool = True,
            write_json: bool = True,
            extra_meta: Optional[dict] = None,
    ) -> dict:
        export_utc = dt.datetime.now(dt.timezone.utc)
        basename   = self.build_basename(tag_description, start_utc, end_utc, export_utc)

        csv_path  = self.data_dir / f"{basename}.csv"
        json_path = self.data_dir / f"{basename}.json"

        meta = {
            "tag_description": tag_description,
            "start_utc":       _format_utc(start_utc),
            "end_utc":         _format_utc(end_utc),
            "exported_utc":    _format_utc(export_utc),
        }
        if extra_meta:
            meta.update(extra_meta)

        if write_csv:
            self.write_csv(csv_path, values)

        if write_json:
            self.write_json(json_path, values, meta=meta)

        return {
            "tag":       tag_description,
            "csv_path":  str(csv_path)  if write_csv  else None,
            "json_path": str(json_path) if write_json else None,
            "count":     len(values),
        }
