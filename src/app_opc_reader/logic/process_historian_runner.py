import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Tuple

from src.app_opc_reader.logic.config_loader import ConfigLoader
from src.app_opc_reader.logic.helper import (
    aware_utc_now,
    ensure_aware_utc,
    load_env_file,
    parse_absolute_range,
    repo_root,
    require_env,
)
from src.app_opc_reader.logic.history_exporter import HistoryDataExporter
from src.app_opc_reader.logic.process_historian_reader import ProcessHistorianReader


class ProcessHistorianRunner:
    def __init__(self) -> None:
        self.cfg = ConfigLoader().load()
        env = load_env_file()
        self.username = require_env(env, "SAGERPH_USERNAME")
        self.password = env.get("SAGERPH_PASSWORD", "")

    # ── Zeitbereich ───────────────────────────────────────────────────────

    @staticmethod
    def _compute_range(ph_cfg: Dict[str, Any]) -> Tuple[dt.datetime, dt.datetime]:
        r = ph_cfg.get("range", {"mode": "last_minutes", "last_minutes": 1440})
        mode = r.get("mode", "last_minutes")

        # UTC-Offset aus Config (default 0 = reines UTC)
        utc_offset = float(ph_cfg.get("utc_offset_hours", 0.0))

        if mode == "last_minutes":
            minutes = int(r.get("last_minutes", 1440))
            end_utc = aware_utc_now()
            return end_utc - dt.timedelta(minutes=minutes), end_utc

        if mode == "absolute":
            start_text = r.get("start")
            end_text = r.get("end")
            end_inclusive = bool(r.get("end_inclusive", True))
            if not start_text or not end_text:
                raise ValueError(
                    "range.mode='absolute' erfordert 'start' und 'end' in config.json"
                )
            return parse_absolute_range(
                start_text=start_text,
                end_text=end_text,
                end_inclusive=end_inclusive,
                utc_offset_hours=utc_offset,  # ← NEU
            )

        raise ValueError(
            f"Unbekannter range.mode: {mode!r}  "
            f"(erlaubt: 'last_minutes', 'absolute')"
        )

    # ── Tags auslesen ─────────────────────────────────────────────────────

    @staticmethod
    def _get_tags(ph_cfg: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Unterstützt beide Formate:
          Neu: "tags": [{"tag_description": "...", "node_id": "..."}, ...]
          Alt: "tag_description": "...", "node_id": "..."  (single tag)
        """
        if "tags" in ph_cfg:
            tags = ph_cfg["tags"]
            if not tags:
                raise ValueError("config.json: 'tags' ist leer.")
            return tags

        # Fallback: altes Einzeltag-Format
        node_id  = ph_cfg.get("node_id")
        tag_desc = ph_cfg.get("tag_description", "tag")
        if not node_id:
            raise ValueError("config.json: Weder 'tags' noch 'node_id' gefunden.")
        return [{"tag_description": tag_desc, "node_id": node_id}]

    # ── Hauptmethode ──────────────────────────────────────────────────────

    def run(self, debug: bool = True) -> List[dict]:
        root   = repo_root()
        ph_cfg: Dict[str, Any] = self.cfg["process_historian"]
        ex_cfg: Dict[str, Any] = self.cfg.get("export", {})

        tags      = self._get_tags(ph_cfg)
        page_size = int(ph_cfg.get("page_size", 10000))

        start_utc, end_utc = self._compute_range(ph_cfg)
        start_utc = ensure_aware_utc(start_utc)
        end_utc   = ensure_aware_utc(end_utc)

        if debug:
            print(f"\n[Range]  {start_utc.isoformat()}  →  {end_utc.isoformat()}")
            print(f"[Tags]   {len(tags)} Tag(s) konfiguriert\n")

        exporter = HistoryDataExporter()
        results: List[dict] = []

        # ── Einzelne OPC-UA-Verbindung für alle Tags ───────────────────
        #    Reader wird hier mit dem node_id des ersten Tags initialisiert,
        #    aber read_history_paged bekommt den node_id pro Tag übergeben.
        #    → Wir nutzen den Reader als Verbindungs-Container;
        #      node_id wird pro Abfrage direkt an den internen Client übergeben.

        first_node_id = tags[0]["node_id"]

        reader = ProcessHistorianReader(
            endpoint_url=ph_cfg["endpoint_url"],
            node_id=first_node_id,          # wird pro Tag überschrieben
            username=self.username,
            password=self.password,
            pki_dir=(root / "security" / "pki_sagerph"),
            application_uri=ph_cfg["application_uri"],
            security_policy=ph_cfg.get("security_policy", "Basic128Rsa15"),
            message_security_mode=ph_cfg.get("message_security_mode", "SignAndEncrypt"),
            timeout_s=int(ph_cfg.get("timeout_s", 30)),
        )

        with reader:
            if debug:
                print("Trusted-Store:", reader.trusted_store)

            for idx, tag_cfg in enumerate(tags):
                node_id  = tag_cfg["node_id"]
                tag_desc = tag_cfg.get("tag_description", f"tag_{idx}")

                # node_id pro Tag setzen
                reader.node_id = node_id

                print(f"\n{'─'*60}")
                print(f"[Tag {idx+1}/{len(tags)}]  {tag_desc}")
                print(f"  NodeId: {node_id[:60]}...")

                if debug:
                    reader.debug_node()

                # Zeitbereich pro Tag (gleicher Bereich für alle Tags)
                t_start = start_utc
                t_end   = end_utc

                values = reader.read_history_paged(t_start, t_end, page_size=page_size)

                # Fallback wenn leer
                fb = ph_cfg.get("fallback", {})
                if not values and bool(fb.get("enabled", False)):
                    hv = reader.probe_bounds_last_value(
                        probe_minutes=int(fb.get("probe_minutes", 30))
                    )
                    if hv and hv.source_timestamp:
                        center   = ensure_aware_utc(hv.source_timestamp)
                        around_h = int(fb.get("around_hours", 48))
                        t_start  = center - dt.timedelta(hours=around_h)
                        t_end    = center + dt.timedelta(hours=around_h)
                        if debug:
                            print(f"  [Fallback] Keine Daten → Bereich um {center.isoformat()}")
                        values = reader.read_history_paged(t_start, t_end, page_size=page_size)

                print(f"  → {len(values)} Werte gelesen")

                # Export
                out = exporter.export(
                    tag_description=tag_desc,
                    start_utc=t_start,
                    end_utc=t_end,
                    values=values,
                    write_csv=bool(ex_cfg.get("write_csv", True)),
                    write_json=bool(ex_cfg.get("write_json", True)),
                    extra_meta={
                        "endpoint_url": ph_cfg["endpoint_url"],
                        "node_id":      node_id,
                        "page_size":    page_size,
                    },
                )

                print(f"  → CSV:  {out['csv_path']}")
                print(f"  → JSON: {out['json_path']}")
                results.append(out)

        print(f"\n{'═'*60}")
        print(f"[Fertig] {len(results)} Tag(s) exportiert.")
        for r in results:
            print(f"  {r['tag']:30s}  {r['count']:>8} Werte")

        return results
