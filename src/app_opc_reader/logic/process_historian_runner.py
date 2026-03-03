import datetime as dt
from pathlib import Path
from typing import Any, Dict, Tuple

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

    @staticmethod
    def _compute_range(ph_cfg: Dict[str, Any]) -> Tuple[dt.datetime, dt.datetime]:
        r    = ph_cfg.get("range", {"mode": "last_minutes", "last_minutes": 1440})
        mode = r.get("mode", "last_minutes")

        # ── Modus 1: Letzte N Minuten ──────────────────────────────
        if mode == "last_minutes":
            minutes = int(r.get("last_minutes", 1440))
            end_utc = aware_utc_now()
            return end_utc - dt.timedelta(minutes=minutes), end_utc

        # ── Modus 2: Absoluter Zeitbereich ─────────────────────────
        if mode == "absolute":
            start_text    = r.get("start")
            end_text      = r.get("end")
            end_inclusive = bool(r.get("end_inclusive", True))

            if not start_text or not end_text:
                raise ValueError(
                    "range.mode='absolute' erfordert 'start' und 'end' in config.json"
                )

            return parse_absolute_range(
                start_text=start_text,
                end_text=end_text,
                end_inclusive=end_inclusive,
            )

        raise ValueError(
            f"Unbekannter range.mode: {mode!r}  "
            f"(erlaubt: 'last_minutes', 'absolute')"
        )

    def run(self, debug: bool = True) -> dict:
        root   = repo_root()
        ph_cfg: Dict[str, Any] = self.cfg["process_historian"]
        ex_cfg: Dict[str, Any] = self.cfg.get("export", {})

        endpoint_url = ph_cfg["endpoint_url"]
        node_id      = ph_cfg["node_id"]
        tag_desc     = ph_cfg.get("tag_description", "tag")
        page_size    = int(ph_cfg.get("page_size", 10000))

        start_utc, end_utc = self._compute_range(ph_cfg)
        start_utc = ensure_aware_utc(start_utc)
        end_utc   = ensure_aware_utc(end_utc)

        if debug:
            print(f"[Range] {start_utc.isoformat()}  →  {end_utc.isoformat()}")

        exporter = HistoryDataExporter()

        reader = ProcessHistorianReader(
            endpoint_url=endpoint_url,
            node_id=node_id,
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
                reader.debug_node()

            values = reader.read_history_paged(start_utc, end_utc, page_size=page_size)

            # Fallback wenn leer
            fb = ph_cfg.get("fallback", {})
            if not values and bool(fb.get("enabled", True)):
                hv = reader.probe_bounds_last_value(
                    probe_minutes=int(fb.get("probe_minutes", 30))
                )
                if hv and hv.source_timestamp:
                    center    = ensure_aware_utc(hv.source_timestamp)
                    around_h  = int(fb.get("around_hours", 48))
                    start_utc = center - dt.timedelta(hours=around_h)
                    end_utc   = center + dt.timedelta(hours=around_h)
                    if debug:
                        print(f"[Fallback] Keine Daten → Bereich um {center.isoformat()}")
                    values = reader.read_history_paged(start_utc, end_utc, page_size=page_size)

        out = exporter.export(
            tag_description=tag_desc,
            start_utc=start_utc,
            end_utc=end_utc,
            values=values,
            write_csv=bool(ex_cfg.get("write_csv", True)),
            write_json=bool(ex_cfg.get("write_json", True)),
            extra_meta={
                "endpoint_url": endpoint_url,
                "node_id": node_id,
                "page_size": page_size,
            },
        )

        if debug:
            print("Export:", out)
        return out
