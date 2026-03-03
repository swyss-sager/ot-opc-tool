import datetime as dt
from pathlib import Path
from typing import Any, Dict, Tuple

from src.app_opc_reader.logic.config_loader import ConfigLoader
from src.app_opc_reader.logic.helper import aware_utc_now, ensure_aware_utc, load_env_file, require_env, repo_root
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
        r = ph_cfg.get("range", {"mode": "last_minutes", "last_minutes": 1440})
        mode = r.get("mode", "last_minutes")
        end_utc = aware_utc_now()

        if mode == "last_minutes":
            minutes = int(r.get("last_minutes", 1440))
            return end_utc - dt.timedelta(minutes=minutes), end_utc

        raise ValueError(f"Unsupported range.mode: {mode}")

    def run(self, debug: bool = True) -> dict:
        root = repo_root()
        ph_cfg: Dict[str, Any] = self.cfg["process_historian"]
        ex_cfg: Dict[str, Any] = self.cfg.get("export", {})

        endpoint_url = ph_cfg["endpoint_url"]
        node_id = ph_cfg["node_id"]
        tag_desc = ph_cfg.get("tag_description", "tag")
        page_size = int(ph_cfg.get("page_size", 10000))

        start_utc, end_utc = self._compute_range(ph_cfg)
        start_utc, end_utc = ensure_aware_utc(start_utc), ensure_aware_utc(end_utc)

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
                print("Trusted-Store (für Server-Zertifikate):", reader.trusted_store)
                reader.debug_node()

            values = reader.read_history_paged(start_utc, end_utc, page_size=page_size)

            # Fallback, wenn leer
            fb = ph_cfg.get("fallback", {})
            if not values and bool(fb.get("enabled", True)):
                hv = reader.probe_bounds_last_value(probe_minutes=int(fb.get("probe_minutes", 30)))
                if hv and hv.source_timestamp:
                    center = ensure_aware_utc(hv.source_timestamp)
                    around_h = int(fb.get("around_hours", 48))
                    start_utc = center - dt.timedelta(hours=around_h)
                    end_utc = center + dt.timedelta(hours=around_h)
                    if debug:
                        print("No data in requested range -> fallback around:", center)
                    values = reader.read_history_paged(start_utc, end_utc, page_size=page_size)

        out = exporter.export(
            tag_description=tag_desc,
            start_utc=start_utc,
            end_utc=end_utc,
            values=values,
            write_csv=bool(ex_cfg.get("write_csv", True)),
            write_json=bool(ex_cfg.get("write_json", True)),
            extra_meta={"endpoint_url": endpoint_url, "node_id": node_id, "page_size": page_size},
        )

        if debug:
            print("Export:", out)
        return out
