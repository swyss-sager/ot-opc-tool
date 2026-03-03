# ---------------------------------------------------------------------------
# Orchestrates config loading, time-range computation, OPC UA reads
# and file export for all configured tags in one session.
# ---------------------------------------------------------------------------

import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
        self.cfg      = ConfigLoader().load()
        env           = load_env_file()
        self.username = require_env(env, "SAGERPH_USERNAME")
        self.password = env.get("SAGERPH_PASSWORD", "")

    # -- Time range ----------------------------------------------------------

    @staticmethod
    def _compute_range(
        ph_cfg: Dict[str, Any]
    ) -> Tuple[dt.datetime, dt.datetime]:
        """
        Resolve start/end UTC datetimes from config.
        Supports modes: 'last_minutes', 'absolute'.
        """
        r              = ph_cfg.get("range", {"mode": "last_minutes", "last_minutes": 1440})
        mode           = r.get("mode", "last_minutes")
        utc_offset     = float(ph_cfg.get("utc_offset_hours", 0.0))

        if mode == "last_minutes":
            minutes = int(r.get("last_minutes", 1440))
            end_utc = aware_utc_now()
            return end_utc - dt.timedelta(minutes=minutes), end_utc

        if mode == "absolute":
            start_text    = r.get("start")
            end_text      = r.get("end")
            end_inclusive = bool(r.get("end_inclusive", True))
            if not start_text or not end_text:
                raise ValueError(
                    "range.mode='absolute' requires 'start' and 'end' in config.json"
                )
            return parse_absolute_range(
                start_text=start_text,
                end_text=end_text,
                end_inclusive=end_inclusive,
                utc_offset_hours=utc_offset,
            )

        raise ValueError(
            f"Unknown range.mode: {mode!r}  "
            f"(allowed: 'last_minutes', 'absolute')"
        )

    # -- Tag list ------------------------------------------------------------

    @staticmethod
    def _get_tags(ph_cfg: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Return the tag list from config.
        Supports both the new 'tags' array and the legacy single-tag format.
        """
        if "tags" in ph_cfg:
            tags = ph_cfg["tags"]
            if not tags:
                raise ValueError("config.json: 'tags' array is empty.")
            return tags

        node_id  = ph_cfg.get("node_id")
        tag_desc = ph_cfg.get("tag_description", "tag")
        if not node_id:
            raise ValueError(
                "config.json: Neither 'tags' array nor 'node_id' found."
            )
        return [{"tag_description": tag_desc, "node_id": node_id}]

    # -- Fallback read -------------------------------------------------------

    @staticmethod
    def _fallback_read(
        reader:    ProcessHistorianReader,
        fb_cfg:    Dict[str, Any],
        page_size: int,
        debug:     bool,
    ) -> Tuple[List, Optional[dt.datetime], Optional[dt.datetime]]:
        """
        Probe the historian for the most recent value and read
        a symmetric window around it.  Returns (values, start, end).
        """
        hv = reader.probe_last_value(
            probe_minutes=int(fb_cfg.get("probe_minutes", 30))
        )
        if hv and hv.source_timestamp:
            center   = ensure_aware_utc(hv.source_timestamp)
            around_h = int(fb_cfg.get("around_hours", 48))
            fb_start = center - dt.timedelta(hours=around_h)
            fb_end   = center + dt.timedelta(hours=around_h)
            if debug:
                print(f"  [fallback] no data -> window around {center.isoformat()}")
            values = reader.read_history_paged(fb_start, fb_end, page_size)
            return values, fb_start, fb_end
        return [], None, None

    # -- Main ----------------------------------------------------------------

    def run(self, debug: bool = True) -> List[Dict[str, Any]]:
        """
        Connect once, iterate over all configured tags, export each to
        its own CSV / JSON file.  Returns a list of export result dicts.
        """
        ph_cfg: Dict[str, Any] = self.cfg["process_historian"]
        ex_cfg: Dict[str, Any] = self.cfg.get("export", {})

        tags             = self._get_tags(ph_cfg)
        page_size        = int(ph_cfg.get("page_size", 10_000))
        utc_offset       = float(ph_cfg.get("utc_offset_hours", 0.0))
        fb_cfg           = ph_cfg.get("fallback", {})
        fallback_enabled = bool(fb_cfg.get("enabled", False))

        range_start, range_end = self._compute_range(ph_cfg)
        range_start = ensure_aware_utc(range_start)
        range_end   = ensure_aware_utc(range_end)

        if debug:
            print(f"\n[range]  {range_start.isoformat()}  ->  {range_end.isoformat()}")
            print(f"[tags]   {len(tags)} configured\n")

        exporter = HistoryDataExporter()
        results:  List[Dict[str, Any]] = []

        reader = ProcessHistorianReader(
            endpoint_url=ph_cfg["endpoint_url"],
            node_id=tags[0]["node_id"],
            username=self.username,
            password=self.password,
            pki_dir=(repo_root() / "security" / "pki_sagerph"),
            application_uri=ph_cfg["application_uri"],
            security_policy=ph_cfg.get("security_policy", "Basic128Rsa15"),
            message_security_mode=ph_cfg.get(
                "message_security_mode", "SignAndEncrypt"),
            timeout_s=int(ph_cfg.get("timeout_s", 30)),
        )

        with reader:
            if debug:
                print(f"[trusted-store]  {reader.trusted_store}\n")

            for idx, tag_cfg in enumerate(tags):
                node_id  = tag_cfg["node_id"]
                tag_desc = tag_cfg.get("tag_description", f"tag_{idx}")

                reader.node_id = node_id

                print(f"{'─' * 60}")
                print(f"[{idx + 1}/{len(tags)}]  {tag_desc}")
                if debug:
                    print(f"  node_id : {node_id[:72]}...")
                    reader.debug_node()

                t_start = range_start
                t_end   = range_end
                values  = reader.read_history_paged(t_start, t_end, page_size)

                if not values and fallback_enabled:
                    values, fb_start, fb_end = self._fallback_read(
                        reader, fb_cfg, page_size, debug
                    )
                    if fb_start and fb_end:
                        t_start, t_end = fb_start, fb_end

                print(f"  -> {len(values)} values read")

                out = exporter.export(
                    tag_description=tag_desc,
                    start_utc=t_start,
                    end_utc=t_end,
                    values=values,
                    write_csv=bool(ex_cfg.get("write_csv", True)),
                    write_json=bool(ex_cfg.get("write_json", True)),
                    utc_offset_hours=utc_offset,
                    extra_meta={
                        "endpoint_url": ph_cfg["endpoint_url"],
                        "node_id":      node_id,
                        "page_size":    page_size,
                    },
                )

                if debug:
                    print(f"  -> csv  : {out['csv_path']}")
                    print(f"  -> json : {out['json_path']}")

                results.append(out)

        print(f"\n{'=' * 60}")
        print(f"[done]  {len(results)} tag(s) exported")
        for r in results:
            print(f"  {r['tag']:<30}  {r['count']:>8} values")

        return results
