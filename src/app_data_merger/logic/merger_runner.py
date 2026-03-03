# ---------------------------------------------------------------------------
# Orchestrates file reading, merging, statistics and file export.
# Can be driven by:
#   (a) standalone merger_config.json  (standalone mode)
#   (b) inline series list             (injected by OPC runner after export)
# ---------------------------------------------------------------------------

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.app_data_merger.logic.file_reader import read_file, read_in_memory
from src.app_data_merger.logic.data_merger import (
    build_merged_table,
    export_merged,
)
from src.app_data_merger.logic.statistics import compute_all_stats, print_stats
from src.app_data_merger.logic.merger_config_loader import MergerConfigLoader


class MergerRunner:

    def __init__(
        self,
        config_path: Optional[Path] = None,
        inline_cfg:  Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Parameters
        ----------
        config_path : path to merger_config.json  (standalone mode)
        inline_cfg  : pre-built config dict        (OPC runner injection)
        """
        if inline_cfg:
            self.cfg = MergerConfigLoader.from_dict(inline_cfg)
        else:
            self.cfg = MergerConfigLoader(config_path).load()

    # -- Series loading ------------------------------------------------------

    @staticmethod
    def _load_series_from_files(
        input_files: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Read each configured file and return tagged series list."""
        series: List[Dict[str, Any]] = []
        for f_cfg in input_files:
            path   = f_cfg["path"]
            column = f_cfg["column"]
            fmt    = f_cfg.get("format", "csv")
            print(f"  [read]  {column}  <-  {path}")
            rows = read_file(path, fmt)
            series.append({"column": column, "rows": rows})
        return series

    @staticmethod
    def _load_series_from_memory(
        in_memory: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Accept series data already in memory (live export).
        in_memory: [{"column": str, "rows": [HistoryValue-like dicts]}, ...]
        """
        series: List[Dict[str, Any]] = []
        for s in in_memory:
            rows = read_in_memory(s["rows"])
            series.append({"column": s["column"], "rows": rows})
        return series

    # -- Main ----------------------------------------------------------------

    def run(
        self,
        in_memory: Optional[List[Dict[str, Any]]] = None,
        debug:     bool = True,
    ) -> Dict[str, Any]:
        """
        Execute the full merge pipeline.

        Parameters
        ----------
        in_memory : optional pre-loaded series (from OPC runner)
        debug     : print progress and statistics to stdout

        Returns
        -------
        {
            "csv_path":   str | None,
            "json_path":  str | None,
            "row_count":  int,
            "columns":    [str],
            "statistics": dict,
        }
        """
        m_cfg = self.cfg.get("merger", self.cfg)

        resolution_ms  = int(m_cfg.get("ts_resolution_ms", 500))
        output_dir     = m_cfg.get("output_dir", "data/merged")
        output_basename = m_cfg.get("output_basename", "merged_output")
        write_csv      = bool(m_cfg.get("write_csv", True))
        write_json_out = bool(m_cfg.get("write_json", True))
        stats_cfg      = m_cfg.get("statistics", {})
        stats_enabled  = bool(stats_cfg.get("enabled", True))
        percentiles    = stats_cfg.get("percentiles", [25, 50, 75, 95])

        # -- Load series -----------------------------------------------------
        if in_memory:
            if debug:
                print("\n[merger]  loading from memory (live export)")
            series = self._load_series_from_memory(in_memory)
        else:
            input_files = m_cfg.get("input_files", [])
            if not input_files:
                raise ValueError(
                    "No input sources defined. "
                    "Set 'input_files' in merger_config.json or pass 'in_memory'."
                )
            if debug:
                print(f"\n[merger]  loading {len(input_files)} file(s)")
            series = self._load_series_from_files(input_files)

        if not series:
            raise ValueError("No series loaded — nothing to merge.")

        columns = [s["column"] for s in series]

        # -- Merge -----------------------------------------------------------
        if debug:
            print(f"[merger]  normalising to {resolution_ms} ms grid ...")
        table = build_merged_table(series, resolution_ms)
        if debug:
            print(f"[merger]  {len(table)} merged rows  |  columns: {columns}")

        # -- Statistics ------------------------------------------------------
        stats: Dict[str, Any] = {}
        if stats_enabled:
            stats = compute_all_stats(table, columns, percentiles)
            if debug:
                print_stats(stats)

        # -- Export ----------------------------------------------------------
        export_result = export_merged(
            series=series,
            output_dir=output_dir,
            output_basename=output_basename,
            resolution_ms=resolution_ms,
            write_csv=write_csv,
            write_json=write_json_out,
            extra_meta={
                "resolution_ms": resolution_ms,
                "columns":       columns,
            },
        )

        if debug:
            print(f"\n[merger]  export complete")
            if export_result["csv_path"]:
                print(f"  -> csv  : {export_result['csv_path']}")
            if export_result["json_path"]:
                print(f"  -> json : {export_result['json_path']}")

        return {
            "csv_path":   export_result["csv_path"],
            "json_path":  export_result["json_path"],
            "row_count":  export_result["row_count"],
            "columns":    export_result["columns"],
            "statistics": stats,
        }

