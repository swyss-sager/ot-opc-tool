# ---------------------------------------------------------------------------
# Orchestrates all analyses based on config.
# ---------------------------------------------------------------------------

import json
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd

from src.app_data_analyzer.logic.analyzer_config_loader import AnalyzerConfigLoader
from src.app_data_analyzer.logic.data_loader           import load_merged_data, prepare_data
from src.app_data_analyzer.logic.descriptive           import compute_descriptive_stats
from src.app_data_analyzer.logic.exploratory           import perform_eda
from src.app_data_analyzer.logic.diagnostic            import perform_diagnostic
from src.app_data_analyzer.logic.inferential           import perform_inferential
from src.app_data_analyzer.logic.predictive            import perform_predictive
from src.app_data_analyzer.logic.prescriptive          import perform_prescriptive
from src.app_data_analyzer.logic.pattern_finder        import find_patterns
from src.app_data_analyzer.logic.distribution_analysis import perform_distribution_analysis
from src.app_data_analyzer.logic.timeseries_analysis   import perform_timeseries_analysis
from src.app_data_analyzer.logic.anomaly_extended      import perform_anomaly_extended
from src.app_data_analyzer.logic.correlation_advanced  import perform_correlation_advanced
from src.app_data_analyzer.logic.quality_metrics       import compute_quality_metrics
from src.app_data_analyzer.logic.visualizations_extended import perform_extended_visualizations
from src.app_data_analyzer.logic.regression_models     import perform_regression_models


class AnalyzerRunner:

    def __init__(
        self,
        config_path: Optional[Path] = None,
        inline_cfg:  Optional[Dict[str, Any]] = None,
    ) -> None:
        if inline_cfg:
            self.cfg = AnalyzerConfigLoader.from_dict(inline_cfg)
        else:
            self.cfg = AnalyzerConfigLoader(config_path).load()

    def run(self, debug: bool = True) -> Dict[str, Any]:
        a_cfg = self.cfg.get("analyzer", self.cfg)

        input_files  = a_cfg.get("input_files", [])
        output_dir   = Path(a_cfg.get("output_dir",  "data/analyzer/_output"))
        plots_dir    = Path(a_cfg.get("plots_dir",   "data/analyzer/analyzer_plots"))
        write_report = bool(a_cfg.get("write_report_json", True))

        output_dir.mkdir(parents=True, exist_ok=True)
        plots_dir.mkdir(parents=True, exist_ok=True)

        if debug:
            print(f"\n[analyzer]  loading data from {len(input_files)} file(s)")

        df = load_merged_data(input_files)
        df = prepare_data(df)

        if debug:
            print(f"[analyzer]  {len(df)} rows  |  columns: {list(df.columns)}")

        report: Dict[str, Any] = {
            "data_summary": {
                "shape":       [len(df), len(df.columns)],
                "index_range": [str(df.index.min()), str(df.index.max())],
                "columns":     list(df.columns),
            },
            "analyses": {},
        }

        an = a_cfg.get("analyses", {})

        # Helper — run a module safely with fallback error entry
        def _run(flag: str, label: str, fn, *args, **kwargs):
            if not a_cfg.get(flag, True):
                return
            if debug:
                print(f"[analyzer]  running {label}")
            try:
                report["analyses"][label] = fn(*args, **kwargs)
            except Exception as exc:
                report["analyses"][label] = {"error": str(exc)}
                if debug:
                    print(f"  [warn]  {label} failed: {exc}")

        # ── Original modules ─────────────────────────────────────────────────
        _run("run_descriptive", "descriptive",
             compute_descriptive_stats,
             df, an.get("descriptive", {}).get("percentiles", [25, 50, 75, 95]))

        _run("run_exploratory", "exploratory",
             perform_eda,
             df, plots_dir,
             **an.get("exploratory", {}))

        _run("run_diagnostic", "diagnostic",
             perform_diagnostic,
             df, **an.get("diagnostic", {}))

        _run("run_inferential", "inferential",
             perform_inferential,
             df, an.get("inferential", {}).get("hypothesis_tests", ["t_test"]))

        _run("run_predictive", "predictive",
             perform_predictive,
             df, **an.get("predictive", {}))

        _run("run_prescriptive", "prescriptive",
             perform_prescriptive,
             df, **an.get("prescriptive", {}))

        _run("run_patterns", "patterns",
             find_patterns,
             df, **an.get("patterns", {}))

        # ── New modules ──────────────────────────────────────────────────────
        _run("run_distribution",    "distribution",
             perform_distribution_analysis,
             df, plots_dir)

        _run("run_timeseries",      "timeseries",
             perform_timeseries_analysis,
             df, plots_dir,
             **an.get("timeseries", {}))

        _run("run_anomaly_ext",     "anomaly_extended",
             perform_anomaly_extended,
             df, plots_dir,
             **an.get("anomaly_extended", {}))

        _run("run_correlation_adv", "correlation_advanced",
             perform_correlation_advanced,
             df, plots_dir,
             **an.get("correlation_advanced", {}))

        _run("run_quality",         "quality_metrics",
             compute_quality_metrics,
             df)

        _run("run_viz_extended",    "visualizations_extended",
             perform_extended_visualizations,
             df, plots_dir)

        _run("run_regression",      "regression_models",
             perform_regression_models,
             df, plots_dir,
             **an.get("regression_models", {}))

        # ── Write report ─────────────────────────────────────────────────────
        if write_report:
            ts          = pd.Timestamp.now().isoformat().replace(":", "_")
            report_path = output_dir / f"analysis_report_{ts}.json"
            with report_path.open("w", encoding="utf-8") as f:
                json.dump(report, f, ensure_ascii=False, indent=2, default=str)
            report["report_path"] = str(report_path)
            if debug:
                print(f"[analyzer]  report saved: {report_path}")

        if debug:
            print(f"\n[analyzer]  all analyses complete  |  plots in: {plots_dir}")

        return report
