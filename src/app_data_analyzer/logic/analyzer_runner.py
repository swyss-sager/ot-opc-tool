# ---------------------------------------------------------------------------
# Orchestrates all analyses based on config.
# Combines results into a single report.
# ---------------------------------------------------------------------------

import json
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd
from src.app_data_analyzer.logic.analyzer_config_loader import AnalyzerConfigLoader
from src.app_data_analyzer.logic.data_loader import load_merged_data, prepare_data
from src.app_data_analyzer.logic.descriptive import compute_descriptive_stats
from src.app_data_analyzer.logic.exploratory import perform_eda
from src.app_data_analyzer.logic.diagnostic import perform_diagnostic
from src.app_data_analyzer.logic.inferential import perform_inferential
from src.app_data_analyzer.logic.predictive import perform_predictive
from src.app_data_analyzer.logic.prescriptive import perform_prescriptive
from src.app_data_analyzer.logic.pattern_finder import find_patterns


class AnalyzerRunner:

    def __init__(
        self,
        config_path: Optional[Path] = None,
        inline_cfg: Optional[Dict[str, Any]] = None,
    ) -> None:
        if inline_cfg:
            self.cfg = AnalyzerConfigLoader.from_dict(inline_cfg)
        else:
            self.cfg = AnalyzerConfigLoader(config_path).load()

    def run(
        self,
        debug: bool = True,
    ) -> Dict[str, Any]:
        a_cfg = self.cfg.get("analyzer", self.cfg)

        input_files = a_cfg.get("input_files", [])
        output_dir = Path(a_cfg.get("output_dir", "data/analyzer_output"))
        plots_dir = Path(a_cfg.get("plots_dir", "data/analyzer_plots"))
        write_report = bool(a_cfg.get("write_report_json", True))

        output_dir.mkdir(parents=True, exist_ok=True)
        plots_dir.mkdir(parents=True, exist_ok=True)

        if debug:
            print(f"\n[analyzer]  loading data from {len(input_files)} file(s)")

        # Load and prepare data
        df = load_merged_data(input_files)
        df = prepare_data(df)
        if debug:
            print(f"[analyzer]  {len(df)} rows loaded  |  columns: {list(df.columns)}")

        # Run selected analyses
        report: Dict[str, Any] = {
            "data_summary": {
                "shape": [len(df), len(df.columns)],
                "index_range": [str(df.index.min()), str(df.index.max())],
            },
            "analyses": {}
        }

        analyses_cfg = a_cfg.get("analyses", {})

        # Descriptive
        if a_cfg.get("run_descriptive", True):
            if debug:
                print("[analyzer]  running descriptive analysis")
            report["analyses"]["descriptive"] = compute_descriptive_stats(
                df, analyses_cfg["descriptive"].get("percentiles", [25,50,75,95])
            )

        # Exploratory
        if a_cfg.get("run_exploratory", True):
            if debug:
                print("[analyzer]  running exploratory analysis")
            report["analyses"]["exploratory"] = perform_eda(
                df,
                plots_dir,
                **analyses_cfg["exploratory"]
            )

        # Diagnostic
        if a_cfg.get("run_diagnostic", True):
            if debug:
                print("[analyzer]  running diagnostic analysis")
            report["analyses"]["diagnostic"] = perform_diagnostic(
                df,
                **analyses_cfg["diagnostic"]
            )

        # Inferential
        if a_cfg.get("run_inferential", True):
            if debug:
                print("[analyzer]  running inferential analysis")
            report["analyses"]["inferential"] = perform_inferential(
                df,
                analyses_cfg["inferential"].get("hypothesis_tests", ["t_test"])
            )

        # Predictive
        if a_cfg.get("run_predictive", True):
            if debug:
                print("[analyzer]  running predictive analysis")
            report["analyses"]["predictive"] = perform_predictive(
                df,
                **analyses_cfg["predictive"]
            )

        # Prescriptive
        if a_cfg.get("run_prescriptive", True):
            if debug:
                print("[analyzer]  running prescriptive analysis")
            report["analyses"]["prescriptive"] = perform_prescriptive(
                df,
                **analyses_cfg["prescriptive"]
            )

        # Patterns
        if a_cfg.get("run_patterns", True):
            if debug:
                print("[analyzer]  running pattern analysis")
            report["analyses"]["patterns"] = find_patterns(
                df,
                **analyses_cfg["patterns"]
            )

        # Write report
        if write_report:
            ts = pd.Timestamp.now().isoformat().replace(":", "_")
            report_path = output_dir / f"analysis_report_{ts}.json"
            with report_path.open("w", encoding="utf-8") as f:
                json.dump(report, f, ensure_ascii=False, indent=2, default=str)
            report["report_path"] = str(report_path)
            if debug:
                print(f"[analyzer]  report saved: {report_path}")

        if debug:
            print(f"\n[analyzer]  all analyses complete  |  plots in: {plots_dir}")

        return report
