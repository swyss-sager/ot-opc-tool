# ---------------------------------------------------------------------------
# Root entry point — runs all configured components in sequence.
# ---------------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))


# ---------------------------------------------------------------------------
# Component flags
# ---------------------------------------------------------------------------

RUN_OPC_READER = True
RUN_MERGER     = False
RUN_ANALYZER   = True   # New: src/app_data_analyzer


def main() -> None:
    print("\n" + "=" * 62)
    print("  PIPELINE START")
    print("=" * 62)

    if RUN_OPC_READER:
        print("\n[component]  app_opc_reader")
        print("─" * 62)
        from src.app_opc_reader.logic.process_historian_runner import ProcessHistorianRunner
        ProcessHistorianRunner().run(debug=True)

    if RUN_MERGER:
        print("\n[component]  app_data_merger  (standalone)")
        print("─" * 62)
        from src.app_data_merger.logic.merger_runner import MergerRunner
        MergerRunner().run(debug=True)

    if RUN_ANALYZER:
        print("\n[component]  app_data_analyzer")
        print("─" * 62)
        from src.app_data_analyzer.logic.analyzer_runner import AnalyzerRunner
        AnalyzerRunner().run(debug=True)

    print("\n" + "=" * 62)
    print("  PIPELINE COMPLETE")
    print("=" * 62 + "\n")


if __name__ == "__main__":
    main()
