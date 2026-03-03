# ---------------------------------------------------------------------------
# Root entry point — runs all configured components in sequence.
# Each component can be toggled independently via boolean flags.
# Individual components are also directly executable via their own main.py.
# ---------------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))


# ---------------------------------------------------------------------------
# Component flags  — set to True / False as needed
# ---------------------------------------------------------------------------

RUN_OPC_READER = True    # src/app_opc_reader  — reads from Process Historian
RUN_MERGER     = False   # src/app_data_merger — standalone merge (no OPC read)
                         # Note: merger also runs automatically after OPC read
                         # when merger.enabled=true & run_after_export=true
                         # in config.json — no need to enable both.


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def main() -> None:
    print("\n" + "=" * 62)
    print("  PIPELINE START")
    print("=" * 62)

    # -- OPC Reader ----------------------------------------------------------
    if RUN_OPC_READER:
        print("\n[component]  app_opc_reader")
        print("─" * 62)
        from src.app_opc_reader.logic.process_historian_runner import (
            ProcessHistorianRunner,
        )
        ProcessHistorianRunner().run(debug=True)

    # -- Standalone Merger ---------------------------------------------------
    if RUN_MERGER:
        print("\n[component]  app_data_merger  (standalone)")
        print("─" * 62)
        from src.app_data_merger.logic.merger_runner import MergerRunner
        MergerRunner().run(debug=True)

    print("\n" + "=" * 62)
    print("  PIPELINE COMPLETE")
    print("=" * 62 + "\n")


if __name__ == "__main__":
    main()
