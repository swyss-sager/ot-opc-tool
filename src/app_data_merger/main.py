# ---------------------------------------------------------------------------
# Standalone entry point for the data merger component.
# Reads from merger_config.json — no OPC UA connection required.
# ---------------------------------------------------------------------------

import sys
from pathlib import Path

# -- allow execution as: python -m src.app_data_merger.main ------------------
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.app_data_merger.logic.merger_runner import MergerRunner


def main(debug: bool = True) -> None:
    print("\n" + "=" * 62)
    print("  DATA MERGER  —  standalone mode")
    print("=" * 62)
    runner = MergerRunner()
    result = runner.run(debug=debug)
    print(f"\n[done]  {result['row_count']} rows  |  columns: {result['columns']}")


if __name__ == "__main__":
    main()
