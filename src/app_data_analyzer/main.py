# ---------------------------------------------------------------------------
# Standalone entry point for the data analyzer component.
# ---------------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.app_data_analyzer.logic.analyzer_runner import AnalyzerRunner


def main(debug: bool = True) -> None:
    print("\n" + "=" * 62)
    print("  DATA ANALYZER  —  standalone mode")
    print("=" * 62)
    runner = AnalyzerRunner()
    result = runner.run(debug=debug)
    print(f"\n[done]  Analysis report generated")


if __name__ == "__main__":
    main()
