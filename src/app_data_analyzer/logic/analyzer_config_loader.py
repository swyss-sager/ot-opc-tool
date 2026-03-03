# ---------------------------------------------------------------------------
# Loads analyzer_config.json from the app_data_analyzer config folder.
# ---------------------------------------------------------------------------

import json
from pathlib import Path
from typing import Any, Dict, Optional


class AnalyzerConfigLoader:

    _DEFAULT_CFG = (
        Path(__file__).resolve().parents[1] / "config" / "analyzer_config.json"
    )

    def __init__(self, config_path: Optional[Path] = None) -> None:
        self.config_path = Path(config_path) if config_path else self._DEFAULT_CFG

    def load(self) -> Dict[str, Any]:
        if not self.config_path.exists():
            raise FileNotFoundError(f"Analyzer config not found: {self.config_path}")
        return json.loads(self.config_path.read_text(encoding="utf-8"))

    @staticmethod
    def from_dict(cfg_dict: Dict[str, Any]) -> Dict[str, Any]:
        return cfg_dict
