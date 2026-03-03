# ---------------------------------------------------------------------------
# Loads merger_config.json from the app_data_merger config folder.
# Can also consume an inline dict (injected by the OPC runner after export).
# ---------------------------------------------------------------------------

import json
from pathlib import Path
from typing import Any, Dict, Optional


class MergerConfigLoader:

    # Default config path relative to this file: ../../config/merger_config.json
    _DEFAULT_CFG = (
        Path(__file__).resolve().parents[1] / "config" / "merger_config.json"
    )

    def __init__(self, config_path: Optional[Path] = None) -> None:
        self.config_path = Path(config_path) if config_path else self._DEFAULT_CFG

    def load(self) -> Dict[str, Any]:
        if not self.config_path.exists():
            raise FileNotFoundError(f"Merger config not found: {self.config_path}")
        return json.loads(self.config_path.read_text(encoding="utf-8"))

    @staticmethod
    def from_dict(cfg_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Pass a pre-built config dict directly (used by OPC runner integration)."""
        return cfg_dict
