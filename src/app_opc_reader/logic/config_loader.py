# ---------------------------------------------------------------------------
# Loads and validates config.json relative to the app package root.
# ---------------------------------------------------------------------------

import json
from typing import Any, Dict

from src.app_opc_reader.logic.helper import project_root


class ConfigLoader:
    """Reads <app_root>/config/config.json and returns it as a plain dict."""

    def __init__(self) -> None:
        self.config_path = (project_root() / "config" / "config.json").resolve()

    def load(self) -> Dict[str, Any]:
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config not found: {self.config_path}")
        return json.loads(self.config_path.read_text(encoding="utf-8"))
