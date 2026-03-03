import json
from pathlib import Path
from typing import Any, Dict, Optional

from src.app_opc_reader.logic.helper import repo_root


class ConfigLoader:
    def __init__(self) -> None:
        root = repo_root()
        self.config_path = (root / "config" / "config.json").resolve()

    def load(self) -> Dict[str, Any]:
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config not found: {self.config_path}")
        return json.loads(self.config_path.read_text(encoding="utf-8"))
