import json
from pathlib import Path
from typing import Any, Dict, Optional

from src.app.logic.helper import project_root


class ConfigLoader:
    def __init__(self, config_path: Optional[Path] = None) -> None:
        root = project_root()
        self.config_path = (config_path or (root / "config" / "config.json")).resolve()

    def load(self) -> Dict[str, Any]:
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config not found: {self.config_path}")
        return json.loads(self.config_path.read_text(encoding="utf-8"))
