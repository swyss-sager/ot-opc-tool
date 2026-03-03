import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Any


@dataclass
class HistoryValue:
    source_timestamp: Optional[dt.datetime]
    server_timestamp: Optional[dt.datetime]
    value: Any
    status_code: str


def aware_utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def to_naive_utc(ts: dt.datetime) -> dt.datetime:
    if ts.tzinfo is None:
        return ts
    return ts.astimezone(dt.timezone.utc).replace(tzinfo=None)


def ensure_aware_utc(ts: dt.datetime) -> dt.datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=dt.timezone.utc)
    return ts.astimezone(dt.timezone.utc)


def project_root() -> Path:
    """
    Sucht nach dem Projekt-Root, indem nach einem 'config' Ordner gesucht wird.
    """
    p = Path(__file__).resolve()
    for parent in [p, *p.parents]:
        if (parent / "config").exists():
            return parent
    # Fallback: 3 Ebenen hoch (src/app/logic -> src)
    return Path(__file__).resolve().parents[3]


def load_env_file(env_path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    if not env_path.exists():
        raise FileNotFoundError(f".env file not found: {env_path}")

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        env[key] = value

    return env


def require_env(env: Dict[str, str], key: str) -> str:
    v = env.get(key)
    if v is None or v == "":
        raise ValueError(f"Missing required env var '{key}' in .env")
    return v
