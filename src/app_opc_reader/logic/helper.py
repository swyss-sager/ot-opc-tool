import datetime as dt
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Any, Tuple


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


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_env_file() -> Dict[str, str]:
    env: Dict[str, str] = {}
    root = repo_root()
    env_path = (root / "security" / ".env").resolve()

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
        if (value.startswith('"') and value.endswith('"')) or \
           (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        env[key] = value

    return env


def require_env(env: Dict[str, str], key: str) -> str:
    v = env.get(key)
    if v is None or v == "":
        raise ValueError(f"Missing required env var '{key}' in .env")
    return v


# ─────────────────────────────────────────────────────────────
#  Datum-Parser für mode="absolute"
#  KEIN ZoneInfo, KEIN tzdata — nur Python stdlib
#  Datum wird direkt als UTC behandelt
# ─────────────────────────────────────────────────────────────

_DATE_RE = re.compile(
    r"^\s*(\d{1,2})\.(\d{1,2})\.(\d{4})"
    r"(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?\s*$"
)


def _parse_de_datetime(s: str) -> Tuple[dt.datetime, bool]:
    """
    Gibt (datetime_utc, has_time) zurück.
    has_time=False  → nur Datum, keine Uhrzeit angegeben.
    """
    s = s.strip()

    if s.lower() in {"heute", "today"}:
        d = dt.date.today()
        return (
            dt.datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=dt.timezone.utc),
            False,
        )

    m = _DATE_RE.match(s)
    if not m:
        raise ValueError(
            f"Ungültiges Datum: {s!r}\n"
            f"Erwartet z.B.: '01.02.2026'  oder  '1.2.2026 13:45'  oder  'heute'"
        )

    day      = int(m.group(1))
    month    = int(m.group(2))
    year     = int(m.group(3))
    hh       = int(m.group(4)) if m.group(4) else 0
    mm_      = int(m.group(5)) if m.group(5) else 0
    ss       = int(m.group(6)) if m.group(6) else 0
    has_time = m.group(4) is not None

    return (
        dt.datetime(year, month, day, hh, mm_, ss, tzinfo=dt.timezone.utc),
        has_time,
    )


def parse_absolute_range(
    start_text: str,
    end_text: str,
    end_inclusive: bool = True,
) -> Tuple[dt.datetime, dt.datetime]:
    """
    Parst start/end → (start_utc, end_utc) aware UTC.
    Kein Timezone-Handling — direkt UTC.
    """
    start_utc, _            = _parse_de_datetime(start_text)
    end_utc,   end_has_time = _parse_de_datetime(end_text)

    if end_inclusive and not end_has_time:
        end_utc = end_utc.replace(
            hour=23, minute=59, second=59, microsecond=999999
        )

    if start_utc > end_utc:
        raise ValueError(
            f"range.start ({start_text!r}) liegt nach range.end ({end_text!r})"
        )

    return start_utc, end_utc
