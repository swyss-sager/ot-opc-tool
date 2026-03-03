# ---------------------------------------------------------------------------
# Shared data types, timezone utilities and date-range parser.
# No third-party timezone libs (tzdata / pytz / ZoneInfo) required.
# ---------------------------------------------------------------------------

import datetime as dt
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class HistoryValue:
    """Single historian data point returned from OPC UA."""
    source_timestamp: Optional[dt.datetime]
    server_timestamp: Optional[dt.datetime]
    value:            Any
    status_code:      str


# ---------------------------------------------------------------------------
# Datetime utilities
# ---------------------------------------------------------------------------

def aware_utc_now() -> dt.datetime:
    """Return current time as timezone-aware UTC datetime."""
    return dt.datetime.now(dt.timezone.utc)


def to_naive_utc(ts: dt.datetime) -> dt.datetime:
    """Convert aware datetime to naive UTC (required by asyncua API)."""
    if ts.tzinfo is None:
        return ts
    return ts.astimezone(dt.timezone.utc).replace(tzinfo=None)


def ensure_aware_utc(ts: dt.datetime) -> dt.datetime:
    """Attach UTC timezone to naive datetime; convert aware datetime to UTC."""
    if ts.tzinfo is None:
        return ts.replace(tzinfo=dt.timezone.utc)
    return ts.astimezone(dt.timezone.utc)


def local_tz(utc_offset_hours: float) -> dt.timezone:
    """Build a fixed-offset timezone from a numeric UTC offset."""
    return dt.timezone(dt.timedelta(hours=utc_offset_hours))


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def repo_root() -> Path:
    """Absolute path to the repository root (4 levels above this file)."""
    return Path(__file__).resolve().parents[3]


def project_root() -> Path:
    """Absolute path to the app package root (2 levels above this file)."""
    return Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Environment / secrets loader
# ---------------------------------------------------------------------------

def load_env_file() -> Dict[str, str]:
    """
    Parse <repo_root>/security/.env into a key/value dict.
    Lines starting with '#' and blank lines are ignored.
    Surrounding quotes on values are stripped.
    """
    env:  Dict[str, str] = {}
    path: Path           = (repo_root() / "security" / ".env").resolve()

    if not path.exists():
        raise FileNotFoundError(f".env not found: {path}")

    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key   = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] in ('"', "'") and value[0] == value[-1]:
            value = value[1:-1]
        env[key] = value

    return env


def require_env(env: Dict[str, str], key: str) -> str:
    """Raise ValueError if a required key is missing or empty."""
    v = env.get(key)
    if not v:
        raise ValueError(f"Missing required env var '{key}' in .env")
    return v


# ---------------------------------------------------------------------------
# Date-range parser  (dd.mm.yyyy, optional HH:MM[:SS], 'heute' / 'today')
# ---------------------------------------------------------------------------

_DATE_RE = re.compile(
    r"^\s*(\d{1,2})\.(\d{1,2})\.(\d{4})"
    r"(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?\s*$"
)


def _parse_de_datetime(s: str, tz: dt.timezone) -> Tuple[dt.datetime, bool]:
    """
    Parse a German-formatted date string into an aware datetime.
    Returns (datetime, has_explicit_time).
    """
    s = s.strip()

    if s.lower() in {"heute", "today"}:
        d = dt.date.today()
        return dt.datetime(d.year, d.month, d.day, tzinfo=tz), False

    m = _DATE_RE.match(s)
    if not m:
        raise ValueError(
            f"Invalid date: {s!r}  "
            f"Expected: '01.02.2026', '1.2.2026 13:45', or 'heute'"
        )

    day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
    hh  = int(m.group(4)) if m.group(4) else 0
    min_= int(m.group(5)) if m.group(5) else 0
    sec = int(m.group(6)) if m.group(6) else 0

    return dt.datetime(year, month, day, hh, min_, sec, tzinfo=tz), m.group(4) is not None


def parse_absolute_range(
    start_text:       str,
    end_text:         str,
    end_inclusive:    bool  = True,
    utc_offset_hours: float = 0.0,
) -> Tuple[dt.datetime, dt.datetime]:
    """
    Parse start/end as local time and return both as UTC.

    If end_inclusive=True and no explicit time was given for end,
    the end datetime is set to 23:59:59.999999 of that day.
    """
    tz = local_tz(utc_offset_hours)

    start_local, _           = _parse_de_datetime(start_text, tz)
    end_local,   end_has_time = _parse_de_datetime(end_text,   tz)

    if end_inclusive and not end_has_time:
        end_local = end_local.replace(hour=23, minute=59, second=59, microsecond=999_999)

    start_utc = start_local.astimezone(dt.timezone.utc)
    end_utc   = end_local.astimezone(dt.timezone.utc)

    if start_utc > end_utc:
        raise ValueError(
            f"range.start ({start_text!r}) is after range.end ({end_text!r})"
        )

    return start_utc, end_utc
