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


def local_tz(utc_offset_hours: float) -> dt.timezone:
    """Erstellt eine einfache fixed-offset Timezone — kein tzdata nötig."""
    return dt.timezone(dt.timedelta(hours=utc_offset_hours))


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
#  Datum-Parser — mit UTC-Offset-Support, kein tzdata
# ─────────────────────────────────────────────────────────────

_DATE_RE = re.compile(
    r"^\s*(\d{1,2})\.(\d{1,2})\.(\d{4})"
    r"(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?\s*$"
)


def _parse_de_datetime(
    s: str,
    tz: dt.timezone,
) -> Tuple[dt.datetime, bool]:
    """
    Parst dd.mm.yyyy [HH:MM[:SS]] oder 'heute'/'today'.
    Gibt (datetime_aware_local, has_time) zurück.
    """
    s = s.strip()

    if s.lower() in {"heute", "today"}:
        d = dt.date.today()
        return (
            dt.datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=tz),
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
        dt.datetime(year, month, day, hh, mm_, ss, tzinfo=tz),
        has_time,
    )


def parse_absolute_range(
    start_text: str,
    end_text: str,
    end_inclusive: bool = True,
    utc_offset_hours: float = 0.0,
) -> Tuple[dt.datetime, dt.datetime]:
    """
    Parst start/end als Lokalzeit → konvertiert zu UTC.

    Beispiel mit utc_offset_hours=1:
      '01.02.2026 00:00:00 CET'  →  '2026-01-31T23:00:00+00:00' UTC
      'heute 23:59:59 CET'       →  'heute 22:59:59+00:00' UTC
    """
    tz = local_tz(utc_offset_hours)

    start_local, _            = _parse_de_datetime(start_text, tz)
    end_local,   end_has_time = _parse_de_datetime(end_text,   tz)

    if end_inclusive and not end_has_time:
        end_local = end_local.replace(
            hour=23, minute=59, second=59, microsecond=999999
        )

    # Lokalzeit → UTC
    start_utc = start_local.astimezone(dt.timezone.utc)
    end_utc   = end_local.astimezone(dt.timezone.utc)

    if start_utc > end_utc:
        raise ValueError(
            f"range.start ({start_text!r}) liegt nach range.end ({end_text!r})"
        )

    return start_utc, end_utc
