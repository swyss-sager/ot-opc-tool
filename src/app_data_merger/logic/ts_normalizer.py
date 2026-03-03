# ---------------------------------------------------------------------------
# Normalises arbitrary ISO timestamps to a fixed grid of N-millisecond slots.
# Default: 500 ms  →  only .000 and .500 timestamps exist after normalisation.
#
# Rounding rule: FLOOR (always round DOWN to nearest slot).
#   ms   0 – 499  →  .000
#   ms 500 – 999  →  .500
# ---------------------------------------------------------------------------

import datetime as dt
import re
from typing import Optional


# ---------------------------------------------------------------------------
# ISO parser — handles both naive and offset-aware strings
# ---------------------------------------------------------------------------

_ISO_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})"
    r"(?:\.(\d+))?"                   # optional fractional seconds
    r"(?:([+-]\d{2}:\d{2})|Z)?"       # optional timezone offset
)


def _parse_iso(ts_str: str) -> Optional[dt.datetime]:
    """
    Robustly parse an ISO 8601 datetime string.
    Returns a timezone-aware datetime if an offset is present,
    otherwise naive.
    """
    ts_str = ts_str.strip()
    m = _ISO_RE.match(ts_str)
    if not m:
        return None

    date_s, time_s, frac_s, tz_s = m.groups()

    # Normalise fractional seconds to microseconds (6 digits)
    if frac_s:
        frac_s = (frac_s + "000000")[:6]
        us = int(frac_s)
    else:
        us = 0

    try:
        base = dt.datetime.strptime(
            f"{date_s} {time_s}", "%Y-%m-%d %H:%M:%S"
        ).replace(microsecond=us)
    except ValueError:
        return None

    if tz_s:
        sign   = 1 if tz_s[0] == "+" else -1
        parts  = tz_s[1:].split(":")
        offset = dt.timedelta(hours=int(parts[0]), minutes=int(parts[1])) * sign
        base   = base.replace(tzinfo=dt.timezone(offset))

    return base


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------

def normalise_ts(
    ts_str:        str,
    resolution_ms: int = 500,
) -> Optional[str]:
    """
    Parse ts_str and FLOOR to the nearest lower <resolution_ms> slot.

    Examples with resolution_ms=500:
        ...T12:00:00.000  ->  ...T12:00:00.000
        ...T12:00:00.123  ->  ...T12:00:00.000
        ...T12:00:00.499  ->  ...T12:00:00.000
        ...T12:00:00.500  ->  ...T12:00:00.500
        ...T12:00:00.750  ->  ...T12:00:00.500
        ...T12:00:00.999  ->  ...T12:00:00.500

    Two source values that floor into the same slot: last one wins
    (handled in data_merger.py — slot_map simply overwrites).

    Returns None if parsing fails.
    """
    parsed = _parse_iso(ts_str)
    if parsed is None:
        return None

    slot_us    = resolution_ms * 1_000          # e.g. 500_000 µs
    total_us   = parsed.microsecond

    # FLOOR: discard remainder — no rounding up
    floored_us = (total_us // slot_us) * slot_us

    normalised = parsed.replace(microsecond=floored_us)

    # Format: always 3 decimal places (milliseconds only)
    ms = normalised.microsecond // 1_000
    return normalised.strftime(f"%Y-%m-%dT%H:%M:%S.{ms:03d}")
