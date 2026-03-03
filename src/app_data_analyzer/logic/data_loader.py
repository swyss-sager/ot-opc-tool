# ---------------------------------------------------------------------------
# Loads merged CSV/JSON into a Pandas DataFrame (time-indexed).
# Path resolution: relative paths are resolved against the repo root
# (3 levels above this file), not the CWD.
# ---------------------------------------------------------------------------

import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any


# ---------------------------------------------------------------------------
# Path helper
# ---------------------------------------------------------------------------

def _repo_root() -> Path:
    """Absolute repo root — 3 levels above this file (ot-opc-tool/)."""
    return Path(__file__).resolve().parents[3]   # ← war parents[4], jetzt parents[3]


def _resolve_path(raw: str) -> Path:
    """
    Resolve a path string.
    - Absolute paths are used as-is.
    - Relative paths are resolved against the repo root.
    """
    p = Path(raw)
    if p.is_absolute():
        return p.resolve()
    return (_repo_root() / p).resolve()


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_merged_data(
    input_files: List[Dict[str, str]],
    format_col: str = "format",
    path_col: str = "path",
) -> pd.DataFrame:
    """
    Load one or more merged files into a single DataFrame.
    Assumes 'timestamp' as first column, numeric columns follow.
    Relative paths resolve against repo root.
    """
    dfs: List[pd.DataFrame] = []

    for file_cfg in input_files:
        raw_path = file_cfg[path_col]
        path     = _resolve_path(raw_path)
        fmt      = file_cfg.get(format_col, "csv").lower()

        if not path.exists():
            raise FileNotFoundError(
                f"Input file not found: {path}\n"
                f"  (configured as: {raw_path!r})\n"
                f"  (repo root:     {_repo_root()})"
            )

        print(f"  [load]  {path}")

        if fmt == "csv":
            df = pd.read_csv(
                path,
                parse_dates=["timestamp"],
                index_col="timestamp",
            )
        elif fmt == "json":
            payload = json.loads(path.read_text(encoding="utf-8"))
            rows    = payload.get("rows", payload)
            df      = pd.DataFrame(rows)
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df.set_index("timestamp", inplace=True)
        else:
            raise ValueError(
                f"Unsupported format: {fmt!r}  (allowed: 'csv', 'json')"
            )

        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        dfs.append(df)

    if not dfs:
        raise ValueError("No input files loaded.")

    combined = pd.concat(dfs, axis=0).sort_index()
    combined = combined[~combined.index.duplicated(keep="last")]
    return combined


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Forward-fill missing values, then fill remaining NaN with 0.
    """
    df = df.ffill().fillna(0.0)
    return df.dropna(how="all")
