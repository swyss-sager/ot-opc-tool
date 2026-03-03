# ---------------------------------------------------------------------------
# Descriptive Analysis: Summary statistics, distributions.
# ---------------------------------------------------------------------------

import pandas as pd
from typing import Dict, Any, List


def compute_descriptive_stats(
    df: pd.DataFrame,
    percentiles: List[int] = None,
) -> Dict[str, Any]:
    if percentiles is None:
        percentiles = [25, 50, 75, 95]

    # Convert integer percentiles (0-100) to fractions (0.0-1.0) for pandas
    pct_fractions = [p / 100.0 for p in percentiles]

    stats: Dict[str, Any] = {
        "total_rows": len(df),
        "time_span": {
            "start": df.index.min().isoformat(),
            "end":   df.index.max().isoformat(),
        },
        "columns":         list(df.columns),
        "summary":         df.describe(percentiles=pct_fractions).to_dict(),
        "missing_per_col": (df.isnull().sum() / len(df) * 100).round(2).to_dict(),
        "skewness":        df.skew(numeric_only=True).to_dict(),
        "kurtosis":        df.kurtosis(numeric_only=True).to_dict(),
    }

    return stats
