# ---------------------------------------------------------------------------
# Inferential Analysis: Hypothesis tests, confidence intervals.
# ---------------------------------------------------------------------------

from scipy import stats
import pandas as pd
from typing import Dict, Any, List


def perform_inferential(
    df: pd.DataFrame,
    hypothesis_tests: List[str] = None,
) -> Dict[str, Any]:
    if hypothesis_tests is None:
        hypothesis_tests = ["t_test", "chi_square"]

    results = {}

    # t-Test: Compare means of two columns (e.g., first two)
    if "t_test" in hypothesis_tests and len(df.columns) >= 2:
        col1, col2 = df.columns[:2]
        t_stat, p_value = stats.ttest_ind(df[col1].dropna(), df[col2].dropna())
        results["t_test"] = {
            f"{col1}_vs_{col2}": {
                "t_statistic": float(t_stat),
                "p_value": float(p_value),
                "significant": p_value < 0.05
            }
        }

    # Chi-Square: For binned data (e.g., classify into low/high)
    if "chi_square" in hypothesis_tests and len(df.columns) >= 2:
        col1, col2 = df.columns[:2]
        # Simple binning: low (0), high (1)
        bin1 = pd.cut(df[col1], bins=2, labels=[0,1]).fillna(0)
        bin2 = pd.cut(df[col2], bins=2, labels=[0,1]).fillna(0)
        contingency = pd.crosstab(bin1, bin2)
        chi2, p, dof, expected = stats.chi2_contingency(contingency)
        results["chi_square"] = {
            f"{col1}_vs_{col2}": {
                "chi2_statistic": float(chi2),
                "p_value": float(p),
                "significant": p < 0.05
            }
        }

    # Confidence Intervals (95% for means)
    results["confidence_intervals"] = {}
    for col in df.columns:
        mean = df[col].mean()
        sem = df[col].sem()
        ci_low, ci_high = stats.t.interval(0.95, len(df[col])-1, loc=mean, scale=sem)
        results["confidence_intervals"][col] = {
            "mean": float(mean),
            "ci_95_low": float(ci_low),
            "ci_95_high": float(ci_high)
        }

    return results
