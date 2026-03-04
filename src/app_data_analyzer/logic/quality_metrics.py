# ---------------------------------------------------------------------------
# Data Quality Metrics — Data Quality Index 0-100.
# ---------------------------------------------------------------------------

import numpy as np
import pandas as pd
from typing import Any, Dict


def compute_quality_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    col_scores: Dict[str, Any] = {}
    overall_scores = []

    for col in df.columns:
        series   = df[col]
        n_total  = len(series)
        n_valid  = int(series.notna().sum())
        n_miss   = n_total - n_valid

        # ── Completeness (0–100) ────────────────────────────────────────
        completeness = round(n_valid / n_total * 100, 2) if n_total else 0.0

        # ── Outlier Ratio ───────────────────────────────────────────────
        q1, q3 = series.quantile(0.25), series.quantile(0.75)
        iqr    = q3 - q1
        n_out  = int(((series < q1 - 1.5 * iqr) | (series > q3 + 1.5 * iqr)).sum())
        outlier_pct  = round(n_out / n_total * 100, 2) if n_total else 0.0
        outlier_score = max(0.0, 100.0 - outlier_pct * 5)   # 1% outliers = −5pts

        # ── Variance Score ──────────────────────────────────────────────
        cv = series.std() / (abs(series.mean()) + 1e-9)      # Coefficient of variation
        variance_score = min(100.0, round(float(cv) * 50, 2))

        # ── Duplicate Timestamps ────────────────────────────────────────
        dup_count = int(df.index.duplicated().sum())

        # ── Composite Quality Score (0–100) ────────────────────────────
        # Weightings: completeness 50%, outlier_score 30%, variance 20%
        q_score = (
            0.50 * completeness +
            0.30 * outlier_score +
            0.20 * min(100.0, variance_score)
        )
        q_score = round(q_score, 2)
        overall_scores.append(q_score)

        col_scores[col] = {
            "completeness_pct":  completeness,
            "missing_count":     n_miss,
            "outlier_count":     n_out,
            "outlier_pct":       outlier_pct,
            "outlier_score":     round(outlier_score, 2),
            "variance_cv":       round(float(cv), 4),
            "variance_score":    round(variance_score, 2),
            "quality_score":     q_score,
            "quality_label": (
                "Excellent" if q_score >= 90 else
                "Good"      if q_score >= 75 else
                "Fair"      if q_score >= 55 else
                "Poor"
            ),
        }

    # ── Dataset-Level Metrics ─────────────────────────────────────────────────
    dup_ts = int(df.index.duplicated().sum())
    results["column_quality"]       = col_scores
    results["duplicate_timestamps"] = dup_ts
    results["overall_quality_score"]= round(float(np.mean(overall_scores)), 2) if overall_scores else 0.0
    results["overall_quality_label"]= (
        "Excellent" if results["overall_quality_score"] >= 90 else
        "Good"      if results["overall_quality_score"] >= 75 else
        "Fair"      if results["overall_quality_score"] >= 55 else
        "Poor"
    )
    results["dataset_shape"]        = {"rows": len(df), "columns": len(df.columns)}

    return results
