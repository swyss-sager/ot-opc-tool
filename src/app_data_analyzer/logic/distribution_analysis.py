# ---------------------------------------------------------------------------
# Distribution Analysis: Normalitätstests, Skewness, Kurtosis, Box-Plots.
# ---------------------------------------------------------------------------

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pathlib import Path
from scipy import stats
from typing import Any, Dict


def perform_distribution_analysis(
    df: pd.DataFrame,
    plots_dir: Path,
) -> Dict[str, Any]:
    plots_dir = Path(plots_dir)
    plots_dir.mkdir(parents=True, exist_ok=True)
    results: Dict[str, Any] = {}

    for col in df.columns:
        series = df[col].dropna()
        if len(series) < 8:
            results[col] = {"error": "insufficient data"}
            continue

        col_res: Dict[str, Any] = {}

        # ── Skewness & Kurtosis ──────────────────────────────────────────
        skew  = float(series.skew())
        kurt  = float(series.kurtosis())
        col_res["skewness"]          = round(skew, 4)
        col_res["skewness_label"]    = (
            "left-skewed"  if skew < -0.5 else
            "right-skewed" if skew >  0.5 else
            "symmetric"
        )
        col_res["kurtosis"]          = round(kurt, 4)
        col_res["kurtosis_label"]    = (
            "leptokurtic"  if kurt >  0.5 else
            "platykurtic"  if kurt < -0.5 else
            "normal"
        )

        # ── Shapiro-Wilk (max 5000 samples for performance) ──────────────
        sample = series.sample(min(5000, len(series)), random_state=42)
        sw_stat, sw_p = stats.shapiro(sample)
        col_res["shapiro_wilk"] = {
            "statistic": round(float(sw_stat), 6),
            "p_value":   round(float(sw_p),   6),
            "normal":    bool(sw_p > 0.05),
        }

        # ── IQR Outlier Detection ─────────────────────────────────────────
        Q1, Q3 = series.quantile(0.25), series.quantile(0.75)
        IQR    = Q3 - Q1
        lo, hi = Q1 - 1.5 * IQR, Q3 + 1.5 * IQR
        n_out  = int(((series < lo) | (series > hi)).sum())
        col_res["iqr_outliers"] = {
            "count":      n_out,
            "pct":        round(n_out / len(series) * 100, 2),
            "lower_fence": round(float(lo), 4),
            "upper_fence": round(float(hi), 4),
        }

        # ── Histogram Plot ────────────────────────────────────────────────
        fig, axes = plt.subplots(1, 2, figsize=(12, 4))
        axes[0].hist(series, bins=60, alpha=0.75, color="steelblue", edgecolor="white")
        axes[0].set_title(f"{col} — Histogram")
        axes[0].set_xlabel("Value")
        axes[0].set_ylabel("Count")

        # KDE overlay
        xs = np.linspace(series.min(), series.max(), 300)
        kde_vals = stats.gaussian_kde(series)(xs)
        ax2 = axes[0].twinx()
        ax2.plot(xs, kde_vals, color="tomato", linewidth=1.5, label="KDE")
        ax2.set_ylabel("Density")

        # Box-Plot
        axes[1].boxplot(series, vert=True, patch_artist=True,
                        boxprops=dict(facecolor="steelblue", alpha=0.6))
        axes[1].set_title(f"{col} — Box-Plot (IQR Outliers: {n_out})")
        axes[1].set_ylabel("Value")

        safe = col.replace(" ", "_").replace("/", "_")
        plt.tight_layout()
        plt.savefig(plots_dir / f"distribution_{safe}.png", dpi=150, bbox_inches="tight")
        plt.close()

        results[col] = col_res

    # ── Combined Box-Plot all columns ────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(max(8, len(df.columns) * 2), 6))
    df.boxplot(ax=ax, rot=20)
    ax.set_title("Box-Plots — alle Kanäle")
    plt.tight_layout()
    plt.savefig(plots_dir / "boxplot_all.png", dpi=150, bbox_inches="tight")
    plt.close()

    return results
