# ---------------------------------------------------------------------------
# Exploratory Data Analysis (EDA): Correlations, plots, missing values.
# ---------------------------------------------------------------------------

import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import pandas as pd
from typing import Dict, Any


def perform_eda(
    df: pd.DataFrame,
    plots_dir: str,
    plot_correlations: bool = True,
    plot_distributions: bool = True,
    handle_missing: str = "drop",
) -> Dict[str, Any]:
    plots_dir = Path(plots_dir)
    plots_dir.mkdir(parents=True, exist_ok=True)

    # Handle missing
    if handle_missing == "drop":
        df_clean = df.dropna()
    else:
        df_clean = df.fillna(0)

    eda_results = {
        "missing_strategy": handle_missing,
        "correlation_matrix": df_clean.corr().to_dict(),
        "outliers_summary": {},  # Per column IQR outliers count
    }

    # Outliers via IQR
    for col in df_clean.columns:
        Q1 = df_clean[col].quantile(0.25)
        Q3 = df_clean[col].quantile(0.75)
        IQR = Q3 - Q1
        outliers = ((df_clean[col] < (Q1 - 1.5 * IQR)) | (df_clean[col] > (Q3 + 1.5 * IQR))).sum()
        eda_results["outliers_summary"][col] = int(outliers)

    # Plots
    if plot_correlations:
        plt.figure(figsize=(10, 8))
        sns.heatmap(df_clean.corr(), annot=True, cmap="coolwarm")
        plt.title("Correlation Matrix")
        plt.savefig(plots_dir / "correlation_heatmap.png", dpi=300, bbox_inches="tight")
        plt.close()

    if plot_distributions:
        for col in df_clean.columns[:4]:  # Limit to first 4 cols for demo
            plt.figure(figsize=(8, 4))
            df_clean[col].hist(bins=50, alpha=0.7)
            plt.title(f"Distribution of {col}")
            plt.savefig(plots_dir / f"dist_{col}.png", dpi=300, bbox_inches="tight")
            plt.close()

    # Time series plot
    plt.figure(figsize=(12, 6))
    for col in df_clean.columns:
        plt.plot(df_clean.index, df_clean[col], label=col, alpha=0.7)
    plt.title("Time Series Overview")
    plt.legend()
    plt.savefig(plots_dir / "time_series_overview.png", dpi=300, bbox_inches="tight")
    plt.close()

    return eda_results
