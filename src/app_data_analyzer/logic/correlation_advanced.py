# ---------------------------------------------------------------------------
# Advanced Correlation: Pearson, Spearman, VIF, Hierarchical Clustering.
# ---------------------------------------------------------------------------

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from pathlib import Path
from scipy.cluster.hierarchy import dendrogram, linkage
from scipy.stats import pearsonr, spearmanr
from statsmodels.stats.outliers_influence import variance_inflation_factor
from typing import Any, Dict


def perform_correlation_advanced(
    df: pd.DataFrame,
    plots_dir: Path,
    vif_threshold: float = 10.0,
) -> Dict[str, Any]:
    plots_dir = Path(plots_dir)
    plots_dir.mkdir(parents=True, exist_ok=True)
    results: Dict[str, Any] = {}

    df_clean = df.dropna()
    if len(df_clean) < 10:
        return {"error": "insufficient data after dropna()"}

    cols = list(df_clean.columns)

    # ── Pearson Correlation ───────────────────────────────────────────────────
    pearson_mat: Dict[str, Dict[str, Any]] = {}
    for i, c1 in enumerate(cols):
        pearson_mat[c1] = {}
        for j, c2 in enumerate(cols):
            if i == j:
                pearson_mat[c1][c2] = {"r": 1.0, "p": 0.0}
                continue
            r, p = pearsonr(df_clean[c1], df_clean[c2])
            pearson_mat[c1][c2] = {
                "r":           round(float(r), 4),
                "p_value":     round(float(p), 6),
                "significant": bool(p < 0.05),
            }
    results["pearson"] = pearson_mat

    # ── Spearman Correlation ──────────────────────────────────────────────────
    spearman_mat: Dict[str, Dict[str, Any]] = {}
    for i, c1 in enumerate(cols):
        spearman_mat[c1] = {}
        for j, c2 in enumerate(cols):
            if i == j:
                spearman_mat[c1][c2] = {"rho": 1.0, "p": 0.0}
                continue
            rho, p = spearmanr(df_clean[c1], df_clean[c2])
            spearman_mat[c1][c2] = {
                "rho":         round(float(rho), 4),
                "p_value":     round(float(p),   6),
                "significant": bool(p < 0.05),
            }
    results["spearman"] = spearman_mat

    # ── Heatmaps ──────────────────────────────────────────────────────────────
    for label, corr_fn in [("pearson", "pearson"), ("spearman", "spearman")]:
        fig, ax = plt.subplots(figsize=(max(6, len(cols)), max(5, len(cols) - 1)))
        mat = df_clean.corr(method=corr_fn)
        sns.heatmap(
            mat, annot=True, fmt=".3f", cmap="coolwarm",
            center=0, vmin=-1, vmax=1,
            linewidths=0.5, ax=ax,
        )
        ax.set_title(f"{label.capitalize()} Correlation Matrix")
        plt.tight_layout()
        plt.savefig(plots_dir / f"correlation_{label}.png", dpi=150, bbox_inches="tight")
        plt.close()

    # ── VIF — Multicollinearity ───────────────────────────────────────────────
    vif_results: Dict[str, Any] = {}
    if len(cols) >= 2:
        try:
            X = df_clean.values
            vif_vals = [
                variance_inflation_factor(X, i)
                for i in range(X.shape[1])
            ]
            for col, v in zip(cols, vif_vals):
                vif_results[col] = {
                    "vif":                round(float(v), 3),
                    "multicollinear":     bool(v > vif_threshold),
                    "interpretation": (
                        "Severe multicollinearity" if v > 10 else
                        "Moderate multicollinearity" if v > 5  else
                        "Low multicollinearity"
                    ),
                }
        except Exception as e:
            vif_results = {"error": str(e)}
    results["vif"] = vif_results

    # ── Hierarchical Clustering of Features ──────────────────────────────────
    try:
        corr_matrix = df_clean.corr("pearson").abs()
        dist_matrix = 1 - corr_matrix          # distance = 1 − |correlation|
        linkage_mat = linkage(dist_matrix, method="ward")

        fig, ax = plt.subplots(figsize=(max(8, len(cols) * 1.5), 5))
        dendrogram(
            linkage_mat,
            labels=cols,
            ax=ax,
            leaf_rotation=30,
            color_threshold=0.4,
        )
        ax.set_title("Hierarchical Feature Clustering (Ward / Pearson distance)")
        ax.set_ylabel("Distance (1 − |r|)")
        plt.tight_layout()
        plt.savefig(plots_dir / "hierarchical_clustering.png", dpi=150, bbox_inches="tight")
        plt.close()

        results["hierarchical_clustering"] = {
            "method":       "ward",
            "metric":       "1 - |pearson_r|",
            "linkage_shape": list(linkage_mat.shape),
        }
    except Exception as e:
        results["hierarchical_clustering"] = {"error": str(e)}

    # ── Strong Pairs Summary ──────────────────────────────────────────────────
    strong_pairs = []
    for i, c1 in enumerate(cols):
        for j, c2 in enumerate(cols):
            if j <= i:
                continue
            r_val = pearson_mat[c1][c2]["r"]
            if abs(r_val) >= 0.7:
                strong_pairs.append({
                    "pair":        [c1, c2],
                    "pearson_r":   r_val,
                    "strength": (
                        "very strong" if abs(r_val) >= 0.9 else
                        "strong"      if abs(r_val) >= 0.7 else
                        "moderate"
                    ),
                    "direction":   "positive" if r_val > 0 else "negative",
                })
    results["strong_pairs"] = sorted(strong_pairs, key=lambda x: abs(x["pearson_r"]), reverse=True)

    return results
