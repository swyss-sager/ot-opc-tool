# ---------------------------------------------------------------------------
# Extended Visualizations: Pair Plots, Violin, KDE, 3D Scatter.
# ---------------------------------------------------------------------------

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from pathlib import Path
from scipy.stats import gaussian_kde
from typing import Any, Dict


def perform_extended_visualizations(
    df: pd.DataFrame,
    plots_dir: Path,
) -> Dict[str, Any]:
    plots_dir = Path(plots_dir)
    plots_dir.mkdir(parents=True, exist_ok=True)
    results: Dict[str, Any] = {}

    df_clean = df.dropna()
    if len(df_clean) < 10:
        return {"error": "insufficient data"}

    cols = list(df_clean.columns)

    # ── Pair Plot (seaborn) ───────────────────────────────────────────────────
    try:
        # Downsample for performance (max 2000 rows)
        df_sample = df_clean.sample(min(2000, len(df_clean)), random_state=42)
        fig = sns.pairplot(df_sample, diag_kind="kde", plot_kws={"alpha": 0.3, "s": 10})
        fig.figure.suptitle("Pair Plot — alle Feature-Beziehungen", y=1.02)
        plt.savefig(plots_dir / "pairplot.png", dpi=120, bbox_inches="tight")
        plt.close()
        results["pairplot"] = "saved: pairplot.png"
    except Exception as e:
        results["pairplot"] = {"error": str(e)}

    # ── Violin Plots ──────────────────────────────────────────────────────────
    try:
        fig, axes = plt.subplots(1, len(cols), figsize=(len(cols) * 3 + 2, 6))
        if len(cols) == 1:
            axes = [axes]
        for ax, col in zip(axes, cols):
            ax.violinplot(df_clean[col].dropna(), showmedians=True,
                          showextrema=True, vert=True)
            ax.set_title(col, fontsize=9)
            ax.set_ylabel("Value")
        fig.suptitle("Violin Plots — Verteilung & Dichte", fontsize=12)
        plt.tight_layout()
        plt.savefig(plots_dir / "violinplot.png", dpi=150, bbox_inches="tight")
        plt.close()
        results["violinplot"] = "saved: violinplot.png"
    except Exception as e:
        results["violinplot"] = {"error": str(e)}

    # ── KDE Plots per Column ──────────────────────────────────────────────────
    kde_files = []
    for col in cols:
        try:
            series = df_clean[col].dropna()
            xs     = np.linspace(series.min(), series.max(), 400)
            kde    = gaussian_kde(series)

            fig, ax = plt.subplots(figsize=(8, 4))
            ax.plot(xs, kde(xs), lw=2, color="steelblue")
            ax.fill_between(xs, kde(xs), alpha=0.25, color="steelblue")
            ax.set_title(f"KDE — {col}")
            ax.set_xlabel("Value")
            ax.set_ylabel("Density")

            safe = col.replace(" ", "_").replace("/", "_")
            fname = f"kde_{safe}.png"
            plt.savefig(plots_dir / fname, dpi=150, bbox_inches="tight")
            plt.close()
            kde_files.append(fname)
        except Exception as e:
            kde_files.append(f"{col}: error — {e}")
    results["kde_plots"] = kde_files

    # ── Scatter Plots with Regression Lines ───────────────────────────────────
    if len(cols) >= 2:
        scatter_files = []
        for i in range(len(cols)):
            for j in range(i + 1, len(cols)):
                c1, c2 = cols[i], cols[j]
                try:
                    x = df_clean[c1].values
                    y = df_clean[c2].values
                    m, b = np.polyfit(x, y, 1)

                    fig, ax = plt.subplots(figsize=(7, 5))
                    ax.scatter(x, y, alpha=0.3, s=8, color="steelblue")
                    xs_line = np.linspace(x.min(), x.max(), 200)
                    ax.plot(xs_line, m * xs_line + b, color="tomato",
                            lw=1.5, label=f"y = {m:.3f}x + {b:.3f}")
                    ax.set_xlabel(c1)
                    ax.set_ylabel(c2)
                    ax.set_title(f"Scatter + Regression: {c1} vs {c2}")
                    ax.legend(fontsize=8)

                    s1 = c1.replace(" ", "_")
                    s2 = c2.replace(" ", "_")
                    fname = f"scatter_{s1}_vs_{s2}.png"
                    plt.savefig(plots_dir / fname, dpi=150, bbox_inches="tight")
                    plt.close()
                    scatter_files.append(fname)
                except Exception as e:
                    scatter_files.append(f"{c1} vs {c2}: error — {e}")
        results["scatter_regression"] = scatter_files

    # ── 3D Scatter (first 3 columns) ──────────────────────────────────────────
    if len(cols) >= 3:
        try:
            from mpl_toolkits.mplot3d import Axes3D  # noqa: F401
            df_3d   = df_clean.sample(min(1500, len(df_clean)), random_state=42)
            c1, c2, c3 = cols[0], cols[1], cols[2]

            fig = plt.figure(figsize=(10, 7))
            ax  = fig.add_subplot(111, projection="3d")
            sc  = ax.scatter(
                df_3d[c1], df_3d[c2], df_3d[c3],
                c=df_3d[c3], cmap="viridis", s=8, alpha=0.5,
            )
            ax.set_xlabel(c1, fontsize=8)
            ax.set_ylabel(c2, fontsize=8)
            ax.set_zlabel(c3, fontsize=8)
            ax.set_title(f"3D Scatter: {c1} | {c2} | {c3}")
            plt.colorbar(sc, shrink=0.5, label=c3)
            plt.savefig(plots_dir / "scatter3d.png", dpi=150, bbox_inches="tight")
            plt.close()
            results["scatter3d"] = "saved: scatter3d.png"
        except Exception as e:
            results["scatter3d"] = {"error": str(e)}

    return results
