# ---------------------------------------------------------------------------
# Extended Anomaly Detection: Z-Score, CUSUM, Ruptures (Changepoints).
# ---------------------------------------------------------------------------

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Any, Dict


def _zscore_anomalies(series: pd.Series, threshold: float = 3.0) -> np.ndarray:
    mu, sigma = series.mean(), series.std()
    if sigma == 0:
        return np.zeros(len(series), dtype=bool)
    z = np.abs((series - mu) / sigma)
    return z > threshold


def _cusum(series: pd.Series, threshold: float = 5.0, drift: float = 0.5
           ) -> Dict[str, Any]:
    """One-sided CUSUM for upward and downward shifts."""
    s = (series - series.mean()) / (series.std() + 1e-9)
    cusum_pos = np.zeros(len(s))
    cusum_neg = np.zeros(len(s))
    for i in range(1, len(s)):
        cusum_pos[i] = max(0, cusum_pos[i-1] + s.iloc[i] - drift)
        cusum_neg[i] = max(0, cusum_neg[i-1] - s.iloc[i] - drift)
    cp_up   = np.where(cusum_pos > threshold)[0].tolist()
    cp_down = np.where(cusum_neg > threshold)[0].tolist()
    return {
        "changepoints_up":   cp_up[:20],
        "changepoints_down": cp_down[:20],
        "cusum_pos": cusum_pos,
        "cusum_neg": cusum_neg,
    }


def perform_anomaly_extended(
    df: pd.DataFrame,
    plots_dir: Path,
    zscore_threshold:  float = 3.0,
    cusum_threshold:   float = 5.0,
    ruptures_n_breaks: int   = 5,
) -> Dict[str, Any]:
    plots_dir = Path(plots_dir)
    plots_dir.mkdir(parents=True, exist_ok=True)
    results: Dict[str, Any] = {}

    for col in df.columns:
        series = df[col].dropna()
        if len(series) < 30:
            results[col] = {"error": "insufficient data"}
            continue

        col_res: Dict[str, Any] = {}
        safe = col.replace(" ", "_").replace("/", "_")

        # ── Z-Score Anomalies ─────────────────────────────────────────────
        z_mask = _zscore_anomalies(series, zscore_threshold)
        col_res["zscore"] = {
            "threshold": zscore_threshold,
            "anomaly_count": int(z_mask.sum()),
            "anomaly_pct": round(float(z_mask.sum() / len(series) * 100), 2),
            "anomaly_indices": np.where(z_mask)[0].tolist()[:50],  # cap at 50
        }

        # Z-Score Plot
        fig, axes = plt.subplots(2, 1, figsize=(14, 6), sharex=True)
        axes[0].plot(series.index, series.values, lw=0.6, color="steelblue", label="Signal")
        axes[0].scatter(
            series.index[z_mask], series.values[z_mask],
            color="tomato", s=20, zorder=5, label=f"Z-Score Anomaly (|Z|>{zscore_threshold})"
        )
        axes[0].set_title(f"Z-Score Anomaly Detection — {col}")
        axes[0].legend(fontsize=8)

        mu, sigma = series.mean(), series.std()
        z_vals = np.abs((series - mu) / (sigma + 1e-9))
        axes[1].plot(series.index, z_vals, lw=0.6, color="orange", label="|Z-Score|")
        axes[1].axhline(zscore_threshold, color="red", linestyle="--", lw=1, label=f"Threshold={zscore_threshold}")
        axes[1].set_title("Z-Score Values")
        axes[1].legend(fontsize=8)
        plt.tight_layout()
        plt.savefig(plots_dir / f"anomaly_zscore_{safe}.png", dpi=150, bbox_inches="tight")
        plt.close()

        # ── CUSUM Changepoint Detection ───────────────────────────────────
        cusum_res = _cusum(series, threshold=cusum_threshold)
        col_res["cusum"] = {
            "threshold": cusum_threshold,
            "changepoints_up": cusum_res["changepoints_up"],
            "changepoints_down": cusum_res["changepoints_down"],
            "total_changepoints": len(cusum_res["changepoints_up"]) +
                                  len(cusum_res["changepoints_down"]),
        }

        # CUSUM Plot
        fig, axes = plt.subplots(3, 1, figsize=(14, 8), sharex=True)
        axes[0].plot(series.index, series.values, lw=0.6, color="steelblue")
        axes[0].set_title(f"CUSUM Changepoint Detection — {col}")
        axes[0].set_ylabel("Value")

        axes[1].plot(series.index, cusum_res["cusum_pos"], lw=0.8, color="tomato", label="CUSUM+")
        axes[1].axhline(cusum_threshold, color="darkred", linestyle="--", lw=1, label="Threshold")
        axes[1].legend(fontsize=8)
        axes[1].set_ylabel("CUSUM+")

        axes[2].plot(series.index, cusum_res["cusum_neg"], lw=0.8, color="navy", label="CUSUM−")
        axes[2].axhline(cusum_threshold, color="darkblue", linestyle="--", lw=1, label="Threshold")
        axes[2].legend(fontsize=8)
        axes[2].set_ylabel("CUSUM−")

        plt.tight_layout()
        plt.savefig(plots_dir / f"cusum_{safe}.png", dpi=150, bbox_inches="tight")
        plt.close()

        # ── Ruptures — Structural Break Detection ─────────────────────────
        try:
            import ruptures as rpt
            signal = series.values
            algo = rpt.Binseg(model="rbf").fit(signal)
            breaks = algo.predict(n_bkps=min(ruptures_n_breaks, len(signal) // 20))
            col_res["ruptures"] = {
                "algorithm": "Binseg (RBF)",
                "n_breaks": ruptures_n_breaks,
                "breakpoints": breaks[:-1],  # last is always len(signal)
            }

            # Ruptures Plot
            fig, ax = plt.subplots(figsize=(14, 4))
            ax.plot(series.index, signal, lw=0.6, color="steelblue", label="Signal")
            for bp in breaks[:-1]:
                if bp < len(series.index):
                    ax.axvline(series.index[bp], color="red", linestyle="--",
                               lw=1.2, alpha=0.8)
            ax.set_title(f"Ruptures Structural Breaks — {col}  "
                         f"({len(breaks) - 1} breakpoints)")
            ax.legend(fontsize=8)
            plt.tight_layout()
            plt.savefig(plots_dir / f"ruptures_{safe}.png", dpi=150, bbox_inches="tight")
            plt.close()

        except ImportError:
            col_res["ruptures"] = {"error": "ruptures library not installed — pip install ruptures"}
        except Exception as e:
            col_res["ruptures"] = {"error": str(e)}

        results[col] = col_res

    return results

