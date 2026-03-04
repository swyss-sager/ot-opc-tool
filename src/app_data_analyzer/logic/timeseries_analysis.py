# ---------------------------------------------------------------------------
# Time Series Analysis: ACF, PACF, ADF, KPSS, STL Decomposition.
# ---------------------------------------------------------------------------

import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.stattools import adfuller, kpss
from statsmodels.tsa.seasonal import STL
from typing import Any, Dict


def perform_timeseries_analysis(
    df: pd.DataFrame,
    plots_dir: Path,
    stl_period: int = 120,   # ~1 hour at 500ms = 7200, use 120 for 1-min seasonality
    acf_lags:   int = 50,
) -> Dict[str, Any]:
    plots_dir = Path(plots_dir)
    plots_dir.mkdir(parents=True, exist_ok=True)
    results: Dict[str, Any] = {}

    for col in df.columns:
        series = df[col].dropna()
        if len(series) < max(50, stl_period * 2):
            results[col] = {"error": "insufficient data for time series analysis"}
            continue

        col_res: Dict[str, Any] = {}

        # ── ADF Test (Stationarity) ───────────────────────────────────────
        try:
            adf_stat, adf_p, adf_lags_used, _, adf_crit, _ = adfuller(series, autolag="AIC")
            col_res["adf_test"] = {
                "statistic":    round(float(adf_stat), 6),
                "p_value":      round(float(adf_p),    6),
                "lags_used":    int(adf_lags_used),
                "critical_values": {k: round(v, 4) for k, v in adf_crit.items()},
                "stationary":   bool(adf_p < 0.05),
                "interpretation": (
                    "Stationary (reject H0: unit root)"
                    if adf_p < 0.05 else
                    "Non-stationary (fail to reject H0: unit root)"
                ),
            }
        except Exception as e:
            col_res["adf_test"] = {"error": str(e)}

        # ── KPSS Test (confirms ADF) ──────────────────────────────────────
        try:
            kpss_stat, kpss_p, kpss_lags, kpss_crit = kpss(series, regression="c", nlags="auto")
            col_res["kpss_test"] = {
                "statistic":      round(float(kpss_stat), 6),
                "p_value":        round(float(kpss_p),    6),
                "lags_used":      int(kpss_lags),
                "critical_values": {k: round(v, 4) for k, v in kpss_crit.items()},
                "stationary":     bool(kpss_p > 0.05),
                "interpretation": (
                    "Stationary (fail to reject H0: trend stationary)"
                    if kpss_p > 0.05 else
                    "Non-stationary (reject H0)"
                ),
            }
        except Exception as e:
            col_res["kpss_test"] = {"error": str(e)}

        # ── Combined stationarity verdict ─────────────────────────────────
        adf_ok  = col_res.get("adf_test",  {}).get("stationary",  None)
        kpss_ok = col_res.get("kpss_test", {}).get("stationary", None)
        if adf_ok is True  and kpss_ok is True:
            verdict = "Clearly stationary"
        elif adf_ok is False and kpss_ok is False:
            verdict = "Clearly non-stationary — consider differencing"
        elif adf_ok is True  and kpss_ok is False:
            verdict = "Difference-stationary"
        elif adf_ok is False and kpss_ok is True:
            verdict = "Trend-stationary"
        else:
            verdict = "Inconclusive"
        col_res["stationarity_verdict"] = verdict

        # ── STL Decomposition ─────────────────────────────────────────────
        try:
            stl    = STL(series, period=stl_period, robust=True)
            stl_fit = stl.fit()
            col_res["stl"] = {
                "trend_strength":    round(float(
                    max(0, 1 - stl_fit.resid.var() /
                        (stl_fit.trend + stl_fit.resid).var())), 4),
                "seasonal_strength": round(float(
                    max(0, 1 - stl_fit.resid.var() /
                        (stl_fit.seasonal + stl_fit.resid).var())), 4),
            }

            fig, axes = plt.subplots(4, 1, figsize=(14, 10), sharex=True)
            axes[0].plot(series.index, stl_fit.observed,  lw=0.7); axes[0].set_ylabel("Observed")
            axes[1].plot(series.index, stl_fit.trend,     lw=1.2, color="tomato");  axes[1].set_ylabel("Trend")
            axes[2].plot(series.index, stl_fit.seasonal,  lw=0.7, color="green");   axes[2].set_ylabel("Seasonal")
            axes[3].plot(series.index, stl_fit.resid,     lw=0.5, color="gray");    axes[3].set_ylabel("Residual")
            fig.suptitle(f"STL Decomposition — {col}")
            plt.tight_layout()
            safe = col.replace(" ", "_").replace("/", "_")
            plt.savefig(plots_dir / f"stl_{safe}.png", dpi=150, bbox_inches="tight")
            plt.close()
        except Exception as e:
            col_res["stl"] = {"error": str(e)}

        # ── ACF / PACF Plots ──────────────────────────────────────────────
        try:
            fig, axes = plt.subplots(2, 1, figsize=(12, 6))
            plot_acf( series, lags=acf_lags, ax=axes[0], title=f"ACF — {col}")
            plot_pacf(series, lags=acf_lags, ax=axes[1], title=f"PACF — {col}", method="ywm")
            plt.tight_layout()
            safe = col.replace(" ", "_").replace("/", "_")
            plt.savefig(plots_dir / f"acf_pacf_{safe}.png", dpi=150, bbox_inches="tight")
            plt.close()
        except Exception as e:
            col_res["acf_pacf"] = {"error": str(e)}

        results[col] = col_res

    return results
