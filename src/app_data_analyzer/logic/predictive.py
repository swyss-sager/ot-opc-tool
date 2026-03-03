# ---------------------------------------------------------------------------
# Predictive Analysis: Forecasting, Classification, Predictive Maintenance.
# ---------------------------------------------------------------------------

import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from statsmodels.tsa.arima.model import ARIMA
from typing import Dict, Any
import warnings

warnings.filterwarnings("ignore")


def perform_predictive(
    df: pd.DataFrame,
    forecast_horizon: int = 24,
    anomaly_detection: bool = True,
    maintenance_model: str = "trend_threshold",
    classification_target: str = None,
) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    anomalies = None

    # -- ARIMA Forecast (first column) ----------------------------------------
    try:
        col   = df.columns[0]
        model = ARIMA(df[col].dropna(), order=(5, 1, 0))
        fitted = model.fit()
        forecast = fitted.forecast(steps=forecast_horizon)
        conf     = fitted.get_forecast(steps=forecast_horizon).conf_int()
        results["forecast_arima"] = {
            "column":               col,
            "values":               forecast.tolist(),
            "confidence_intervals": {
                "lower": conf.iloc[:, 0].tolist(),
                "upper": conf.iloc[:, 1].tolist(),
            },
        }
    except Exception as e:
        results["forecast_arima"] = {"error": f"ARIMA failed: {e}"}

    # -- Prophet Forecast (optional — lazy import) ----------------------------
    try:
        from prophet import Prophet  # noqa: PLC0415

        col        = df.columns[0]
        # Reset index so timestamp becomes a plain column named 'ds'
        prophet_df = (
            df[[col]]
            .reset_index()
            .rename(columns={"timestamp": "ds", col: "y"})
        )
        prophet_df["ds"] = pd.to_datetime(prophet_df["ds"]).dt.tz_localize(None)

        m      = Prophet(daily_seasonality=True)
        m.fit(prophet_df)
        future = m.make_future_dataframe(periods=forecast_horizon, freq="30s")
        pred   = m.predict(future)
        tail   = pred[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(forecast_horizon)
        results["forecast_prophet"] = {
            "column":      col,
            "predictions": tail.to_dict("records"),
        }
    except Exception as e:
        results["forecast_prophet"] = {"error": f"Prophet failed: {e}"}

    # -- Anomaly Detection (Isolation Forest) ---------------------------------
    if anomaly_detection:
        try:
            iso      = IsolationForest(contamination=0.05, random_state=42)
            anomalies = iso.fit_predict(df.fillna(0))
            results["anomalies"] = {
                "anomaly_count":   int(np.sum(anomalies == -1)),
                "anomaly_indices": np.where(anomalies == -1)[0].tolist(),
            }
        except Exception as e:
            results["anomalies"] = {"error": f"IsolationForest failed: {e}"}

    # -- Classification (synthetic labels from anomaly result) ----------------
    if classification_target and anomalies is not None:
        try:
            labels = (anomalies == -1).astype(int)
            if len(np.unique(labels)) > 1 and len(df) > 20:
                split  = max(10, len(df) - 50)
                X      = df.fillna(0).values
                clf    = RandomForestClassifier(n_estimators=50, random_state=42)
                clf.fit(X[:split], labels[:split])
                acc    = clf.score(X[split:], labels[split:])
                results["classification"] = {
                    "accuracy":           round(float(acc), 4),
                    "feature_importance": dict(
                        zip(df.columns, clf.feature_importances_.round(4))
                    ),
                }
            else:
                results["classification"] = {
                    "error": "Not enough label variation or data for classification"
                }
        except Exception as e:
            results["classification"] = {"error": f"Classification failed: {e}"}

    # -- Predictive Maintenance (trend-threshold) ------------------------------
    if maintenance_model == "trend_threshold":
        alerts: Dict[str, str] = {}
        for col in df.columns:
            series = df[col].dropna()
            if len(series) < 10:
                continue
            trend = series.rolling(window=min(10, len(series))).mean().iloc[-1]
            mean  = series.mean()
            sigma = series.std()
            if sigma == 0:
                continue
            if trend > mean + 2 * sigma:
                alerts[col] = (
                    f"HIGH trend ({trend:.3f} > mean+2σ={mean + 2 * sigma:.3f})"
                    f" — potential overload / failure"
                )
            elif trend < mean - 2 * sigma:
                alerts[col] = (
                    f"LOW trend ({trend:.3f} < mean-2σ={mean - 2 * sigma:.3f})"
                    f" — potential failure / sensor dropout"
                )
        results["maintenance_alerts"] = alerts

    return results
