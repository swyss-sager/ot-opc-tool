# ---------------------------------------------------------------------------
# Diagnostic Analysis: Root causes, feature importance, causality.
# ---------------------------------------------------------------------------

import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from statsmodels.tsa.stattools import grangercausalitytests
from typing import Dict, Any, List


def perform_diagnostic(
    df: pd.DataFrame,
    feature_importance: bool = True,
    granger_causality: bool = True,
) -> Dict[str, Any]:
    results = {}

    if feature_importance:
        # Assume first column as target (e.g., main sensor)
        target_col = df.columns[0]
        X = df.drop(target_col, axis=1)
        y = df[target_col]

        model = RandomForestRegressor(n_estimators=50, random_state=42)
        model.fit(X, y)

        results["feature_importance"] = dict(zip(X.columns, model.feature_importances_))

    if granger_causality:
        gc_results = {}
        for col in df.columns[1:]:  # Test each vs. first col
            try:
                gc_test = grangercausalitytests(df[[df.columns[0], col]], maxlag=5, verbose=False)
                p_values = [round(gc_test[i+1][0]['ssr_ftest'][1], 4) for i in range(5)]
                gc_results[col] = {
                    "p_values_lags_1-5": p_values,
                    "causal": any(p < 0.05 for p in p_values)
                }
            except:
                gc_results[col] = {"error": "Insufficient data"}
        results["granger_causality"] = gc_results

    # Simple diagnostic: High variance periods
    results["high_variance_periods"] = df.idxmax().to_dict()  # Peaks per column

    return results
