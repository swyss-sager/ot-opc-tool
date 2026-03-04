# ---------------------------------------------------------------------------
# Regression Models: 15 Modelle, CV, Hyperparameter-Tuning,
# Feature Importance, Residual Diagnostik, Learning Curves, Ensemble.
# ---------------------------------------------------------------------------

import warnings
warnings.filterwarnings("ignore")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from pathlib import Path
from scipy import stats
from sklearn.ensemble import (
    AdaBoostRegressor, ExtraTreesRegressor,
    GradientBoostingRegressor, RandomForestRegressor,
    VotingRegressor,
)
from sklearn.linear_model import (
    ElasticNet, Lasso, LinearRegression, Ridge,
)
from sklearn.model_selection import (
    GridSearchCV, KFold, cross_val_score, learning_curve, train_test_split,
)
from sklearn.neighbors import KNeighborsRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import PolynomialFeatures, StandardScaler
from sklearn.svm import SVR
from sklearn.tree import DecisionTreeRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from typing import Any, Dict, List, Tuple


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    mse  = mean_squared_error(y_true, y_pred)
    return {
        "r2":   round(float(r2_score(y_true, y_pred)), 4),
        "rmse": round(float(np.sqrt(mse)),              4),
        "mae":  round(float(mean_absolute_error(y_true, y_pred)), 4),
    }


def _split(df: pd.DataFrame) -> Tuple[np.ndarray, ...]:
    """Use first column as target, rest as features. 80/20 split."""
    target = df.columns[0]
    X      = StandardScaler().fit_transform(df.drop(target, axis=1).fillna(0))
    y      = df[target].fillna(0).values
    return train_test_split(X, y, test_size=0.2, random_state=42)


# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------

def perform_regression_models(
    df: pd.DataFrame,
    plots_dir: Path,
    cv_folds:  int = 5,
    run_gridsearch: bool = True,
) -> Dict[str, Any]:
    plots_dir = Path(plots_dir)
    plots_dir.mkdir(parents=True, exist_ok=True)
    results: Dict[str, Any] = {}

    if len(df.columns) < 2:
        return {"error": "Need at least 2 columns (1 target + 1 feature)"}

    target_col = df.columns[0]
    feature_cols = list(df.columns[1:])
    results["target"]   = target_col
    results["features"] = feature_cols

    X_train, X_test, y_train, y_test = _split(df)

    # ── 1. All Base Models ────────────────────────────────────────────────────
    base_models: Dict[str, Any] = {
        "OLS":          LinearRegression(),
        "Ridge":        Ridge(alpha=1.0),
        "Lasso":        Lasso(alpha=0.01, max_iter=5000),
        "ElasticNet":   ElasticNet(alpha=0.01, l1_ratio=0.5, max_iter=5000),
        "KNN":          KNeighborsRegressor(n_neighbors=5),
        "DecisionTree": DecisionTreeRegressor(max_depth=10, random_state=42),
        "RandomForest": RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1),
        "GradBoost":    GradientBoostingRegressor(n_estimators=100, random_state=42),
        "ExtraTrees":   ExtraTreesRegressor(n_estimators=100, random_state=42, n_jobs=-1),
        "AdaBoost":     AdaBoostRegressor(n_estimators=100, learning_rate=0.1, random_state=42),
        "SVR":          SVR(kernel="rbf", C=100),
    }

    # Polynomial (as pipeline)
    poly_pipe = Pipeline([
        ("poly",  PolynomialFeatures(degree=2, include_bias=False)),
        ("scale", StandardScaler()),
        ("reg",   LinearRegression()),
    ])
    base_models["Polynomial_deg2"] = poly_pipe

    # XGBoost (optional)
    try:
        from xgboost import XGBRegressor
        base_models["XGBoost"] = XGBRegressor(
            n_estimators=100, random_state=42,
            verbosity=0, n_jobs=-1,
        )
    except ImportError:
        pass

    # Fit all models + collect metrics
    model_metrics: Dict[str, Dict[str, float]] = {}
    fitted_models: Dict[str, Any] = {}

    for name, mdl in base_models.items():
        try:
            mdl.fit(X_train, y_train)
            y_pred = mdl.predict(X_test)
            model_metrics[name] = _metrics(y_test, y_pred)
            fitted_models[name] = mdl
        except Exception as e:
            model_metrics[name] = {"error": str(e)}

    results["model_metrics"] = model_metrics

    # ── 2. Ensemble: Voting Regressor ─────────────────────────────────────────
    try:
        voting = VotingRegressor(estimators=[
            ("rf",    RandomForestRegressor(n_estimators=50, random_state=42)),
            ("gb",    GradientBoostingRegressor(n_estimators=50, random_state=42)),
            ("ridge", Ridge(alpha=1.0)),
        ])
        voting.fit(X_train, y_train)
        y_v = voting.predict(X_test)
        model_metrics["VotingEnsemble"] = _metrics(y_test, y_v)
        fitted_models["VotingEnsemble"] = voting
    except Exception as e:
        model_metrics["VotingEnsemble"] = {"error": str(e)}

    # ── 3. Weighted Ensemble (60% RF + 40% GB) ────────────────────────────────
    try:
        rf_fitted = fitted_models.get("RandomForest")
        gb_fitted = fitted_models.get("GradBoost")
        if rf_fitted and gb_fitted:
            y_weighted = 0.6 * rf_fitted.predict(X_test) + 0.4 * gb_fitted.predict(X_test)
            model_metrics["WeightedEnsemble_60RF_40GB"] = _metrics(y_test, y_weighted)
        else:
            model_metrics["WeightedEnsemble_60RF_40GB"] = {"error": "RF or GB not fitted"}
    except Exception as e:
        model_metrics["WeightedEnsemble_60RF_40GB"] = {"error": str(e)}

    results["model_metrics"] = model_metrics

    # ── 4. Model Comparison Bar Chart ─────────────────────────────────────────
    try:
        valid = {
            k: v for k, v in model_metrics.items()
            if "r2" in v
        }
        names = list(valid.keys())
        r2s = [valid[n]["r2"] for n in names]
        rmses = [valid[n]["rmse"] for n in names]

        fig, axes = plt.subplots(1, 2, figsize=(max(12, len(names) * 1.2), 5))

        colors_r2 = ["steelblue" if v >= 0 else "tomato" for v in r2s]
        axes[0].barh(names, r2s, color=colors_r2)
        axes[0].axvline(0, color="black", lw=0.8)
        axes[0].set_title("R² Score — alle Modelle  (höher = besser)")
        axes[0].set_xlabel("R²")
        for i, v in enumerate(r2s):
            axes[0].text(max(v, 0) + 0.005, i, f"{v:.3f}", va="center", fontsize=7)

        axes[1].barh(names, rmses, color="darkorange")
        axes[1].set_title("RMSE — alle Modelle  (niedriger = besser)")
        axes[1].set_xlabel("RMSE")
        for i, v in enumerate(rmses):
            axes[1].text(v + 0.005, i, f"{v:.3f}", va="center", fontsize=7)

        plt.tight_layout()
        plt.savefig(plots_dir / "model_comparison.png", dpi=150, bbox_inches="tight")
        plt.close()
        results["model_comparison_plot"] = "saved: model_comparison.png"
    except Exception as e:
        results["model_comparison_plot"] = {"error": str(e)}

    # ── 5. Cross-Validation ───────────────────────────────────────────────────
    cv_models = {
        k: v for k, v in {
            "Ridge": Ridge(alpha=1.0),
            "RandomForest": RandomForestRegressor(n_estimators=50, random_state=42, n_jobs=-1),
            "GradBoost": GradientBoostingRegressor(n_estimators=50, random_state=42),
            "SVR": SVR(kernel="rbf", C=100),
        }.items()
    }

    X_all = np.vstack([X_train, X_test])
    y_all = np.concatenate([y_train, y_test])
    kf = KFold(n_splits=cv_folds, shuffle=True, random_state=42)
    cv_results: Dict[str, Any] = {}

    for name, mdl in cv_models.items():
        try:
            scores = cross_val_score(mdl, X_all, y_all, cv=kf, scoring="r2", n_jobs=-1)
            cv_results[name] = {
                "mean_r2": round(float(scores.mean()), 4),
                "std_r2": round(float(scores.std()), 4),
                "min_r2": round(float(scores.min()), 4),
                "max_r2": round(float(scores.max()), 4),
                "scores": [round(float(s), 4) for s in scores],
                "stability": (
                    "High" if scores.std() < 0.05 else
                    "Medium" if scores.std() < 0.15 else
                    "Low"
                ),
            }
        except Exception as e:
            cv_results[name] = {"error": str(e)}

    results["cross_validation"] = cv_results

    # CV Comparison Plot
    try:
        valid_cv = {k: v for k, v in cv_results.items() if "mean_r2" in v}
        fig, ax = plt.subplots(figsize=(max(8, len(valid_cv) * 2), 5))
        positions = np.arange(len(valid_cv))
        means = [valid_cv[k]["mean_r2"] for k in valid_cv]
        stds = [valid_cv[k]["std_r2"] for k in valid_cv]

        ax.bar(positions, means, yerr=stds, capsize=5,
               color="steelblue", alpha=0.75, edgecolor="white")
        ax.set_xticks(positions)
        ax.set_xticklabels(list(valid_cv.keys()), rotation=20, ha="right")
        ax.set_title(f"{cv_folds}-Fold Cross-Validation — Mean R² ± Std")
        ax.set_ylabel("R²")
        ax.axhline(0, color="black", lw=0.8)

        for i, (m, s) in enumerate(zip(means, stds)):
            ax.text(i, m + s + 0.01, f"{m:.3f}", ha="center", fontsize=8)

        plt.tight_layout()
        plt.savefig(plots_dir / "cross_validation.png", dpi=150, bbox_inches="tight")
        plt.close()
    except Exception as e:
        results["cv_plot"] = {"error": str(e)}

    # ── 6. Hyperparameter Tuning (GridSearchCV) ───────────────────────────────
    if run_gridsearch:
        grid_results: Dict[str, Any] = {}

        # Random Forest Grid
        try:
            rf_grid = GridSearchCV(
                RandomForestRegressor(random_state=42, n_jobs=-1),
                param_grid={
                    "n_estimators": [50, 100],
                    "max_depth": [5, 10, 20],
                    "min_samples_split": [2, 5],
                },
                cv=3, scoring="r2", n_jobs=-1,
            )
            rf_grid.fit(X_train, y_train)
            y_pred_best = rf_grid.best_estimator_.predict(X_test)
            grid_results["RandomForest"] = {
                "best_params": rf_grid.best_params_,
                "best_cv_r2": round(float(rf_grid.best_score_), 4),
                "test_r2": round(float(r2_score(y_test, y_pred_best)), 4),
            }
            fitted_models["RandomForest_tuned"] = rf_grid.best_estimator_
        except Exception as e:
            grid_results["RandomForest"] = {"error": str(e)}

        # SVR Grid
        try:
            svr_grid = GridSearchCV(
                SVR(),
                param_grid={
                    "C": [0.1, 1, 10, 100],
                    "gamma": ["scale", "auto"],
                    "kernel": ["rbf", "linear"],
                },
                cv=3, scoring="r2", n_jobs=-1,
            )
            svr_grid.fit(X_train, y_train)
            y_pred_svr = svr_grid.best_estimator_.predict(X_test)
            grid_results["SVR"] = {
                "best_params": svr_grid.best_params_,
                "best_cv_r2": round(float(svr_grid.best_score_), 4),
                "test_r2": round(float(r2_score(y_test, y_pred_svr)), 4),
            }
            fitted_models["SVR_tuned"] = svr_grid.best_estimator_
        except Exception as e:
            grid_results["SVR"] = {"error": str(e)}

        results["hyperparameter_tuning"] = grid_results

    # ── 7. Feature Importance ─────────────────────────────────────────────────
    fi_results: Dict[str, Any] = {}
    for name in ["RandomForest", "GradBoost", "ExtraTrees",
                 "RandomForest_tuned", "AdaBoost"]:
        mdl = fitted_models.get(name)
        if mdl is None:
            continue
        base = mdl.named_steps["reg"] if hasattr(mdl, "named_steps") else mdl
        if not hasattr(base, "feature_importances_"):
            continue
        imp = base.feature_importances_
        fi_results[name] = {
            col: round(float(v), 5)
            for col, v in zip(feature_cols, imp)
        }

    if fi_results:
        # Plot — top model (RandomForest_tuned preferred, else RandomForest)
        best_fi_key = next(
            (k for k in ["RandomForest_tuned", "RandomForest"] if k in fi_results),
            next(iter(fi_results))
        )
        fi_data = fi_results[best_fi_key]
        sorted_fi = sorted(fi_data.items(), key=lambda x: x[1], reverse=True)
        f_names = [x[0] for x in sorted_fi]
        f_vals = [x[1] for x in sorted_fi]
        pcts = [v / sum(f_vals) * 100 for v in f_vals]

        fig, ax = plt.subplots(figsize=(8, max(4, len(f_names) * 0.5 + 2)))
        bars = ax.barh(f_names[::-1], f_vals[::-1], color="steelblue", alpha=0.8)
        ax.set_title(f"Feature Importance — {best_fi_key}")
        ax.set_xlabel("Importance (Mean Decrease Impurity)")
        for bar, pct in zip(bars, pcts[::-1]):
            ax.text(bar.get_width() + 0.001, bar.get_y() + bar.get_height() / 2,
                    f"{pct:.1f}%", va="center", fontsize=8)
        plt.tight_layout()
        plt.savefig(plots_dir / "feature_importance.png", dpi=150, bbox_inches="tight")
        plt.close()

    results["feature_importance"] = fi_results

    # ── 8. Residual Diagnostics ───────────────────────────────────────────────
    residual_results: Dict[str, Any] = {}
    for name in ["RandomForest", "GradBoost"]:
        mdl = fitted_models.get(name)
        if mdl is None:
            continue
        try:
            y_pred = mdl.predict(X_test)
            resid = y_test - y_pred

            fig, axes = plt.subplots(1, 3, figsize=(15, 4))

            # Residuals vs Fitted
            axes[0].scatter(y_pred, resid, alpha=0.4, s=10, color="steelblue")
            axes[0].axhline(0, color="red", lw=1, linestyle="--")
            axes[0].set_xlabel("Fitted Values")
            axes[0].set_ylabel("Residuals")
            axes[0].set_title(f"{name} — Residuals vs Fitted")

            # Q-Q Plot
            (osm, osr), (slope, intercept, _) = stats.probplot(resid)
            axes[1].scatter(osm, osr, s=10, alpha=0.5, color="steelblue")
            axes[1].plot(
                [osm[0], osm[-1]],
                [slope * osm[0] + intercept, slope * osm[-1] + intercept],
                color="red", lw=1.5,
            )
            axes[1].set_title(f"{name} — Q-Q Plot")
            axes[1].set_xlabel("Theoretical Quantiles")
            axes[1].set_ylabel("Sample Quantiles")

            # Residual Histogram
            axes[2].hist(resid, bins=40, color="steelblue", alpha=0.7, edgecolor="white")
            axes[2].axvline(0, color="red", lw=1.5, linestyle="--")
            axes[2].set_title(f"{name} — Residual Distribution")
            axes[2].set_xlabel("Residual")

            plt.suptitle(f"Residual Diagnostics — {name}", fontsize=12)
            plt.tight_layout()
            plt.savefig(plots_dir / f"residuals_{name}.png", dpi=150, bbox_inches="tight")
            plt.close()

            residual_results[name] = {
                "mean_residual": round(float(resid.mean()), 5),
                "std_residual": round(float(resid.std()), 5),
                "max_residual": round(float(np.abs(resid).max()), 5),
            }
        except Exception as e:
            residual_results[name] = {"error": str(e)}

    results["residual_diagnostics"] = residual_results

    # ── 9. Prediction vs Actual Plots ─────────────────────────────────────────
    pred_plot_models = ["Ridge", "RandomForest", "GradBoost"]
    for name in pred_plot_models:
        mdl = fitted_models.get(name)
        if mdl is None:
            continue
        try:
            y_pred = mdl.predict(X_test)
            m = _metrics(y_test, y_pred)

            fig, ax = plt.subplots(figsize=(6, 6))
            ax.scatter(y_test, y_pred, alpha=0.4, s=12, color="steelblue", label="Predictions")
            lims = [
                min(y_test.min(), y_pred.min()),
                max(y_test.max(), y_pred.max()),
            ]
            ax.plot(lims, lims, "r--", lw=1.5, label="Perfect (45°)")
            ax.set_xlabel("Actual")
            ax.set_ylabel("Predicted")
            ax.set_title(
                f"Predicted vs Actual — {name}\n"
                f"R²={m['r2']}  RMSE={m['rmse']}  MAE={m['mae']}"
            )
            ax.legend(fontsize=8)
            plt.tight_layout()
            plt.savefig(plots_dir / f"pred_vs_actual_{name}.png", dpi=150, bbox_inches="tight")
            plt.close()
        except Exception as e:
            results.setdefault("pred_vs_actual_errors", {})[name] = str(e)

    # ── 10. Learning Curves ───────────────────────────────────────────────────
    lc_results: Dict[str, Any] = {}
    lc_models = {
        "RandomForest": RandomForestRegressor(n_estimators=50, random_state=42, n_jobs=-1),
        "GradBoost": GradientBoostingRegressor(n_estimators=50, random_state=42),
    }
    train_sizes = np.linspace(0.1, 1.0, 8)

    for name, mdl in lc_models.items():
        try:
            t_sizes, t_scores, v_scores = learning_curve(
                mdl, X_all, y_all,
                cv=3,
                train_sizes=train_sizes,
                scoring="r2",
                n_jobs=-1,
            )
            t_mean = t_scores.mean(axis=1)
            v_mean = v_scores.mean(axis=1)
            gap = float((t_mean - v_mean).mean())

            status = (
                "Overfitting" if gap > 0.2 else
                "Underfitting" if v_mean[-1] < 0.5 else
                "Balanced"
            )
            lc_results[name] = {
                "final_train_r2": round(float(t_mean[-1]), 4),
                "final_val_r2": round(float(v_mean[-1]), 4),
                "mean_gap": round(gap, 4),
                "status": status,
            }

            fig, ax = plt.subplots(figsize=(8, 5))
            ax.plot(t_sizes, t_mean, "o-", color="steelblue", label="Training Score")
            ax.plot(t_sizes, v_mean, "o-", color="tomato", label="Validation Score")
            ax.fill_between(t_sizes,
                            t_scores.mean(axis=1) - t_scores.std(axis=1),
                            t_scores.mean(axis=1) + t_scores.std(axis=1),
                            alpha=0.1, color="steelblue")
            ax.fill_between(t_sizes,
                            v_scores.mean(axis=1) - v_scores.std(axis=1),
                            v_scores.mean(axis=1) + v_scores.std(axis=1),
                            alpha=0.1, color="tomato")
            ax.set_xlabel("Training Samples")
            ax.set_ylabel("R² Score")
            ax.set_title(f"Learning Curve — {name}  [{status}]")
            ax.legend(fontsize=9)
            ax.axhline(0, color="black", lw=0.8, linestyle="--")
            plt.tight_layout()
            plt.savefig(plots_dir / f"learning_curve_{name}.png", dpi=150, bbox_inches="tight")
            plt.close()
        except Exception as e:
            lc_results[name] = {"error": str(e)}

    results["learning_curves"] = lc_results

    # ── Best Model Summary ────────────────────────────────────────────────────
    valid_metrics = {
        k: v for k, v in model_metrics.items()
        if "r2" in v and isinstance(v["r2"], float)
    }
    if valid_metrics:
        best_name = max(valid_metrics, key=lambda k: valid_metrics[k]["r2"])
        results["best_model"] = {
            "name": best_name,
            "metrics": valid_metrics[best_name],
        }

    return results

