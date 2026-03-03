# app_data_analyzer — OT OPC Tool: Datenanalyse Engine

## Übersicht

`app_data_analyzer` ist eine modulare, vollständige Datenanalyse-Pipeline für
Zeitreihendaten aus dem OPC UA Process Historian (Siemens). Sie verarbeitet
die gemergten CSV/JSON-Daten aus `app_data_merger` und führt automatisch
**18+ Analysemodule** mit über **40 ML-Modellen** und **100+ Metriken** aus.

---

## Projektstruktur

```
src/app_data_analyzer/
├── main.py                        ← Standalone Entry Point
├── config/
│   └── analyzer_config.json       ← Zentrale Konfiguration
└── logic/
    ├── analyzer_config_loader.py  ← Config-Loader
    ├── analyzer_runner.py         ← Orchestrierung aller Module
    ├── data_loader.py             ← CSV/JSON Loader + Pfad-Auflösung
    ├── descriptive.py             ← Deskriptive Statistik
    ├── exploratory.py             ← EDA + Visualisierungen
    ├── diagnostic.py              ← Feature Importance + Granger
    ├── inferential.py             ← Hypothesentests + Konfidenzintervalle
    ├── predictive.py              ← Forecasting + Anomalien + Maintenance
    ├── prescriptive.py            ← Alerts + LP-Optimierung
    ├── pattern_finder.py          ← Clustering + Sequenzen
    ├── distribution_analysis.py   ← Verteilungen + Normalitätstests  [NEU]
    ├── timeseries_analysis.py     ← ACF/PACF/STL/ADF/KPSS            [NEU]
    ├── anomaly_extended.py        ← Z-Score/CUSUM/Ruptures            [NEU]
    ├── correlation_advanced.py    ← Pearson/Spearman/VIF/Clustering   [NEU]
    ├── quality_metrics.py         ← Data Quality Index (0-100)        [NEU]
    ├── visualizations_extended.py ← Pair/Violin/KDE/3D Plots          [NEU]
    └── regression_models.py       ← 15+ Regressionsmodelle + CV       [NEU]
```

---

## Ausführung

```bash
# Standalone
python -m src.app_data_analyzer.main

# Als Teil der Pipeline
python -m src.main   # RUN_ANALYZER = True in src/main.py
```

---

## Konfiguration (`analyzer_config.json`)

```json
{
  "analyzer": {
    "input_files": [{"path": "data/merged/...", "format": "csv"}],
    "output_dir":  "data/analyzer/_output",
    "plots_dir":   "data/analyzer/analyzer_plots",
    "run_descriptive":    true,
    "run_exploratory":    true,
    "run_diagnostic":     true,
    "run_inferential":    true,
    "run_predictive":     true,
    "run_prescriptive":   true,
    "run_patterns":       true,
    "run_distribution":   true,
    "run_timeseries":     true,
    "run_anomaly_ext":    true,
    "run_correlation_adv":true,
    "run_quality":        true,
    "run_viz_extended":   true,
    "run_regression":     true
  }
}
```

**Alle Flags sind unabhängig** — jedes Modul kann einzeln aktiviert/deaktiviert werden.

---

## Analyse-Module im Detail

### 1. Deskriptive Analyse (`descriptive.py`)
**Ziel**: Statistische Basis-Kennzahlen aller Zeitreihen.

| Metrik | Beschreibung |
|--------|-------------|
| `count`, `valid`, `missing` | Datenvollständigkeit |
| `mean`, `median`, `std` | Lagemaße + Streuung |
| `min`, `max` | Wertebereich |
| `p25`, `p50`, `p75`, `p95` | Perzentile (konfigurierbar) |
| `skewness` | Schiefe der Verteilung |
| `kurtosis` | Wölbung (Tailedness) |

**Output**: JSON-Bericht in `data/analyzer/_output/`

---

### 2. Explorative Analyse / EDA (`exploratory.py`)
**Ziel**: Visuelle und strukturelle Daten-Exploration.

| Funktion | Beschreibung |
|----------|-------------|
| Korrelations-Matrix | Pearson-Korrelationen als Heatmap |
| IQR Outlier Detection | Ausreißer je Spalte zählen |
| Distributions-Histogramme | Je Spalte (erste 4) |
| Time Series Overview | Alle Kanäle in einem Plot |
| Missing Value Strategie | `drop` oder `fill` |

**Plots**: `correlation_heatmap.png`, `dist_*.png`, `time_series_overview.png`

---

### 3. Diagnostische Analyse (`diagnostic.py`)
**Ziel**: Ursachenanalyse — welche Features beeinflussen andere?

| Methode | Beschreibung |
|---------|-------------|
| **Random Forest Regressor** | Feature Importance (Mean Decrease Impurity) |
| **Granger Causality Test** | Kausalitätstest für je 5 Lags, p-Werte |
| Peak-Analyse | `idxmax()` — Zeitpunkte maximaler Werte |

**Interpretation Granger**:
- `p < 0.05` → Spalte X *Granger-verursacht* Spalte Y
- `causal: true/false` im JSON-Report

---

### 4. Inferenzielle Analyse (`inferential.py`)
**Ziel**: Statistische Signifikanztests + Konfidenzintervalle.

| Test | Beschreibung |
|------|-------------|
| **Welch's t-Test** | Mittelwertvergleich zweier Spalten |
| **Chi-Quadrat-Test** | Unabhängigkeitstest (binned Daten) |
| **95% Konfidenzintervall** | t-Verteilung, für alle Spalten |

**Interpretation**:
- `significant: true` → p-Wert < 0.05 → Unterschied statistisch signifikant
- CI gibt Vertrauensbereich für den wahren Mittelwert an

---

### 5. Prädiktive Analyse (`predictive.py`)
**Ziel**: Zeitreihen-Vorhersage + Anomalieerkennung + Predictive Maintenance.

#### 5.1 ARIMA Forecasting
- **Modell**: ARIMA(5,1,0) auf erste Spalte
- **Output**: Forecasts + 95% Konfidenzintervalle für `forecast_horizon` Steps
- **Stärke**: Autoregressive + Moving-Average Zeitreihenkomponenten

#### 5.2 Prophet Forecasting (Meta/Facebook)
- **Modell**: Prophet mit Daily Seasonality
- **Frequenz**: 30s (passend zu 500ms-Grid der Merger-Daten)
- **Output**: `yhat`, `yhat_lower`, `yhat_upper` für Vorhersage-Horizon
- **Stärke**: Robust gegenüber Missing Data, Trend-Changepoints, Saisonalität

#### 5.3 Isolation Forest — Anomalie-Erkennung
- **Modell**: Unsupervised, `contamination=0.05` (5% erwartet anomal)
- **Output**: Anomalie-Indizes + Anzahl
- **Stärke**: Keine Annahmen über Verteilung nötig

#### 5.4 Classification (synthetisch)
- **Modell**: Random Forest Classifier auf Anomalie-Labels
- **Output**: Accuracy + Feature Importance
- **Hinweis**: Labels aus Isolation Forest — für echte Labels anpassen

#### 5.5 Predictive Maintenance
- **Methode**: Rolling Mean (Window=10) vs. Mean ± 2σ
- **Alert Typen**:
  - `HIGH trend` → potentielle Überlast / Ausfall
  - `LOW trend` → Sensor-Dropout / Wartung erforderlich

---

### 6. Präskriptive Analyse (`prescriptive.py`)
**Ziel**: Handlungsempfehlungen + Ressourcen-Optimierung.

| Funktion | Beschreibung |
|----------|-------------|
| **Threshold Alerts** | Min/Max-Schwellen aus Config → Aktionsempfehlung |
| **Linear Programming (PuLP)** | CBC MILP Solver: Balance-Optimierung |

**LP-Modell**:
- Zielfunktion: Maximiere Balanz-Score über alle Kanäle
- Constraint: Σ(efforts) ≤ 1
- Output: Optimale Gewichtung je Kanal + Objective Value

---

### 7. Pattern Finder (`pattern_finder.py`)
**Ziel**: Verborgene Muster + strukturelle Abhängigkeiten finden.

| Methode | Beschreibung |
|---------|-------------|
| **K-Means Clustering** | n_clusters=3, StandardScaler, n_init=10 |
| **Sequence Correlation** | Rolling Window Korrelation (Window=10) |
| **High Dependency Detection** | Alle Paare mit \|Korrelation\| > 0.8 |

---

### 8. Verteilungsanalyse (`distribution_analysis.py`) *[NEU]*
→ Siehe Implementierung unten

### 9. Zeitreihen-Analyse (`timeseries_analysis.py`) *[NEU]*
→ ACF, PACF, ADF, KPSS, STL

### 10. Erweiterte Anomalien (`anomaly_extended.py`) *[NEU]*
→ Z-Score, CUSUM, Ruptures

### 11. Korrelation Erweitert (`correlation_advanced.py`) *[NEU]*
→ Pearson, Spearman, VIF, Hierarchical Clustering

### 12. Qualitäts-Metriken (`quality_metrics.py`) *[NEU]*
→ Data Quality Index 0–100

### 13. Erweiterte Visualisierungen (`visualizations_extended.py`) *[NEU]*
→ Pair Plots, Violin, KDE, 3D

### 14. Regressionsmodelle (`regression_models.py`) *[NEU]*
→ 15 Modelle: OLS, Ridge, Lasso, ElasticNet, Poly, SVR, RF, GB, XGB, KNN, DT, ExtraTrees, AdaBoost, Voting, Weighted Ensemble

---

## Alle ML-Modelle auf einen Blick

| Kategorie | Modell | Typ |
|-----------|--------|-----|
| Linear | OLS, Ridge (L2), Lasso (L1), Elastic Net | Regression |
| Polynomial | Degree-2 Polynomial + LinearRegression | Regression |
| Kernel | SVR (RBF, C=100) | Regression |
| Ensemble | Random Forest (n=100) | Regression |
| Ensemble | Gradient Boosting (n=100) | Regression |
| Ensemble | XGBoost (n=100) | Regression |
| Ensemble | Extra Trees (n=100) | Regression |
| Ensemble | AdaBoost (n=100, lr=0.1) | Regression |
| Ensemble | Voting (RF+GB+Ridge avg) | Regression |
| Ensemble | Weighted (60% RF + 40% GB) | Regression |
| Instance | KNN (k=5) | Regression |
| Tree | Decision Tree (max_depth=10) | Regression |
| Forecasting | ARIMA(5,1,0) | Time Series |
| Forecasting | Prophet (daily seasonality) | Time Series |
| Anomaly | Isolation Forest (contamination=5%) | Unsupervised |
| Anomaly | Z-Score (\|Z\|>3) | Statistical |
| Anomaly | CUSUM | Change Detection |
| Anomaly | Ruptures (Binseg) | Change Detection |
| Clustering | K-Means (k=3) | Unsupervised |
| Clustering | Hierarchical (Ward) | Unsupervised |
| Causal | Granger Causality (lag 1-5) | Statistical |
| Causal | VIF (Variance Inflation Factor) | Statistical |

---

## Output-Struktur

```
data/
├── analyzer/_output/
│   └── analysis_report_<ts>.json    ← Vollständiger JSON-Bericht
└── analyzer/analyzer_plots/
    ├── correlation_heatmap.png
    ├── dist_<col>.png
    ├── time_series_overview.png
    ├── distribution_<col>.png
    ├── boxplot_all.png
    ├── acf_pacf_<col>.png
    ├── stl_decomposition_<col>.png
    ├── anomaly_zscore_<col>.png
    ├── cusum_<col>.png
    ├── correlation_pearson.png
    ├── correlation_spearman.png
    ├── hierarchical_clustering.png
    ├── pairplot.png
    ├── violinplot.png
    ├── kde_<col>.png
    ├── scatter3d.png
    ├── regression_predictions.png
    ├── feature_importance.png
    ├── learning_curves.png
    └── residuals_<model>.png
```

---

## Dependencies (`requirements.txt`)

```
pandas>=2.1.0
numpy>=1.24.0
scikit-learn>=1.3.0
statsmodels>=0.14.0
matplotlib>=3.8.0
seaborn>=0.13.0
scipy>=1.11.0
prophet>=1.1.5
pulp>=2.7.0
xgboost>=2.0.0
ruptures>=1.1.7
```

---

## Glossar

| Begriff | Bedeutung |
|---------|-----------|
| **ADF** | Augmented Dickey-Fuller — Stationaritätstest (p<0.05 = stationär) |
| **KPSS** | Kwiatkowski-Phillips-Schmidt-Shin — bestätigt ADF |
| **ACF** | Auto-Correlation Function — Korrelation mit sich selbst zu Lag k |
| **PACF** | Partial ACF — direkte Korrelation ohne Zwischenwerte |
| **STL** | Seasonal-Trend Decomposition via Loess |
| **VIF** | Variance Inflation Factor — >10 = starke Multikollinearität |
| **CUSUM** | Cumulative Sum — erkennt Mittelwert-Shifts (Changepoints) |
| **IQR** | Interquartile Range — Q3−Q1, robust für Ausreißer |
| **RMSE** | Root Mean Square Error — Vorhersagefehler |
| **R²** | Bestimmtheitsmass — 1.0 = perfekte Vorhersage |
```