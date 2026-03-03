# ---------------------------------------------------------------------------
# Pattern Finding: Clustering, Sequence Analysis.
# ---------------------------------------------------------------------------

import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from typing import Dict, Any, List


def find_patterns(
    df: pd.DataFrame,
    clustering_n_clusters: int = 3,
    sequence_window: int = 10,
) -> Dict[str, Any]:
    results = {}

    # Clustering (K-Means on features)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df)
    kmeans = KMeans(n_clusters=min(clustering_n_clusters, len(df)), random_state=42, n_init=10)
    clusters = kmeans.fit_predict(X_scaled)
    results["clusters"] = {
        "labels": clusters.tolist(),
        "cluster_centers": kmeans.cluster_centers_.tolist(),
        "silhouette_score": float(kmeans.score(X_scaled))  # Negative inertia as proxy
    }

    # Sequence Patterns (simple rolling correlation as pattern)
    seq_patterns = {}
    for i in range(len(df) - sequence_window):
        window = df.iloc[i:i+sequence_window]
        corr = window.corr().iloc[0,1] if len(window.columns) > 1 else None  # Corr between first two
        seq_patterns[f"seq_{i}"] = {"correlation": float(corr) if corr else None}

    results["sequences"] = {
        "window_size": sequence_window,
        "patterns_sample": list(seq_patterns.values())[:5]  # Sample
    }

    # Dependency Patterns: High correlations (>0.8)
    high_deps = []
    corr_matrix = df.corr()
    for i in range(len(corr_matrix.columns)):
        for j in range(i+1, len(corr_matrix.columns)):
            if abs(corr_matrix.iloc[i,j]) > 0.8:
                high_deps.append({
                    "pair": [corr_matrix.columns[i], corr_matrix.columns[j]],
                    "correlation": float(corr_matrix.iloc[i,j])
                })
    results["high_dependencies"] = high_deps

    return results
