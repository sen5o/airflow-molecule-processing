"""K-means clustering of molecules by their physicochemical property vector.

New processing module (no prior worker existed for this step). Follows the
same "pure function" shape as generation.py / properties.py: takes rows
already enriched by properties.calculate_properties, returns the same rows
with a "cluster" column appended. No DB, no S3 — I/O lives in the DAG.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)

# Must be a subset of the columns produced by properties.PROPERTIES.
DEFAULT_FEATURE_COLUMNS = [
    "mol_weight",
    "log_p",
    "tpsa",
    "hba",
    "hbd",
    "rotatable_bonds",
    "aromatic_rings",
]

DEFAULT_N_CLUSTERS = 5
RANDOM_STATE = 42


def _build_feature_matrix(
    rows: list[dict[str, Any]],
    feature_columns: list[str],
) -> np.ndarray:
    missing = [col for col in feature_columns if rows and col not in rows[0]]
    if missing:
        raise ValueError(
            f"Rows are missing required property column(s): {missing}. "
            "Did you run properties calculation before clustering?"
        )
    return np.array([[float(row[col]) for col in feature_columns] for row in rows])


def choose_k_by_silhouette(
    features: np.ndarray,
    k_min: int = 2,
    k_max: int = 10,
) -> int:
    """Pick the k in [k_min, k_max] with the highest silhouette score.

    k_max is clamped to n_samples - 1, since silhouette requires at least
    2 clusters and fewer clusters than samples.
    """
    n_samples = features.shape[0]
    k_max = min(k_max, n_samples - 1)
    if k_max < k_min:
        logger.warning(
            "Not enough samples (%d) to search k in [%d, %d]; defaulting to k=%d.",
            n_samples, k_min, k_max, DEFAULT_N_CLUSTERS,
        )
        return min(DEFAULT_N_CLUSTERS, max(1, n_samples))

    best_k, best_score = k_min, -1.0
    for k in range(k_min, k_max + 1):
        labels = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init="auto").fit_predict(features)
        score = silhouette_score(features, labels)
        logger.debug("k=%d silhouette=%.4f", k, score)
        if score > best_score:
            best_k, best_score = k, score

    logger.info("Selected k=%d by silhouette score (%.4f).", best_k, best_score)
    return best_k


def cluster_molecules(
    rows: list[dict[str, Any]],
    n_clusters: int | None = DEFAULT_N_CLUSTERS,
    feature_columns: list[str] | None = None,
    auto_k_range: tuple[int, int] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Cluster molecules by their (scaled) property vector using K-means.

    Args:
        rows: property rows, each containing at least `feature_columns`.
        n_clusters: fixed k to use. Ignored if `auto_k_range` is given.
        feature_columns: which property columns to cluster on. Defaults to
            DEFAULT_FEATURE_COLUMNS.
        auto_k_range: optional (k_min, k_max) to auto-select k via silhouette
            score instead of using a fixed n_clusters.

    Returns (rows_with_cluster, stats). Each row gets an integer "cluster"
    column (0-indexed). Raises ValueError if there are fewer rows than the
    number of clusters requested, or if required columns are missing.
    """
    if not rows:
        raise ValueError("Clustering received 0 input rows.")

    feature_columns = feature_columns or DEFAULT_FEATURE_COLUMNS
    features = _build_feature_matrix(rows, feature_columns)

    scaler = StandardScaler()
    scaled = scaler.fit_transform(features)

    if auto_k_range is not None:
        k = choose_k_by_silhouette(scaled, *auto_k_range)
    else:
        k = n_clusters or DEFAULT_N_CLUSTERS

    if k > len(rows):
        raise ValueError(
            f"Requested {k} clusters but only {len(rows)} molecule(s) were provided."
        )

    model = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init="auto")
    labels = model.fit_predict(scaled)

    result = [{**row, "cluster": int(label)} for row, label in zip(rows, labels, strict=True)]

    cluster_sizes = {
        int(c): int(n)
        for c, n in zip(*np.unique(labels, return_counts=True), strict=True)
    }
    stats = {
        "n_clusters": k,
        "n_molecules": len(rows),
        "feature_columns": feature_columns,
        "cluster_sizes": cluster_sizes,
    }
    logger.info("Clustering complete: %s", stats)
    return result, stats
