"""Data quality checks for the cheminformatics pipeline.

Pure functions (no Airflow/S3 coupling) that inspect data at pipeline
checkpoints and raise DataQualityError with a descriptive report when a
check fails. The DAG wires these in as explicit tasks between processing
steps; a failure here is a normal task failure like any other, so it
cascades to on_failure_callback (Teams notification) the same way.

These check the same things the original per-worker `_validate_params`
methods used to guard against, but as first-class, reportable pipeline
gates rather than silent internal validation — see the project README's
"Architecture notes" for why that split happened.
"""

from __future__ import annotations

import logging
from typing import Any

from rdkit import Chem

log = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    """Raised when a data quality check fails. Carries a `report` dict."""

    def __init__(self, message: str, report: dict[str, Any]):
        super().__init__(message)
        self.report = report


def validate_smiles_ratio(
    rows: list[dict[str, Any]],
    smiles_column: str = "smiles",
    min_ratio: float = 0.8,
) -> dict[str, Any]:
    """Checks that at least `min_ratio` of rows have a parseable SMILES.

    Returns a report dict on success; raises DataQualityError on failure.
    """
    total = len(rows)
    if total == 0:
        report = {"total": 0, "valid": 0, "invalid": 0, "valid_ratio": 0.0}
        raise DataQualityError("No rows to validate (0 rows).", report)

    valid = sum(
        1
        for row in rows
        if Chem.MolFromSmiles(str(row.get(smiles_column, "")).strip()) is not None
    )
    ratio = valid / total
    report = {
        "total": total,
        "valid": valid,
        "invalid": total - valid,
        "valid_ratio": round(ratio, 4),
    }
    if ratio < min_ratio:
        raise DataQualityError(
            f"SMILES validity ratio {ratio:.1%} is below the required {min_ratio:.0%}.",
            report,
        )
    log.info("validate_smiles_ratio passed: %s", report)
    return report


def check_scaffold_attachment_points(
    scaffold_rows: list[dict[str, Any]],
    n_r_group_columns: int,
    smiles_column: str = "smiles",
) -> dict[str, Any]:
    """Checks that at least one scaffold has attachment points matching the
    number of R-group columns (i.e. enumeration is actually possible)."""
    matching = 0
    for row in scaffold_rows:
        smi = str(row.get(smiles_column, "")).strip()
        mol = Chem.MolFromSmiles(smi) if smi else None
        if mol is None:
            continue
        n_points = sum(1 for atom in mol.GetAtoms() if atom.GetAtomicNum() == 0)
        if n_points == n_r_group_columns:
            matching += 1

    report = {
        "total_scaffolds": len(scaffold_rows),
        "r_group_columns": n_r_group_columns,
        "scaffolds_with_matching_attachment_points": matching,
    }
    if matching == 0:
        raise DataQualityError(
            f"No scaffold has {n_r_group_columns} attachment point(s) matching "
            f"the {n_r_group_columns} R-group column(s) provided.",
            report,
        )
    log.info("check_scaffold_attachment_points passed: %s", report)
    return report


def check_min_molecule_count(molecule_count: int, minimum: int = 1) -> dict[str, Any]:
    """Checks that generation produced at least `minimum` molecule(s)."""
    report = {"molecule_count": molecule_count, "minimum_required": minimum}
    if molecule_count < minimum:
        raise DataQualityError(
            f"Generated {molecule_count} molecule(s), below the minimum of {minimum}.",
            report,
        )
    log.info("check_min_molecule_count passed: %s", report)
    return report


def check_cluster_balance(
    cluster_sizes: dict[int, int],
    max_dominant_ratio: float = 0.9,
) -> dict[str, Any]:
    """Checks that no single cluster contains more than `max_dominant_ratio`
    of all molecules (a near-total dominance suggests clustering isn't
    adding value, e.g. the property vector is nearly constant)."""
    total = sum(cluster_sizes.values())
    largest = max(cluster_sizes.values()) if cluster_sizes else 0
    ratio = (largest / total) if total else 1.0
    report = {
        "n_clusters": len(cluster_sizes),
        "total_molecules": total,
        "largest_cluster_size": largest,
        "largest_cluster_ratio": round(ratio, 4),
    }
    if ratio > max_dominant_ratio:
        raise DataQualityError(
            f"One cluster contains {ratio:.1%} of all molecules "
            f"(threshold: {max_dominant_ratio:.0%}) — clustering may not be meaningful.",
            report,
        )
    log.info("check_cluster_balance passed: %s", report)
    return report
