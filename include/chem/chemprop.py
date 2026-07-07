"""Optional molecular property prediction with ChemProp (D-MPNN).

Variant A ("honest stub"): the integration is fully wired, but because
training a model is out of scope for this project, the step is designed to
skip gracefully when no checkpoint is provided.

  * No checkpoint configured  -> raises ChemPropNotConfigured. The DAG turns
    this into an Airflow *skip* (the branch simply doesn't run, the pipeline
    does not fail).
  * A checkpoint is provided   -> runs `chemprop predict` (ChemProp v2 CLI)
    and merges the predictions back onto the input rows.

Framework-agnostic on purpose (no Airflow import): the DAG decides what to do
with ChemPropNotConfigured. `chemprop`/`torch` are heavy and are only needed
when a real checkpoint exists, so they are never imported at module load.
"""

from __future__ import annotations

import csv
import logging
import os
import subprocess
import tempfile
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_PREDICTION_COLUMN = "chemprop_prediction"


class ChemPropNotConfigured(RuntimeError):
    """Raised when no usable ChemProp checkpoint is available."""


def is_configured(checkpoint_path: str | None) -> bool:
    """True if a checkpoint path is set and the file actually exists on disk."""
    if not checkpoint_path:
        return False
    return os.path.exists(checkpoint_path)


def predict_properties(
    rows: list[dict[str, Any]],
    checkpoint_path: str | None = None,
    smiles_column: str = "smiles",
    prediction_column: str = DEFAULT_PREDICTION_COLUMN,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Predict molecular properties with a trained ChemProp model.

    Args:
        rows: molecule rows (must contain `smiles_column`).
        checkpoint_path: path to a ChemProp .ckpt/.pt file. If missing or
            unset, ChemPropNotConfigured is raised (the DAG skips this step).
        smiles_column: name of the SMILES column.
        prediction_column: name for the appended prediction column (used when
            the model has a single target).

    Returns (rows_with_predictions, stats).

    NOTE: the exact CLI flags below target ChemProp v2.x. If the pinned
    chemprop version differs, verify `chemprop predict --help` and adjust.
    """
    if not is_configured(checkpoint_path):
        raise ChemPropNotConfigured(
            f"No ChemProp checkpoint at {checkpoint_path!r}; skipping prediction step."
        )
    if not rows:
        raise ValueError("ChemProp received 0 input rows.")

    with tempfile.TemporaryDirectory() as tmp:
        test_path = os.path.join(tmp, "input.csv")
        preds_path = os.path.join(tmp, "preds.csv")

        with open(test_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow([smiles_column])
            for row in rows:
                writer.writerow([row.get(smiles_column, "")])

        cmd = [
            "chemprop", "predict",
            "--test-path", test_path,
            "--model-path", str(checkpoint_path),
            "--preds-path", preds_path,
            "--smiles-columns", smiles_column,
        ]
        logger.info("Running ChemProp: %s", " ".join(cmd))
        completed = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.debug("ChemProp stdout: %s", completed.stdout)

        with open(preds_path, encoding="utf-8") as fh:
            predictions = list(csv.DictReader(fh))

    result: list[dict[str, Any]] = []
    for row, pred in zip(rows, predictions, strict=False):
        merged = {**row}
        target_cols = [col for col in pred if col != smiles_column]
        if len(target_cols) == 1:
            merged[prediction_column] = pred[target_cols[0]]
        else:
            for col in target_cols:
                merged[col] = pred[col]
        result.append(merged)

    stats = {"input": len(rows), "predicted": len(predictions)}
    logger.info("ChemProp prediction complete: %s", stats)
    return result, stats
