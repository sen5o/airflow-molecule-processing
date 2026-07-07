"""Combinatorial molecule generation from a scaffold and R-groups.

Pure processing logic, extracted from the ``molecules_generation`` worker.
All database / artifact / S3 coupling has been removed: these functions take
in-memory data and return in-memory data. I/O is the DAG's responsibility
(via S3Hook), state is Airflow's responsibility.
"""

from __future__ import annotations

import csv
import io
import itertools
import logging
from typing import Any

from rdkit import Chem

logger = logging.getLogger(__name__)

MAX_MOLECULES_DEFAULT = 50_000


def load_r_groups(csv_text: str) -> dict[str, list[str]]:
    """Parse an R-groups CSV into ``{column: [valid SMILES, ...]}``.

    Each column is one attachment position. A row is skipped if any of its
    cells is empty or is not a parseable SMILES. This validation is kept on
    purpose: it is chemistry-intrinsic (``molzip`` cannot consume an invalid
    fragment), not framework plumbing.
    """
    reader = csv.DictReader(io.StringIO(csv_text))
    result: dict[str, list[str]] = {col: [] for col in (reader.fieldnames or [])}

    skipped = 0
    for row in reader:
        cleaned = {col: (smi or "").strip() for col, smi in row.items()}
        if all(smi and Chem.MolFromSmiles(smi) is not None for smi in cleaned.values()):
            for col, smi in cleaned.items():
                result[col].append(smi)
        else:
            skipped += 1

    if skipped:
        logger.warning("Skipped %d invalid R-group row(s).", skipped)
    logger.info("Loaded R-groups: %s", {col: len(vals) for col, vals in result.items()})
    return result


def generate_molecules(
    scaffold: str,
    r_groups: dict[str, list[str]],
    max_molecules: int = MAX_MOLECULES_DEFAULT,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Enumerate molecules by attaching R-groups to a scaffold (RDKit molzip).

    The Cartesian product of all R-groups across all positions is built, each
    combination is zipped onto the scaffold, sanitised, and canonicalised.

    Returns ``(rows, stats)`` where each row is ``{"smiles": <canonical>,
    <label>: <r_group_smiles>, ...}``.

    Kept validations (chemistry-intrinsic):
      * scaffold must be a valid molecule;
      * number of attachment points must match number of R-group columns;
      * a combination that fails to sanitise is skipped, not fatal.
    """
    scaffold_mol = Chem.MolFromSmiles(scaffold)
    if scaffold_mol is None:
        raise ValueError(f"Invalid scaffold SMILES: {scaffold!r}")

    ordered_labels = sorted(r_groups.keys())
    attachment_points = [a for a in scaffold_mol.GetAtoms() if a.GetAtomicNum() == 0]
    if len(attachment_points) != len(ordered_labels):
        raise ValueError(
            f"Scaffold has {len(attachment_points)} attachment point(s) but "
            f"{len(ordered_labels)} R-group column(s) were provided: {ordered_labels}."
        )

    rows: list[dict[str, Any]] = []
    attempted = 0
    skipped = 0

    for combo in itertools.product(*(r_groups[label] for label in ordered_labels)):
        if attempted >= max_molecules:
            logger.warning("Reached max_molecules cap (%d); stopping early.", max_molecules)
            break
        attempted += 1

        try:
            rw = Chem.RWMol(scaffold_mol)
            for smi in combo:
                rw.InsertMol(Chem.MolFromSmiles(smi))
            product = Chem.molzip(rw)
            Chem.SanitizeMol(product)
        except Exception as exc:  # noqa: BLE001 - one bad combo must not kill the batch
            logger.debug("Skipping combo %s: %s", combo, exc)
            skipped += 1
            continue

        row: dict[str, Any] = {"smiles": Chem.MolToSmiles(product)}
        for label, smi in zip(ordered_labels, combo, strict=True):
            row[label] = smi
        rows.append(row)

    stats = {"attempted": attempted, "valid": len(rows), "skipped": skipped}
    logger.info("Generation complete: %s", stats)
    return rows, stats
