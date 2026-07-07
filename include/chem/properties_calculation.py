"""Physicochemical property calculation for a set of molecules.

Pure processing logic, extracted from the ``properties_calculation`` worker.
Database / artifact / S3 coupling removed. Operates on a list of row dicts
(as produced by ``generation.generate_molecules`` or read from a CSV) and
returns the same rows enriched with property columns.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from rdkit import Chem
from rdkit.Chem import Descriptors, rdMolDescriptors

logger = logging.getLogger(__name__)

# Property name -> RDKit descriptor function. Matches the columns the original
# worker produced, so downstream steps (clustering, graph) stay compatible.
PROPERTIES: dict[str, Callable[[Any], float]] = {
    "mol_weight": Descriptors.MolWt,  # type: ignore[attr-defined]  # rdkit-stubs gap; exists at runtime
    "log_p": Descriptors.MolLogP,  # type: ignore[attr-defined]  # rdkit-stubs gap; exists at runtime
    "tpsa": Descriptors.TPSA,  # type: ignore[attr-defined]  # rdkit-stubs gap; exists at runtime
    "hba": rdMolDescriptors.CalcNumHBA,
    "hbd": rdMolDescriptors.CalcNumHBD,
    "rotatable_bonds": rdMolDescriptors.CalcNumRotatableBonds,
    "aromatic_rings": rdMolDescriptors.CalcNumAromaticRings,
}


def calculate_properties(
    rows: list[dict[str, Any]],
    smiles_column: str = "smiles",
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Compute descriptors for each row's SMILES.

    Returns ``(rows_with_properties, stats)``. Original columns are preserved
    and the property columns are appended. Rows whose SMILES cannot be parsed
    are skipped (chemistry-intrinsic validation; the "does the smiles column
    exist" check is intentionally left to a DQ task in the DAG).
    """
    result: list[dict[str, Any]] = []
    failed = 0

    for row in rows:
        smi = str(row.get(smiles_column, "")).strip()
        mol = Chem.MolFromSmiles(smi) if smi else None
        if mol is None:
            failed += 1
            continue
        props = {name: round(float(fn(mol)), 4) for name, fn in PROPERTIES.items()}
        result.append({**row, **props})

    stats = {"input": len(rows), "calculated": len(result), "failed": failed}
    logger.info("Properties calculation complete: %s", stats)
    return result, stats
