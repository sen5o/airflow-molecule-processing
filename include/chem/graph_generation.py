"""Molecular similarity graph (TMAP + Faerun) from a set of molecules.

Pure processing logic, extracted from the ``graph_generation`` worker.
Database / artifact coupling removed. S3 I/O is the DAG's responsibility.

Optional step: requires `tmap` and `faerun`, which need conda (see the
original worker's Dockerfile). Import errors are only raised when
``build_similarity_graph`` is actually called, so the rest of the pipeline
does not need these dependencies installed.

Unlike generation/properties/clustering, `generate_tmap_graph` still touches
the local filesystem (writes graph.html/graph.js, zips them). This is
intrinsic to how faerun's `Faerun.plot()` works — it writes files, it does
not return bytes. The DAG task is expected to read the returned path and
upload it to S3 itself, then clean up.
"""

from __future__ import annotations

import logging
import os
import zipfile
from collections.abc import Callable
from typing import Any

import pandas as pd
from rdkit import Chem

logger = logging.getLogger(__name__)

SUPPORTED_FINGERPRINTS = ("ECFP4", "ECFP6", "MACCS")

SMILES_ALIASES = {"smiles", "smile", "structure", "canonical_smiles"}
MOL_ID_ALIASES = {"molecule id", "molecule_id", "mol_id", "compound_id", "name", "id"}


def get_fingerprint_function(fp_type: str) -> Callable[[Any], Any | None]:
    """Returns a safe RDKit fingerprint function based on the requested type."""
    from rdkit.Chem import AllChem, MACCSkeys

    factories: dict[str, Callable[[Any], Any]] = {
        "ECFP4": lambda mol: AllChem.GetMorganFingerprintAsBitVect(mol, radius=2, nBits=2048),  # type: ignore[attr-defined]
        "ECFP6": lambda mol: AllChem.GetMorganFingerprintAsBitVect(mol, radius=3, nBits=2048),  # type: ignore[attr-defined]
        "MACCS": lambda mol: MACCSkeys.GenMACCSKeys(mol),  # type: ignore[attr-defined]
    }
    if fp_type not in factories:
        raise ValueError(
            f"Unsupported fingerprint type: '{fp_type}'. "
            f"Available types: {', '.join(factories.keys())}"
        )
    base_func = factories[fp_type]

    def safe_func(mol: Any) -> Any | None:
        try:
            return base_func(mol)
        except Exception as e:  # noqa: BLE001 - one bad molecule must not kill the batch
            logger.warning("Failed to generate fingerprint: %s", e)
            return None

    return safe_func


def _find_column(df: pd.DataFrame, aliases: set[str]) -> str | None:
    for col in df.columns:
        if str(col) in aliases:
            return str(col)
    return None


def process_molecules_file(df: pd.DataFrame, fp_type: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalizes columns, separates valid/invalid molecules, computes fingerprints.

    Kept validations (chemistry-intrinsic): a 'smiles' column must exist
    (under one of its known aliases); rows with unparseable SMILES or
    failed fingerprint calculation are routed to `invalid_df` instead of
    being fatal for the whole batch.
    """
    df = df.copy()
    df.columns = df.columns.str.lower().str.strip()

    smiles_col = _find_column(df, SMILES_ALIASES)
    mol_id_col = _find_column(df, MOL_ID_ALIASES)

    if smiles_col is None:
        raise ValueError("The input file must contain a 'smiles' column.")
    if smiles_col != "smiles":
        df.rename(columns={smiles_col: "smiles"}, inplace=True)

    if mol_id_col is None:
        df["molecule id"] = [f"mol_{i}" for i in range(len(df))]
    elif mol_id_col != "molecule id":
        df.rename(columns={mol_id_col: "molecule id"}, inplace=True)

    def parse_smile(smile: Any) -> Any | None:
        if not isinstance(smile, str) or not smile.strip():
            return None
        try:
            return Chem.MolFromSmiles(smile)
        except Exception:  # noqa: BLE001
            return None

    df["rdkit_mol"] = df["smiles"].apply(parse_smile)
    valid_mask = df["rdkit_mol"].notnull()
    valid_df = df[valid_mask].copy()
    invalid_df = df[~valid_mask].copy()

    if not valid_df.empty:
        fp_func = get_fingerprint_function(fp_type)
        valid_df["fingerprint"] = valid_df["rdkit_mol"].apply(fp_func)
        failed_fp_mask = valid_df["fingerprint"].isnull()
        if failed_fp_mask.any():
            invalid_df = pd.concat([invalid_df, valid_df[failed_fp_mask]])
            valid_df = valid_df[~failed_fp_mask]

    valid_df.drop(columns=["rdkit_mol"], errors="ignore", inplace=True)
    invalid_df.drop(columns=["rdkit_mol"], errors="ignore", inplace=True)

    if valid_df.empty:
        raise RuntimeError("No valid molecules found in the dataset to build a graph.")

    return valid_df, invalid_df


def generate_tmap_graph(valid_df: pd.DataFrame, output_prefix: str = "molecules_graph") -> str:
    """Builds a TMAP layout + Faerun scatter/tree plot, zips html+js, returns the zip path.

    Any numeric or categorical column other than smiles/molecule id/fingerprint
    becomes a color layer (e.g. a "cluster" column from clustering.py will show
    up as a selectable legend entry automatically).
    """
    import tmap as tm
    from faerun import Faerun

    logger.info("Starting TMAP graph generation...")

    try:
        sample_fp = valid_df["fingerprint"].iloc[0]
        fp_size = len(sample_fp)

        sparse_fps = [tm.VectorUint(fp.GetOnBits()) for fp in valid_df["fingerprint"]]
        enc = tm.Minhash(fp_size)
        lf = tm.LSHForest(fp_size, 128)
        lf.batch_add(enc.batch_from_sparse_binary_array(sparse_fps))
        lf.index()

        cfg = tm.LayoutConfiguration()
        x, y, s, t, _ = tm.layout_from_lsh_forest(lf, cfg)

        exclude_cols = {"smiles", "molecule id", "fingerprint"}
        prop_cols = [col for col in valid_df.columns if col not in exclude_cols]

        color_data = []
        series_titles = []
        categoricals = []

        for col in prop_cols:
            if pd.api.types.is_numeric_dtype(valid_df[col]):
                valid_df[col] = valid_df[col].fillna(0)
                color_data.append([float(val) for val in valid_df[col].tolist()])
                categoricals.append(False)
            else:
                valid_df[col] = valid_df[col].fillna("N/A").astype(str)
                unique_vals = valid_df[col].unique()
                val_to_idx = {val: i for i, val in enumerate(unique_vals)}
                mapped_vals = valid_df[col].map(val_to_idx).tolist()
                color_data.append([int(val) for val in mapped_vals])
                categoricals.append(True)
            series_titles.append(col)

        if not color_data:
            color_data = [[0] * len(valid_df)]
            series_titles = ["Default Map"]
            categoricals = [False]

        def escape_label(s: str) -> str:
            return s.replace("\\", "").replace('"', "").replace("'", "")

        labels_series = (
            valid_df["smiles"].astype(str).apply(escape_label)
            + "__ID: " + valid_df["molecule id"].astype(str).apply(escape_label)
        )
        for col in prop_cols:
            labels_series += "__" + col + ": " + valid_df[col].astype(str).apply(escape_label)
        labels = labels_series.tolist()

        f = Faerun(view="front", coords=False, title="Molecules Map", clear_color="#ffffff")
        f.add_scatter(
            "molecules",
            {"x": x, "y": y, "c": color_data, "labels": labels},
            colormap=["rainbow"] * len(color_data),
            point_scale=1.5,
            categorical=categoricals,
            series_title=series_titles,
            has_legend=True,
        )
        f.add_tree(
            "molecules_tree",
            {"from": s, "to": t},
            point_helper="molecules",
            color="#cccccc",
        )
        f.plot(output_prefix, template="smiles")

        html_file = f"{output_prefix}.html"
        js_file = f"{output_prefix}.js"
        archive_name = f"{output_prefix}.zip"

        with zipfile.ZipFile(archive_name, "w", zipfile.ZIP_DEFLATED) as zipf:
            if os.path.exists(html_file):
                zipf.write(html_file, arcname=os.path.basename(html_file))
            if os.path.exists(js_file):
                zipf.write(js_file, arcname=os.path.basename(js_file))

        if os.path.exists(html_file):
            os.remove(html_file)
        if os.path.exists(js_file):
            os.remove(js_file)

        logger.info("Graph successfully generated and zipped into %s", archive_name)
        return archive_name

    except Exception as e:
        logger.error("Error during graph generation: %s", e)
        raise RuntimeError(f"Failed to generate TMAP graph: {e}") from e


def build_similarity_graph(
    df: pd.DataFrame,
    fingerprint: str = "ECFP4",
    output_prefix: str = "molecules_graph",
) -> tuple[str, pd.DataFrame]:
    """End-to-end: validate/fingerprint molecules, build graph, return (zip_path, invalid_df).

    Convenience wrapper mirroring what the original worker's `run()` did,
    minus the DB/S3 parts. The caller (a DAG task) is expected to upload the
    returned zip and, if `invalid_df` is non-empty, the invalid rows too.
    """
    valid_df, invalid_df = process_molecules_file(df, fingerprint)
    archive_path = generate_tmap_graph(valid_df, output_prefix=output_prefix)
    return archive_path, invalid_df
