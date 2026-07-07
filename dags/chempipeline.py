"""Cheminformatics pipeline DAG.

Iteration 1: process a single dataset identified by the `dataset_id` param.

Flow (per dataset):
    check_inputs -> generate -> calculate_properties -> cluster
                                                          |-> build_graph   (optional)
                                                          |-> predict_chemprop (optional)

Scientists drop two files per dataset into S3:
    <dataset_id>_scaffolds.csv   (column: smiles - scaffolds with [*:n] points)
    <dataset_id>_r_groups.csv    (one column per attachment position)

Intermediate artifacts are written back to S3 under processed/<dataset_id>/.
State is tracked by Airflow (task statuses); no database is used. All chemistry
lives in include/chem/* as pure functions; this DAG only orchestrates + does S3 I/O.
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import Any

import pendulum
from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from include.chem import (
    chemprop,
    graph_generation,
    molecules_clustering,
    molecules_generation,
    properties_calculation,
    s3_io,
)

log = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Configuration (overridable via DAG params at trigger time)
# --------------------------------------------------------------------------- #
DEFAULT_BUCKET = "pipeline-results"
AWS_CONN_ID = "aws_default"
MAX_MOLECULES = 50_000
FINGERPRINT = "ECFP4"


def _output_key(dataset_id: str, name: str) -> str:
    return f"processed/{dataset_id}/{name}"


def _extract_smiles(row: dict[str, Any]) -> str:
    """Case-insensitive lookup of a 'smiles' cell in a scaffold row."""
    for key, value in row.items():
        if str(key).strip().lower() == "smiles":
            return str(value or "").strip()
    return ""


DEFAULT_ARGS = {
    "owner": "cheminformatics-pipeline",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=2),
}


@dag(
    dag_id="chempipeline",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["cheminformatics"],
    default_args=DEFAULT_ARGS,
    params={
        "dataset_id": Param(
            "",
            type="string",
            title="Dataset id",
            description="Id prefix of the <id>_scaffolds.csv / <id>_r_groups.csv pair in S3. Required.",
        ),
        "bucket": Param(DEFAULT_BUCKET, type="string", title="S3 bucket"),
        "n_clusters": Param(5, type="integer", minimum=2, title="K (number of clusters)"),
        "chemprop_checkpoint": Param(
            os.environ.get("CHEMPROP_CHECKPOINT_PATH", ""),
            type="string",
            title="ChemProp checkpoint path (optional)",
            description="Leave empty to skip the optional ChemProp prediction step.",
        ),
    },
)
def chempipeline() -> None:
    @task
    def resolve_config() -> dict[str, Any]:
        params = get_current_context()["params"]
        dataset_id = str(params.get("dataset_id") or "").strip()
        if not dataset_id:
            raise ValueError("Parameter 'dataset_id' is required.")
        cfg = {
            "dataset_id": dataset_id,
            "bucket": str(params.get("bucket") or DEFAULT_BUCKET),
            "n_clusters": int(params.get("n_clusters") or 5),
            "chemprop_checkpoint": str(params.get("chemprop_checkpoint") or "").strip(),
            "scaffolds_key": f"{dataset_id}_scaffolds.csv",
            "r_groups_key": f"{dataset_id}_r_groups.csv",
        }
        log.info("Resolved config: %s", cfg)
        return cfg

    @task
    def check_inputs(cfg: dict[str, Any]) -> dict[str, Any]:
        for key in (cfg["scaffolds_key"], cfg["r_groups_key"]):
            if not s3_io.object_exists(cfg["bucket"], key, AWS_CONN_ID):
                raise FileNotFoundError(f"Missing input: s3://{cfg['bucket']}/{key}")
        log.info("Both input files present for dataset '%s'.", cfg["dataset_id"])
        return cfg

    @task
    def generate(cfg: dict[str, Any]) -> str:
        scaffold_rows = s3_io.read_rows(cfg["bucket"], cfg["scaffolds_key"], AWS_CONN_ID)
        r_groups = molecules_generation.load_r_groups(
            s3_io.read_text(cfg["bucket"], cfg["r_groups_key"], AWS_CONN_ID)
        )

        all_rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        for scaffold_row in scaffold_rows:
            scaffold = _extract_smiles(scaffold_row)
            if not scaffold:
                continue
            try:
                rows, stats = molecules_generation.generate_molecules(scaffold, r_groups, MAX_MOLECULES)
            except ValueError as exc:
                # e.g. attachment-point / R-group-column mismatch for this scaffold
                log.warning("Skipping scaffold %r: %s", scaffold, exc)
                continue
            for row in rows:
                if row["smiles"] not in seen:
                    seen.add(row["smiles"])
                    all_rows.append(row)

        if not all_rows:
            raise ValueError("Generation produced 0 molecules for this dataset.")

        out_key = _output_key(cfg["dataset_id"], "molecules.csv")
        s3_io.write_rows(all_rows, cfg["bucket"], out_key, AWS_CONN_ID)
        log.info("Generated %d unique molecule(s) -> %s", len(all_rows), out_key)
        return out_key

    @task
    def calculate_properties(molecules_key: str, cfg: dict[str, Any]) -> str:
        rows = s3_io.read_rows(cfg["bucket"], molecules_key, AWS_CONN_ID)
        enriched, _stats = properties_calculation.calculate_properties(rows)
        out_key = _output_key(cfg["dataset_id"], "properties.csv")
        s3_io.write_rows(enriched, cfg["bucket"], out_key, AWS_CONN_ID)
        return out_key

    @task
    def cluster(properties_key: str, cfg: dict[str, Any]) -> str:
        rows = s3_io.read_rows(cfg["bucket"], properties_key, AWS_CONN_ID)
        clustered, _stats = molecules_clustering.cluster_molecules(rows, n_clusters=cfg["n_clusters"])
        out_key = _output_key(cfg["dataset_id"], "clusters.csv")
        s3_io.write_rows(clustered, cfg["bucket"], out_key, AWS_CONN_ID)
        return out_key

    @task
    def build_graph(clusters_key: str, cfg: dict[str, Any]) -> str:
        """Optional Faerun/TMAP graph. Skips gracefully if tmap/faerun aren't installed."""
        df = s3_io.read_df(cfg["bucket"], clusters_key, AWS_CONN_ID)
        with tempfile.TemporaryDirectory() as tmp:
            prefix = os.path.join(tmp, "molecules_graph")
            try:
                archive_path, _invalid = graph_generation.build_similarity_graph(df, FINGERPRINT, prefix)
            except ImportError as exc:
                raise AirflowSkipException(
                    f"tmap/faerun not installed; skipping graph: {exc}"
                ) from exc
            with open(archive_path, "rb") as fh:
                data = fh.read()
        out_key = _output_key(cfg["dataset_id"], "graph.zip")
        s3_io.write_bytes(data, cfg["bucket"], out_key, AWS_CONN_ID)
        return out_key

    @task
    def predict_chemprop(clusters_key: str, cfg: dict[str, Any]) -> str:
        """Optional ChemProp prediction. Skips gracefully if no checkpoint is configured."""
        checkpoint = cfg["chemprop_checkpoint"] or None
        if not chemprop.is_configured(checkpoint):
            raise AirflowSkipException("No ChemProp checkpoint configured; skipping prediction.")
        rows = s3_io.read_rows(cfg["bucket"], clusters_key, AWS_CONN_ID)
        predicted, _stats = chemprop.predict_properties(rows, checkpoint)
        out_key = _output_key(cfg["dataset_id"], "predictions.csv")
        s3_io.write_rows(predicted, cfg["bucket"], out_key, AWS_CONN_ID)
        return out_key

    # NOTE: mypy flags these as "XComArg" vs the annotated dict/str types below.
    # This is a known, long-standing Airflow/mypy limitation (apache/airflow#39514):
    # calling a @task function at DAG-authoring time returns an XComArg proxy for
    # the future value, not the literal annotated return type, and there is no
    # first-party mypy plugin that resolves this. Ignored deliberately, not a bug.
    cfg = check_inputs(resolve_config())  # type: ignore[arg-type]
    molecules_key = generate(cfg)  # type: ignore[arg-type]
    properties_key = calculate_properties(molecules_key, cfg)  # type: ignore[arg-type]
    clusters_key = cluster(properties_key, cfg)  # type: ignore[arg-type]

    # optional branches off the clustered result
    build_graph(clusters_key, cfg)  # type: ignore[arg-type]
    predict_chemprop(clusters_key, cfg)  # type: ignore[arg-type]


chempipeline()
