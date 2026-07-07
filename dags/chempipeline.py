"""Cheminformatics pipeline DAG.

Iteration 2: weekly schedule, automatic multi-dataset discovery, and an
`overwrite` param.

Flow:
    discover_datasets -> [process_dataset (mapped, one per discovered id)]
                              |
                              +-- check_inputs -> generate -> calculate_properties -> cluster
                                                                                        |-> build_graph        (optional)
                                                                                        +-> predict_chemprop   (optional)

Scientists drop two files per dataset into S3:
    <dataset_id>_scaffolds.csv   (column: smiles - scaffolds with [*:n] points)
    <dataset_id>_r_groups.csv    (one column per attachment position)

"New" is defined by output existence, not by file modification time: a
dataset is (re)processed if `processed/<dataset_id>/clusters.csv` does not
yet exist in the bucket, or if `overwrite=True` is passed at trigger time.
This is robust to manual re-runs, missed schedule windows, and backfills
without depending on `data_interval`/catchup semantics.

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
from airflow.sdk import Param, dag, get_current_context, task, task_group
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
SCAFFOLDS_SUFFIX = "_scaffolds.csv"
R_GROUPS_SUFFIX = "_r_groups.csv"


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
    schedule="@weekly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["cheminformatics"],
    default_args=DEFAULT_ARGS,
    params={
        "dataset_id": Param(
            "",
            type="string",
            title="Dataset id filter (optional)",
            description=(
                "Leave empty for normal weekly behavior: automatically discover "
                "every complete <id>_scaffolds.csv / <id>_r_groups.csv pair in the "
                "bucket. Set this to restrict a manual run to a single dataset id "
                "(still subject to the overwrite rule below)."
            ),
        ),
        "bucket": Param(DEFAULT_BUCKET, type="string", title="S3 bucket"),
        "n_clusters": Param(5, type="integer", minimum=2, title="K (number of clusters)"),
        "overwrite": Param(
            False,
            type="boolean",
            title="Overwrite existing outputs",
            description=(
                "If false (default), datasets that already have a "
                "processed/<id>/clusters.csv are skipped. If true, reprocess "
                "every discovered dataset regardless of existing output."
            ),
        ),
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
    def discover_datasets() -> list[str]:
        """Find dataset ids with a complete scaffold/R-group pair that need
        (re)processing.

        A dataset is a candidate if both `<id>_scaffolds.csv` and
        `<id>_r_groups.csv` exist. It is included in the result unless it was
        already processed (processed/<id>/clusters.csv exists) and
        `overwrite` is False.
        """
        params = get_current_context()["params"]
        bucket = str(params.get("bucket") or DEFAULT_BUCKET)
        overwrite = bool(params.get("overwrite") or False)
        dataset_filter = str(params.get("dataset_id") or "").strip()

        if dataset_filter:
            candidate_ids = {dataset_filter}
        else:
            keys = s3_io.list_keys(bucket, aws_conn_id=AWS_CONN_ID)
            scaffold_ids = {
                key[: -len(SCAFFOLDS_SUFFIX)] for key in keys if key.endswith(SCAFFOLDS_SUFFIX)
            }
            r_group_ids = {
                key[: -len(R_GROUPS_SUFFIX)] for key in keys if key.endswith(R_GROUPS_SUFFIX)
            }
            candidate_ids = scaffold_ids & r_group_ids

        to_process: list[str] = []
        skipped: list[str] = []
        for dataset_id in sorted(candidate_ids):
            already_done = s3_io.object_exists(
                bucket, _output_key(dataset_id, "clusters.csv"), AWS_CONN_ID
            )
            if already_done and not overwrite:
                skipped.append(dataset_id)
            else:
                to_process.append(dataset_id)

        log.info(
            "Discovery: %d dataset(s) to process %s; %d already processed and "
            "skipped %s (overwrite=%s).",
            len(to_process), to_process, len(skipped), skipped, overwrite,
        )
        return to_process

    @task
    def build_config(dataset_id: str) -> dict[str, Any]:
        params = get_current_context()["params"]
        cfg = {
            "dataset_id": dataset_id,
            "bucket": str(params.get("bucket") or DEFAULT_BUCKET),
            "n_clusters": int(params.get("n_clusters") or 5),
            "chemprop_checkpoint": str(params.get("chemprop_checkpoint") or "").strip(),
            "scaffolds_key": f"{dataset_id}{SCAFFOLDS_SUFFIX}",
            "r_groups_key": f"{dataset_id}{R_GROUPS_SUFFIX}",
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
    @task_group(group_id="process_dataset")
    def process_dataset(dataset_id: str) -> None:
        cfg = check_inputs(build_config(dataset_id))  # type: ignore[arg-type]
        molecules_key = generate(cfg)  # type: ignore[arg-type]
        properties_key = calculate_properties(molecules_key, cfg)  # type: ignore[arg-type]
        clusters_key = cluster(properties_key, cfg)  # type: ignore[arg-type]

        # optional branches off the clustered result
        build_graph(clusters_key, cfg)  # type: ignore[arg-type]
        predict_chemprop(clusters_key, cfg)  # type: ignore[arg-type]

    dataset_ids = discover_datasets()
    process_dataset.expand(dataset_id=dataset_ids)


chempipeline()
