# Cheminformatics Pipeline — Airflow DAG

An Apache Airflow 3 pipeline that turns scaffold + R-group CSV files into
clustered, property-enriched molecule sets for a drug-discovery
cheminformatics workflow — with automatic weekly discovery of new datasets,
data quality gates, and MS Teams notifications for scientists.

Scientists drop two files per dataset into S3 (or a local MinIO bucket in
dev):

```
<dataset_id>_scaffolds.csv   # column: smiles (scaffolds with [*:n] attachment points)
<dataset_id>_r_groups.csv    # one column per attachment position (e.g. R1, R2, ...)
```

The DAG discovers new dataset pairs, enumerates molecules from each
scaffold/R-group pair, computes physicochemical descriptors, clusters
molecules by property similarity (K-means), validates data quality at each
stage, and — as optional steps — builds a TMAP/Faerun similarity graph and
runs ChemProp property prediction if a trained checkpoint is available.

## Pipeline

```
discover_datasets -> [process_dataset (mapped, one per discovered dataset)] -> notify_summary
                          |
                          +-- check_inputs -> validate_inputs -> generate -> validate_generation
                                  -> calculate_properties -> cluster -> validate_clustering
                                                                          |-> build_graph        (optional)
                                                                          +-> predict_chemprop   (optional)
```

State is tracked entirely by Airflow (task statuses); there is no database
layer for pipeline state. Intermediate artifacts are read from / written to
S3 as CSV; only S3 keys are passed between tasks via XCom. A dataset is
considered "new" if `processed/<dataset_id>/clusters.csv` does not exist yet
(or unconditionally, if `overwrite=True`) — robust to manual re-runs and
missed schedule windows, without depending on `data_interval`/catchup
semantics.

Each `validate_*` task is a **data quality gate**: a pass-through that
returns its input unchanged, so the next real processing step has an
explicit dependency on the check having passed. A failed gate stops that
dataset's pipeline exactly like any other task failure — including
triggering a Teams notification.

| Stage | Module | Notes |
|---|---|---|
| 1. Molecule generation | `include/chem/molecules_generation.py` | RDKit `molzip`; combinatorial enumeration of scaffold × R-groups |
| 2. Property calculation | `include/chem/properties_calculation.py` | MolWt, LogP, TPSA, HBA, HBD, rotatable bonds, aromatic rings |
| 3. Clustering | `include/chem/molecules_clustering.py` | K-means over a standardized property vector; fixed `k` or auto-selected via silhouette score |
| Data quality gates | `include/chem/quality_checks.py` | SMILES validity ratio, scaffold/R-group attachment-point compatibility, minimum molecule yield, cluster balance |
| Notifications | `include/chem/notifications.py` | MS Teams (Power Automate Workflows webhook + Adaptive Cards); no-op if unconfigured |
| 4. ChemProp prediction *(optional)* | `include/chem/chemprop.py` | Wraps the `chemprop predict` CLI; gracefully skips with no checkpoint trained |
| 5. Similarity graph *(optional)* | `include/chem/graph_generation.py` | TMAP layout + Faerun plot; gracefully skips if `tmap`/`faerun` aren't installed |
| S3 I/O | `include/chem/s3_io.py` | Thin wrapper over Airflow's `S3Hook`; works against real AWS S3 or local MinIO via a Connection's `endpoint_url` |

All chemistry and quality-check modules are pure functions (`list[dict] →
list[dict]`, or `DataFrame → DataFrame`) with no S3/Airflow coupling, so
they're testable in isolation without Docker or a running Airflow instance.
`notifications.py` is likewise framework-agnostic — no Airflow import.

## Project status

All three iterations from the assignment are complete:

- ✅ **Iteration 1** — single-dataset run via the `dataset_id` param.
- ✅ **Iteration 2** — weekly schedule, `overwrite` param, automatic
  multi-dataset discovery via a mapped `@task_group`.
- ✅ **Iteration 3** — data quality gates between processing stages, MS
  Teams notifications (failure alerts + per-run summary).

All tested end-to-end: real `DagBag` import, real `S3Hook` against MinIO,
a deliberately broken dataset to confirm DQ gates actually block the
pipeline, real HTTP delivery of Teams notifications, mypy-clean, ruff-clean.

## Prerequisites

- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
- Docker Desktop (or compatible engine)

## Setup

```bash
git clone <this-repo-url>
cd <repo-folder>
cp .env.example .env   # adjust MinIO creds if you want non-default values
astro dev init         # only adds missing Astro scaffolding; won't touch dags/ or include/
astro dev start
```

This starts Airflow (webserver/API server, scheduler, triggerer, Postgres)
plus a local MinIO instance defined in `docker-compose.override.yml`.

- Airflow UI: printed in the terminal after `astro dev start` (default
  `admin` / `admin` unless overridden)
- MinIO console: `http://localhost:9001` (default `minioadmin` / `minioadmin`,
  or whatever you set in `.env`)

### One-time local setup

1. In the MinIO console, create a bucket named `pipeline-results`.
2. Upload the sample files from `sample_data/` (`demo_scaffolds.csv`,
   `demo_r_groups.csv`) into that bucket.
3. In the Airflow UI, go to **Admin → Connections** and create:
   - Connection Id: `aws_default`
   - Connection Type: `Amazon Web Services`
   - AWS Access Key ID: `minioadmin` (or your `.env` value)
   - AWS Secret Access Key: `minioadmin` (or your `.env` value)
   - Extra: `{"endpoint_url": "http://minio:9000"}`

   (`minio` — the Docker service name — not `localhost`: Airflow containers
   reach MinIO over the internal Docker network.)

### Run

The DAG runs automatically every Sunday at midnight (`@weekly`), discovering
and processing every new `<id>_scaffolds.csv` / `<id>_r_groups.csv` pair in
the bucket. For manual testing, trigger `chempipeline` with config:

```json
{
  "dataset_id": "",
  "bucket": "pipeline-results",
  "n_clusters": 3,
  "overwrite": true,
  "chemprop_checkpoint": ""
}
```

Leave `dataset_id` empty to discover and process every dataset in the
bucket, or set it to a specific id to target just one (useful for testing
a single dataset without waiting on a full bucket scan).

`check_inputs` through `cluster` (and their `validate_*` gates in between)
should complete successfully for a healthy dataset; `build_graph` and
`predict_chemprop` are expected to show as **skipped** unless you've
installed `tmap`/`faerun` or provided a real ChemProp checkpoint via
`CHEMPROP_CHECKPOINT_PATH` in `.env`.

To see a data quality gate actually block a dataset, upload a pair of files
with invalid SMILES (e.g. `bad_scaffolds.csv` containing `NOT_A_MOLECULE`)
and trigger with that `dataset_id` — `validate_inputs` will fail with a
`DataQualityError`, downstream tasks will show `upstream_failed`, and
`notify_summary` will still run (`trigger_rule="all_done"`).

## Configuration reference

| DAG param | Default | Description |
|---|---|---|
| `dataset_id` | `""` | Empty = discover and process every new dataset in the bucket. Set to restrict a run to one specific dataset id. |
| `bucket` | `pipeline-results` | S3/MinIO bucket to read inputs from and write outputs to |
| `n_clusters` | `5` | Number of K-means clusters |
| `overwrite` | `False` | If true, reprocess every discovered dataset even if it already has output |
| `chemprop_checkpoint` | from `CHEMPROP_CHECKPOINT_PATH` env var, else `""` | Path to a trained ChemProp checkpoint; empty = skip |

| `.env` variable | Purpose |
|---|---|
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | Local MinIO admin credentials |
| `CHEMPROP_CHECKPOINT_PATH` | Optional default for the `chemprop_checkpoint` param |
| `MS_TEAMS_WEBHOOK_URL` | Power Automate Workflows webhook URL. No webhook has been issued for this course yet — notifications are a no-op while this is empty. |

## Data quality gates

| Gate | Check | Threshold |
|---|---|---|
| `validate_inputs` | SMILES validity ratio in scaffolds and R-groups; at least one scaffold has attachment points matching the R-group column count | ≥ 80% valid SMILES |
| `validate_generation` | Enumeration produced at least this many molecules | ≥ 1 |
| `validate_clustering` | No single cluster dominates the result | largest cluster ≤ 90% of all molecules |

Each raises `DataQualityError` (`include/chem/quality_checks.py`) with a
report dict on failure, which propagates as a normal Airflow task failure.

## Testing

Chemistry and quality-check modules are pure functions and unit-testable
without Airflow or Docker:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt mypy ruff

mypy dags include --config-file mypy.ini
ruff check dags include --config ruff.toml
```

**Known gotcha:** `rdkit`'s bundled type stubs are broken in some releases
(as of writing, `2026.3.3` ships a `Chem/rdchem.pyi` with an invalid
parameter ordering, which fails mypy with a syntax error before any real
type-checking happens — unrelated to this project's code, and it does not
affect runtime behavior). If `mypy` fails with a `[syntax]` error pointing
into `rdkit-stubs`, downgrade rdkit for your local venv only:

```bash
pip install "rdkit==2025.9.6"
```

`mypy.ini` / `ruff.toml` deliberately silence a few other known third-party
stub gaps (RDKit functions that exist and work fine at runtime but aren't
declared in its stubs) — see inline comments in the affected modules for
specifics. `mypy.ini` also sets `explicit_package_bases = True` and
`mypy_path = .`: without `include/__init__.py` and `include/chem/__init__.py`
present (required — Python's implicit namespace packages let imports work
at runtime either way, but mypy needs explicit packages to resolve
`include.chem.*` consistently), mypy can report the same file "found twice
under different module names."

## Branching policy

This repo follows `feature -> dev -> prod`:

- `feature/*` — one branch per iteration/task, merged into `dev` via PR.
- `dev` — integration branch.
- `prod` — reflects the latest working, demoed state.

## Architecture notes

- **No database layer.** Earlier drafts of this pipeline (see project
  history) used Postgres to track task/artifact state across separately
  invoked worker containers. Since Airflow itself owns orchestration and
  state here, that layer was intentionally removed — this is also why there
  is no `schema.sql` in this repo.
- **S3Hook over boto3 directly.** Using Airflow's `S3Hook` means the same
  DAG code runs unchanged against local MinIO (dev) or real AWS S3
  (production) — only the Connection differs.
- **`include/` over `dags/` for shared code.** Airflow 3 on Astro isolates
  DAG bundles; bare imports of helper modules placed inside `dags/` break.
  `include/` is the officially supported, PYTHONPATH-safe location for
  shared non-DAG code.
- **"New" is defined by output existence, not timestamps.** `discover_datasets`
  treats a dataset as needing (re)processing based on whether
  `processed/<id>/clusters.csv` exists, not by comparing file modification
  times against the last scheduled run. This is deliberately more robust
  under manual re-triggers, missed schedule windows, and backfills.
- **Data quality gates are pass-through tasks, not side checks.** Each
  `validate_*` task returns its input unchanged so the next processing step
  has a real XCom dependency on it — guaranteeing sequential execution
  (check completes before the next stage starts) rather than an implicit
  ordering that Airflow might parallelize.
- **Notifications never break the pipeline.** `notify_failure` and
  `notify_run_summary` are no-ops when `MS_TEAMS_WEBHOOK_URL` is unset, and
  any network failure when posting is caught and logged, never raised.
