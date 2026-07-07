# Cheminformatics Pipeline — Airflow DAG

An Apache Airflow 3 pipeline that turns scaffold + R-group CSV files into
clustered, property-enriched molecule sets for a drug-discovery
cheminformatics workflow.

Scientists drop two files per dataset into S3 (or a local MinIO bucket in
dev):

```
<dataset_id>_scaffolds.csv   # column: smiles (scaffolds with [*:n] attachment points)
<dataset_id>_r_groups.csv    # one column per attachment position (e.g. R1, R2, ...)
```

The DAG enumerates molecules from the scaffold/R-group pair, computes
physicochemical descriptors, clusters molecules by property similarity
(K-means), and — as optional steps — builds a TMAP/Faerun similarity graph
and runs ChemProp property prediction if a trained checkpoint is available.

## Pipeline

```
resolve_config → check_inputs → generate → calculate_properties → cluster
                                                                     ├── build_graph        (optional, skips if tmap/faerun unavailable)
                                                                     └── predict_chemprop    (optional, skips if no checkpoint configured)
```

State is tracked entirely by Airflow (task statuses); there is no database
layer for pipeline state. Intermediate artifacts are read from / written to
S3 as CSV; only S3 keys are passed between tasks via XCom.

| Stage | Module | Notes |
|---|---|---|
| 1. Molecule generation | `include/chem/molecules_generation.py` | RDKit `molzip`; combinatorial enumeration of scaffold × R-groups |
| 2. Property calculation | `include/chem/properties_calculation.py` | MolWt, LogP, TPSA, HBA, HBD, rotatable bonds, aromatic rings |
| 3. Clustering | `include/chem/molecules_clustering.py` | K-means over a standardized property vector; fixed `k` or auto-selected via silhouette score |
| 4. ChemProp prediction *(optional)* | `include/chem/chemprop.py` | Wraps the `chemprop predict` CLI; gracefully skips with no checkpoint trained |
| 5. Similarity graph *(optional)* | `include/chem/graph_generation.py` | TMAP layout + Faerun plot; gracefully skips if `tmap`/`faerun` aren't installed |
| S3 I/O | `include/chem/s3_io.py` | Thin wrapper over Airflow's `S3Hook`; works against real AWS S3 or local MinIO via a Connection's `endpoint_url` |

All chemistry modules are pure functions (`list[dict] → list[dict]`, or
`DataFrame → DataFrame`) with no S3/Airflow coupling, so they're testable in
isolation without Docker or a running Airflow instance.

## Project status

- ✅ **Iteration 1** — single-dataset run via the `dataset_id` param. Done,
  tested end-to-end (real `DagBag` import, real `S3Hook` against MinIO,
  mypy-clean).
- ⏳ **Iteration 2** — weekly schedule, `overwrite` param, dataset discovery
  in S3, dynamic task mapping across datasets.
- ⏳ **Iteration 3** — data quality checks, MS Teams notifications for
  scientists.

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

Trigger the `chempipeline` DAG with config:

```json
{
  "dataset_id": "demo",
  "bucket": "pipeline-results",
  "n_clusters": 3,
  "chemprop_checkpoint": ""
}
```

`check_inputs` through `cluster` should complete successfully;
`build_graph` and `predict_chemprop` are expected to show as **skipped**
unless you've installed `tmap`/`faerun` or provided a real ChemProp
checkpoint via `CHEMPROP_CHECKPOINT_PATH` in `.env`.

## Configuration reference

| DAG param | Default | Description |
|---|---|---|
| `dataset_id` | `""` (required) | Id prefix of the `<id>_scaffolds.csv` / `<id>_r_groups.csv` pair |
| `bucket` | `pipeline-results` | S3/MinIO bucket to read inputs from and write outputs to |
| `n_clusters` | `5` | Number of K-means clusters |
| `chemprop_checkpoint` | from `CHEMPROP_CHECKPOINT_PATH` env var, else `""` | Path to a trained ChemProp checkpoint; empty = skip |

| `.env` variable | Purpose |
|---|---|
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | Local MinIO admin credentials |
| `CHEMPROP_CHECKPOINT_PATH` | Optional default for the `chemprop_checkpoint` param |
| `MS_TEAMS_WEBHOOK_URL` | Reserved for Iteration 3 notifications; no-op while empty |

## Testing

Chemistry modules are pure functions and unit-testable without Airflow or
Docker:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt mypy
mypy dags include/chem --config-file mypy.ini
```

`mypy.ini` deliberately silences known third-party stub gaps in RDKit
(some `rdkit-stubs` releases ship broken or incomplete `.pyi` files for
functions that work fine at runtime) — see inline comments in the affected
modules for specifics.

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
