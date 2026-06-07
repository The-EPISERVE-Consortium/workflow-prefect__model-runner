# workflow-prefect__model-runner

Prefect 3 flow that orchestrates epidemiological model containers on Kubernetes. It stages input data in lakeFS, submits a Kubernetes Job using a three-container pattern (pull → model → push), writes RO-Crate + FDO provenance metadata, and returns the lakeFS path to the output.

## Flow: `model_pipeline`

**File:** `flow/run_model.py`

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_data_files` | `list[[uri, filename]]` | *(required)* | List of `[lakefs_uri, target_filename]` pairs. The target filename is what the model container sees under `/work/input/`. |
| `model_image` | `str` | *(required)* | Full GHCR image name, e.g. `ghcr.io/the-episerve-consortium/model__prediction__grippeweb__baseline-nullmodel` |
| `config_json` | `str` | *(required)* | JSON string written verbatim as `/work/input/config.json`, e.g. `{"horizon_weeks": 4, "n_reference_weeks": 4}` |
| `model_tag` | `str` | `latest` | Image tag |
| `namespace` | `str` | `default` | Kubernetes namespace in which the Job is created |
| `data_transformation_sql` | `list[str] \| null` | `null` | Optional per-file SQL filter applied before staging (runs against a DuckDB table called `df`), parallel list to `input_data_files` |

### Example parameter payload

```json
{
  "parameters": {
    "input_data_files": [
      [
        "lakefs://data-processed/main/32/74/12/Q3274128860531/components/GrippeWeb_Daten_des_Wochenberichts.parquet",
        "input.parquet"
      ]
    ],
    "model_image": "ghcr.io/the-episerve-consortium/model__prediction__grippeweb__baseline-nullmodel",
    "model_tag": "latest",
    "config_json": "{\"horizon_weeks\": 4, \"n_reference_weeks\": 4}",
    "data_transformation_sql": [
      "SELECT * FROM df WHERE saison = '25'"
    ]
  }
}
```

### What it does

1. **Mints a QID** of the form `Q<unix-timestamp><3-digit-random>`, e.g. `Q1748526042817`. Used as the run identifier and sharded into `pp/qq/rr/QID` paths in lakeFS.

2. **`stage_input`** — Downloads each input file (lakefs URI or HTTP URL), optionally filters it with DuckDB SQL, then uploads to `model-runs/main/<pp>/<qq>/<rr>/<QID>/components/input/<target_filename>`. Also uploads `config.json` and `config_prefect.json` (the full Prefect flow run parameters) to the same directory.

3. **`submit_and_wait`** — Submits a Kubernetes Job with three containers sharing an ephemeral `/work` volume:

   | Container | Type | What it does |
   |---|---|---|
   | `lakefs-pull` | init | Downloads `components/input/` from lakeFS → `/work/input/` |
   | `model` | init | Runs the model image; reads `/work/input/`, writes `/work/output/` |
   | `lakefs-push` | main | Uploads `/work/output/` → `components/output/` and commits |

   Polls until the Job succeeds or fails. Fails fast on `ImagePullBackOff` / `ErrImagePull` / `InvalidImageName`.

4. **`write_metadata`** (always runs, even on failure) — Scans `components/input/` and `components/output/` in lakeFS, then writes:
   - `components/ro-crate-metadata.json` — RO-Crate 1.1 + Workflow Run Provenance record
   - `<QID>.fdo.json` — FAIR Digital Object descriptor

### Output

The flow returns the lakeFS path to the output directory:

```
lakefs://model-runs/main/<pp>/<qq>/<rr>/<QID>/components/output/
```

### Model container contract

Each model image must:
- Read input files from `/work/input/` (filenames as specified in `input_data_files`)
- Read config from `/work/input/config.json`
- Write all output to `/work/output/`

The `baseline-nullmodel` specifically expects `/work/input/input.parquet` with columns `Erkrankung`, `Altersgruppe`, `Region`, `Kalenderwoche`, `Inzidenz`.

## Deployment

Register or update the flow with the Prefect server:

```bash
python -m venv --upgrade .venv && source .venv/bin/activate && pip install -r requirements.txt
```

```bash
PREFECT_API_URL=https://prefect.episerve.zib.de/api python deploy.py
```

This creates (or updates) a deployment named `model-runner` on the `kubernetes-pool` work pool.  
Override names via `WORK_POOL_NAME` and `DEPLOYMENT_NAME` env vars.

## Triggering manually via the Prefect UI

1. Open the Prefect UI → **Flows → model-pipeline**.
2. Click **Run → Custom run**.
3. Fill in `input_data_files`, `model_image`, `config_json` (and optionally `data_transformation_sql`).
4. Click **Run**.

## Environment variables (flow)

| Variable | Purpose |
|---|---|
| `LAKEFS_HOST` | lakeFS server URL |
| `LAKEFS_ACCESS_KEY` | lakeFS access key |
| `LAKEFS_SECRET_KEY` | lakeFS secret key |
| `PREFECT_API_URL` | Prefect server (used by `deploy.py`) |
