# workflow-prefect__model-runner

Prefect 3 flow that orchestrates epidemiological model containers on Kubernetes. It pins each input source to a lakeFS commit, stages the run config in lakeFS, submits a Kubernetes Job using a three-container pattern (pull → model → push), writes RO-Crate + FDO provenance metadata, and returns the lakeFS path to the output.

## Flow: `model_pipeline`

**File:** `flow/run_model.py`

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_data_files` | `list[[uri, filename]]` | *(required)* | List of `[source_uri, target_filename]` pairs. The source URI is either `lakefs://<repo>/<branch>/<path>` or a DOIP retrieve URL — see [Supported input URIs](#supported-input-uris). The target filename is what the model container sees under `/work/input/`. |
| `model_image` | `str` | *(required)* | Full GHCR image name, e.g. `ghcr.io/the-episerve-consortium/model__prediction__grippeweb__baseline-nullmodel` |
| `config_json` | `str` | *(required)* | JSON string written verbatim as `/work/input/config.json`, e.g. `{"horizon_weeks": 4, "n_reference_weeks": 4}` |
| `model_tag` | `str` | `latest` | Image tag |
| `namespace` | `str` | `default` | Kubernetes namespace in which the Job is created |
| `data_transformation_sql` | `list[str] \| null` | `null` | Optional per-file SQL filter applied in the pull container after download (runs against a DuckDB table called `df`), parallel list to `input_data_files` |

### Supported input URIs

The `lakefs-pull` init container (`lakectl-python/pull.py`) understands exactly two forms; anything else fails with `Unsupported source URI scheme`:

- `lakefs://<repo>/<branch>/<path>` — rewritten to `lakefs://<repo>/<commit_id>/<path>` using the commit pinned by `stage_input`
- `https://…/doip/retrieve/<QID>/<component>` — converted to a lakeFS URI at the pinned commit; the repositories are tried in order (`DOIP_LAKEFS_REPOS`, default `data-processed` then `benchmarks`, since a commit ID exists in one repository only). A DOIP URL with no resolved `?version=` commit is rejected.

Plain HTTP(S) URLs are not supported.

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

2. **`stage_input`** — Does not move any data. Resolves the source commit for each input (branch HEAD for `lakefs://` URIs, the `DOIP_LAKEFS_REPO`/`DOIP_LAKEFS_BRANCH` HEAD for DOIP URLs) and appends `?version=<commit_id>` to the URI, so the Job fetches exactly that commit rather than whatever HEAD is when the pod starts. Uploads `config.json` (verbatim) and `config_prefect.json` (the full Prefect flow run parameters, with the versioned URIs substituted in) to `model-runs/main/<pp>/<qq>/<rr>/<QID>/components/input/`, and returns the versioned `input_data_files`.

3. **`submit_and_wait`** — Submits a Kubernetes Job named `model-runner-<qid>` (lowercase) with three containers sharing an ephemeral `/work` volume:

   | Container | Type | Image | What it does |
   |---|---|---|---|
   | `lakefs-pull` | init | `ghcr.io/the-episerve-consortium/lakectl-python:latest` | Downloads `config.json`, then runs `pull.py` over the `PULL_SPEC` env var: fetches each pinned source file to `/work/input/<target_filename>` and applies its DuckDB SQL filter in place |
   | `model` | init | `<model_image>:<model_tag>` | Reads `/work/input/`, writes `/work/output/` |
   | `lakefs-push` | main | `treeverse/lakectl:latest` | Uploads `/work/output/` → `components/output/` and commits |

   Polls every 5 s until the Job succeeds or fails. Fails fast on `ImagePullBackOff` / `ErrImagePull` / `InvalidImageName`: the Job is then deleted (it would never finish on its own) and `run.log` starts with the reason. The Job has `backoff_limit=0`; a finished Job is removed by Kubernetes after 10 minutes.

   Whatever the outcome, the per-container pod logs (last 200 lines of each container) are uploaded to `components/output/run.log`. On failure the first log line starting with `ERROR:` is appended to the raised `RuntimeError`, so the model's own message is visible in the Prefect UI.

4. **`write_metadata`** (runs whether the Job succeeded or failed; not when `stage_input` itself fails) — Scans `components/input/` (plus `components/output/` on success) in lakeFS, then writes:
   - `components/ro-crate-metadata.json` — RO-Crate 1.1 + Workflow Run Process 0.4 record. The model is referenced as a `SoftwareApplication` under a model QID derived from `sha256(model_image)`, stable across runs of the same image.
   - `<QID>.fdo.json` — FAIR Digital Object descriptor. `kernel.fdo:hasComponent` lists every scanned file with its MIME type (the DOIP server only serves listed components; on a failed run `components/output/`, including `run.log`, is not listed). `provenance.prov:used` lists the pinned input sources, carrying the applied SQL as `schema:query`: a `lakefs://` input under `<QID>/components/` becomes a DOIP retrieve URL with `?version=<commit>`, any other `lakefs://` input (e.g. a benchmark's generated input file) keeps its lakeFS URI with `?version=<commit>`, and a DOIP URL input is kept as given.

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
- Exit non-zero on failure, printing `ERROR: <message>` — that line is surfaced in the Prefect failure message

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
| `DOIP_BASE_URL` | Base URL for DOIP links written into the FDO (default `https://doip.episerve.zib.de`) |
| `DOIP_LAKEFS_REPO` | lakeFS repo backing DOIP objects (default `data-processed`) |
| `DOIP_LAKEFS_REPOS` | comma-separated repos tried by the pull step for DOIP URLs (default: `DOIP_LAKEFS_REPO`, then `benchmarks`). This and `DOIP_LAKEFS_REPO` are passed on to the `lakefs-pull` container when set |
| `DOIP_LAKEFS_BRANCH` | Branch of that repo (default `main`) |
| `PREFECT_API_URL` | Prefect server (used by `deploy.py`) |
| `WORK_POOL_NAME` | Work pool for `deploy.py` (default `kubernetes-pool`) |
| `DEPLOYMENT_NAME` | Deployment name for `deploy.py` (default `model-runner`) |

Inside the Kubernetes Job the containers use `LAKECTL_SERVER_ENDPOINT_URL` plus `LAKECTL_CREDENTIALS_ACCESS_KEY_ID` / `LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY` from the `lakefs-credentials` Secret.

## Repository layout

```
flow/run_model.py           mint QID → stage_input → submit_and_wait → write_metadata
tasks/stage_input.py        resolve source commits, stage config.json + config_prefect.json
tasks/submit_and_wait.py    build/submit the K8s Job, poll, save run.log
tasks/write_metadata.py     RO-Crate + FDO
tools/k8_tools.py           stuck-pod detection, per-container log collection
tools/lakefs_helpers.py     lakeFS client, repo/branch constants
tools/sharding.py           QID → sharded path
lakectl-python/             image for the lakefs-pull init container (Dockerfile + pull.py)
```
