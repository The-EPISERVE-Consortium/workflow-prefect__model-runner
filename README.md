# workflow-prefect__model-runner

Prefect flow that orchestrates epidemiological model containers on Kubernetes. It stages input data in LakeFS, submits a Kubernetes Job using a three-container pattern (pull → model → push), and returns the LakeFS path to the output once the Job completes.

## Flow: `model_pipeline`

**File:** `flows/run_model.py`

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_path` | `str` | *(required)* | LakeFS path to the input TSV, e.g. `lakefs://data-raw/main/grippeweb/grippeweb-2026-W20.tsv` |
| `model_image` | `str` | *(required)* | GHCR image name, e.g. `ghcr.io/the-episerve-consortium/model__prediction__grippeweb__baseline-nullmodel` |
| `model_config` | `str` | *(required)* | JSON string written verbatim as `config.json` in the input directory, e.g. `{"horizon_weeks": 4, "n_reference_weeks": 4}` |
| `model_tag` | `str` | `latest` | Image tag |
| `namespace` | `str` | `default` | Kubernetes namespace in which the Job is created |

### What it does

1. Generates a unique `run_id` of the form `<model-slug>-<YYYYMMDD-HHMMSS>`.
2. Copies the input TSV from `data-raw` and writes `config.json` to `lakefs://model-runs/main/<run-id>/input/`.
3. Submits a Kubernetes Job with three containers sharing an ephemeral `/work` volume:
   - **init `lakefs-pull`** — downloads `/input/` from LakeFS into `/work/input/`
   - **init `model`** — runs the prediction container, reads `/work/input/`, writes `/work/output/`
   - **`lakefs-push`** — uploads `/work/output/` to `lakefs://model-runs/main/<run-id>/output/`
4. Polls until the Job succeeds, then returns the output path.

Output is available at:
```
lakefs://model-runs/main/<run-id>/output/predictions.tsv
```

## Deployment

Register the flow with the Prefect server by running `deploy.py` from any machine that can reach the server:

```bash
PREFECT_API_URL=http://prefect-server.default.svc.cluster.local:4200/api \
    python deploy.py
```

This creates (or updates) a deployment named `model-runner` on the `kubernetes-pool` work pool, pointing at the `main` branch of this repo. Both names can be overridden via `WORK_POOL_NAME` and `DEPLOYMENT_NAME` environment variables.

Re-run `deploy.py` whenever you want to update the deployment (e.g. after changing default parameters or the image tag).

## Triggering manually via the Prefect UI

1. Open the Prefect UI and navigate to **Flows → model-pipeline**.
2. Click **Run → Custom run**.
3. Fill in the parameters (at minimum `input_path`).
4. Click **Run** — the flow run appears in the dashboard with its run ID, parameters, and live logs.

## Prefect server

```
prefect-server.default.svc.cluster.local:4200
```

## Secrets

The Kubernetes secret `lakefs-credentials` must exist in the target namespace with keys `lakefs-access-key` and `lakefs-secret-key`.
