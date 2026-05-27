from datetime import datetime
from prefect import flow, task
from kubernetes import client, config as k8s_config
import lakefs_sdk


LAKEFS_ENDPOINT  = "https://lake-episerve.zib.de/"
LAKEFS_DATA_REPO = "data-raw"
LAKEFS_RUN_REPO  = "model-runs"
LAKEFS_BRANCH    = "main"


def lakefs_client() -> lakefs_sdk.ApiClient:
    cfg = lakefs_sdk.Configuration(host=LAKEFS_ENDPOINT)
    # credentials injected via env: LAKEFS_ACCESS_KEY_ID, LAKEFS_SECRET_ACCESS_KEY
    return lakefs_sdk.ApiClient(cfg)


@task
def stage_input(input_path: str, config: dict, run_id: str):
    """
    Copy the input file from data-raw and write config.json into the run path.

    input_path: e.g. lakefs://data-raw/main/grippeweb/grippeweb-2026-W20.tsv
    Writes to:
      lakefs://model-runs/main/<run-id>/input/data.tsv
      lakefs://model-runs/main/<run-id>/input/config.json
    """
    import json
    with lakefs_sdk.ApiClient() as api:
        objects_api = lakefs_sdk.ObjectsApi(api)

        # read source file from data-raw
        # parse lakefs://data-raw/main/grippeweb/grippeweb-2026-W20.tsv
        _, _, path = input_path.replace("lakefs://", "").split("/", 2)
        data = objects_api.get_object(LAKEFS_DATA_REPO, LAKEFS_BRANCH, path)

        # write data.tsv to model-runs
        objects_api.upload_object(
            LAKEFS_RUN_REPO, LAKEFS_BRANCH,
            f"{run_id}/input/data.tsv",
            content=data,
        )

        # write config.json to model-runs
        objects_api.upload_object(
            LAKEFS_RUN_REPO, LAKEFS_BRANCH,
            f"{run_id}/input/config.json",
            content=json.dumps(config).encode(),
        )


@task
def submit_and_wait(run_id: str, model_image: str, model_tag: str, namespace: str = "default"):
    """
    Submit a Kubernetes Job with the three-container pattern and wait for completion.

    Pod structure:
      init: lakefs-pull  → downloads run_id/input/ from LakeFS → /work/input/
      init: model        → reads /work/input/, writes /work/output/
      container: lakefs-push → uploads /work/output/ → LakeFS run_id/output/
    """
    k8s_config.load_incluster_config()
    batch_v1 = client.BatchV1Api()

    lakefs_run_path = f"lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH}/{run_id}"

    lakefs_env = [
        client.V1EnvVar(name="LAKECTL_SERVER_ENDPOINT_URL", value=LAKEFS_ENDPOINT),
        client.V1EnvVar(
            name="LAKECTL_CREDENTIALS_ACCESS_KEY_ID",
            value_from=client.V1EnvVarSource(
                secret_key_ref=client.V1SecretKeySelector(
                    name="lakefs-credentials", key="lakefs-access-key"
                )
            ),
        ),
        client.V1EnvVar(
            name="LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY",
            value_from=client.V1EnvVarSource(
                secret_key_ref=client.V1SecretKeySelector(
                    name="lakefs-credentials", key="lakefs-secret-key"
                )
            ),
        ),
    ]

    workdir_mount = client.V1VolumeMount(name="workdir", mount_path="/work")

    job = client.V1Job(
        metadata=client.V1ObjectMeta(name=run_id, namespace=namespace),
        spec=client.V1JobSpec(
            backoff_limit=0,
            ttl_seconds_after_finished=600,
            template=client.V1PodTemplateSpec(
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    init_containers=[
                        client.V1Container(
                            name="lakefs-pull",
                            image="treeverse/lakectl:latest",
                            command=["/bin/sh", "-c"],
                            args=[
                                f"mkdir -p /work/input /work/output && "
                                f"lakectl fs cp --recursive {lakefs_run_path}/input/ /work/input/"
                            ],
                            env=lakefs_env,
                            volume_mounts=[workdir_mount],
                        ),
                        client.V1Container(
                            name="model",
                            image=f"{model_image}:{model_tag}",
                            volume_mounts=[workdir_mount],
                        ),
                    ],
                    containers=[
                        client.V1Container(
                            name="lakefs-push",
                            image="treeverse/lakectl:latest",
                            command=["/bin/sh", "-c"],
                            args=[
                                f"lakectl fs cp --recursive "
                                f"/work/output/ {lakefs_run_path}/output/"
                            ],
                            env=lakefs_env,
                            volume_mounts=[workdir_mount],
                        ),
                    ],
                    volumes=[
                        client.V1Volume(
                            name="workdir",
                            empty_dir=client.V1EmptyDirVolumeSource(),
                        )
                    ],
                )
            ),
        ),
    )

    batch_v1.create_namespaced_job(namespace=namespace, body=job)

    # wait for completion
    import time
    while True:
        status = batch_v1.read_namespaced_job(name=run_id, namespace=namespace).status
        if status.succeeded:
            break
        if status.failed:
            raise RuntimeError(f"Job {run_id} failed")
        time.sleep(5)


@flow
def model_pipeline(
    input_path: str,
    model_image: str = "ghcr.io/the-episerve-consortium/model__prediction__grippeweb__baseline-nullmodel",
    model_tag: str = "latest",
    horizon_weeks: int = 4,
    n_reference_weeks: int = 4,
    namespace: str = "default",
):
    """
    Run a model container on Kubernetes, reading input from LakeFS and
    writing output back to LakeFS.

    Args:
        input_path:        LakeFS path to the input TSV,
                           e.g. lakefs://data-raw/main/grippeweb/grippeweb-2026-W20.tsv
        model_image:       GHCR image name
        model_tag:         Image tag
        horizon_weeks:     Forecast horizon passed to the model via config.json
        n_reference_weeks: Reference window passed to the model via config.json
        namespace:         Kubernetes namespace
    """
    run_id = f"{model_image.split('/')[-1]}-{datetime.now():%Y%m%d-%H%M%S}"

    config = {
        "horizon_weeks": horizon_weeks,
        "n_reference_weeks": n_reference_weeks,
    }

    stage_input(input_path=input_path, config=config, run_id=run_id)
    submit_and_wait(
        run_id=run_id,
        model_image=model_image,
        model_tag=model_tag,
        namespace=namespace,
    )

    return f"lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH}/{run_id}/output/"
