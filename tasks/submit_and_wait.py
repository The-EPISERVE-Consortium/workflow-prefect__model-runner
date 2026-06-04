import os
import time

from kubernetes import client, config as k8s_config
from prefect import task
from prefect.logging import get_run_logger

from tools.k8_tools import _check_for_stuck_pods, _collect_pod_logs
from tools.lakefs_helpers import LAKEFS_RUN_REPO, LAKEFS_BRANCH
from tools.sharding import shard_qid


@task
def submit_and_wait(run_id: str, model_image: str, model_tag: str, qid: str, namespace: str = "default"):
    """
    Submit a Kubernetes Job with the three-container pattern and wait for completion.

    Pod structure:
      init: lakefs-pull  → downloads qid/input/ from LakeFS → /work/input/
      init: model        → reads /work/input/, writes /work/output/
      container: lakefs-push → uploads /work/output/ → LakeFS qid/output/
    """
    logger = get_run_logger()

    k8s_config.load_incluster_config()
    batch_v1 = client.BatchV1Api()
    core_v1  = client.CoreV1Api()

    lakefs_run_path = f"lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH}/{shard_qid(qid)}"
    lakefs_host = os.environ["LAKEFS_HOST"]

    lakefs_env = [
        client.V1EnvVar(name="LAKECTL_SERVER_ENDPOINT_URL", value=lakefs_host),
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
                                f"lakectl fs download --recursive {lakefs_run_path}/components/input/ /work/input/"
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
                                f"lakectl fs upload --source /work/output -r {lakefs_run_path}/components/output/ && "
                                f"lakectl commit lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH} -m 'model run {run_id}'"
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

    logger.info(f"Submitting Kubernetes Job: {run_id}")
    batch_v1.create_namespaced_job(namespace=namespace, body=job)
    logger.info("Job submitted, waiting for completion...")

    while True:
        status = batch_v1.read_namespaced_job(name=run_id, namespace=namespace).status
        if status.succeeded:
            logger.info(f"Job {run_id} completed successfully")
            break
        if status.failed:
            pod_logs = _collect_pod_logs(core_v1, run_id, namespace)
            raise RuntimeError(
                f"Job {run_id} failed\n\nPod logs:\n{pod_logs}"
            )
        _check_for_stuck_pods(core_v1, run_id, namespace)
        time.sleep(5)
