"""
Register the model_pipeline flow as a Prefect deployment.

Run once (or on every release) from inside the cluster or any machine that
can reach the Prefect server:

    PREFECT_API_URL=http://prefect-server.default.svc.cluster.local:4200/api \
        python deploy.py
"""

import os
from prefect.runner.storage import GitRepository
from flow.run_model import model_pipeline

GITHUB_REPO_URL = "https://github.com/The-EPISERVE-Consortium/workflow-prefect__model-runner"
DOCKER_IMAGE    = "ghcr.io/the-episerve-consortium/workflow-prefect__model-runner:main"
WORK_POOL_NAME  = os.getenv("WORK_POOL_NAME", "kubernetes-pool")
DEPLOYMENT_NAME = os.getenv("DEPLOYMENT_NAME", "model-runner")

if __name__ == "__main__":
    model_pipeline.from_source(
        source=GitRepository(url=GITHUB_REPO_URL, branch="main"),
        entrypoint="flow/run_model.py:model_pipeline",
    ).deploy(
        name=DEPLOYMENT_NAME,
        work_pool_name=WORK_POOL_NAME,
        job_variables={
            "image": DOCKER_IMAGE,
            "image_pull_policy": "Always",
        },
    )
