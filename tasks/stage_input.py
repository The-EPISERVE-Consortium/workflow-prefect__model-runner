import json
import os

import lakefs
from prefect import task
from prefect.logging import get_run_logger

from tools.lakefs_helpers import LAKEFS_RUN_REPO, LAKEFS_BRANCH, lakefs_client
from tools.sharding import shard_qid

DOIP_LAKEFS_REPO = os.getenv("DOIP_LAKEFS_REPO", "data-processed")
DOIP_LAKEFS_BRANCH = os.getenv("DOIP_LAKEFS_BRANCH", "main")


def _resolve_doip_commit_via_lakefs(lc, logger) -> str | None:
    """Return the HEAD commit ID of the lakeFS branch that backs DOIP objects."""
    try:
        branch_handle = lakefs.repository(DOIP_LAKEFS_REPO, client=lc).branch(DOIP_LAKEFS_BRANCH)
        commit_id = branch_handle.head.id
        logger.info(f"Resolved DOIP source HEAD commit ({DOIP_LAKEFS_REPO}/{DOIP_LAKEFS_BRANCH}): {commit_id}")
        return commit_id
    except Exception as e:
        logger.warning(f"Could not resolve HEAD commit for {DOIP_LAKEFS_REPO}/{DOIP_LAKEFS_BRANCH}: {e}")
        return None


@task
def stage_input(
    input_data_files: list[list[str]],
    config_json: str,
    prefect_payload_json: str,
    qid: str,
    data_transformation_sql: list[str] | None = None,
) -> list[list[str]]:
    """
    Resolve source commit IDs and stage config files to lakeFS.

    Data files are not downloaded or uploaded here — the lakefs-pull init container
    fetches them directly from their versioned source URLs at job runtime.

    Returns:
      versioned_input_data_files: each source URL has ?version=<commit_id> appended
      where a commit ID was resolved. The commit IDs are embedded in the URLs.
    """
    logger = get_run_logger()
    lc = lakefs_client()
    dst_prefix = f"{shard_qid(qid)}/components/input"
    dst_branch_handle = lakefs.repository(LAKEFS_RUN_REPO, client=lc).branch(LAKEFS_BRANCH)
    commit_ids: list[str | None] = []

    for src_uri, _ in input_data_files:
        logger.info(f"Resolving commit ID for: {src_uri}")
        try:
            if src_uri.startswith("lakefs://"):
                src_repo, src_branch, _ = src_uri[len("lakefs://"):].split("/", 2)
                src_branch_handle = lakefs.repository(src_repo, client=lc).branch(src_branch)
                commit_id = src_branch_handle.head.id
                commit_ids.append(commit_id)
                logger.info(f"Source branch HEAD commit: {commit_id}")
            elif "/doip/retrieve/" in src_uri:
                commit_ids.append(_resolve_doip_commit_via_lakefs(lc, logger))
            else:
                commit_ids.append(None)
        except Exception as e:
            logger.warning(f"Could not resolve commit ID for {src_uri}: {e}")
            commit_ids.append(None)

    versioned_input_data_files = []
    for i, (src_uri, filename) in enumerate(input_data_files):
        commit_id = commit_ids[i] if i < len(commit_ids) else None
        if commit_id and "?" not in src_uri:
            src_uri = f"{src_uri}?version={commit_id}"
        versioned_input_data_files.append([src_uri, filename])

    dst_config = f"{dst_prefix}/config.json"
    logger.info(f"Staging config.json -> lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH}/{dst_config}")
    try:
        dst_branch_handle.object(dst_config).upload(
            data=config_json.encode(), content_type="application/json"
        )
        logger.info("config.json staged successfully")
    except Exception as e:
        raise RuntimeError(f"Failed to stage config.json to {dst_config}") from e

    try:
        payload = json.loads(prefect_payload_json)
        payload["input_data_files"] = versioned_input_data_files
        prefect_payload_json = json.dumps(payload)
    except Exception as e:
        logger.warning(f"Could not enrich config_prefect.json with commit IDs: {e}")

    dst_prefect = f"{dst_prefix}/config_prefect.json"
    logger.info(f"Staging config_prefect.json -> lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH}/{dst_prefect}")
    try:
        dst_branch_handle.object(dst_prefect).upload(
            data=prefect_payload_json.encode(), content_type="application/json"
        )
        logger.info("config_prefect.json staged successfully")
    except Exception as e:
        raise RuntimeError(f"Failed to stage config_prefect.json to {dst_prefect}") from e

    return versioned_input_data_files
