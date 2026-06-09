import io
import os
import re

import duckdb
import lakefs
import pandas as pd
import requests
from prefect import task
from prefect.logging import get_run_logger

from tools.lakefs_helpers import LAKEFS_RUN_REPO, LAKEFS_BRANCH, lakefs_client
from tools.sharding import shard_qid

DOIP_BASE_URL = os.getenv("DOIP_BASE_URL", "https://doip.episerve.zib.de")


def _resolve_doip_commit(src_uri: str, logger) -> str | None:
    """Return the HEAD commit ID for a DOIP URL by calling /doip/versions?limit=1."""
    m = re.search(r"/doip/retrieve/(Q\d+)/", src_uri, re.IGNORECASE)
    if not m:
        return None
    qid = m.group(1).upper()
    try:
        resp = requests.get(f"{DOIP_BASE_URL}/doip/versions/{qid}?limit=1", timeout=10)
        resp.raise_for_status()
        versions = resp.json().get("versions", [])
        if versions:
            commit_id = versions[0]["commit_id"]
            logger.info(f"Resolved HEAD commit for {qid}: {commit_id}")
            return commit_id
    except Exception as e:
        logger.warning(f"Could not resolve commit ID for {src_uri}: {e}")
    return None


def _apply_sql(data_bytes: bytes, sql: str) -> bytes:
    df = pd.read_parquet(io.BytesIO(data_bytes))
    conn = duckdb.connect()
    conn.register("df", df)
    result = conn.execute(sql).df()
    buf = io.BytesIO()
    result.to_parquet(buf, index=False)
    return buf.getvalue()


@task
def stage_input(
    input_data_files: list[list[str]],
    config_json: str,
    prefect_payload_json: str,
    qid: str,
    data_transformation_sql: list[str] | None = None,
) -> list[str | None]:
    """
    Copy input files from data-processed and write config.json into the run path.

    input_data_files: list of [lakefs_uri, target_filename] pairs
    data_transformation_sql: optional per-file SQL filter applied before staging
    Writes to:
      lakefs://model-runs/main/<sharded-qid>/components/input/<filename>
      lakefs://model-runs/main/<sharded-qid>/components/input/config.json
    Returns:
      list of lakeFS HEAD commit IDs at download time (None for non-lakeFS sources)
    """
    logger = get_run_logger()
    lc = lakefs_client()
    dst_prefix = f"{shard_qid(qid)}/components/input"
    dst_branch_handle = lakefs.repository(LAKEFS_RUN_REPO, client=lc).branch(LAKEFS_BRANCH)
    sql_list = data_transformation_sql or []
    commit_ids: list[str | None] = []

    for i, (src_uri, filename) in enumerate(input_data_files):
        sql = sql_list[i] if i < len(sql_list) else ""

        logger.info(f"Downloading input: {src_uri}")
        try:
            if src_uri.startswith("lakefs://"):
                src_repo, src_branch, path = src_uri[len("lakefs://"):].split("/", 2)
                src_branch_handle = lakefs.repository(src_repo, client=lc).branch(src_branch)
                commit_id = src_branch_handle.head.id
                commit_ids.append(commit_id)
                logger.info(f"Source branch HEAD commit: {commit_id}")
                data = src_branch_handle.object(path).reader().read()
            elif src_uri.startswith("https://") and "/doip/retrieve/" in src_uri:
                commit_ids.append(_resolve_doip_commit(src_uri, logger))
                resp = requests.get(src_uri, timeout=120)
                resp.raise_for_status()
                data = resp.content
            else:
                commit_ids.append(None)
                resp = requests.get(src_uri, timeout=120)
                resp.raise_for_status()
                data = resp.content
            logger.info(f"Downloaded {len(data)} bytes from {src_uri}")
        except Exception as e:
            raise RuntimeError(
                f"Failed to read input file: {src_uri} — "
                f"make sure the file exists and credentials are correct."
            ) from e

        if sql:
            logger.info(f"Applying SQL transformation to {filename}")
            try:
                data = _apply_sql(data, sql)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to apply SQL transformation to {filename}: {sql!r}"
                ) from e

        dst_path = f"{dst_prefix}/{filename}"
        logger.info(f"Staging {filename} -> lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH}/{dst_path}")
        try:
            dst_branch_handle.object(dst_path).upload(
                data=data, content_type="application/octet-stream"
            )
            logger.info(f"{filename} staged successfully")
        except Exception as e:
            raise RuntimeError(f"Failed to stage {filename} to {dst_path}") from e

    dst_config = f"{dst_prefix}/config.json"
    logger.info(f"Staging config.json -> lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH}/{dst_config}")
    try:
        dst_branch_handle.object(dst_config).upload(
            data=config_json.encode(), content_type="application/json"
        )
        logger.info("config.json staged successfully")
    except Exception as e:
        raise RuntimeError(f"Failed to stage config.json to {dst_config}") from e

    dst_prefect = f"{dst_prefix}/config_prefect.json"
    logger.info(f"Staging config_prefect.json -> lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH}/{dst_prefect}")
    try:
        dst_branch_handle.object(dst_prefect).upload(
            data=prefect_payload_json.encode(), content_type="application/json"
        )
        logger.info("config_prefect.json staged successfully")
    except Exception as e:
        raise RuntimeError(f"Failed to stage config_prefect.json to {dst_prefect}") from e

    return commit_ids
