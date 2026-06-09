#!/usr/bin/env python3
"""
Init-container script for the model-runner K8s job.

Reads PULL_SPEC from the environment — a JSON list of
  [src_uri, filename]            or
  [src_uri, filename, sql]
entries — downloads each file to /work/input/ and applies the optional
DuckDB SQL filter in-place.

lakeFS URIs with ?version=<commit_id> are rewritten to the lakectl
ref format (lakefs://repo/<commit_id>/path) before calling lakectl.
DOIP retrieve URLs are converted to lakeFS URIs via _doip_to_lakefs_uri.
"""
import io
import json
import os
import re
import subprocess

import duckdb
import pandas as pd

WORK_INPUT = "/work/input"


def _shard_qid(qid: str) -> str:
    digits = re.sub(r"[^0-9]", "", qid).zfill(6)
    return f"{digits[0:2]}/{digits[2:4]}/{digits[4:6]}/{qid}"


def _lakefs_versioned_to_ref(src_uri: str) -> str:
    """Convert lakefs://repo/branch/path?version=<id> to lakefs://repo/<id>/path."""
    if "?version=" not in src_uri:
        return src_uri
    uri_part, commit_id = src_uri.split("?version=", 1)
    repo, _, path = uri_part[len("lakefs://"):].split("/", 2)
    return f"lakefs://{repo}/{commit_id}/{path}"


def _doip_to_lakefs_uri(src_uri: str) -> str:
    """Convert a DOIP retrieve URL to a lakeFS URI using the embedded ?version= commit ID."""
    m = re.match(r".*/doip/retrieve/([^/?]+)/(.+?)(?:\?version=(.+))?$", src_uri)
    if not m:
        raise ValueError(f"Cannot parse DOIP URL: {src_uri}")
    qid, component_path, commit_id = m.group(1), m.group(2), m.group(3)
    if not commit_id:
        raise ValueError(f"DOIP URL has no ?version= commit ID: {src_uri}")
    repo = os.environ.get("DOIP_LAKEFS_REPO", "data-processed")
    return f"lakefs://{repo}/{commit_id}/{_shard_qid(qid)}/components/{component_path}"


def download(src_uri: str, filename: str) -> None:
    out_path = os.path.join(WORK_INPUT, filename)
    if src_uri.startswith("lakefs://"):
        lakectl_uri = _lakefs_versioned_to_ref(src_uri)
    elif "/doip/retrieve/" in src_uri:
        lakectl_uri = _doip_to_lakefs_uri(src_uri)
    else:
        raise ValueError(f"Unsupported source URI scheme: {src_uri}")
    subprocess.run(["lakectl", "fs", "download", lakectl_uri, out_path], check=True)
    print(f"download {filename} ... done!")


def apply_sql(filename: str, sql: str) -> None:
    path = os.path.join(WORK_INPUT, filename)
    df = pd.read_parquet(path)
    conn = duckdb.connect()
    conn.register("df", df)
    result = conn.execute(sql).df()
    buf = io.BytesIO()
    result.to_parquet(buf, index=False)
    with open(path, "wb") as fh:
        fh.write(buf.getvalue())
    print(f"applied SQL to {filename}")


def main() -> None:
    pull_spec = json.loads(os.environ["PULL_SPEC"])
    os.makedirs(WORK_INPUT, exist_ok=True)
    for entry in pull_spec:
        src_uri, filename = entry[0], entry[1]
        sql = entry[2] if len(entry) > 2 else ""
        download(src_uri, filename)
        if sql:
            apply_sql(filename, sql)


if __name__ == "__main__":
    main()
