import os

import lakefs
from lakefs.client import Client
from urllib.parse import quote


LAKEFS_DATA_REPO = "data-raw"
LAKEFS_RUN_REPO  = "model-runs"
LAKEFS_BRANCH    = "main"


def lakefs_client() -> Client:
    return Client(
        host=os.environ["LAKEFS_HOST"],
        username=os.environ["LAKEFS_ACCESS_KEY"],
        password=os.environ["LAKEFS_SECRET_KEY"],
    )


def lakefs_uri_to_http(uri: str) -> str:
    without_scheme = uri[len("lakefs://"):]
    repo, branch, *parts = without_scheme.split("/")
    path = "/".join(parts)
    host = os.environ["LAKEFS_HOST"].rstrip("/")
    return (
        f"{host}/api/v1/repositories/{repo}/refs/{branch}/objects"
        f"?path={quote(path, safe='')}&presign=false"
    )
