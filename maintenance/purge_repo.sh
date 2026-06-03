#!/bin/bash
# Deletes all files from the main branch of a lakeFS repository.
# The repository and branch are preserved.
#
# Usage: purge_repo.sh <repo-name>
# Requires: lakectl, jq

set -euo pipefail

if [ -z "${1:-}" ]; then
    echo "Usage: $0 <repo-name>"
    echo ""
    echo "Deletes all files from the main branch of the given lakeFS repository."
    echo "The repository and branch are preserved."
    echo ""
    echo "Example: $0 model-runs"
    exit 1
fi
REPO="$1"
BRANCH="main"

echo "Purging all files from lakefs://${REPO}/${BRANCH}"

if lakectl fs ls "lakefs://${REPO}/${BRANCH}/" --output json 2>/dev/null | jq -e 'length > 0' > /dev/null; then
    lakectl fs rm -r "lakefs://${REPO}/${BRANCH}/"
    lakectl commit "lakefs://${REPO}/${BRANCH}" -m "maintenance: purge all files"
    echo "Done. lakefs://${REPO}/${BRANCH} is now empty."
else
    echo "lakefs://${REPO}/${BRANCH} is already empty, nothing to do."
fi
