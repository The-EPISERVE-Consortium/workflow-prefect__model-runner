FROM python:3.12-slim
ARG LAKECTL_VERSION=1.81.1
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    curl -sL "https://github.com/treeverse/lakeFS/releases/download/v${LAKECTL_VERSION}/lakeFS_${LAKECTL_VERSION}_Linux_x86_64.tar.gz" \
    | tar xz -C /usr/local/bin lakectl
RUN pip install --no-cache-dir duckdb pandas pyarrow
COPY pull.py /usr/local/bin/pull.py
