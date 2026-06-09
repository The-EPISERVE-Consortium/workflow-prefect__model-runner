FROM treeverse/lakectl:latest
RUN apk add --no-cache python3 py3-pip && \
    pip install --no-cache-dir duckdb pandas pyarrow requests
COPY pull.py /usr/local/bin/pull.py
