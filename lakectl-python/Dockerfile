FROM treeverse/lakectl:latest
USER root
RUN apk add --no-cache python3 py3-pip && \
    pip install --no-cache-dir --break-system-packages duckdb pandas pyarrow
COPY pull.py /usr/local/bin/pull.py
