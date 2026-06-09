FROM treeverse/lakectl:latest AS lakectl

FROM python:3.12-slim
COPY --from=lakectl /usr/local/bin/lakectl /usr/local/bin/lakectl
RUN pip install --no-cache-dir duckdb pandas pyarrow
COPY pull.py /usr/local/bin/pull.py
