# pipeline/Dockerfile
FROM bitnamilegacy/spark:3.5.1

WORKDIR /app
COPY ingest.py /app/ingest.py

# Bitnami runs as non-root, which is a K8s best practice
USER 1001
