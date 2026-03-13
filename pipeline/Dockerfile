FROM bitnamilegacy/spark:3.5.3

# 1. Temporarily switch to root to fix the OS
USER root

# 2. Add UID 1001 to the Linux passwd file so Hadoop stops panicking.
# We set the home directory to /tmp to permanently solve the Ivy issue too!
RUN echo "spark:x:1001:0:spark,,,:/tmp:/bin/bash" >> /etc/passwd

WORKDIR /app
COPY ingest.py /app/ingest.py

# 3. Switch back to the secure non-root user (Kubernetes best practice)
USER 1001
