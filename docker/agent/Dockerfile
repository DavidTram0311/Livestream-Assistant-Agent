# Agent Dockerfile - FastAPI + Spark NLP for sentiment analysis
# Build context: project root (to access common module)
FROM ghcr.io/astral-sh/uv:latest AS uv_bin
FROM python:3.10-slim-bullseye

# 1. Install Java and system tools for ARM64
RUN apt-get update && apt-get install -y --no-install-recommends --fix-missing \
    openjdk-11-jdk-headless \
    curl \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

# 2. Set Environment Variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV SPARK_VERSION=3.5.3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3

# 3. Install Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# 4. Install uv
COPY --from=uv_bin /uv /uvx /bin/

# 5. Set working directory
WORKDIR /app

# 6. Copy and install requirements
COPY src/agent/requirements.txt /tmp/requirements.txt
RUN uv pip install --system --no-cache -r /tmp/requirements.txt && rm /tmp/requirements.txt

# 7. Copy common module (shared utilities)
COPY src/common /app/common

# 8. Copy agent module
COPY src/agent/core /app/core
COPY src/agent/routers.py /app/
COPY src/agent/feature_source.py /app/
COPY src/agent/main.py /app/
COPY src/agent/config.py /app/
COPY src/agent/start.sh /app/

# 9. Set PYTHONPATH to include app directory for imports
ENV PYTHONPATH=/app

# 10. Run the start script
CMD ["sh", "-c", "bash start.sh"]
