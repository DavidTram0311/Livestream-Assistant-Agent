FROM ghcr.io/astral-sh/uv:latest AS uv_bin
FROM python:3.10-slim-bullseye

# 1. Install Java and system tools for ARM64
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jdk-headless \
    curl \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*


# 3. Set Environment Variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV SPARK_VERSION=3.3.1
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3

# 4. Install Spark 3.3.1 (using the archive for consistency)
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# 3. Install uv
COPY --from=uv_bin /uv /uvx /bin/

# 4. Python deps
COPY requirements.txt /tmp/requirements.txt
RUN uv pip install --system --no-cache -r /tmp/requirements.txt && rm /tmp/requirements.txt

# 5. App
WORKDIR /app
COPY ./core /app/core
COPY ./routers.py /app
COPY ./feature_source.py /app
COPY ./main.py /app
COPY ./start.sh /app

# 6. Run the start script
CMD ["sh", "-c", "bash start.sh"]