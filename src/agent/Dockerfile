FROM ghcr.io/astral-sh/uv:latest AS uv_bin
FROM python:3.10-slim-bookworm

# 1. Install system deps and OpenJDK 17 (Default for Debian Bookworm)
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    procps \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# 2. Set JAVA_HOME
# Debian Bookworm uses Java 17 by default
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:$PATH"

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