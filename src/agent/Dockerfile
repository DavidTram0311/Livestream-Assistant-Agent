FROM ghcr.io/astral-sh/uv:latest AS uv_bin
FROM python:3.10-slim

# 1. Install Java 21 and procps
RUN apt-get update && apt-get install -y \
    curl \
    openjdk-21-jre-headless \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Set JAVA_HOME (Updated for Java 21 and Apple Silicon/ARM64)
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64
# Note: If you build on an Intel machine, this would be:
# ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# 3. ADD THIS: Unlock the memory access Spark needs for Java 17+
ENV JAVA_TOOL_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED \
                        --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
                        --add-opens=java.base/java.lang=ALL-UNNAMED \
                        --add-opens=java.base/java.util=ALL-UNNAMED \
                        --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
                        --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
                        --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
                        --add-opens=java.base/sun.security.action=ALL-UNNAMED \
                        --add-opens=java.base/sun.net.util=ALL-UNNAMED \
                        --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
                        --add-opens=java.base/java.io=ALL-UNNAMED \
                        --add-opens=java.base/java.net=ALL-UNNAMED"

# 3. Install uv into the python image
COPY --from=uv_bin /uv /uvx /bin/

# 4. Copy dependency files first (for better caching)
COPY ./requirements.txt /tmp/requirements.txt

# 5. Install dependencies using uv
# --system tells uv to install into the global site-packages
# --frozen ensures your versions stay exact
RUN uv pip install --system --no-cache -r /tmp/requirements.txt

# 6. Remove the requirements.txt file
RUN rm /tmp/requirements.txt

# 7. Set the working directory
WORKDIR /app

# 8. Copy the rest of your app
COPY ./core /app/core
COPY ./routers.py /app
COPY ./feature_source.py /app
COPY ./main.py /app
COPY ./start.sh /app

# 9. Run the start script
CMD ["sh", "-c", "bash start.sh"]