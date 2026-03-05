# Streaming Enrichment Service Dockerfile
# Build context: project root (to access common module)
FROM ghcr.io/astral-sh/uv:latest AS uv_bin
FROM python:3.10-slim

# Install curl for healthchecks
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 1. Install uv into the python image
COPY --from=uv_bin /uv /uvx /bin/

# 2. Copy and install requirements
COPY docker/streaming/requirements.txt /tmp/requirements.txt
RUN uv pip install --system --no-cache -r /tmp/requirements.txt && rm /tmp/requirements.txt

# 3. Copy source code with proper structure for imports
COPY src/common /app/src/common
COPY src/streaming /app/src/streaming

# 4. Set PYTHONPATH to include app directory for imports
ENV PYTHONPATH=/app

# 5. Run the streaming enrichment service
CMD ["python", "-m", "src.streaming.main"]
