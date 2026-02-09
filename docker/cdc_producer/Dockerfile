# CDC Producer Dockerfile - PostgreSQL CDC event producer
# Build context: project root (to access common module)
FROM ghcr.io/astral-sh/uv:latest AS uv_bin
FROM python:3.10-slim

# Install curl for healthchecks
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 1. Install uv into the python image
COPY --from=uv_bin /uv /uvx /bin/

# 2. Copy and install requirements
COPY src/cdc_producer/requirements.txt /tmp/requirements.txt
RUN uv pip install --system --no-cache -r /tmp/requirements.txt && rm /tmp/requirements.txt

# 3. Copy common module (shared utilities)
COPY src/common /app/common

# 4. Copy cdc_producer module
COPY src/cdc_producer/db /app/db
COPY src/cdc_producer/configs /app/configs
COPY src/cdc_producer/config.py /app/
COPY src/cdc_producer/cdc_produce.py /app/
COPY src/cdc_producer/create_table.py /app/
COPY src/cdc_producer/run.sh /app/
COPY src/cdc_producer/start.sh /app/

# 5. Copy data if exists
COPY src/cdc_producer/data /app/data

# 6. Set PYTHONPATH to include app directory for imports
ENV PYTHONPATH=/app

# 7. Run the start script
CMD ["sh", "-c", "bash start.sh"]
