# Producer Dockerfile - Kafka producer with Avro serialization
# Build context: project root (to access common module)
FROM ghcr.io/astral-sh/uv:latest AS uv_bin
FROM python:3.10-slim

WORKDIR /app

# 1. Install uv into the python image
COPY --from=uv_bin /uv /uvx /bin/

# 2. Copy and install requirements
COPY src/producer/requirements.txt /tmp/requirements.txt
RUN uv pip install --system --no-cache -r /tmp/requirements.txt && rm /tmp/requirements.txt

# 3. Create necessary directories
RUN mkdir -p /app/data /app/avro_schemas

# 4. Copy common module (shared utilities)
COPY src/common /app/common

# 5. Copy producer module
COPY src/producer/produce.py /app/
COPY src/producer/config.py /app/
COPY src/producer/avro_schemas /app/avro_schemas
COPY src/producer/data/user_comments.parquet /app/data/user_comments.parquet

# 6. Set PYTHONPATH to include app directory for imports
ENV PYTHONPATH=/app

CMD ["python", "-u", "produce.py", "--mode", "setup", "--type", "containers"]
