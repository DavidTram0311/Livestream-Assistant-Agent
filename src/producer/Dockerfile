FROM ghcr.io/astral-sh/uv:latest AS uv_bin
FROM python:3.10-slim

WORKDIR /app

# 1. Install uv into the python image
COPY --from=uv_bin /uv /uvx /bin/

# 2. Copy dependency files first (for better caching)
COPY ./requirements.txt /app/requirements.txt

# 3. Install dependencies using uv
# --system tells uv to install into the global site-packages
# --frozen ensures your versions stay exact
RUN uv pip install --system --no-cache -r requirements.txt

RUN mkdir -p /app/data /app/avro_schemas

# 4. Copy the rest of your app
COPY ./produce.py /app/produce.py
COPY ./avro_schemas /app/avro_schemas
COPY ./data/user_comments.parquet /app/data/user_comments.parquet

CMD [ "python", "-u", "produce.py", "--mode", "setup", "--type", "containers"]