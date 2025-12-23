FROM ghcr.io/astral-sh/uv:latest AS uv_bin
FROM python:3.10-slim

# Install curl
RUN apt-get update && apt-get install -y curl

# 1. Install uv into the python image
COPY --from=uv_bin /uv /uvx /bin/

# 2. Copy dependency files first (for better caching)
COPY ./requirements.txt /tmp/requirements.txt

# 3. Install dependencies using uv
# --system tells uv to install into the global site-packages
# --frozen ensures your versions stay exact
RUN uv pip install --system --no-cache -r /tmp/requirements.txt

# 4. Remove the requirements.txt file
RUN rm /tmp/requirements.txt

# 5. Set the working directory
WORKDIR /app

# 6. Copy the rest of your app
COPY ./db /app/db
COPY ./configs /app/configs
COPY ./create_table.py /app
COPY ./run.sh /app
COPY ./start.sh /app

# 7. Run the start script
CMD ["sh", "-c", "bash start.sh"]