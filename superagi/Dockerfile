FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    build-essential \
    libpq-dev \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Clone SuperAGI (use stable tag if available)
RUN git clone --depth 1 https://github.com/TransformerOptimus/SuperAGI.git /tmp/superagi \
    && cp -r /tmp/superagi/* /app/ \
    && rm -rf /tmp/superagi

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install additional dependencies for AWET integration
RUN pip install --no-cache-dir \
    confluent-kafka \
    fastavro \
    asyncpg \
    structlog \
    pyyaml \
    httpx

# Copy patched files to fix SuperAGI bugs
COPY patches/auth.py /app/superagi/helper/auth.py

# Copy AWET custom tools
COPY tools/ /app/superagi/tools/awet/

# Copy config
COPY config.yaml /app/config.yaml

# Make scripts executable
RUN chmod +x /app/wait-for-it.sh /app/entrypoint.sh /app/install_tool_dependencies.sh 2>/dev/null || true

EXPOSE 8001

# Use the original entrypoint which runs alembic migrations
CMD ["bash", "-c", "alembic upgrade head && python -m uvicorn main:app --host 0.0.0.0 --port 8001"]
