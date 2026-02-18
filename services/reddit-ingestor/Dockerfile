FROM python:3.13-slim AS base

LABEL maintainer="awet" \
      service="reddit-ingestor"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install system deps for confluent-kafka (librdkafka)
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY services/reddit-ingestor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy schemas (needed by EventPublisher at runtime)
COPY schemas /app/schemas

# Copy service source
COPY services/reddit-ingestor/reddit_ingestor /app/reddit_ingestor

EXPOSE 8090

HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8090/healthz')"

CMD ["python", "-m", "reddit_ingestor"]
