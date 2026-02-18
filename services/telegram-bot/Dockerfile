# ─────────────────────────────────────────────────────────────────────────────
# Stage 1: builder — install deps into a clean prefix
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.13-slim AS builder

WORKDIR /build

COPY requirements.txt .

RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2: runtime — minimal surface area, non-root user
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.13-slim AS runtime

# Security: non-root user
RUN addgroup --system bot && adduser --system --ingroup bot bot

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Create state directory and set ownership
RUN mkdir -p /var/lib/telegram-bot && chown bot:bot /var/lib/telegram-bot

WORKDIR /app

# Copy source code
COPY telegram_bot/ telegram_bot/

USER bot

# Expose observability port (not 80 — no privilege needed)
EXPOSE 9200

# State file volume mount point
VOLUME ["/var/lib/telegram-bot"]

# Health check — calls own /healthz endpoint
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:9200/healthz')"

ENTRYPOINT ["python", "-m", "telegram_bot.main"]
