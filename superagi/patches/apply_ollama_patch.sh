#!/bin/bash
# Apply Ollama compatibility patch to SuperAGI OpenAI client
# This patches the running SuperAGI containers to handle Ollama's response format
#
# IMPORTANT: This patch is automatically applied via docker-compose volume mounts.
# You only need to run this script if you want to apply it to already-running containers.
#
# The patch adds a _normalize_ollama_response() function that handles Ollama's
# OpenAI-compatible response format, which differs slightly from the real OpenAI API.
#
# NOTE: With CPU-only Ollama, responses take 6-7 minutes per LLM call.
# For faster testing, use GPU Ollama or increase timeouts:
#   E2E_TIMEOUT_SECONDS=1800 make e2e-superagi

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PATCH_FILE="$SCRIPT_DIR/openai_ollama_compat.py"

echo "🔧 Applying Ollama OpenAI compatibility patch to SuperAGI..."

# Check if patch file exists
if [[ ! -f "$PATCH_FILE" ]]; then
    echo "❌ Patch file not found: $PATCH_FILE"
    exit 1
fi

# Check if SuperAGI containers are running
if ! docker ps --format '{{.Names}}' | grep -q "superagi-backend"; then
    echo "❌ SuperAGI backend container not running. Start with: make superagi-up"
    exit 1
fi

# Copy patch to backend container
echo "📦 Copying patch to superagi-backend..."
docker cp "$PATCH_FILE" superagi-backend:/app/superagi/llms/openai.py

# Copy patch to celery container (this is where the actual execution happens)
if docker ps --format '{{.Names}}' | grep -q "superagi-celery"; then
    echo "📦 Copying patch to superagi-celery..."
    docker cp "$PATCH_FILE" superagi-celery:/app/superagi/llms/openai.py
    
    # Kill the celery worker process to force reload (without restarting container)
    echo "🔄 Forcing Celery worker reload..."
    docker exec superagi-celery pkill -f "celery" 2>/dev/null || true
    
    # Wait for celery to auto-restart (supervisor handles this)
    sleep 5
fi

# Verify the patch was applied
echo "✅ Verifying patch..."
if docker exec superagi-celery grep -q "_normalize_ollama_response" /app/superagi/llms/openai.py 2>/dev/null; then
    echo "✅ Patch applied successfully!"
    echo ""
    echo "The SuperAGI OpenAI client now supports Ollama's response format."
    echo ""
    echo "⚠️  NOTE: With CPU Ollama, each LLM call takes 6-7 minutes!"
    echo "   Increase timeout for e2e tests:"
    echo "   E2E_TIMEOUT_SECONDS=1800 make e2e-superagi"
    echo ""
    echo "   For faster testing, use GPU Ollama:"
    echo "   docker run --gpus all -p 11434:11434 ollama/ollama"
else
    echo "❌ Patch verification failed"
    exit 1
fi
