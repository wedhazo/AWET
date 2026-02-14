# Local LLM Setup (Ollama with GPU)

This project uses **Ollama** as a local OpenAI-compatible LLM server with GPU acceleration.

## Quick Start

```bash
# Verify GPU is working
make llm-gpu

# Test LLM connectivity
make llm-test
```

## 1) Install Ollama

```bash
# Install Ollama
curl -fsSL https://ollama.com/install.sh | sh

# Pull a model
ollama pull llama3.2:3b      # Fast, 2GB
ollama pull llama3.2:1b      # Faster, 1.3GB
ollama pull llama3.1:70b     # Best quality, requires ~40GB VRAM
```

## 2) GPU Configuration

Ollama automatically uses GPU if available. To verify and optimize:

### Verify GPU is Being Used
```bash
# Run the GPU verification script
make llm-gpu

# Or manually check
nvidia-smi  # Should show Ollama using GPU memory
```

### Service Configuration (systemd)

Create/edit `/etc/systemd/system/ollama.service.d/override.conf`:

```ini
[Service]
# Bind to all interfaces (for Docker access)
Environment="OLLAMA_HOST=0.0.0.0:11434"

# Explicitly use GPU 0
Environment="CUDA_VISIBLE_DEVICES=0"

# Force CUDA driver
Environment="OLLAMA_GPU_DRIVER=cuda"

# Use all GPU layers (99 = max)
Environment="OLLAMA_NUM_GPU=99"
```

Apply changes:
```bash
sudo systemctl daemon-reload
sudo systemctl restart ollama
```

### Performance Tuning

```bash
# For larger context windows
Environment="OLLAMA_CONTEXT_LENGTH=8192"

# Load multiple models
Environment="OLLAMA_MAX_LOADED_MODELS=2"

# Keep models in memory longer (5 minutes)
Environment="OLLAMA_KEEP_ALIVE=5m"
```

## 3) Environment Variables

Add to your `.env`:

```bash
LLM_BASE_URL=http://localhost:11434/v1
LLM_MODEL=llama3.2:3b
```

## 4) Test Connectivity

```bash
# Basic test
make llm-test

# GPU verification
make llm-gpu

# Direct API test
curl http://localhost:11434/api/generate -d '{"model": "llama3.2:3b", "prompt": "Hello", "stream": false}'
```

### Expected Output

```
✅ GPU Found: NVIDIA GeForce RTX 5090 Laptop GPU
✅ Model appears to be loaded in GPU memory
✅ Speed (291 tokens/sec) indicates GPU acceleration
✅ All checks passed! Ollama is using GPU for inference.
```

## 5) Docker Integration

For containers needing LLM access:

```yaml
# docker-compose.yml
services:
  my-service:
    environment:
      - LLM_BASE_URL=http://host.docker.internal:11434/v1
      - OPENAI_API_BASE=http://host.docker.internal:11434/v1
```

## 6) Troubleshooting

### GPU Not Being Used
```bash
# Check Ollama logs
journalctl -u ollama -f

# Check GPU processes
nvidia-smi pmon -d 1

# Verify CUDA is available
nvidia-smi --query-gpu=compute_cap --format=csv
```

### Slow Performance
```bash
# Check if model fits in VRAM
nvidia-smi --query-gpu=memory.used,memory.total --format=csv

# Use a smaller model
ollama pull llama3.2:1b
```

### Connection Refused
```bash
# Ensure Ollama is running
sudo systemctl start ollama

# Check if bound to correct interface
ss -tlnp | grep 11434
```

## 7) Model Recommendations

| Model | Size | VRAM | Use Case |
|-------|------|------|----------|
| `llama3.2:1b` | 1.3 GB | ~2 GB | Fast responses, simple tasks |
| `llama3.2:3b` | 2.0 GB | ~3 GB | Good balance (recommended) |
| `llama3.1:8b` | 4.7 GB | ~6 GB | Better reasoning |
| `llama3.1:70b` | 40 GB | ~45 GB | Best quality, needs high-end GPU |

Your RTX 5090 (24GB VRAM) can run up to `llama3.1:70b-q4` quantized versions.
