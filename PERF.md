# AWET SuperAGI Latency Notes

## LLM latency (standalone)

Test the local LLM gateway directly (OpenAI-compatible):

```bash
curl -sS http://localhost:11434/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "llama-3.1-70b-instruct-q4_k_m",
    "messages": [{"role": "user", "content": "ping"}],
    "max_tokens": 16,
    "temperature": 0.1
  }'
```

If this is slow, the **Thinking** delay in SuperAGI will also be slow.

## Lightweight SuperAGI run (fast check)

Use **AWET Trade Watchdog** or **AWET Morning Deployer** with a minimal goal:

```
Run one health check and summarize in 2 bullet points.
```

This should only call `awet_check_pipeline_health` and finish quickly.

## Tool timing logs

Each AWET tool now logs a timing line like:

```
[awet_tool_timing] tool=awet_check_pipeline_health duration_s=1.832
```

Check these in the SuperAGI backend logs to see which tool is slow:

```bash
docker compose logs -f superagi-backend
```

## Suggested tool cadence (docs only)

- **Fast checks** (run often):
  - `awet_check_pipeline_health`
  - `awet_read_directive`
- **Heavy / slow** (run less often):
  - `awet_run_backfill`
  - `awet_train_model`
  - `awet_run_demo`
- **Occasional** (on demand):
  - `awet_promote_model`
  - `awet_run_live_ingestion`

## Likely latency sources

1. **LLM response time** (local Llama is slow for large models).
2. **Heavy tools** (`awet_run_backfill`, `awet_train_model`, `awet_run_demo`).
3. **Kafka lag / agent health** when downstream services are busy.
