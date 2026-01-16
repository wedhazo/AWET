# LM Studio LLM Setup

This project uses LM Studio as a local OpenAI-compatible LLM server.

## 1) Start LM Studio

1. Open **LM Studio**.
2. Select the model: `llama-3.1-70b-instruct-q4_k_m`.
3. Start the **OpenAI-compatible server**.
4. Set the server to:
   - **Host**: `localhost`
   - **Port**: `11434`
   - **Base URL**: `http://localhost:11434/v1`

## 2) Verify environment variables (optional)

You can export these (or put them in your `.env`):

```bash
LLM_BASE_URL=http://localhost:11434/v1
LLM_MODEL=llama-3.1-70b-instruct-q4_k_m
```

## 3) Test connectivity

Run:

```bash
make llm-test
```

### Expected success output

You should see:

- A ✅ message for the OpenAI-compatible endpoint
- A ✅ result summary at the end

Example (abbreviated):

```
AWET LLM Integration Test
...
openai_endpoint: ✅ PASS
...
🎉 All LLM tests passed!
```
