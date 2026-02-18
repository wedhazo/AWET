#!/usr/bin/env python3
"""Run AWET Telegram bot locally with Ollama."""
import os
import sys

# Set environment for local Ollama
os.environ.setdefault("TELEGRAM_BOT_TOKEN", os.getenv("TELEGRAM_BOT_TOKEN", ""))
os.environ.setdefault("KB_QUERY_BASE_URL", "http://localhost:8000")
os.environ.setdefault("LOG_LEVEL", "INFO")
os.environ.setdefault("METRICS_PORT", "9200")
os.environ.setdefault("STATE_FILE", "/tmp/telegram-bot-state.json")

# Configure for local Ollama (OpenAI-compatible)
os.environ.setdefault("LLM_PROVIDER", "openai")
os.environ.setdefault("LLM_API_KEY", "dummy")  # Ollama doesn't need real key
os.environ.setdefault("LLM_MODEL", "llama3.2:1b")

if not os.getenv("TELEGRAM_BOT_TOKEN"):
    print("❌ TELEGRAM_BOT_TOKEN not set!")
    sys.exit(1)

# Import and run
sys.path.insert(0, "services/telegram-bot")
from telegram_bot.main import main

if __name__ == "__main__":
    print("🤖 Starting AWET Telegram Bot with Local Ollama...")
    print(f"   Model: llama3.2:1b")
    print(f"   Ollama: http://localhost:11434/v1")
    print()
    main()
