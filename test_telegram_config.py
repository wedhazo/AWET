#!/usr/bin/env python3
"""Quick test of Telegram bot with Ollama configured."""
import os
import sys

# Load .env
from pathlib import Path
env_file = Path("/home/kironix/Awet/.env")
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            if "=" in line:
                key, value = line.split("=", 1)
                os.environ[key] = value

# Configure defaults
os.environ.setdefault("KB_QUERY_BASE_URL", "http://localhost:8000")
os.environ.setdefault("LOG_LEVEL", "INFO")
os.environ.setdefault("METRICS_PORT", "9201")  # Different port to avoid conflict
os.environ.setdefault("STATE_FILE", "/tmp/awet-telegram-bot-state.json")

print("🤖 AWET Telegram Bot Configuration:")
print(f"   Bot Token: {os.getenv('TELEGRAM_BOT_TOKEN', '')[:20]}...")
print(f"   LLM Provider: {os.getenv('LLM_PROVIDER', 'none')}")
print(f"   LLM Model: {os.getenv('LLM_MODEL', 'none')}")
print(f"   LLM Base URL: {os.getenv('LLM_BASE_URL', 'none')}")
print(f"   LLM API Key: {'***set***' if os.getenv('LLM_API_KEY') else 'not set'}")
print()

if not os.environ.get("TELEGRAM_BOT_TOKEN"):
    print("❌ TELEGRAM_BOT_TOKEN not set in .env!")
    sys.exit(1)

if not os.environ.get("LLM_BASE_URL"):
    print("❌ LLM_BASE_URL not set!")
    sys.exit(1)

print("✅ Configuration looks good!")
print()
print("To run the bot:")
print("  cd services/telegram-bot")
print("  export $(grep -v '^#' ../../.env | xargs)")
print("  python -m telegram_bot.main")
print()
print("Or use: make telegram-bot-local")
