#!/bin/bash
# Steps to get a NEW Telegram Bot Token:
# 
# 1. Open Telegram and search for: @BotFather
# 2. Send message: /mybots
# 3. Select: Kironix Alert Bot
# 4. Select: API Token
# 5. Select: ⚠️ Revoke current token
# 6. Click ✅ to confirm
# 7. Then do /mybots again, select Kironix, select API Token
# 8. You'll get a NEW token that looks like: 123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefgh
#
# Copy that NEW token and paste it as:
#    export NEW_TOKEN="your-new-token-here"
# Then run: ./setup_new_token.sh $NEW_TOKEN

if [ -z "$1" ]; then
    echo "❌ Usage: ./setup_new_token.sh <new_token>"
    echo "   Example: ./setup_new_token.sh 123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcd"
    exit 1
fi

NEW_TOKEN="$1"
echo "🔄 Updating .env with new token..."

# Backup old .env
cp .env .env.backup
echo "📁 Backed up to .env.backup"

# Replace token in .env
sed -i.bak "s/TELEGRAM_BOT_TOKEN=.*/TELEGRAM_BOT_TOKEN=$NEW_TOKEN/" .env

echo "✅ Updated TELEGRAM_BOT_TOKEN in .env"
echo ""
echo "Verifying token..."
python3 << 'EOF'
import httpx
import os

token = os.getenv("TELEGRAM_BOT_TOKEN")
if not token:
    print("❌ Token not found in .env")
    exit(1)

url = f"https://api.telegram.org/bot{token}/getMe"
response = httpx.get(url, timeout=10)
data = response.json()

if data.get("ok"):
    bot = data.get("result", {})
    print(f"✅ New token is VALID!")
    print(f"   Bot: @{bot.get('username')}")
    print(f"   ID: {bot.get('id')}")
else:
    print(f"❌ Token invalid: {data.get('description')}")
    exit(1)
EOF

echo ""
echo "🚀 Killing old bot and restarting..."
pkill -f "telegram_bot.main"
sleep 2

echo "Starting bot with new token in background..."
cd /home/kironix/Awet
make telegram-bot-local > /tmp/telegram-bot-new.log 2>&1 &
sleep 3

echo "✅ Bot restarted!"
echo "📝 View logs: tail -f /tmp/telegram-bot-new.log"
