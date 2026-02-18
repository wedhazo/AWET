#!/usr/bin/env python3
"""Test Telegram Bot connection and basic functionality."""
import os
import httpx
import json

def test_telegram_bot():
    """Test Telegram bot token and connectivity."""
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    owner_id = os.environ.get("TELEGRAM_OWNER_ID")
    
    if not bot_token:
        print("❌ No TELEGRAM_BOT_TOKEN found")
        return False
    
    print(f"🧪 Testing Telegram Bot...")
    print(f"   Token: {bot_token[:20]}...")
    if owner_id:
        print(f"   Owner ID: {owner_id}")
    print()
    
    base_url = f"https://api.telegram.org/bot{bot_token}"
    
    try:
        with httpx.Client(timeout=30.0) as client:
            # Test 1: Get bot info
            print("📡 Test 1: Getting bot info (getMe)...")
            response = client.get(f"{base_url}/getMe")
            
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                if data.get("ok"):
                    bot_info = data.get("result", {})
                    username = bot_info.get("username", "unknown")
                    first_name = bot_info.get("first_name", "unknown")
                    bot_id = bot_info.get("id", "unknown")
                    
                    print(f"\n✅ Bot authenticated successfully!")
                    print(f"   ID: {bot_id}")
                    print(f"   Name: {first_name}")
                    print(f"   Username: @{username}")
                    print(f"   Can read all messages: {bot_info.get('can_read_all_group_messages', False)}")
                    print()
                else:
                    print(f"❌ Bot API returned ok=false")
                    print(f"   {data}")
                    return False
            else:
                print(f"❌ HTTP Error: {response.status_code}")
                print(f"   Response: {response.text[:500]}")
                return False
            
            # Test 2: Send test message to owner (optional)
            if owner_id:
                print(f"📡 Test 2: Sending test message to owner (ID: {owner_id})...")
                
                test_message = (
                    "🤖 *AWET Bot Test*\n\n"
                    "✅ Bot is online and connected!\n"
                    "✅ Telegram API working\n"
                    "✅ Ready to receive commands\n\n"
                    "_Available commands:_\n"
                    "• `/start` - Get started\n"
                    "• `/help` - Show help\n"
                    "• `/status` - Check status\n"
                    "• `/search` - Search knowledge base"
                )
                
                payload = {
                    "chat_id": owner_id,
                    "text": test_message,
                    "parse_mode": "Markdown",
                }
                
                response = client.post(f"{base_url}/sendMessage", json=payload)
                
                print(f"   Status: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get("ok"):
                        msg_id = data.get("result", {}).get("message_id", "unknown")
                        print(f"\n✅ Test message sent successfully!")
                        print(f"   Message ID: {msg_id}")
                        print(f"\n💡 Check your Telegram app - you should see a message from @{username}")
                        return True
                    else:
                        error_desc = data.get("description", "Unknown error")
                        print(f"❌ Failed to send message: {error_desc}")
                        
                        # Common errors
                        if "bot was blocked" in error_desc.lower():
                            print(f"\n💡 The bot was blocked by user. Unblock it in Telegram and try again.")
                        elif "chat not found" in error_desc.lower():
                            print(f"\n💡 Chat not found. Start a conversation with @{username} first.")
                        
                        return False
                else:
                    print(f"❌ HTTP Error: {response.status_code}")
                    print(f"   Response: {response.text[:500]}")
                    return False
            else:
                print("⚠️  No TELEGRAM_OWNER_ID set - skipping test message")
                print("   Set TELEGRAM_OWNER_ID to test message sending")
                return True
                
    except httpx.TimeoutException:
        print("❌ Request timed out after 30 seconds")
        return False
    except Exception as e:
        print(f"❌ Error: {type(e).__name__}: {e}")
        return False

if __name__ == "__main__":
    success = test_telegram_bot()
    exit(0 if success else 1)
