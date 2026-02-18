#!/usr/bin/env python3
"""Quick test of Kimi/Moonshot API connection."""
import os
import httpx
import json

def test_kimi_api():
    """Test Kimi API with a simple query."""
    api_key = os.environ.get("KIMI_API_KEY") or os.environ.get("LLM_API_KEY")
    model = os.environ.get("LLM_MODEL", "moonshot-v1-8k")
    
    if not api_key:
        print("❌ No API key found. Set KIMI_API_KEY or LLM_API_KEY")
        return False
    
    print(f"🧪 Testing Kimi API...")
    print(f"   Model: {model}")
    print(f"   API Key: {api_key[:20]}...")
    print()
    
    url = "https://api.moonshot.cn/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": "You are a helpful AI assistant. Keep responses concise."
            },
            {
                "role": "user",
                "content": "Say 'Hello from Kimi!' and tell me what model you are."
            }
        ],
        "max_tokens": 150,
        "temperature": 0.7,
    }
    
    try:
        with httpx.Client(timeout=30.0) as client:
            print("📡 Sending request to api.moonshot.cn...")
            response = client.post(url, json=payload, headers=headers)
            
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract response
                if "choices" in data and len(data["choices"]) > 0:
                    message = data["choices"][0].get("message", {})
                    content = message.get("content", "").strip()
                    
                    print(f"\n✅ SUCCESS!")
                    print(f"\n📝 Response from Kimi:\n")
                    print(f"   {content}")
                    print()
                    
                    # Show usage stats
                    if "usage" in data:
                        usage = data["usage"]
                        print(f"📊 Token usage:")
                        print(f"   Prompt: {usage.get('prompt_tokens', 0)}")
                        print(f"   Completion: {usage.get('completion_tokens', 0)}")
                        print(f"   Total: {usage.get('total_tokens', 0)}")
                    
                    return True
                else:
                    print(f"❌ Unexpected response format:")
                    print(json.dumps(data, indent=2))
                    return False
            else:
                print(f"❌ API Error: {response.status_code}")
                print(f"   Response: {response.text[:500]}")
                return False
                
    except httpx.TimeoutException:
        print("❌ Request timed out after 30 seconds")
        return False
    except Exception as e:
        print(f"❌ Error: {type(e).__name__}: {e}")
        return False

if __name__ == "__main__":
    success = test_kimi_api()
    exit(0 if success else 1)
