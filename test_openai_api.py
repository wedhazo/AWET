#!/usr/bin/env python3
"""Test OpenAI API connection."""
import os
import httpx
import json

def test_openai_api():
    """Test OpenAI API with a simple query."""
    api_key = os.environ.get("OPENAI_API_KEY")
    model = "gpt-4o-mini"
    
    if not api_key:
        print("❌ No OPENAI_API_KEY found")
        return False
    
    print(f"🧪 Testing OpenAI API...")
    print(f"   Model: {model}")
    print(f"   API Key: {api_key[:20]}...")
    print()
    
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    
    payload = {
        "model": model,
        "max_tokens": 150,
        "messages": [
            {
                "role": "system",
                "content": "You are a helpful AI assistant."
            },
            {
                "role": "user",
                "content": "Say 'Hello from OpenAI!' and tell me which model you are."
            }
        ],
    }
    
    try:
        with httpx.Client(timeout=30.0) as client:
            print("📡 Sending request to api.openai.com...")
            response = client.post(url, json=payload, headers=headers)
            
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract response
                if "choices" in data and len(data["choices"]) > 0:
                    message = data["choices"][0].get("message", {})
                    content = message.get("content", "").strip()
                    
                    print(f"\n✅ SUCCESS!")
                    print(f"\n📝 Response from OpenAI:\n")
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
    success = test_openai_api()
    exit(0 if success else 1)
