#!/usr/bin/env python3
"""Test Claude/Anthropic API connection."""
import os
import httpx
import json

def test_anthropic_api():
    """Test Anthropic Claude API with a simple query."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    model = "claude-sonnet-4-20250514"
    
    if not api_key:
        print("❌ No ANTHROPIC_API_KEY found")
        return False
    
    print(f"🧪 Testing Anthropic Claude API...")
    print(f"   Model: {model}")
    print(f"   API Key: {api_key[:20]}...")
    print()
    
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
    }
    
    payload = {
        "model": model,
        "max_tokens": 150,
        "messages": [
            {
                "role": "user",
                "content": "Say 'Hello from Claude!' and tell me which version you are."
            }
        ],
    }
    
    try:
        with httpx.Client(timeout=30.0) as client:
            print("📡 Sending request to api.anthropic.com...")
            response = client.post(url, json=payload, headers=headers)
            
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract response
                if "content" in data and len(data["content"]) > 0:
                    content = data["content"][0].get("text", "").strip()
                    
                    print(f"\n✅ SUCCESS!")
                    print(f"\n📝 Response from Claude:\n")
                    print(f"   {content}")
                    print()
                    
                    # Show usage stats
                    if "usage" in data:
                        usage = data["usage"]
                        print(f"📊 Token usage:")
                        print(f"   Input: {usage.get('input_tokens', 0)}")
                        print(f"   Output: {usage.get('output_tokens', 0)}")
                    
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
    success = test_anthropic_api()
    exit(0 if success else 1)
