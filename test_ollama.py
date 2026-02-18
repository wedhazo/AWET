#!/usr/bin/env python3
"""Test local Ollama API."""
import httpx
import json

def test_ollama():
    """Test local Ollama LLM."""
    base_url = "http://localhost:11434"
    model = "llama3.2:1b"
    
    print(f"🧪 Testing Local Ollama...")
    print(f"   URL: {base_url}")
    print(f"   Model: {model}")
    print()
    
    # Test OpenAI-compatible endpoint
    url = f"{base_url}/v1/chat/completions"
    
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
                "content": "Say 'Hello from Ollama!' and tell me which model you are."
            }
        ],
    }
    
    try:
        with httpx.Client(timeout=60.0) as client:
            print("📡 Sending request to local Ollama...")
            response = client.post(url, json=payload)
            
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract response
                if "choices" in data and len(data["choices"]) > 0:
                    message = data["choices"][0].get("message", {})
                    content = message.get("content", "").strip()
                    
                    print(f"\n✅ SUCCESS!")
                    print(f"\n📝 Response from Ollama:\n")
                    print(f"   {content}")
                    print()
                    
                    # Show usage stats
                    if "usage" in data:
                        usage = data["usage"]
                        print(f"📊 Token usage:")
                        print(f"   Prompt: {usage.get('prompt_tokens', 0)}")
                        print(f"   Completion: {usage.get('completion_tokens', 0)}")
                        print(f"   Total: {usage.get('total_tokens', 0)}")
                    
                    print("\n💡 Your local Ollama is working! No API costs!")
                    return True
                else:
                    print(f"❌ Unexpected response format:")
                    print(json.dumps(data, indent=2)[:500])
                    return False
            else:
                print(f"❌ API Error: {response.status_code}")
                print(f"   Response: {response.text[:500]}")
                return False
                
    except httpx.TimeoutException:
        print("❌ Request timed out after 60 seconds")
        return False
    except Exception as e:
        print(f"❌ Error: {type(e).__name__}: {e}")
        return False

if __name__ == "__main__":
    success = test_ollama()
    exit(0 if success else 1)
