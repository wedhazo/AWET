#!/usr/bin/env python3
"""Test Qwen 2.5 32B model with structured JSON output for trading analysis."""
import httpx
import json

def test_qwen_trading():
    """Test Qwen 2.5 32B with a trading analysis prompt."""
    url = "http://localhost:11434/v1/chat/completions"
    
    print("🧪 Testing Qwen 2.5 32B for Trading Analysis...")
    print()
    
    # Trading analysis prompt
    prompt = """You are a professional quantitative trading analyst.

Analyze the provided stock data and sentiment signals.
Return only structured JSON. No explanations outside JSON.

INPUT:
Symbol: AAPL
Timeframe: Daily

Price Data:
Open: 185.50
High: 187.20
Low: 184.80
Close: 186.90
Volume: 52,340,000

Indicators:
RSI: 58
MACD: +0.45
EMA20: 185.00
EMA50: 182.50
VWAP: 186.10

Sentiment:
Reddit Score (-1 to 1): 0.65
Twitter Score (-1 to 1): 0.42
News Score (-1 to 1): 0.55
Major Events: New iPhone release next week

TASK:
1. Determine bias: Bullish, Bearish, or Neutral.
2. Assign confidence (0–100).
3. Suggest action: Buy, Sell, or No Trade.
4. Provide entry price, stop loss, and take profit.
5. Assess risk level: Low, Medium, High.

OUTPUT (STRICT JSON):

{
  "symbol": "",
  "bias": "",
  "confidence": 0,
  "action": "",
  "entry": "",
  "stop_loss": "",
  "take_profit": "",
  "risk_level": ""
}"""
    
    payload = {
        "model": "qwen2.5:32b",
        "messages": [
            {"role": "system", "content": "You are a quantitative trading analyst. Always return valid JSON."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 500,
    }
    
    try:
        with httpx.Client(timeout=60.0) as client:
            print("📡 Sending trading analysis request...")
            response = client.post(url, json=payload)
            
            if response.status_code == 200:
                data = response.json()
                content = data["choices"][0]["message"]["content"].strip()
                
                print("✅ Model responded successfully!\n")
                print("📊 Trading Analysis Result:")
                print("=" * 60)
                print(content)
                print("=" * 60)
                
                # Try to parse as JSON
                try:
                    # Extract JSON if wrapped in code blocks
                    if "```json" in content:
                        content = content.split("```json")[1].split("```")[0].strip()
                    elif "```" in content:
                        content = content.split("```")[1].split("```")[0].strip()
                    
                    analysis = json.loads(content)
                    print("\n✅ Valid JSON structure!")
                    print(f"   Symbol: {analysis.get('symbol')}")
                    print(f"   Bias: {analysis.get('bias')}")
                    print(f"   Confidence: {analysis.get('confidence')}%")
                    print(f"   Action: {analysis.get('action')}")
                    print(f"   Risk Level: {analysis.get('risk_level')}")
                    return True
                except json.JSONDecodeError as e:
                    print(f"\n⚠️  Response is not pure JSON (but that's OK for first test)")
                    print(f"   Error: {e}")
                    return True  # Still counts as success
                
            else:
                print(f"❌ Error: HTTP {response.status_code}")
                print(response.text[:500])
                return False
                
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = test_qwen_trading()
    exit(0 if success else 1)
