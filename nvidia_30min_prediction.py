#!/usr/bin/env python3
"""
Extended 30-Minute NVIDIA Prediction
Custom model for longer timeframe prediction
"""

import yfinance as yf
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

def create_extended_prediction():
    print("🔮 NVIDIA 30-MINUTE EXTENDED PREDICTION")
    print("=" * 50)
    
    # Get recent NVIDIA data
    print("📊 Fetching recent NVIDIA data...")
    nvda = yf.download('NVDA', period='5d', interval='1m')
    
    if nvda.empty:
        print("❌ Could not fetch NVIDIA data")
        return
    
    # Prepare features for 30-minute prediction
    print("⚙️ Creating features for 30-minute model...")
    
    # Calculate technical indicators
    nvda['returns'] = nvda['Close'].pct_change()
    nvda['sma_5'] = nvda['Close'].rolling(5).mean()
    nvda['sma_15'] = nvda['Close'].rolling(15).mean()
    nvda['sma_30'] = nvda['Close'].rolling(30).mean()
    nvda['rsi'] = calculate_rsi(nvda['Close'])
    nvda['volatility'] = nvda['returns'].rolling(30).std()
    nvda['volume_sma'] = nvda['Volume'].rolling(15).mean()
    nvda['price_volume'] = nvda['Close'] * nvda['Volume']
    
    # Create 30-minute ahead target
    nvda['target_30min'] = nvda['Close'].shift(-30)  # 30 minutes ahead
    
    # Feature columns
    features = ['returns', 'sma_5', 'sma_15', 'sma_30', 'rsi', 
                'volatility', 'volume_sma', 'price_volume']
    
    # Clean data
    nvda_clean = nvda.dropna()
    
    if len(nvda_clean) < 100:
        print("❌ Insufficient data for 30-minute prediction")
        return
    
    print(f"✅ Using {len(nvda_clean)} data points")
    
    # Prepare training data
    X = nvda_clean[features].iloc[:-30]  # Exclude last 30 minutes (no target)
    y = nvda_clean['target_30min'].iloc[:-30]
    
    # Train 30-minute prediction model
    print("🤖 Training 30-minute prediction model...")
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X, y)
    
    # Get current data for prediction
    current_data = nvda_clean[features].iloc[-1:].values
    current_price_val = float(nvda_clean['Close'].iloc[-1])
    
    # Make 30-minute prediction
    predicted_price = model.predict(current_data)[0]
    change_pct = ((predicted_price - current_price_val) / current_price_val) * 100
    
    # Calculate confidence based on model variance
    predictions = [model.estimators_[i].predict(current_data)[0] for i in range(50)]
    std_dev = np.std(predictions)
    confidence = max(50, min(95, 95 - (std_dev / current_price_val) * 1000))
    
    # Get current time
    now = datetime.now()
    target_time = now + timedelta(minutes=30)
    
    print("\n🎯 NVIDIA 30-MINUTE PREDICTION RESULTS")
    print("=" * 45)
    print(f"📅 Current Time: {now.strftime('%H:%M:%S EST')}")
    print(f"⏰ Target Time: {target_time.strftime('%H:%M:%S EST')} (30 min ahead)")
    print(f"💰 Current Price: ${current_price_val:.2f}")
    print(f"🔮 Predicted Price: ${predicted_price:.2f}")
    print(f"📈 Expected Change: {change_pct:+.2f}%")
    print(f"🎯 AI Confidence: {confidence:.1f}%")
    print()
    
    # Trading recommendation
    if abs(change_pct) >= 1.5 and confidence >= 70:
        if change_pct > 0:
            action = "🟢 BUY SIGNAL"
            reason = f"Strong upward prediction: {change_pct:+.2f}%"
        else:
            action = "🔴 SELL SIGNAL" 
            reason = f"Strong downward prediction: {change_pct:+.2f}%"
    else:
        action = "🟡 HOLD"
        if abs(change_pct) < 1.5:
            reason = f"Change too small: {abs(change_pct):.2f}% < 1.5%"
        else:
            reason = f"Confidence too low: {confidence:.1f}% < 70%"
    
    print(f"🤖 AI RECOMMENDATION: {action}")
    print(f"📋 Reason: {reason}")
    
    return {
        'current_price': current_price_val,
        'predicted_price': predicted_price,
        'change_percent': change_pct,
        'confidence': confidence,
        'action': action,
        'target_time': target_time
    }

def calculate_rsi(prices, period=14):
    """Calculate RSI indicator"""
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

if __name__ == "__main__":
    result = create_extended_prediction()
    
    if result:
        print("\n🎉 30-MINUTE NVIDIA PREDICTION COMPLETE!")
        print("✅ Extended timeframe analysis successful")
        print("🔮 Prediction ready for 30-minute ahead forecast")