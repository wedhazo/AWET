#!/usr/bin/env python3
"""
COMPREHENSIVE TRADING ANALYSIS FOR TOMORROW
Advanced market prediction and trading signals
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

print("📊 TRADING ANALYSIS FOR TOMORROW")
print("=" * 50)
print(f"📅 Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M EST')}")
print(f"🎯 Target Date: {(datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')}")
print()

# Top stocks to analyze
STOCKS = ['AAPL', 'MSFT', 'NVDA', 'TSLA', 'GOOGL', 'AMZN', 'META', 'NFLX', 'AMD', 'ADBE']

def calculate_technical_indicators(df):
    """Calculate comprehensive technical indicators"""
    
    # Moving averages
    df['SMA_20'] = df['Close'].rolling(window=20).mean()
    df['SMA_50'] = df['Close'].rolling(window=50).mean()
    df['EMA_12'] = df['Close'].ewm(span=12).mean()
    df['EMA_26'] = df['Close'].ewm(span=26).mean()
    
    # MACD
    df['MACD'] = df['EMA_12'] - df['EMA_26']
    df['MACD_Signal'] = df['MACD'].ewm(span=9).mean()
    df['MACD_Histogram'] = df['MACD'] - df['MACD_Signal']
    
    # RSI
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    
    # Bollinger Bands
    df['BB_Upper'] = df['SMA_20'] + (df['Close'].rolling(window=20).std() * 2)
    df['BB_Lower'] = df['SMA_20'] - (df['Close'].rolling(window=20).std() * 2)
    df['BB_Width'] = (df['BB_Upper'] - df['BB_Lower']) / df['SMA_20']
    
    # Volume indicators
    df['Volume_SMA'] = df['Volume'].rolling(window=20).mean()
    df['Volume_Ratio'] = df['Volume'] / df['Volume_SMA']
    
    # Volatility
    df['Volatility'] = df['Close'].rolling(window=20).std() / df['Close'].rolling(window=20).mean()
    
    return df

def generate_trading_signals(df, symbol):
    """Generate comprehensive trading signals"""
    
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    
    signals = {
        'symbol': symbol,
        'current_price': latest['Close'],
        'change_1d': ((latest['Close'] - prev['Close']) / prev['Close']) * 100,
        'signals': [],
        'score': 0,
        'recommendation': 'HOLD'
    }
    
    # Trend signals
    if latest['Close'] > latest['SMA_20'] > latest['SMA_50']:
        signals['signals'].append("🟢 Strong Uptrend (Price > SMA20 > SMA50)")
        signals['score'] += 2
    elif latest['Close'] > latest['SMA_20']:
        signals['signals'].append("🟡 Mild Uptrend (Price > SMA20)")
        signals['score'] += 1
    elif latest['Close'] < latest['SMA_20'] < latest['SMA_50']:
        signals['signals'].append("🔴 Strong Downtrend (Price < SMA20 < SMA50)")
        signals['score'] -= 2
    else:
        signals['signals'].append("🟡 Mild Downtrend (Price < SMA20)")
        signals['score'] -= 1
    
    # MACD signals
    if latest['MACD'] > latest['MACD_Signal'] and prev['MACD'] <= prev['MACD_Signal']:
        signals['signals'].append("🚀 MACD Bullish Crossover")
        signals['score'] += 2
    elif latest['MACD'] < latest['MACD_Signal'] and prev['MACD'] >= prev['MACD_Signal']:
        signals['signals'].append("📉 MACD Bearish Crossover")
        signals['score'] -= 2
    elif latest['MACD'] > latest['MACD_Signal']:
        signals['signals'].append("✅ MACD Above Signal")
        signals['score'] += 1
    else:
        signals['signals'].append("⚠️ MACD Below Signal")
        signals['score'] -= 1
    
    # RSI signals
    if latest['RSI'] < 30:
        signals['signals'].append("🟢 RSI Oversold (Potential Buy)")
        signals['score'] += 2
    elif latest['RSI'] > 70:
        signals['signals'].append("🔴 RSI Overbought (Potential Sell)")
        signals['score'] -= 2
    elif 40 <= latest['RSI'] <= 60:
        signals['signals'].append("🟡 RSI Neutral Zone")
    
    # Volume signals
    if latest['Volume_Ratio'] > 1.5:
        signals['signals'].append("📈 High Volume Confirmation")
        signals['score'] += 1
    elif latest['Volume_Ratio'] < 0.7:
        signals['signals'].append("📉 Low Volume Warning")
        signals['score'] -= 1
    
    # Bollinger Bands
    if latest['Close'] < latest['BB_Lower']:
        signals['signals'].append("🟢 Below Lower Bollinger Band (Oversold)")
        signals['score'] += 1
    elif latest['Close'] > latest['BB_Upper']:
        signals['signals'].append("🔴 Above Upper Bollinger Band (Overbought)")
        signals['score'] -= 1
    
    # Final recommendation
    if signals['score'] >= 3:
        signals['recommendation'] = 'STRONG BUY'
    elif signals['score'] >= 1:
        signals['recommendation'] = 'BUY'
    elif signals['score'] <= -3:
        signals['recommendation'] = 'STRONG SELL'
    elif signals['score'] <= -1:
        signals['recommendation'] = 'SELL'
    else:
        signals['recommendation'] = 'HOLD'
    
    return signals

# Main analysis
print("🔍 FETCHING MARKET DATA...")
all_results = []

for symbol in STOCKS:
    try:
        print(f"📊 Analyzing {symbol}...")
        
        # Get 3 months of data for analysis
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="3mo", interval="1d")
        
        if df.empty:
            print(f"❌ No data for {symbol}")
            continue
        
        # Calculate indicators
        df = calculate_technical_indicators(df)
        
        # Generate signals
        signals = generate_trading_signals(df, symbol)
        all_results.append(signals)
        
    except Exception as e:
        print(f"❌ Error analyzing {symbol}: {e}")

print("\n🎯 TOMORROW'S TRADING RECOMMENDATIONS")
print("=" * 60)

# Sort by recommendation strength
recommendation_order = {'STRONG BUY': 5, 'BUY': 4, 'HOLD': 3, 'SELL': 2, 'STRONG SELL': 1}
all_results.sort(key=lambda x: recommendation_order.get(x['recommendation'], 0), reverse=True)

for result in all_results:
    print(f"\n📈 {result['symbol']}")
    print(f"💰 Current Price: ${result['current_price']:.2f}")
    print(f"📊 24h Change: {result['change_1d']:+.2f}%")
    print(f"🎯 Recommendation: {result['recommendation']} (Score: {result['score']})")
    print("📋 Signals:")
    for signal in result['signals']:
        print(f"   {signal}")

# Market summary
print("\n📊 MARKET SUMMARY FOR TOMORROW")
print("=" * 40)

buy_stocks = [r for r in all_results if 'BUY' in r['recommendation']]
sell_stocks = [r for r in all_results if 'SELL' in r['recommendation']]
hold_stocks = [r for r in all_results if r['recommendation'] == 'HOLD']

print(f"🟢 BUY Recommendations: {len(buy_stocks)}")
if buy_stocks:
    for stock in buy_stocks:
        print(f"   📈 {stock['symbol']}: {stock['recommendation']}")

print(f"🔴 SELL Recommendations: {len(sell_stocks)}")
if sell_stocks:
    for stock in sell_stocks:
        print(f"   📉 {stock['symbol']}: {stock['recommendation']}")

print(f"🟡 HOLD Recommendations: {len(hold_stocks)}")

print("\n⚡ QUICK TRADING PLAN FOR TOMORROW:")
print("=" * 45)

if buy_stocks:
    print("🎯 FOCUS ON BUYING:")
    for stock in buy_stocks[:3]:  # Top 3 buy recommendations
        print(f"   • {stock['symbol']} at ${stock['current_price']:.2f}")

if sell_stocks:
    print("⚠️ CONSIDER SELLING:")
    for stock in sell_stocks[:2]:  # Top 2 sell recommendations
        print(f"   • {stock['symbol']} at ${stock['current_price']:.2f}")

print("\n📅 Tomorrow's Trading Session:")
print(f"🕘 Market Open: 9:30 AM EST")
print(f"🕐 Market Close: 4:00 PM EST")
print(f"⏰ Analysis Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S EST')}")

print("\n✅ ANALYSIS COMPLETE - Ready for tomorrow's trading!")