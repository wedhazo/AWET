#!/usr/bin/env python3
"""
Quick Live Stock Demo - Shows real prices immediately
"""

import yfinance as yf
import pandas as pd
from datetime import datetime
import json

def get_live_demo():
    print("🚀 AWET LIVE STOCK DEMO")
    print("="*60)
    
    # TOP 20 American stocks
    symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA',
        'TSLA', 'META', 'BRK-B', 'UNH', 'JNJ', 
        'XOM', 'JPM', 'V', 'PG', 'HD',
        'MA', 'CVX', 'ABBV', 'PFE', 'KO'
    ]
    
    print(f"📊 Fetching LIVE prices for TOP 20 stocks...")
    print(f"⏰ Time: {datetime.now().strftime('%H:%M:%S')}")
    print("-" * 60)
    
    live_data = []
    
    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            # Get last 2 days to calculate change
            hist = ticker.history(period="2d", interval="1d")
            
            if len(hist) >= 1:
                current_price = float(hist['Close'].iloc[-1])
                volume = int(hist['Volume'].iloc[-1])
                
                # Calculate change
                if len(hist) >= 2:
                    prev_close = float(hist['Close'].iloc[-2])
                    change = current_price - prev_close
                    change_pct = (change / prev_close) * 100
                else:
                    change = 0
                    change_pct = 0
                
                live_data.append({
                    'symbol': symbol,
                    'price': current_price,
                    'change': change,
                    'change_pct': change_pct,
                    'volume': volume
                })
                
                # Color formatting
                color = "🟢" if change >= 0 else "🔴"
                sign = "+" if change >= 0 else ""
                
                print(f"{color} {symbol:>6}: ${current_price:>7.2f} {sign}{change:>6.2f} ({sign}{change_pct:>5.1f}%) Vol:{volume:>10,}")
            
        except Exception as e:
            print(f"❌ {symbol:>6}: Failed to fetch - {e}")
    
    print("-" * 60)
    print(f"✅ Successfully fetched {len(live_data)}/20 stocks")
    
    # Market summary
    if live_data:
        positive_stocks = sum(1 for stock in live_data if stock['change'] >= 0)
        negative_stocks = len(live_data) - positive_stocks
        avg_change = sum(stock['change_pct'] for stock in live_data) / len(live_data)
        
        print(f"📈 Market Summary:")
        print(f"   🟢 Positive: {positive_stocks} stocks")
        print(f"   🔴 Negative: {negative_stocks} stocks") 
        print(f"   📊 Average Change: {avg_change:+.2f}%")
    
    return live_data

if __name__ == "__main__":
    get_live_demo()