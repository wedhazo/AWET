#!/usr/bin/env python3
"""
AUTOMATED 30-MINUTE TRADING BOT
Paper trading with real-time decisions every 30 minutes
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import json
import os
from pathlib import Path

class AutoTrader:
    def __init__(self):
        self.portfolio = {
            'cash': 100000,  # Starting with $100k paper money
            'positions': {},
            'trades': [],
            'daily_pnl': 0
        }
        self.symbols = ['NVDA', 'TSLA', 'META', 'AAPL', 'MSFT', 'GOOGL', 'AMZN']
        self.portfolio_file = '/home/kironix/Awet/automated_portfolio.json'
        self.load_portfolio()
        
    def load_portfolio(self):
        """Load existing portfolio if available"""
        try:
            if os.path.exists(self.portfolio_file):
                with open(self.portfolio_file, 'r') as f:
                    self.portfolio = json.load(f)
                print("📂 Portfolio loaded from file")
        except Exception as e:
            print(f"⚠️ Could not load portfolio: {e}")
    
    def save_portfolio(self):
        """Save portfolio to file"""
        try:
            with open(self.portfolio_file, 'w') as f:
                json.dump(self.portfolio, f, indent=2)
        except Exception as e:
            print(f"❌ Could not save portfolio: {e}")
    
    def get_market_signal(self, symbol):
        """Get real-time trading signal for a symbol"""
        try:
            # Get recent data
            ticker = yf.Ticker(symbol)
            df = ticker.history(period="5d", interval="5m")
            
            if len(df) < 50:
                return "HOLD", 0
            
            # Calculate quick indicators
            df['SMA_20'] = df['Close'].rolling(window=20).mean()
            df['RSI'] = self.calculate_rsi(df['Close'], 14)
            df['Price_Change'] = df['Close'].pct_change()
            
            latest = df.iloc[-1]
            prev = df.iloc[-2]
            
            score = 0
            
            # Trend signals
            if latest['Close'] > latest['SMA_20']:
                score += 1
            else:
                score -= 1
            
            # RSI signals
            if latest['RSI'] < 35:  # Oversold
                score += 2
            elif latest['RSI'] > 65:  # Overbought
                score -= 2
            
            # Momentum
            if latest['Price_Change'] > 0.005:  # +0.5%
                score += 1
            elif latest['Price_Change'] < -0.005:  # -0.5%
                score -= 1
            
            # Recent volatility
            recent_vol = df['Price_Change'].tail(10).std()
            if recent_vol > 0.02:  # High volatility
                score *= 0.8  # Reduce confidence
            
            # Decision logic
            if score >= 2:
                return "BUY", min(score, 5)
            elif score <= -2:
                return "SELL", max(score, -5)
            else:
                return "HOLD", score
                
        except Exception as e:
            print(f"❌ Error analyzing {symbol}: {e}")
            return "HOLD", 0
    
    def calculate_rsi(self, prices, window=14):
        """Calculate RSI"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
    
    def execute_trade(self, symbol, action, confidence):
        """Execute a paper trade"""
        try:
            ticker = yf.Ticker(symbol)
            current_price = ticker.history(period="1d", interval="1m")['Close'].iloc[-1]
            
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            if action == "BUY":
                # Calculate position size based on confidence and available cash
                max_position_value = self.portfolio['cash'] * 0.1  # Max 10% per trade
                position_value = max_position_value * (confidence / 5.0)
                shares = int(position_value / current_price)
                
                if shares > 0 and self.portfolio['cash'] >= shares * current_price:
                    cost = shares * current_price
                    self.portfolio['cash'] -= cost
                    
                    if symbol in self.portfolio['positions']:
                        self.portfolio['positions'][symbol]['shares'] += shares
                        self.portfolio['positions'][symbol]['avg_price'] = (
                            (self.portfolio['positions'][symbol]['avg_price'] * 
                             (self.portfolio['positions'][symbol]['shares'] - shares) + cost) /
                            self.portfolio['positions'][symbol]['shares']
                        )
                    else:
                        self.portfolio['positions'][symbol] = {
                            'shares': shares,
                            'avg_price': current_price
                        }
                    
                    trade = {
                        'timestamp': timestamp,
                        'symbol': symbol,
                        'action': 'BUY',
                        'shares': shares,
                        'price': current_price,
                        'value': cost,
                        'confidence': confidence
                    }
                    self.portfolio['trades'].append(trade)
                    
                    print(f"✅ BUY {shares} {symbol} @ ${current_price:.2f} = ${cost:.2f}")
                    return True
            
            elif action == "SELL" and symbol in self.portfolio['positions']:
                # Sell percentage based on confidence
                position = self.portfolio['positions'][symbol]
                sell_ratio = min(abs(confidence) / 5.0, 1.0)
                shares_to_sell = int(position['shares'] * sell_ratio)
                
                if shares_to_sell > 0:
                    proceeds = shares_to_sell * current_price
                    self.portfolio['cash'] += proceeds
                    
                    self.portfolio['positions'][symbol]['shares'] -= shares_to_sell
                    
                    if self.portfolio['positions'][symbol]['shares'] <= 0:
                        del self.portfolio['positions'][symbol]
                    
                    trade = {
                        'timestamp': timestamp,
                        'symbol': symbol,
                        'action': 'SELL',
                        'shares': shares_to_sell,
                        'price': current_price,
                        'value': proceeds,
                        'confidence': confidence
                    }
                    self.portfolio['trades'].append(trade)
                    
                    print(f"✅ SELL {shares_to_sell} {symbol} @ ${current_price:.2f} = ${proceeds:.2f}")
                    return True
            
            return False
            
        except Exception as e:
            print(f"❌ Trade execution error: {e}")
            return False
    
    def calculate_portfolio_value(self):
        """Calculate total portfolio value"""
        total_value = self.portfolio['cash']
        
        for symbol, position in self.portfolio['positions'].items():
            try:
                current_price = yf.Ticker(symbol).history(period="1d", interval="1m")['Close'].iloc[-1]
                position_value = position['shares'] * current_price
                total_value += position_value
            except:
                continue
        
        return total_value
    
    def trading_cycle(self):
        """Execute one 30-minute trading cycle"""
        print(f"\n🤖 TRADING CYCLE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S EST')}")
        print("=" * 60)
        
        decisions = []
        
        for symbol in self.symbols:
            action, confidence = self.get_market_signal(symbol)
            
            print(f"📊 {symbol}: {action} (Confidence: {confidence})")
            
            if action in ["BUY", "SELL"]:
                success = self.execute_trade(symbol, action, confidence)
                if success:
                    decisions.append(f"{action} {symbol}")
        
        # Portfolio summary
        portfolio_value = self.calculate_portfolio_value()
        print(f"\n💰 Portfolio Value: ${portfolio_value:,.2f}")
        print(f"💵 Available Cash: ${self.portfolio['cash']:,.2f}")
        
        if self.portfolio['positions']:
            print("📈 Current Positions:")
            for symbol, pos in self.portfolio['positions'].items():
                try:
                    current_price = yf.Ticker(symbol).history(period="1d", interval="1m")['Close'].iloc[-1]
                    position_value = pos['shares'] * current_price
                    pnl = (current_price - pos['avg_price']) * pos['shares']
                    print(f"   {symbol}: {pos['shares']} shares @ ${pos['avg_price']:.2f} → ${current_price:.2f} = ${position_value:.2f} (P&L: ${pnl:+.2f})")
                except:
                    continue
        
        self.save_portfolio()
        return decisions

def run_trading_bot():
    """Main function to run the automated trading bot"""
    
    trader = AutoTrader()
    
    print("🚀 AUTOMATED TRADING BOT STARTED")
    print("=" * 50)
    print("📅 Paper Trading Mode - No Real Money")
    print("⏰ Trading every 30 minutes during market hours")
    print("🕘 Market Hours: 9:30 AM - 4:00 PM EST")
    print()
    
    # Market hours check
    now = datetime.now()
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    
    # Weekend check
    if now.weekday() >= 5:  # Saturday or Sunday
        print("📅 Market is closed (Weekend)")
        return
    
    if now < market_open:
        print(f"⏰ Market opens in {market_open - now}")
        print("🔍 Running pre-market analysis...")
        trader.trading_cycle()
        return
    
    if now > market_close:
        print("📈 FINAL DAILY REPORT")
        print("=" * 30)
        
        final_value = trader.calculate_portfolio_value()
        daily_pnl = final_value - 100000  # Starting amount
        
        print(f"💰 Final Portfolio Value: ${final_value:,.2f}")
        print(f"📊 Daily P&L: ${daily_pnl:+,.2f} ({daily_pnl/1000:+.1f}%)")
        
        if trader.portfolio['trades']:
            print(f"📋 Total Trades Today: {len(trader.portfolio['trades'])}")
            
        return final_value, daily_pnl
    
    # During market hours - execute trading cycle
    decisions = trader.trading_cycle()
    
    return trader.calculate_portfolio_value()

if __name__ == "__main__":
    result = run_trading_bot()
    print(f"\n✅ Trading cycle complete!")