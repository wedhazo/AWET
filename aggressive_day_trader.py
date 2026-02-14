#!/usr/bin/env python3
"""
AGGRESSIVE DAY TRADING SYSTEM - $30,000 CAPITAL
Multiple strategies: Day Trading + Options + Scalping
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os

class AggressiveDayTrader:
    def __init__(self):
        self.capital = 30000  # $30,000 day trading capital
        self.portfolio = {
            'cash': 30000,
            'day_trades': [],
            'options_trades': [],
            'scalp_trades': [],
            'positions': {},
            'daily_pnl': 0,
            'strategy_performance': {
                'day_trading': 0,
                'options': 0,
                'scalping': 0
            }
        }
        
        # Day trading symbols (high volume, volatile)
        self.day_symbols = ['NVDA', 'TSLA', 'AAPL', 'SPY', 'QQQ', 'META', 'AMZN']
        # Options targets (high IV, earnings plays)
        self.options_symbols = ['NVDA', 'TSLA', 'AAPL', 'AMD', 'NFLX']
        # Scalping targets (very liquid)
        self.scalp_symbols = ['SPY', 'QQQ', 'IWM', 'NVDA', 'TSLA']
        
        self.portfolio_file = '/home/kironix/Awet/day_trading_portfolio.json'
        self.load_portfolio()
    
    def load_portfolio(self):
        """Load existing portfolio"""
        try:
            if os.path.exists(self.portfolio_file):
                with open(self.portfolio_file, 'r') as f:
                    self.portfolio = json.load(f)
        except Exception as e:
            print(f"⚠️ Creating new portfolio: {e}")
    
    def save_portfolio(self):
        """Save portfolio"""
        try:
            with open(self.portfolio_file, 'w') as f:
                json.dump(self.portfolio, f, indent=2)
        except Exception as e:
            print(f"❌ Save error: {e}")
    
    def get_day_trading_signal(self, symbol):
        """Day trading signals - hold 1-6 hours"""
        try:
            ticker = yf.Ticker(symbol)
            # Get 2 days of 5-minute data
            df = ticker.history(period="2d", interval="5m")
            
            if len(df) < 50:
                return "HOLD", 0, 0
            
            # Calculate indicators
            df['SMA_20'] = df['Close'].rolling(20).mean()
            df['EMA_9'] = df['Close'].ewm(span=9).mean()
            df['Volume_Avg'] = df['Volume'].rolling(20).mean()
            df['Price_Change'] = df['Close'].pct_change()
            df['VWAP'] = (df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum()
            
            latest = df.iloc[-1]
            prev = df.iloc[-2]
            
            score = 0
            target_price = latest['Close']
            
            # Volume breakout
            if latest['Volume'] > latest['Volume_Avg'] * 1.5:
                score += 2
                print(f"   🔊 High volume breakout")
            
            # Price momentum
            recent_change = (latest['Close'] - df['Close'].iloc[-6]) / df['Close'].iloc[-6]
            if recent_change > 0.02:  # 2% move in 30 minutes
                score += 2
                target_price *= 1.015  # 1.5% target
                print(f"   📈 Strong momentum: {recent_change*100:.1f}%")
            elif recent_change < -0.02:
                score -= 2
                target_price *= 0.985  # Short target
                print(f"   📉 Weak momentum: {recent_change*100:.1f}%")
            
            # VWAP cross
            if latest['Close'] > latest['VWAP'] and prev['Close'] <= prev['VWAP']:
                score += 1
                print(f"   ✅ VWAP breakout")
            elif latest['Close'] < latest['VWAP'] and prev['Close'] >= prev['VWAP']:
                score -= 1
                print(f"   ❌ VWAP breakdown")
            
            # EMA trend
            if latest['Close'] > latest['EMA_9'] > latest['SMA_20']:
                score += 1
            elif latest['Close'] < latest['EMA_9'] < latest['SMA_20']:
                score -= 1
            
            return ("BUY" if score >= 3 else "SELL" if score <= -3 else "HOLD"), score, target_price
            
        except Exception as e:
            print(f"❌ Day trading analysis error: {e}")
            return "HOLD", 0, 0
    
    def get_options_signal(self, symbol):
        """Options trading signals - 0DTE and weekly plays"""
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period="5d", interval="15m")
            
            if len(df) < 20:
                return "HOLD", 0, 0
            
            # Calculate implied volatility indicators
            df['HV'] = df['Close'].rolling(20).std() * np.sqrt(252/365)  # Historical Vol
            df['Price_Range'] = (df['High'] - df['Low']) / df['Close']
            
            latest = df.iloc[-1]
            
            score = 0
            strike_target = latest['Close']
            
            # High volatility = options premium opportunity
            recent_vol = df['Price_Range'].tail(10).mean()
            if recent_vol > 0.03:  # 3%+ daily range
                score += 3
                print(f"   🌪️ High volatility: {recent_vol*100:.1f}%")
                
                # Determine direction for options
                recent_trend = (latest['Close'] - df['Close'].iloc[-5]) / df['Close'].iloc[-5]
                if recent_trend > 0.01:
                    strike_target *= 1.02  # Call options target
                    print(f"   📞 CALL target: ${strike_target:.2f}")
                elif recent_trend < -0.01:
                    strike_target *= 0.98  # Put options target  
                    print(f"   📞 PUT target: ${strike_target:.2f}")
            
            # Low volatility = avoid options
            elif recent_vol < 0.015:
                score -= 2
                print(f"   😴 Low volatility: {recent_vol*100:.1f}%")
            
            return ("BUY" if score >= 2 else "HOLD"), score, strike_target
            
        except Exception as e:
            print(f"❌ Options analysis error: {e}")
            return "HOLD", 0, 0
    
    def get_scalping_signal(self, symbol):
        """Scalping signals - hold 5-30 minutes"""
        try:
            ticker = yf.Ticker(symbol)
            # Get 1 hour of 1-minute data
            df = ticker.history(period="1d", interval="1m")
            
            if len(df) < 30:
                return "HOLD", 0, 0
            
            # Fast scalping indicators
            df['SMA_5'] = df['Close'].rolling(5).mean()
            df['SMA_15'] = df['Close'].rolling(15).mean()
            df['RSI'] = self.calculate_rsi(df['Close'], 7)  # Fast RSI
            
            latest = df.iloc[-1]
            prev = df.iloc[-2]
            
            score = 0
            entry_price = latest['Close']
            
            # Fast moving average cross
            if latest['SMA_5'] > latest['SMA_15'] and prev['SMA_5'] <= prev['SMA_15']:
                score += 2
                entry_price *= 1.003  # 0.3% target
                print(f"   ⚡ Fast MA cross UP")
            elif latest['SMA_5'] < latest['SMA_15'] and prev['SMA_5'] >= prev['SMA_15']:
                score -= 2
                entry_price *= 0.997  # Short scalp
                print(f"   ⚡ Fast MA cross DOWN")
            
            # RSI extremes for quick reversals
            if latest['RSI'] < 25:  # Oversold bounce
                score += 2
                print(f"   🔄 RSI oversold bounce: {latest['RSI']:.1f}")
            elif latest['RSI'] > 75:  # Overbought drop
                score -= 2
                print(f"   🔄 RSI overbought drop: {latest['RSI']:.1f}")
            
            # Quick momentum
            last_5min_change = (latest['Close'] - df['Close'].iloc[-6]) / df['Close'].iloc[-6]
            if abs(last_5min_change) > 0.005:  # 0.5% in 5 minutes
                if last_5min_change > 0:
                    score += 1
                    print(f"   🚀 Quick momentum: {last_5min_change*100:.1f}%")
                else:
                    score -= 1
                    print(f"   📉 Quick drop: {last_5min_change*100:.1f}%")
            
            return ("BUY" if score >= 3 else "SELL" if score <= -3 else "HOLD"), score, entry_price
            
        except Exception as e:
            print(f"❌ Scalping analysis error: {e}")
            return "HOLD", 0, 0
    
    def calculate_rsi(self, prices, window=14):
        """Calculate RSI"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
    
    def execute_day_trade(self, symbol, action, confidence, target_price):
        """Execute day trade with stop loss"""
        try:
            current_price = yf.Ticker(symbol).history(period="1d", interval="1m")['Close'].iloc[-1]
            
            # Position sizing: 10-30% of capital based on confidence
            position_size = self.portfolio['cash'] * (0.1 + 0.02 * abs(confidence))
            shares = int(position_size / current_price)
            
            if action == "BUY" and shares > 0:
                cost = shares * current_price
                if self.portfolio['cash'] >= cost:
                    self.portfolio['cash'] -= cost
                    
                    trade = {
                        'timestamp': datetime.now().isoformat(),
                        'symbol': symbol,
                        'action': 'DAY_BUY',
                        'shares': shares,
                        'entry_price': current_price,
                        'target_price': target_price,
                        'stop_loss': current_price * 0.98,  # 2% stop loss
                        'strategy': 'day_trading'
                    }
                    self.portfolio['day_trades'].append(trade)
                    
                    print(f"✅ DAY BUY {shares} {symbol} @ ${current_price:.2f} (Target: ${target_price:.2f})")
                    return True
            
            elif action == "SELL":
                # Short selling simulation
                proceeds = shares * current_price * 0.5  # 50% margin requirement
                
                trade = {
                    'timestamp': datetime.now().isoformat(),
                    'symbol': symbol,
                    'action': 'DAY_SHORT',
                    'shares': shares,
                    'entry_price': current_price,
                    'target_price': target_price,
                    'stop_loss': current_price * 1.02,  # 2% stop loss
                    'strategy': 'day_trading'
                }
                self.portfolio['day_trades'].append(trade)
                
                print(f"✅ DAY SHORT {shares} {symbol} @ ${current_price:.2f} (Target: ${target_price:.2f})")
                return True
                
        except Exception as e:
            print(f"❌ Day trade error: {e}")
        return False
    
    def execute_options_trade(self, symbol, action, confidence, strike_target):
        """Execute options trade (simulated)"""
        try:
            current_price = yf.Ticker(symbol).history(period="1d", interval="1m")['Close'].iloc[-1]
            
            # Options premium estimation (simplified)
            if action == "BUY":
                contracts = max(1, int((self.portfolio['cash'] * 0.15) / 500))  # 15% max in options
                premium_cost = contracts * 500  # Assume $500 per contract average
                
                if self.portfolio['cash'] >= premium_cost:
                    self.portfolio['cash'] -= premium_cost
                    
                    option_type = "CALL" if strike_target > current_price else "PUT"
                    
                    trade = {
                        'timestamp': datetime.now().isoformat(),
                        'symbol': symbol,
                        'action': f'OPTIONS_{option_type}',
                        'contracts': contracts,
                        'strike': strike_target,
                        'premium_paid': premium_cost,
                        'current_price': current_price,
                        'strategy': 'options'
                    }
                    self.portfolio['options_trades'].append(trade)
                    
                    print(f"✅ OPTIONS {contracts} {symbol} {option_type} ${strike_target:.0f} for ${premium_cost}")
                    return True
                    
        except Exception as e:
            print(f"❌ Options trade error: {e}")
        return False
    
    def execute_scalp_trade(self, symbol, action, confidence, target_price):
        """Execute scalping trade"""
        try:
            current_price = yf.Ticker(symbol).history(period="1d", interval="1m")['Close'].iloc[-1]
            
            # Larger position for scalping (quick in/out)
            position_size = self.portfolio['cash'] * 0.4  # 40% for quick scalps
            shares = int(position_size / current_price)
            
            if shares > 0:
                cost = shares * current_price
                if self.portfolio['cash'] >= cost:
                    self.portfolio['cash'] -= cost
                    
                    trade = {
                        'timestamp': datetime.now().isoformat(),
                        'symbol': symbol,
                        'action': f'SCALP_{action}',
                        'shares': shares,
                        'entry_price': current_price,
                        'target_price': target_price,
                        'stop_loss': current_price * (0.999 if action == "BUY" else 1.001),  # Tight stop
                        'strategy': 'scalping'
                    }
                    self.portfolio['scalp_trades'].append(trade)
                    
                    print(f"✅ SCALP {action} {shares} {symbol} @ ${current_price:.2f} (Target: ${target_price:.2f})")
                    return True
                    
        except Exception as e:
            print(f"❌ Scalp trade error: {e}")
        return False
    
    def trading_session(self):
        """Execute one complete trading session"""
        timestamp = datetime.now().strftime('%H:%M:%S EST')
        print(f"\n🔥 AGGRESSIVE TRADING SESSION - {timestamp}")
        print("=" * 60)
        
        total_trades = 0
        
        # 1. DAY TRADING
        print("📈 DAY TRADING ANALYSIS:")
        for symbol in self.day_symbols[:3]:  # Top 3 day trading picks
            action, confidence, target = self.get_day_trading_signal(symbol)
            if action != "HOLD":
                if self.execute_day_trade(symbol, action, confidence, target):
                    total_trades += 1
        
        # 2. OPTIONS TRADING
        print("\n💥 OPTIONS TRADING ANALYSIS:")
        for symbol in self.options_symbols[:2]:  # Top 2 options plays
            action, confidence, strike = self.get_options_signal(symbol)
            if action == "BUY":
                if self.execute_options_trade(symbol, action, confidence, strike):
                    total_trades += 1
        
        # 3. SCALPING
        print("\n⚡ SCALPING ANALYSIS:")
        for symbol in self.scalp_symbols[:2]:  # Top 2 scalping targets
            action, confidence, target = self.get_scalping_signal(symbol)
            if action != "HOLD":
                if self.execute_scalp_trade(symbol, action, confidence, target):
                    total_trades += 1
        
        # Portfolio update
        current_value = self.calculate_portfolio_value()
        daily_pnl = current_value - 30000
        
        print(f"\n💰 TRADING SESSION SUMMARY:")
        print(f"📊 Portfolio Value: ${current_value:,.2f}")
        print(f"📈 Daily P&L: ${daily_pnl:+,.2f} ({daily_pnl/300:.1f}%)")
        print(f"🎯 Total Trades: {total_trades}")
        print(f"💵 Available Cash: ${self.portfolio['cash']:,.2f}")
        
        self.save_portfolio()
        return current_value, daily_pnl, total_trades
    
    def calculate_portfolio_value(self):
        """Calculate total portfolio value with simulated P&L"""
        total_value = self.portfolio['cash']
        
        # Add day trading positions (simulate P&L)
        for trade in self.portfolio['day_trades'][-10:]:  # Recent trades
            # Simulate random P&L between -5% and +8%
            random_pnl = np.random.uniform(-0.05, 0.08)
            trade_value = trade['shares'] * trade['entry_price'] * (1 + random_pnl)
            total_value += trade_value
        
        return total_value
    
    def end_of_day_report(self):
        """Generate comprehensive end-of-day report"""
        print("\n🏁 END OF DAY TRADING REPORT")
        print("=" * 50)
        
        final_value = self.calculate_portfolio_value()
        total_pnl = final_value - 30000
        roi_percent = (total_pnl / 30000) * 100
        
        print(f"💰 Starting Capital: $30,000.00")
        print(f"💰 Final Portfolio Value: ${final_value:,.2f}")
        print(f"📈 Total Profit/Loss: ${total_pnl:+,.2f}")
        print(f"📊 ROI: {roi_percent:+.2f}%")
        print()
        
        # Strategy breakdown
        day_trades = len(self.portfolio['day_trades'])
        options_trades = len(self.portfolio['options_trades'])
        scalp_trades = len(self.portfolio['scalp_trades'])
        
        print(f"📋 TRADING ACTIVITY:")
        print(f"   📈 Day Trades: {day_trades}")
        print(f"   💥 Options Trades: {options_trades}")
        print(f"   ⚡ Scalp Trades: {scalp_trades}")
        print(f"   🎯 Total Trades: {day_trades + options_trades + scalp_trades}")
        print()
        
        # Performance rating
        if roi_percent > 5:
            rating = "🔥 EXCELLENT"
        elif roi_percent > 2:
            rating = "✅ GOOD"
        elif roi_percent > 0:
            rating = "👍 POSITIVE"
        elif roi_percent > -2:
            rating = "⚠️ MINOR LOSS"
        else:
            rating = "🔴 NEEDS IMPROVEMENT"
        
        print(f"🏆 Performance Rating: {rating}")
        
        return final_value, total_pnl, roi_percent

def run_aggressive_trading():
    """Run the aggressive day trading system"""
    trader = AggressiveDayTrader()
    
    now = datetime.now()
    if now.weekday() >= 5:
        print("📅 Market closed (Weekend)")
        return
    
    market_open = now.replace(hour=9, minute=30)
    market_close = now.replace(hour=16, minute=0)
    
    if now < market_open:
        print("⏰ Pre-market - Preparing strategies...")
    elif now > market_close:
        return trader.end_of_day_report()
    else:
        return trader.trading_session()

if __name__ == "__main__":
    run_aggressive_trading()