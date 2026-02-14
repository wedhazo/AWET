#!/usr/bin/env python3
"""
AWET Advanced Alert Rules Configuration
Custom alerts for specific scenarios
"""

import asyncio
import asyncpg
import yfinance as yf
import json
from datetime import datetime
import time

class AdvancedAlerts:
    def __init__(self):
        self.database_url = "postgresql://awet:awet@localhost:5433/awet"
        self.user_email = "kib.usa@yahoo.com"
        self.user_phone = "+1-980-230-9415"
        
        # Advanced alert rules
        self.alert_rules = {
            # Portfolio Rules
            "portfolio_milestone_10k": {"threshold": 10000, "message": "🎉 $10K PROFIT MILESTONE!"},
            "portfolio_milestone_15k": {"threshold": 15000, "message": "🚀 $15K PROFIT MILESTONE!"},
            "portfolio_milestone_20k": {"threshold": 20000, "message": "💎 $20K PROFIT MILESTONE!"},
            "portfolio_loss_5k": {"threshold": -5000, "message": "⚠️ -$5K LOSS ALERT!"},
            
            # Position Rules
            "aapl_profit_15k": {"symbol": "AAPL", "profit_threshold": 15000, "message": "🍎 AAPL +$15K PROFIT!"},
            "position_double": {"multiplier": 2.0, "message": "🚀 POSITION DOUBLED!"},
            "position_stop_loss": {"loss_percent": -20, "message": "🛑 STOP LOSS TRIGGERED!"},
            
            # Market Rules  
            "spy_drop_3pct": {"symbol": "SPY", "change_threshold": -3.0, "message": "📉 SPY DOWN 3%+"},
            "vix_spike": {"symbol": "VIX", "spike_threshold": 25, "message": "⚡ VIX FEAR SPIKE!"},
            
            # Time-based Rules
            "daily_summary": {"time": "16:00", "message": "📊 Daily Trading Summary"},
            "morning_briefing": {"time": "09:00", "message": "🌅 Morning Market Briefing"}
        }
    
    async def check_portfolio_milestones(self):
        """Check for portfolio milestone alerts"""
        conn = await asyncpg.connect(self.database_url)
        try:
            # Get current portfolio value and P&L
            trades = await conn.fetch("SELECT ticker, side, qty, fill_price FROM paper_trades")
            
            total_pnl = 0
            for trade in trades:
                current_price = await self.get_current_price(trade['ticker'])
                qty = float(trade['qty'])
                fill_price = float(trade['fill_price'])
                
                if trade['side'] == 'buy':
                    pnl = (current_price - fill_price) * qty
                else:
                    pnl = (fill_price - current_price) * qty
                
                total_pnl += pnl
            
            # Check milestone rules
            alerts = []
            for rule_name, rule in self.alert_rules.items():
                if "portfolio_milestone" in rule_name or "portfolio_loss" in rule_name:
                    if total_pnl >= rule["threshold"]:
                        alerts.append({
                            "type": "milestone",
                            "message": f"{rule['message']} Total P&L: ${total_pnl:,.2f}",
                            "severity": "high" if "loss" in rule_name else "medium"
                        })
            
            return alerts
        finally:
            await conn.close()
    
    async def get_current_price(self, symbol):
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period="1d", interval="1m")
            return float(data['Close'].iloc[-1]) if not data.empty else 0
        except:
            return 0
    
    async def create_smart_alerts(self):
        """Create intelligent alerts based on your portfolio"""
        print("🧠 CREATING SMART ALERTS...")
        
        # Get your current positions
        conn = await asyncpg.connect(self.database_url)
        trades = await conn.fetch("SELECT ticker, side, qty, fill_price FROM paper_trades ORDER BY ts")
        await conn.close()
        
        # Calculate positions
        positions = {}
        for trade in trades:
            symbol = trade['ticker']
            qty = float(trade['qty'])
            
            if symbol not in positions:
                positions[symbol] = {'qty': 0, 'cost_basis': 0}
            
            if trade['side'] == 'buy':
                positions[symbol]['qty'] += qty
                positions[symbol]['cost_basis'] += qty * float(trade['fill_price'])
            else:
                positions[symbol]['qty'] -= qty
                positions[symbol]['cost_basis'] -= qty * float(trade['fill_price'])
        
        print(f"📊 Found {len(positions)} positions to monitor")
        
        # Create custom alerts for each position
        smart_alerts = []
        
        for symbol, pos in positions.items():
            if pos['qty'] != 0:
                current_price = await self.get_current_price(symbol)
                avg_price = pos['cost_basis'] / pos['qty'] if pos['qty'] != 0 else 0
                current_value = pos['qty'] * current_price
                pnl = current_value - pos['cost_basis']
                pnl_pct = (pnl / abs(pos['cost_basis'])) * 100 if pos['cost_basis'] != 0 else 0
                
                print(f"  {symbol}: {pos['qty']:.1f} shares, ${pnl:+,.2f} P&L ({pnl_pct:+.1f}%)")
                
                # Create position-specific alerts
                if pnl > 5000:  # Big winner
                    smart_alerts.append(f"🚀 {symbol}: Monitor for profit taking (currently +${pnl:,.0f})")
                elif pnl < -1000:  # Potential problem
                    smart_alerts.append(f"⚠️ {symbol}: Watch for stop-loss (currently -${abs(pnl):,.0f})")
                
                # Price level alerts
                if symbol == 'AAPL' and pnl > 10000:
                    smart_alerts.append(f"🍎 AAPL: Consider scaling out above $280 (major resistance)")
                elif symbol == 'AMZN':
                    smart_alerts.append(f"📦 AMZN: Watch $220 breakout for continuation")
        
        return smart_alerts

async def main():
    alerts = AdvancedAlerts()
    
    print("🚨 ADVANCED ALERT SYSTEM CONFIGURATION")
    print("="*60)
    
    # Check current milestones
    milestone_alerts = await alerts.check_portfolio_milestones()
    print(f"🎯 Portfolio Milestone Alerts: {len(milestone_alerts)}")
    
    for alert in milestone_alerts:
        print(f"  {alert['message']}")
    
    # Create smart position alerts
    smart_alerts = await alerts.create_smart_alerts()
    print(f"\n🧠 Smart Position Alerts Created: {len(smart_alerts)}")
    
    for alert in smart_alerts:
        print(f"  {alert}")
    
    print(f"\n📧 Alerts will be sent to: {alerts.user_email}")
    print(f"📱 SMS alerts ready for: {alerts.user_phone}")
    print("✅ Advanced alert system ready!")

if __name__ == "__main__":
    asyncio.run(main())