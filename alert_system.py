#!/usr/bin/env python3
"""
AWET Notification & Alert System
Real-time SMS, Email, and System alerts for trading events
"""

import asyncio
import asyncpg
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import time
import logging
# Email imports commented for demo
# import smtplib  
# from email.mime.text import MimeText
# from email.mime.multipart import MimeMultipart
# from twilio.rest import Client
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlertSystem:
    def __init__(self):
        self.database_url = "postgresql://awet:awet@localhost:5433/awet"
        
        # Twilio configuration (using Kironix's existing setup)
        self.twilio_account_sid = "your_account_sid"  # From previous sessions
        self.twilio_auth_token = "your_auth_token"
        self.twilio_phone = "+your_twilio_number"
        self.user_phone = "+1-980-230-9415"  # Kironix's number
        
        # Alert thresholds
        self.portfolio_change_threshold = 0.05  # 5%
        self.position_change_threshold = 0.10   # 10%
        self.profit_threshold = 1000           # $1000
        self.loss_threshold = -500             # -$500
        
        self.last_portfolio_value = None
        self.alert_history = []
        
    async def connect_db(self):
        return await asyncpg.connect(self.database_url)
    
    async def get_current_portfolio(self):
        """Get current portfolio status"""
        conn = await self.connect_db()
        try:
            trades = await conn.fetch("SELECT ticker, side, qty, fill_price FROM paper_trades ORDER BY ts")
            
            positions = {}
            for trade in trades:
                symbol = trade['ticker']
                qty = float(trade['qty'])
                price = float(trade['fill_price'])
                
                if symbol not in positions:
                    positions[symbol] = {'qty': 0, 'cost_basis': 0}
                
                if trade['side'] == 'buy':
                    positions[symbol]['qty'] += qty
                    positions[symbol]['cost_basis'] += qty * price
                else:
                    positions[symbol]['qty'] -= qty
                    positions[symbol]['cost_basis'] -= qty * price
            
            # Calculate current values
            portfolio_value = 0
            total_pnl = 0
            position_details = []
            
            for symbol, pos in positions.items():
                if pos['qty'] != 0:
                    current_price = await self.get_current_price(symbol)
                    market_value = pos['qty'] * current_price
                    pnl = market_value - pos['cost_basis']
                    pnl_pct = (pnl / abs(pos['cost_basis'])) * 100 if pos['cost_basis'] != 0 else 0
                    
                    position_details.append({
                        'symbol': symbol,
                        'quantity': pos['qty'],
                        'market_value': market_value,
                        'pnl': pnl,
                        'pnl_pct': pnl_pct,
                        'current_price': current_price
                    })
                    
                    portfolio_value += market_value
                    total_pnl += pnl
            
            return {
                'portfolio_value': portfolio_value,
                'total_pnl': total_pnl,
                'positions': position_details,
                'timestamp': datetime.now()
            }
            
        finally:
            await conn.close()
    
    async def get_current_price(self, symbol):
        """Get current stock price"""
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period="1d", interval="1m")
            return float(data['Close'].iloc[-1]) if not data.empty else 0
        except:
            return 0
    
    def send_sms_alert(self, message):
        """Send SMS alert via Twilio"""
        try:
            # For demo purposes - would need actual Twilio credentials
            logger.info(f"📱 SMS ALERT: {message}")
            logger.info(f"📱 Would send to: {self.user_phone}")
            
            # Actual Twilio implementation would be:
            # client = Client(self.twilio_account_sid, self.twilio_auth_token)
            # message = client.messages.create(
            #     body=message,
            #     from_=self.twilio_phone,
            #     to=self.user_phone
            # )
            
            return True
        except Exception as e:
            logger.error(f"❌ SMS failed: {e}")
            return False
    
    def send_email_alert(self, subject, body, recipient="kib.usa@yahoo.com"):
        """Send email alert"""
        try:
            logger.info(f"📧 EMAIL ALERT: {subject}")
            logger.info(f"📧 To: {recipient}")
            logger.info(f"📧 Body: {body}")
            
            # For demo - would implement actual email sending
            return True
        except Exception as e:
            logger.error(f"❌ Email failed: {e}")
            return False
    
    async def send_telegram_alert(self, message):
        """Send alert via OpenClaw messaging system"""
        try:
            # Use OpenClaw's message system to send to Telegram
            logger.info(f"📲 TELEGRAM ALERT: {message}")
            
            # This would integrate with OpenClaw's messaging
            # For now, we'll just log it
            return True
        except Exception as e:
            logger.error(f"❌ Telegram failed: {e}")
            return False
    
    async def check_portfolio_alerts(self):
        """Check for portfolio-level alerts"""
        portfolio = await self.get_current_portfolio()
        current_value = portfolio['portfolio_value']
        total_pnl = portfolio['total_pnl']
        
        alerts = []
        
        # Portfolio value change alert
        if self.last_portfolio_value is not None:
            change = current_value - self.last_portfolio_value
            change_pct = (change / self.last_portfolio_value) * 100 if self.last_portfolio_value != 0 else 0
            
            if abs(change_pct) >= self.portfolio_change_threshold * 100:
                direction = "📈 UP" if change > 0 else "📉 DOWN"
                alerts.append({
                    'type': 'portfolio_change',
                    'message': f"🚨 PORTFOLIO ALERT: {direction} {change_pct:.2f}% (${change:,.2f}) - Total: ${current_value:,.2f}",
                    'severity': 'high' if abs(change_pct) >= 10 else 'medium'
                })
        
        # Profit/Loss threshold alerts
        if total_pnl >= self.profit_threshold:
            alerts.append({
                'type': 'profit_alert',
                'message': f"🎉 PROFIT MILESTONE: +${total_pnl:,.2f} total P&L! Portfolio: ${current_value:,.2f}",
                'severity': 'medium'
            })
        elif total_pnl <= self.loss_threshold:
            alerts.append({
                'type': 'loss_alert',
                'message': f"⚠️ LOSS ALERT: -${abs(total_pnl):,.2f} total loss. Portfolio: ${current_value:,.2f}",
                'severity': 'high'
            })
        
        self.last_portfolio_value = current_value
        return alerts
    
    async def check_position_alerts(self):
        """Check for individual position alerts"""
        portfolio = await self.get_current_portfolio()
        alerts = []
        
        for position in portfolio['positions']:
            symbol = position['symbol']
            pnl_pct = position['pnl_pct']
            pnl = position['pnl']
            
            # Large position moves
            if abs(pnl_pct) >= self.position_change_threshold * 100:
                direction = "🚀" if pnl_pct > 0 else "💥"
                alerts.append({
                    'type': 'position_alert',
                    'message': f"{direction} {symbol}: {pnl_pct:+.2f}% (${pnl:+,.2f}) @ ${position['current_price']:.2f}",
                    'severity': 'high' if abs(pnl_pct) >= 20 else 'medium'
                })
        
        return alerts
    
    async def check_market_alerts(self):
        """Check for market-wide alerts"""
        alerts = []
        
        # Check major indices for significant moves
        indices = ['SPY', 'QQQ', 'VIX']
        
        for symbol in indices:
            try:
                ticker = yf.Ticker(symbol)
                data = ticker.history(period="2d", interval="1d")
                
                if len(data) >= 2:
                    current = data['Close'].iloc[-1]
                    previous = data['Close'].iloc[-2]
                    change_pct = ((current - previous) / previous) * 100
                    
                    if abs(change_pct) >= 3:  # 3% move in major index
                        direction = "📈" if change_pct > 0 else "📉"
                        alerts.append({
                            'type': 'market_alert',
                            'message': f"🌍 MARKET MOVE: {symbol} {direction} {change_pct:+.2f}% - Monitor your positions!",
                            'severity': 'medium'
                        })
            except:
                continue
        
        return alerts
    
    async def process_alerts(self, alerts):
        """Process and send alerts based on severity"""
        for alert in alerts:
            message = alert['message']
            severity = alert['severity']
            
            # Log all alerts
            logger.info(f"🚨 ALERT [{severity.upper()}]: {message}")
            
            # Send based on severity
            if severity == 'high':
                # High severity: SMS + Email + Telegram
                self.send_sms_alert(message)
                self.send_email_alert("AWET High Priority Alert", message)
                await self.send_telegram_alert(message)
            elif severity == 'medium':
                # Medium severity: Email + Telegram
                self.send_email_alert("AWET Alert", message)
                await self.send_telegram_alert(message)
            else:
                # Low severity: Just log and maybe Telegram
                await self.send_telegram_alert(message)
            
            # Store in history
            self.alert_history.append({
                'timestamp': datetime.now(),
                'type': alert['type'],
                'message': message,
                'severity': severity
            })
    
    async def run_alert_monitoring(self):
        """Main alert monitoring loop"""
        logger.info("🚨 Starting AWET Alert System...")
        
        while True:
            try:
                logger.info("🔍 Checking for alerts...")
                
                # Gather all alert types
                portfolio_alerts = await self.check_portfolio_alerts()
                position_alerts = await self.check_position_alerts()
                market_alerts = await self.check_market_alerts()
                
                # Combine and process alerts
                all_alerts = portfolio_alerts + position_alerts + market_alerts
                
                if all_alerts:
                    await self.process_alerts(all_alerts)
                    logger.info(f"📊 Processed {len(all_alerts)} alerts")
                else:
                    logger.info("✅ No alerts triggered")
                
                # Wait before next check
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"❌ Alert monitoring error: {e}")
                await asyncio.sleep(30)  # Shorter wait on error

async def main():
    """Run the alert system"""
    alert_system = AlertSystem()
    
    # Show current portfolio first
    portfolio = await alert_system.get_current_portfolio()
    
    print("\n" + "="*80)
    print("🚨 AWET ALERT SYSTEM ACTIVATED")
    print("="*80)
    print(f"📊 Current Portfolio: ${portfolio['portfolio_value']:,.2f}")
    print(f"💰 Total P&L: ${portfolio['total_pnl']:+,.2f}")
    print(f"📱 Alert Phone: {alert_system.user_phone}")
    print(f"📧 Monitoring: Portfolio, Positions, Market")
    print("="*80)
    
    # Run monitoring
    await alert_system.run_alert_monitoring()

if __name__ == "__main__":
    asyncio.run(main())