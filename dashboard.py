#!/usr/bin/env python3
"""
AWET Enhanced Live Trading Dashboard
Real-time trading analytics with beautiful visualizations
"""

import asyncio
import asyncpg
import json
import time
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify
import yfinance as yf
import pandas as pd
import numpy as np
from threading import Thread
import plotly.graph_objs as go
import plotly.utils

app = Flask(__name__)

class LiveTradingDashboard:
    def __init__(self):
        self.database_url = "postgresql://awet:awet@localhost:5433/awet"
        self.live_data = {
            "portfolio_value": 0,
            "daily_pnl": 0,
            "total_pnl": 0,
            "active_positions": [],
            "recent_trades": [],
            "performance_metrics": {},
            "live_prices": {},
            "signals": []
        }
        self.symbols = ['AAPL', 'MSFT', 'NVDA', 'TSLA']
        
    async def connect_db(self):
        """Connect to database"""
        return await asyncpg.connect(self.database_url)
    
    async def get_portfolio_summary(self):
        """Get current portfolio status"""
        conn = await self.connect_db()
        try:
            # Get all trades
            trades = await conn.fetch("""
                SELECT ticker, side, qty, fill_price, ts 
                FROM paper_trades 
                ORDER BY ts DESC
            """)
            
            # Calculate positions
            positions = {}
            for trade in trades:
                symbol = trade['ticker']
                qty = float(trade['qty'])
                price = float(trade['fill_price'])
                
                if symbol not in positions:
                    positions[symbol] = {'qty': 0, 'avg_price': 0, 'total_cost': 0}
                
                if trade['side'] == 'buy':
                    old_value = positions[symbol]['qty'] * positions[symbol]['avg_price']
                    new_qty = positions[symbol]['qty'] + qty
                    new_value = old_value + (qty * price)
                    positions[symbol]['avg_price'] = new_value / new_qty if new_qty > 0 else 0
                    positions[symbol]['qty'] = new_qty
                else:  # sell
                    positions[symbol]['qty'] -= qty
                    
                positions[symbol]['total_cost'] += qty * price if trade['side'] == 'buy' else -qty * price
            
            # Get current prices and calculate P&L
            portfolio_value = 0
            total_cost = 0
            active_positions = []
            
            for symbol, pos in positions.items():
                if pos['qty'] != 0:  # Active position
                    current_price = await self.get_current_price(symbol)
                    market_value = pos['qty'] * current_price
                    pnl = market_value - (pos['qty'] * pos['avg_price'])
                    pnl_pct = (pnl / (pos['qty'] * pos['avg_price'])) * 100 if pos['avg_price'] > 0 else 0
                    
                    active_positions.append({
                        'symbol': symbol,
                        'quantity': pos['qty'],
                        'avg_price': pos['avg_price'],
                        'current_price': current_price,
                        'market_value': market_value,
                        'pnl': pnl,
                        'pnl_pct': pnl_pct
                    })
                    
                    portfolio_value += market_value
                    total_cost += pos['qty'] * pos['avg_price']
            
            total_pnl = portfolio_value - total_cost
            daily_pnl = await self.calculate_daily_pnl()
            
            return {
                'portfolio_value': portfolio_value,
                'total_pnl': total_pnl,
                'daily_pnl': daily_pnl,
                'active_positions': active_positions,
                'total_cost': total_cost
            }
            
        finally:
            await conn.close()
    
    async def get_current_price(self, symbol):
        """Get current stock price"""
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period="1d", interval="1m")
            if not data.empty:
                return float(data['Close'].iloc[-1])
            return 0
        except:
            return 0
    
    async def calculate_daily_pnl(self):
        """Calculate today's P&L"""
        conn = await self.connect_db()
        try:
            today = datetime.now().date()
            trades_today = await conn.fetch("""
                SELECT ticker, side, qty, fill_price 
                FROM paper_trades 
                WHERE ts::date = $1
            """, today)
            
            daily_pnl = 0
            for trade in trades_today:
                current_price = await self.get_current_price(trade['ticker'])
                trade_pnl = (current_price - float(trade['fill_price'])) * float(trade['qty'])
                if trade['side'] == 'sell':
                    trade_pnl *= -1
                daily_pnl += trade_pnl
            
            return daily_pnl
        finally:
            await conn.close()
    
    async def get_recent_trades(self, limit=10):
        """Get recent trades"""
        conn = await self.connect_db()
        try:
            trades = await conn.fetch("""
                SELECT ticker, side, qty, fill_price, ts, 
                       EXTRACT(EPOCH FROM (NOW() - ts))/60 as minutes_ago
                FROM paper_trades 
                ORDER BY ts DESC 
                LIMIT $1
            """, limit)
            
            return [dict(trade) for trade in trades]
        finally:
            await conn.close()
    
    async def get_performance_metrics(self):
        """Calculate performance metrics"""
        conn = await self.connect_db()
        try:
            # Total trades
            total_trades = await conn.fetchval("SELECT COUNT(*) FROM paper_trades")
            
            # Win rate calculation (simplified)
            trades = await conn.fetch("""
                SELECT ticker, side, fill_price, ts
                FROM paper_trades 
                ORDER BY ts
            """)
            
            wins = 0
            total_analyzed = 0
            
            # Simple win/loss calculation
            for i in range(len(trades) - 1):
                trade = trades[i]
                next_trade = trades[i + 1]
                
                if trade['ticker'] == next_trade['ticker']:
                    if trade['side'] == 'buy' and next_trade['side'] == 'sell':
                        if float(next_trade['fill_price']) > float(trade['fill_price']):
                            wins += 1
                        total_analyzed += 1
                    elif trade['side'] == 'sell' and next_trade['side'] == 'buy':
                        if float(trade['fill_price']) > float(next_trade['fill_price']):
                            wins += 1
                        total_analyzed += 1
            
            win_rate = (wins / total_analyzed * 100) if total_analyzed > 0 else 0
            
            # Average trade size
            avg_trade_size = await conn.fetchval("""
                SELECT AVG(qty * fill_price) FROM paper_trades
            """)
            
            return {
                'total_trades': total_trades,
                'win_rate': win_rate,
                'avg_trade_size': float(avg_trade_size) if avg_trade_size else 0,
                'trades_analyzed': total_analyzed
            }
        finally:
            await conn.close()
    
    async def update_live_data(self):
        """Update all live data"""
        portfolio = await self.get_portfolio_summary()
        recent_trades = await self.get_recent_trades()
        performance = await self.get_performance_metrics()
        
        # Get live prices
        live_prices = {}
        for symbol in self.symbols:
            live_prices[symbol] = await self.get_current_price(symbol)
        
        self.live_data.update({
            'portfolio_value': portfolio['portfolio_value'],
            'daily_pnl': portfolio['daily_pnl'],
            'total_pnl': portfolio['total_pnl'],
            'active_positions': portfolio['active_positions'],
            'recent_trades': recent_trades,
            'performance_metrics': performance,
            'live_prices': live_prices,
            'last_updated': datetime.now().isoformat()
        })
    
    def create_portfolio_chart(self):
        """Create portfolio value chart"""
        positions = self.live_data['active_positions']
        
        if not positions:
            return json.dumps({})
        
        symbols = [pos['symbol'] for pos in positions]
        values = [pos['market_value'] for pos in positions]
        
        fig = go.Figure(data=[
            go.Pie(labels=symbols, values=values, hole=0.3)
        ])
        
        fig.update_layout(
            title="Portfolio Allocation",
            font=dict(size=14),
            showlegend=True
        )
        
        return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    
    def create_pnl_chart(self):
        """Create P&L chart"""
        positions = self.live_data['active_positions']
        
        if not positions:
            return json.dumps({})
        
        symbols = [pos['symbol'] for pos in positions]
        pnls = [pos['pnl'] for pos in positions]
        colors = ['green' if pnl >= 0 else 'red' for pnl in pnls]
        
        fig = go.Figure(data=[
            go.Bar(x=symbols, y=pnls, marker_color=colors)
        ])
        
        fig.update_layout(
            title="P&L by Position",
            xaxis_title="Symbol",
            yaxis_title="P&L ($)",
            font=dict(size=14)
        )
        
        return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

# Create dashboard instance
dashboard = LiveTradingDashboard()

# Flask routes
@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/data')
def get_dashboard_data():
    return jsonify(dashboard.live_data)

@app.route('/api/portfolio_chart')
def get_portfolio_chart():
    chart_json = dashboard.create_portfolio_chart()
    return chart_json

@app.route('/api/pnl_chart')
def get_pnl_chart():
    chart_json = dashboard.create_pnl_chart()
    return chart_json

async def update_data_loop():
    """Background task to update data"""
    while True:
        try:
            await dashboard.update_live_data()
            print(f"📊 Dashboard data updated: Portfolio Value: ${dashboard.live_data['portfolio_value']:.2f}")
        except Exception as e:
            print(f"❌ Error updating dashboard: {e}")
        
        await asyncio.sleep(30)  # Update every 30 seconds

def run_background_updates():
    """Run the async update loop"""
    asyncio.run(update_data_loop())

if __name__ == "__main__":
    # Start background data updates
    update_thread = Thread(target=run_background_updates, daemon=True)
    update_thread.start()
    
    print("🚀 AWET Live Trading Dashboard Starting...")
    print("📊 Dashboard available at: http://localhost:5050")
    print("📈 Real-time data updates every 30 seconds")
    
    app.run(host='0.0.0.0', port=5050, debug=True)