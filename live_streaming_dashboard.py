#!/usr/bin/env python3
"""
AWET Live Streaming Dashboard with Real-Time Charts
Top 20 American stocks with live price feeds and time-flow graphs
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
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
import plotly.graph_objs as go
import plotly.utils
from threading import Thread
import queue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'awet-live-streaming'
socketio = SocketIO(app, cors_allowed_origins="*")

class LiveStreamingDashboard:
    def __init__(self):
        self.database_url = "postgresql://awet:awet@localhost:5433/awet"
        
        # TOP 20 AMERICAN STOCKS
        self.top20_symbols = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA',
            'TSLA', 'META', 'BRK-B', 'UNH', 'JNJ', 
            'XOM', 'JPM', 'V', 'PG', 'HD',
            'MA', 'CVX', 'ABBV', 'PFE', 'KO'
        ]
        
        # Live data storage
        self.live_prices = {}
        self.price_history = {symbol: [] for symbol in self.top20_symbols}
        self.portfolio_history = []
        self.live_data = {}
        
        # WebSocket data queue
        self.data_queue = queue.Queue()
        
    async def connect_db(self):
        return await asyncpg.connect(self.database_url)
    
    def get_live_price_batch(self, symbols):
        """Get live prices for multiple symbols efficiently"""
        try:
            # Use yfinance to get current prices
            tickers = yf.Tickers(' '.join(symbols))
            prices = {}
            
            for symbol in symbols:
                try:
                    ticker = tickers.tickers[symbol]
                    hist = ticker.history(period="1d", interval="1m")
                    if not hist.empty:
                        current_price = float(hist['Close'].iloc[-1])
                        volume = int(hist['Volume'].iloc[-1])
                        
                        # Calculate change from previous day
                        if len(hist) > 1:
                            prev_close = float(hist['Close'].iloc[0])
                            change = current_price - prev_close
                            change_pct = (change / prev_close) * 100
                        else:
                            change = 0
                            change_pct = 0
                        
                        prices[symbol] = {
                            'price': current_price,
                            'change': change,
                            'change_pct': change_pct,
                            'volume': volume,
                            'timestamp': datetime.now().isoformat()
                        }
                except Exception as e:
                    logger.warning(f"Failed to get price for {symbol}: {e}")
                    continue
            
            return prices
        except Exception as e:
            logger.error(f"Batch price fetch failed: {e}")
            return {}
    
    async def update_portfolio_data(self):
        """Update portfolio data from database"""
        conn = await self.connect_db()
        try:
            # Get all trades
            trades = await conn.fetch("""
                SELECT ticker, side, qty, fill_price, ts 
                FROM paper_trades 
                ORDER BY ts
            """)
            
            # Calculate current positions
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
            
            # Get current prices for positions
            position_symbols = [s for s, p in positions.items() if p['qty'] != 0]
            if position_symbols:
                current_prices = self.get_live_price_batch(position_symbols)
                
                portfolio_value = 0
                total_cost = 0
                active_positions = []
                
                for symbol, pos in positions.items():
                    if pos['qty'] != 0:
                        current_data = current_prices.get(symbol, {})
                        current_price = current_data.get('price', pos['avg_price'])
                        
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
                            'pnl_pct': pnl_pct,
                            'change': current_data.get('change', 0),
                            'change_pct': current_data.get('change_pct', 0)
                        })
                        
                        portfolio_value += market_value
                        total_cost += pos['qty'] * pos['avg_price']
                
                total_pnl = portfolio_value - total_cost
                
                # Store portfolio history for time-flow graph
                portfolio_point = {
                    'timestamp': datetime.now().isoformat(),
                    'portfolio_value': portfolio_value,
                    'total_pnl': total_pnl,
                    'positions_count': len(active_positions)
                }
                
                self.portfolio_history.append(portfolio_point)
                
                # Keep only last 100 points (about 50 minutes of data)
                if len(self.portfolio_history) > 100:
                    self.portfolio_history = self.portfolio_history[-100:]
                
                return {
                    'portfolio_value': portfolio_value,
                    'total_pnl': total_pnl,
                    'active_positions': active_positions,
                    'portfolio_history': self.portfolio_history
                }
        finally:
            await conn.close()
        
        return None
    
    async def update_live_data(self):
        """Update all live streaming data"""
        # Get live prices for TOP 20 stocks
        live_prices = self.get_live_price_batch(self.top20_symbols)
        
        # Store price history for time-flow charts
        current_time = datetime.now().isoformat()
        for symbol, data in live_prices.items():
            price_point = {
                'timestamp': current_time,
                'price': data['price'],
                'volume': data['volume'],
                'change_pct': data['change_pct']
            }
            
            self.price_history[symbol].append(price_point)
            
            # Keep only last 50 points per symbol (about 25 minutes)
            if len(self.price_history[symbol]) > 50:
                self.price_history[symbol] = self.price_history[symbol][-50:]
        
        # Update portfolio data
        portfolio_data = await self.update_portfolio_data()
        
        # Combine all data
        self.live_data = {
            'top20_prices': live_prices,
            'price_history': self.price_history,
            'portfolio': portfolio_data,
            'last_updated': current_time,
            'update_count': len(live_prices)
        }
        
        return self.live_data
    
    def create_live_chart(self, symbol):
        """Create live time-flow chart for a symbol"""
        if symbol not in self.price_history or len(self.price_history[symbol]) < 2:
            return json.dumps({})
        
        history = self.price_history[symbol]
        timestamps = [point['timestamp'] for point in history]
        prices = [point['price'] for point in history]
        
        # Create the chart
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=timestamps,
            y=prices,
            mode='lines+markers',
            name=symbol,
            line=dict(color='#1f77b4', width=2),
            marker=dict(size=4)
        ))
        
        fig.update_layout(
            title=f"{symbol} Live Price Flow",
            xaxis_title="Time",
            yaxis_title="Price ($)",
            font=dict(size=12),
            height=300,
            margin=dict(l=50, r=50, t=50, b=50),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            xaxis=dict(
                showgrid=True,
                gridcolor='rgba(255,255,255,0.1)',
                type='date'
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor='rgba(255,255,255,0.1)'
            )
        )
        
        return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    
    def create_portfolio_flow_chart(self):
        """Create portfolio value time-flow chart"""
        if len(self.portfolio_history) < 2:
            return json.dumps({})
        
        timestamps = [point['timestamp'] for point in self.portfolio_history]
        values = [point['portfolio_value'] for point in self.portfolio_history]
        pnl_values = [point['total_pnl'] for point in self.portfolio_history]
        
        fig = go.Figure()
        
        # Portfolio value line
        fig.add_trace(go.Scatter(
            x=timestamps,
            y=values,
            mode='lines+markers',
            name='Portfolio Value',
            line=dict(color='#00ff88', width=3),
            marker=dict(size=5)
        ))
        
        # P&L line on secondary y-axis
        fig.add_trace(go.Scatter(
            x=timestamps,
            y=pnl_values,
            mode='lines+markers',
            name='Total P&L',
            line=dict(color='#ff6b6b', width=3, dash='dash'),
            marker=dict(size=5),
            yaxis='y2'
        ))
        
        fig.update_layout(
            title="Portfolio Value & P&L Live Flow",
            xaxis_title="Time",
            font=dict(size=12),
            height=400,
            margin=dict(l=50, r=50, t=50, b=50),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            xaxis=dict(
                showgrid=True,
                gridcolor='rgba(255,255,255,0.1)',
                type='date'
            ),
            yaxis=dict(
                title="Portfolio Value ($)",
                showgrid=True,
                gridcolor='rgba(255,255,255,0.1)',
                side='left'
            ),
            yaxis2=dict(
                title="Total P&L ($)",
                showgrid=False,
                overlaying='y',
                side='right'
            ),
            legend=dict(
                x=0.02,
                y=0.98,
                bgcolor='rgba(0,0,0,0.5)',
                bordercolor='rgba(255,255,255,0.2)',
                borderwidth=1
            )
        )
        
        return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

# Global dashboard instance
dashboard = LiveStreamingDashboard()

# WebSocket events
@socketio.on('connect')
def handle_connect():
    logger.info("Client connected to live stream")
    emit('status', {'msg': 'Connected to AWET Live Stream'})

@socketio.on('disconnect')
def handle_disconnect():
    logger.info("Client disconnected from live stream")

@socketio.on('request_data')
def handle_data_request():
    emit('live_data', dashboard.live_data)

# Flask routes
@app.route('/')
def index():
    return render_template('live_dashboard.html')

@app.route('/api/live_data')
def get_live_data():
    return jsonify(dashboard.live_data)

@app.route('/api/chart/<symbol>')
def get_symbol_chart(symbol):
    chart_json = dashboard.create_live_chart(symbol.upper())
    return chart_json

@app.route('/api/portfolio_flow')
def get_portfolio_flow():
    chart_json = dashboard.create_portfolio_flow_chart()
    return chart_json

async def live_data_updater():
    """Background task to update live data and broadcast via WebSocket"""
    while True:
        try:
            # Update data
            await dashboard.update_live_data()
            
            # Broadcast to all connected clients
            socketio.emit('live_update', dashboard.live_data)
            
            logger.info(f"📡 Live update sent: {dashboard.live_data.get('update_count', 0)} symbols")
            
        except Exception as e:
            logger.error(f"Live update failed: {e}")
        
        # Update every 30 seconds
        await asyncio.sleep(30)

def run_live_updater():
    """Run the live updater in background"""
    asyncio.run(live_data_updater())

if __name__ == "__main__":
    # Start background live data updates
    update_thread = Thread(target=run_live_updater, daemon=True)
    update_thread.start()
    
    print("🚀 AWET LIVE STREAMING DASHBOARD")
    print("📊 Tracking TOP 20 American stocks")
    print("📈 Live time-flow charts enabled")
    print("🔴 Available at: http://localhost:5051")
    print("📡 WebSocket streaming active")
    
    socketio.run(app, host='0.0.0.0', port=5051, debug=False, allow_unsafe_werkzeug=True)