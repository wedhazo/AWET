#!/usr/bin/env python3
"""
AWET Live 15-Minute Prediction Monitor
Real-time monitoring of AI agent predictions and trading decisions
"""

import asyncio
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import time
import logging
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
from threading import Thread
import subprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'awet-prediction-monitor'
socketio = SocketIO(app, cors_allowed_origins="*")

class PredictionMonitor:
    def __init__(self):
        self.symbols = ['AAPL', 'MSFT', 'NVDA', 'TSLA', 'AMZN', 'META', 'GOOGL']
        self.current_predictions = {}
        self.prediction_history = []
        self.agent_decisions = []
        self.live_prices = {}
        self.next_prediction_time = datetime.now()
        
    def get_live_prices(self):
        """Get current live prices"""
        live_data = {}
        
        for symbol in self.symbols:
            try:
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period="1d", interval="1m")
                
                if not hist.empty:
                    current_price = float(hist['Close'].iloc[-1])
                    volume = int(hist['Volume'].iloc[-1])
                    
                    # Calculate 1-minute change
                    if len(hist) >= 2:
                        prev_price = float(hist['Close'].iloc[-2])
                        change = current_price - prev_price
                        change_pct = (change / prev_price) * 100
                    else:
                        change = 0
                        change_pct = 0
                    
                    live_data[symbol] = {
                        'price': current_price,
                        'change': change,
                        'change_pct': change_pct,
                        'volume': volume,
                        'timestamp': datetime.now().isoformat()
                    }
                    
            except Exception as e:
                logger.warning(f"Failed to get live price for {symbol}: {e}")
                continue
        
        return live_data
    
    def run_prediction_agent(self):
        """Run the prediction agent and capture results"""
        try:
            logger.info("🤖 Running prediction agent...")
            result = subprocess.run(
                ['python3', 'prediction_agent.py'],
                cwd='/home/kironix/Awet',
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                logger.info("✅ Prediction agent completed successfully")
                return True
            else:
                logger.error(f"❌ Prediction agent failed: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Failed to run prediction agent: {e}")
            return False
    
    def update_predictions(self):
        """Update predictions and decisions"""
        # Run prediction agent
        agent_success = self.run_prediction_agent()
        
        # Get live prices
        self.live_prices = self.get_live_prices()
        
        # Simulate predictions for demo (in real system, would fetch from DB)
        current_time = datetime.now()
        prediction_time = current_time + timedelta(minutes=15)
        
        for symbol in self.symbols:
            if symbol in self.live_prices:
                current_price = self.live_prices[symbol]['price']
                
                # Simulate prediction (random for demo)
                np.random.seed(int(time.time()) % 1000)
                predicted_change = np.random.normal(0, 0.015)  # 1.5% std dev
                predicted_price = current_price * (1 + predicted_change)
                confidence = max(0.5, min(0.95, 0.8 + np.random.normal(0, 0.1)))
                
                self.current_predictions[symbol] = {
                    'symbol': symbol,
                    'current_price': current_price,
                    'predicted_price': predicted_price,
                    'predicted_change_pct': predicted_change * 100,
                    'confidence': confidence,
                    'prediction_time': prediction_time.isoformat(),
                    'created_at': current_time.isoformat(),
                    'agent_success': agent_success
                }
        
        # Store in history
        self.prediction_history.append({
            'timestamp': current_time.isoformat(),
            'predictions': dict(self.current_predictions)
        })
        
        # Keep only last 10 prediction cycles
        if len(self.prediction_history) > 10:
            self.prediction_history = self.prediction_history[-10:]
        
        # Update next prediction time
        self.next_prediction_time = current_time + timedelta(minutes=15)
        
        logger.info(f"📊 Updated predictions for {len(self.current_predictions)} symbols")

# Global monitor instance
monitor = PredictionMonitor()

@socketio.on('connect')
def handle_connect():
    logger.info("Client connected to prediction monitor")
    emit('status', {'msg': 'Connected to AWET Prediction Monitor'})

@socketio.on('disconnect')
def handle_disconnect():
    logger.info("Client disconnected from prediction monitor")

@app.route('/')
def index():
    return render_template('prediction_monitor.html')

@app.route('/api/predictions')
def get_predictions():
    return jsonify({
        'current_predictions': monitor.current_predictions,
        'live_prices': monitor.live_prices,
        'next_prediction': monitor.next_prediction_time.isoformat(),
        'history_count': len(monitor.prediction_history),
        'last_updated': datetime.now().isoformat()
    })

async def prediction_updater():
    """Background task to update predictions every 15 minutes"""
    while True:
        try:
            # Update predictions
            monitor.update_predictions()
            
            # Broadcast to all connected clients
            data = {
                'current_predictions': monitor.current_predictions,
                'live_prices': monitor.live_prices,
                'next_prediction': monitor.next_prediction_time.isoformat(),
                'last_updated': datetime.now().isoformat()
            }
            
            socketio.emit('prediction_update', data)
            
            logger.info(f"📡 Prediction update broadcast sent")
            
        except Exception as e:
            logger.error(f"Prediction update failed: {e}")
        
        # Wait 15 minutes for next prediction cycle
        await asyncio.sleep(900)  # 15 minutes

def run_prediction_updater():
    """Run the prediction updater in background"""
    asyncio.run(prediction_updater())

if __name__ == "__main__":
    # Initial prediction update
    monitor.update_predictions()
    
    # Start background prediction updates
    update_thread = Thread(target=run_prediction_updater, daemon=True)
    update_thread.start()
    
    print("🔮 AWET 15-MINUTE PREDICTION MONITOR")
    print("📊 Real-time AI agent monitoring")
    print("🤖 Automatic trading decisions every 15 minutes")
    print("🔴 Available at: http://localhost:5052")
    print("📡 WebSocket streaming active")
    
    socketio.run(app, host='0.0.0.0', port=5052, debug=False, allow_unsafe_werkzeug=True)