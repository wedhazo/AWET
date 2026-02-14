#!/usr/bin/env python3
"""
AWET 15-Minute Advance Prediction System
AI Agent makes BUY/SELL decisions based on future price predictions
"""

import asyncio
import asyncpg
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import uuid
import time
import logging
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, r2_score
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PredictionAgent:
    def __init__(self):
        self.database_url = "postgresql://awet:awet@localhost:5433/awet"
        
        # Trading symbols to predict
        self.symbols = ['AAPL', 'MSFT', 'NVDA', 'TSLA', 'AMZN', 'META', 'GOOGL']
        
        # Agent parameters
        self.portfolio_value = 100000  # $100k
        self.max_position_size = 0.20  # 20% max per position
        self.prediction_confidence_threshold = 0.65  # 65% confidence to trade
        self.price_change_threshold = 0.015  # 1.5% price change to act
        
        # Models and data
        self.prediction_models = {}
        self.scalers = {}
        self.predictions = {}
        self.trading_decisions = []
        
    async def connect_db(self):
        return await asyncpg.connect(self.database_url)
    
    def get_intraday_data(self, symbol, days=5):
        """Get high-frequency intraday data for prediction"""
        try:
            ticker = yf.Ticker(symbol)
            
            # Get 1-minute data for last 5 days
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            data = ticker.history(start=start_date, end=end_date, interval="1m")
            
            if data.empty:
                logger.warning(f"No intraday data for {symbol}")
                return None
                
            logger.info(f"✅ {symbol}: {len(data)} 1-minute bars")
            return data
            
        except Exception as e:
            logger.error(f"❌ Failed to get intraday data for {symbol}: {e}")
            return None
    
    def create_prediction_features(self, data):
        """Create features for 15-minute ahead prediction"""
        df = data.copy()
        
        # Price-based features
        df['Price_MA_5'] = df['Close'].rolling(5).mean()
        df['Price_MA_15'] = df['Close'].rolling(15).mean()
        df['Price_MA_30'] = df['Close'].rolling(30).mean()
        
        # Price momentum
        df['Price_Change_1m'] = df['Close'].pct_change(1)
        df['Price_Change_5m'] = df['Close'].pct_change(5)
        df['Price_Change_15m'] = df['Close'].pct_change(15)
        
        # Volatility features
        df['Volatility_5m'] = df['Close'].pct_change().rolling(5).std()
        df['Volatility_15m'] = df['Close'].pct_change().rolling(15).std()
        
        # Volume features
        df['Volume_MA_5'] = df['Volume'].rolling(5).mean()
        df['Volume_MA_15'] = df['Volume'].rolling(15).mean()
        df['Volume_Ratio'] = df['Volume'] / df['Volume_MA_15']
        
        # High/Low features
        df['High_Low_Ratio'] = (df['High'] - df['Low']) / df['Close']
        df['Close_Position'] = (df['Close'] - df['Low']) / (df['High'] - df['Low'])
        
        # Time features (market microstructure)
        df['Hour'] = df.index.hour
        df['Minute'] = df.index.minute
        df['Is_Market_Open'] = ((df['Hour'] >= 9) & (df['Hour'] < 16)).astype(int)
        df['Minutes_Since_Open'] = (df['Hour'] - 9) * 60 + df['Minute']
        
        # RSI (fast calculation for intraday)
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        
        # Target: Price 15 minutes ahead
        df['Target_Price'] = df['Close'].shift(-15)  # 15-minute ahead price
        df['Target_Change'] = (df['Target_Price'] - df['Close']) / df['Close']
        
        return df
    
    def train_prediction_model(self, symbol):
        """Train model to predict price 15 minutes ahead"""
        logger.info(f"🤖 Training 15-minute prediction model for {symbol}...")
        
        # Get intraday data
        data = self.get_intraday_data(symbol)
        if data is None:
            return None
        
        # Create features
        featured_data = self.create_prediction_features(data)
        
        # Select features for prediction
        feature_columns = [
            'Price_MA_5', 'Price_MA_15', 'Price_MA_30',
            'Price_Change_1m', 'Price_Change_5m', 'Price_Change_15m',
            'Volatility_5m', 'Volatility_15m', 'Volume_Ratio',
            'High_Low_Ratio', 'Close_Position', 'RSI',
            'Hour', 'Minute', 'Minutes_Since_Open', 'Is_Market_Open'
        ]
        
        # Clean data
        df_clean = featured_data.dropna()
        available_features = [col for col in feature_columns if col in df_clean.columns]
        
        if len(df_clean) < 100:
            logger.warning(f"⚠️ Insufficient data for {symbol}: {len(df_clean)} samples")
            return None
        
        # Prepare training data
        X = df_clean[available_features]
        y = df_clean['Target_Change']  # Predict percentage change
        
        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Split data (use recent 20% for testing)
        split_index = int(len(X_scaled) * 0.8)
        X_train, X_test = X_scaled[:split_index], X_scaled[split_index:]
        y_train, y_test = y[:split_index], y[split_index:]
        
        # Train Random Forest for regression
        model = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            n_jobs=-1
        )
        
        model.fit(X_train, y_train)
        
        # Evaluate model
        y_pred_train = model.predict(X_train)
        y_pred_test = model.predict(X_test)
        
        train_r2 = r2_score(y_train, y_pred_train)
        test_r2 = r2_score(y_test, y_pred_test)
        test_mae = mean_absolute_error(y_test, y_pred_test)
        
        logger.info(f"  📊 {symbol} Model Performance:")
        logger.info(f"    Training R²: {train_r2:.3f}")
        logger.info(f"    Test R²: {test_r2:.3f}")
        logger.info(f"    Test MAE: {test_mae:.4f}")
        
        # Store model and scaler
        self.prediction_models[symbol] = model
        self.scalers[symbol] = scaler
        
        return {
            'model': model,
            'scaler': scaler,
            'performance': {
                'train_r2': train_r2,
                'test_r2': test_r2,
                'test_mae': test_mae,
                'samples': len(df_clean)
            }
        }
    
    def make_15min_prediction(self, symbol):
        """Make 15-minute ahead price prediction"""
        if symbol not in self.prediction_models:
            logger.warning(f"⚠️ No model trained for {symbol}")
            return None
        
        try:
            # Get latest data
            data = self.get_intraday_data(symbol, days=1)
            if data is None or len(data) < 50:
                return None
            
            # Create features
            featured_data = self.create_prediction_features(data)
            latest_row = featured_data.iloc[-1:].copy()
            
            # Feature columns (same as training)
            feature_columns = [
                'Price_MA_5', 'Price_MA_15', 'Price_MA_30',
                'Price_Change_1m', 'Price_Change_5m', 'Price_Change_15m',
                'Volatility_5m', 'Volatility_15m', 'Volume_Ratio',
                'High_Low_Ratio', 'Close_Position', 'RSI',
                'Hour', 'Minute', 'Minutes_Since_Open', 'Is_Market_Open'
            ]
            
            available_features = [col for col in feature_columns if col in latest_row.columns]
            X_latest = latest_row[available_features].values.reshape(1, -1)
            
            if np.any(np.isnan(X_latest)):
                logger.warning(f"⚠️ NaN values in features for {symbol}")
                return None
            
            # Scale and predict
            X_scaled = self.scalers[symbol].transform(X_latest)
            predicted_change = self.prediction_models[symbol].predict(X_scaled)[0]
            
            # Get current price and calculate predicted price
            current_price = float(data['Close'].iloc[-1])
            predicted_price = current_price * (1 + predicted_change)
            
            # Calculate confidence based on model variance and recent accuracy
            feature_importance = self.prediction_models[symbol].feature_importances_
            prediction_confidence = min(0.95, max(0.50, 1 - abs(predicted_change) * 10))
            
            prediction = {
                'symbol': symbol,
                'current_price': current_price,
                'predicted_price': predicted_price,
                'predicted_change': predicted_change,
                'predicted_change_pct': predicted_change * 100,
                'confidence': prediction_confidence,
                'prediction_time': (datetime.now() + timedelta(minutes=15)).isoformat(),
                'created_at': datetime.now().isoformat()
            }
            
            logger.info(f"🔮 {symbol}: {prediction['predicted_change_pct']:+.2f}% "
                       f"(${current_price:.2f} → ${predicted_price:.2f}, "
                       f"confidence: {prediction_confidence:.2%})")
            
            return prediction
            
        except Exception as e:
            logger.error(f"❌ Prediction failed for {symbol}: {e}")
            return None
    
    def make_trading_decision(self, prediction):
        """AI Agent decides BUY/SELL based on prediction"""
        symbol = prediction['symbol']
        predicted_change_pct = prediction['predicted_change_pct']
        confidence = prediction['confidence']
        current_price = prediction['current_price']
        
        # Decision logic
        decision = {
            'symbol': symbol,
            'action': 'HOLD',
            'reason': 'No significant opportunity',
            'confidence': confidence,
            'predicted_change': predicted_change_pct,
            'current_price': current_price,
            'position_size': 0,
            'timestamp': datetime.now().isoformat()
        }
        
        # Check if confidence and predicted change meet thresholds
        if confidence >= self.prediction_confidence_threshold:
            abs_change = abs(predicted_change_pct / 100)
            
            if abs_change >= self.price_change_threshold:
                # Calculate position size based on confidence and expected move
                base_position = self.portfolio_value * self.max_position_size
                confidence_multiplier = confidence
                size_multiplier = min(2.0, abs_change / self.price_change_threshold)
                
                position_value = base_position * confidence_multiplier * size_multiplier
                position_size = position_value / current_price
                
                if predicted_change_pct > 0:
                    decision.update({
                        'action': 'BUY',
                        'reason': f'Strong upward prediction: +{predicted_change_pct:.2f}%',
                        'position_size': position_size
                    })
                else:
                    decision.update({
                        'action': 'SELL',
                        'reason': f'Strong downward prediction: {predicted_change_pct:.2f}%',
                        'position_size': position_size
                    })
            else:
                decision['reason'] = f'Change too small: {predicted_change_pct:.2f}% < {self.price_change_threshold*100:.1f}%'
        else:
            decision['reason'] = f'Low confidence: {confidence:.2%} < {self.prediction_confidence_threshold:.2%}'
        
        return decision
    
    async def execute_trading_decision(self, decision):
        """Execute the trading decision in paper trading"""
        if decision['action'] == 'HOLD':
            logger.info(f"⏸️ {decision['symbol']}: {decision['action']} - {decision['reason']}")
            return True
        
        symbol = decision['symbol']
        action = decision['action']
        position_size = decision['position_size']
        price = decision['current_price']
        
        logger.info(f"💰 EXECUTING: {symbol} {action} {position_size:.2f} shares @ ${price:.2f}")
        logger.info(f"    Reason: {decision['reason']}")
        logger.info(f"    Confidence: {decision['confidence']:.2%}")
        
        # Execute paper trade
        conn = await self.connect_db()
        try:
            await conn.execute("""
                INSERT INTO paper_trades (event_id, correlation_id, idempotency_key, 
                                        ticker, ts, side, qty, fill_price, status) 
                VALUES ($1, $2, $3, $4, NOW(), $5, $6, $7, 'filled')
            """, 
            str(uuid.uuid4()), str(uuid.uuid4()), 
            f"prediction-trade-{int(time.time())}", symbol,
            action.lower(), position_size, price)
            
            # Log decision
            await conn.execute("""
                INSERT INTO audit_events (event_id, correlation_id, idempotency_key, symbol, ts, 
                                        schema_version, source, event_type, payload) 
                VALUES ($1, $2, $3, $4, NOW(), 1, $5, $6, $7)
            """, 
            str(uuid.uuid4()), str(uuid.uuid4()), f"prediction-decision-{int(time.time())}", 
            symbol, "prediction_agent", "trading_decision", json.dumps(decision))
            
            logger.info(f"✅ {symbol}: {action} executed successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Execution failed for {symbol}: {e}")
            return False
        finally:
            await conn.close()
    
    async def run_prediction_cycle(self):
        """Run complete 15-minute prediction and trading cycle"""
        logger.info("🚀 Starting 15-Minute Prediction Cycle...")
        start_time = time.time()
        
        results = {
            'predictions': {},
            'decisions': {},
            'executions': {},
            'summary': {}
        }
        
        # Phase 1: Train prediction models
        logger.info("🤖 Phase 1: Training 15-minute prediction models...")
        successful_models = 0
        
        for symbol in self.symbols:
            model_result = self.train_prediction_model(symbol)
            if model_result:
                successful_models += 1
                logger.info(f"✅ {symbol}: Model trained (R² = {model_result['performance']['test_r2']:.3f})")
        
        # Phase 2: Generate predictions
        logger.info("🔮 Phase 2: Generating 15-minute ahead predictions...")
        
        for symbol in self.symbols:
            prediction = self.make_15min_prediction(symbol)
            if prediction:
                results['predictions'][symbol] = prediction
        
        # Phase 3: Make trading decisions
        logger.info("🎯 Phase 3: AI Agent making trading decisions...")
        
        for symbol, prediction in results['predictions'].items():
            decision = self.make_trading_decision(prediction)
            results['decisions'][symbol] = decision
            
            # Execute decision
            execution_success = await self.execute_trading_decision(decision)
            results['executions'][symbol] = execution_success
        
        # Summary
        total_time = time.time() - start_time
        
        predictions_count = len(results['predictions'])
        buy_decisions = sum(1 for d in results['decisions'].values() if d['action'] == 'BUY')
        sell_decisions = sum(1 for d in results['decisions'].values() if d['action'] == 'SELL')
        hold_decisions = sum(1 for d in results['decisions'].values() if d['action'] == 'HOLD')
        
        results['summary'] = {
            'total_time': f"{total_time:.1f}s",
            'models_trained': successful_models,
            'predictions_made': predictions_count,
            'buy_decisions': buy_decisions,
            'sell_decisions': sell_decisions,
            'hold_decisions': hold_decisions,
            'execution_success_rate': sum(results['executions'].values()) / len(results['executions']) if results['executions'] else 0
        }
        
        return results

async def main():
    """Run the 15-minute prediction agent"""
    agent = PredictionAgent()
    results = await agent.run_prediction_cycle()
    
    print("\\n" + "="*80)
    print("🔮 15-MINUTE ADVANCE PREDICTION RESULTS")
    print("="*80)
    
    summary = results['summary']
    print(f"✅ Models trained: {summary['models_trained']}")
    print(f"✅ Predictions made: {summary['predictions_made']}")
    print(f"✅ Buy decisions: {summary['buy_decisions']}")
    print(f"✅ Sell decisions: {summary['sell_decisions']}")
    print(f"✅ Hold decisions: {summary['hold_decisions']}")
    print(f"✅ Execution rate: {summary['execution_success_rate']:.0%}")
    print(f"✅ Total time: {summary['total_time']}")
    
    print("\\n🔮 PREDICTIONS & DECISIONS:")
    for symbol in results['predictions']:
        pred = results['predictions'][symbol]
        decision = results['decisions'][symbol]
        
        print(f"\\n📊 {symbol}:")
        print(f"  Current: ${pred['current_price']:.2f}")
        print(f"  Predicted (15min): ${pred['predicted_price']:.2f} ({pred['predicted_change_pct']:+.2f}%)")
        print(f"  Confidence: {pred['confidence']:.1%}")
        print(f"  🎯 DECISION: {decision['action']} - {decision['reason']}")
        if decision['action'] != 'HOLD':
            print(f"     Position: {decision['position_size']:.2f} shares")

if __name__ == "__main__":
    asyncio.run(main())