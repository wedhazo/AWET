#!/usr/bin/env python3
"""
AWET Complete End-to-End Trading Pipeline
Data Collection → Cleaning → Training → Testing → Buy/Sell Execution
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
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FullTradingPipeline:
    def __init__(self):
        self.database_url = "postgresql://awet:awet@localhost:5433/awet"
        self.symbols = ['AAPL', 'MSFT', 'NVDA', 'TSLA']
        self.model = None
        self.features = None
        self.performance_metrics = {}
        
    async def connect_db(self):
        """Connect to TimescaleDB"""
        return await asyncpg.connect(self.database_url)
    
    async def log_event(self, event_type, symbol, source, payload):
        """Log events to audit trail"""
        conn = await self.connect_db()
        try:
            await conn.execute("""
                INSERT INTO audit_events (event_id, correlation_id, idempotency_key, symbol, ts, 
                                        schema_version, source, event_type, payload) 
                VALUES ($1, $2, $3, $4, NOW(), 1, $5, $6, $7)
            """, 
            str(uuid.uuid4()), str(uuid.uuid4()), f"{event_type}-{int(time.time())}", 
            symbol, source, event_type, json.dumps(payload))
            logger.info(f"✅ Logged {event_type} for {symbol}")
        finally:
            await conn.close()
    
    async def step1_data_collection(self):
        """Step 1: Collect real market data"""
        logger.info("🔄 STEP 1: DATA COLLECTION")
        
        all_data = {}
        for symbol in self.symbols:
            logger.info(f"📊 Fetching data for {symbol}...")
            
            # Get 6 months of historical data
            stock = yf.Ticker(symbol)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=180)
            
            hist = stock.history(start=start_date, end=end_date, interval="1d")
            if not hist.empty:
                all_data[symbol] = hist
                logger.info(f"✅ {symbol}: {len(hist)} days of data")
                
                await self.log_event(
                    "data_collected", symbol, "data_collection",
                    {"days": len(hist), "start": start_date.isoformat(), "end": end_date.isoformat()}
                )
            else:
                logger.warning(f"❌ No data for {symbol}")
        
        return all_data
    
    async def step2_data_cleaning(self, raw_data):
        """Step 2: Clean and preprocess data"""
        logger.info("🧹 STEP 2: DATA CLEANING")
        
        cleaned_data = {}
        for symbol, data in raw_data.items():
            logger.info(f"🔧 Cleaning {symbol}...")
            
            # Remove missing values
            data = data.dropna()
            
            # Remove extreme outliers (beyond 3 standard deviations)
            for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
                if col in data.columns:
                    mean = data[col].mean()
                    std = data[col].std()
                    data = data[(data[col] >= mean - 3*std) & (data[col] <= mean + 3*std)]
            
            # Ensure volume is positive
            if 'Volume' in data.columns:
                data = data[data['Volume'] > 0]
            
            cleaned_data[symbol] = data
            logger.info(f"✅ {symbol}: {len(data)} clean records")
            
            await self.log_event(
                "data_cleaned", symbol, "data_cleaning",
                {"clean_records": len(data), "outliers_removed": len(raw_data[symbol]) - len(data)}
            )
        
        return cleaned_data
    
    async def step3_feature_engineering(self, cleaned_data):
        """Step 3: Create trading features"""
        logger.info("⚙️ STEP 3: FEATURE ENGINEERING")
        
        featured_data = {}
        for symbol, data in cleaned_data.items():
            logger.info(f"🔧 Creating features for {symbol}...")
            
            df = data.copy()
            
            # Technical indicators
            df['SMA_10'] = df['Close'].rolling(window=10).mean()
            df['SMA_30'] = df['Close'].rolling(window=30).mean()
            df['RSI'] = self.calculate_rsi(df['Close'])
            df['MACD'] = df['Close'].ewm(span=12).mean() - df['Close'].ewm(span=26).mean()
            df['BB_upper'], df['BB_lower'] = self.calculate_bollinger_bands(df['Close'])
            df['Volatility'] = df['Close'].pct_change().rolling(window=20).std()
            
            # Price changes
            df['Price_Change'] = df['Close'].pct_change()
            df['Price_Change_1d'] = df['Close'].pct_change(1)
            df['Price_Change_5d'] = df['Close'].pct_change(5)
            
            # Volume features
            df['Volume_SMA'] = df['Volume'].rolling(window=20).mean()
            df['Volume_Ratio'] = df['Volume'] / df['Volume_SMA']
            
            # Target variable: 1 if price goes up next day, 0 otherwise
            df['Target'] = (df['Close'].shift(-1) > df['Close']).astype(int)
            
            # Remove NaN values
            df = df.dropna()
            
            featured_data[symbol] = df
            feature_count = len([col for col in df.columns if col not in ['Open', 'High', 'Low', 'Close', 'Volume', 'Target']])
            logger.info(f"✅ {symbol}: {feature_count} features created, {len(df)} samples")
            
            await self.log_event(
                "features_engineered", symbol, "feature_engineering",
                {"features_count": feature_count, "samples": len(df)}
            )
        
        return featured_data
    
    def calculate_rsi(self, prices, window=14):
        """Calculate RSI indicator"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_bollinger_bands(self, prices, window=20):
        """Calculate Bollinger Bands"""
        sma = prices.rolling(window=window).mean()
        std = prices.rolling(window=window).std()
        upper_band = sma + (std * 2)
        lower_band = sma - (std * 2)
        return upper_band, lower_band
    
    async def step4_train_model(self, featured_data):
        """Step 4: Train ML model"""
        logger.info("🤖 STEP 4: MODEL TRAINING")
        
        # Combine all symbols for training
        all_features = []
        all_targets = []
        
        feature_columns = ['SMA_10', 'SMA_30', 'RSI', 'MACD', 'BB_upper', 'BB_lower', 
                          'Volatility', 'Price_Change_1d', 'Price_Change_5d', 'Volume_Ratio']
        
        for symbol, data in featured_data.items():
            # Select only feature columns that exist
            available_features = [col for col in feature_columns if col in data.columns]
            if available_features:
                features = data[available_features].copy()
                targets = data['Target'].copy()
                
                # Remove any remaining NaN
                mask = ~(features.isna().any(axis=1) | targets.isna())
                features = features[mask]
                targets = targets[mask]
                
                if len(features) > 0:
                    all_features.append(features)
                    all_targets.append(targets)
                    logger.info(f"✅ {symbol}: {len(features)} training samples")
        
        if not all_features:
            logger.error("❌ No valid features for training")
            return None
            
        # Combine all data
        X = pd.concat(all_features, ignore_index=True)
        y = pd.concat(all_targets, ignore_index=True)
        
        logger.info(f"📊 Total training samples: {len(X)}")
        
        # Split train/test
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Train Random Forest model
        self.model = RandomForestClassifier(n_estimators=100, random_state=42)
        self.model.fit(X_train, y_train)
        self.features = X.columns.tolist()
        
        # Test model
        y_pred = self.model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        
        self.performance_metrics = {
            "accuracy": accuracy,
            "training_samples": len(X_train),
            "test_samples": len(X_test),
            "features": len(self.features)
        }
        
        logger.info(f"✅ Model trained! Accuracy: {accuracy:.2%}")
        
        await self.log_event(
            "model_trained", "ALL", "model_training",
            self.performance_metrics
        )
        
        return self.model
    
    async def step5_generate_signals(self, featured_data):
        """Step 5: Generate buy/sell signals"""
        logger.info("📈 STEP 5: SIGNAL GENERATION")
        
        if not self.model or not self.features:
            logger.error("❌ No trained model available")
            return {}
        
        signals = {}
        for symbol, data in featured_data.items():
            logger.info(f"🎯 Generating signals for {symbol}...")
            
            # Get latest data point
            latest_data = data.iloc[-1:][self.features]
            
            # Handle missing features
            for feature in self.features:
                if feature not in latest_data.columns:
                    latest_data[feature] = 0
            
            # Predict
            latest_data = latest_data[self.features]  # Ensure correct order
            if not latest_data.isna().any().any():
                prediction = self.model.predict(latest_data)[0]
                confidence = self.model.predict_proba(latest_data)[0].max()
                
                signal = "BUY" if prediction == 1 else "SELL"
                signals[symbol] = {
                    "signal": signal,
                    "confidence": confidence,
                    "price": data['Close'].iloc[-1],
                    "timestamp": datetime.now().isoformat()
                }
                
                logger.info(f"✅ {symbol}: {signal} (confidence: {confidence:.2%})")
                
                await self.log_event(
                    "signal_generated", symbol, "signal_generation",
                    signals[symbol]
                )
            else:
                logger.warning(f"⚠️ {symbol}: Missing data for prediction")
        
        return signals
    
    async def step6_execute_trades(self, signals):
        """Step 6: Execute paper trades"""
        logger.info("💰 STEP 6: TRADE EXECUTION")
        
        executed_trades = []
        
        for symbol, signal_data in signals.items():
            signal = signal_data["signal"]
            confidence = signal_data["confidence"]
            price = signal_data["price"]
            
            # Only trade if confidence > 60%
            if confidence > 0.6:
                # Calculate position size (simple: $1000 per trade)
                position_value = 1000
                quantity = int(position_value / price)
                
                if quantity > 0:
                    # Execute paper trade
                    trade = {
                        "symbol": symbol,
                        "side": signal.lower(),
                        "quantity": quantity,
                        "price": price,
                        "confidence": confidence,
                        "value": quantity * price,
                        "timestamp": datetime.now().isoformat()
                    }
                    
                    # Insert into paper_trades table
                    conn = await self.connect_db()
                    try:
                        await conn.execute("""
                            INSERT INTO paper_trades (event_id, correlation_id, idempotency_key, 
                                                    ticker, ts, side, qty, fill_price, status) 
                            VALUES ($1, $2, $3, $4, NOW(), $5, $6, $7, 'filled')
                        """, 
                        str(uuid.uuid4()), str(uuid.uuid4()), 
                        f"e2e-trade-{int(time.time())}", symbol, 
                        signal.lower(), quantity, price)
                        
                        executed_trades.append(trade)
                        logger.info(f"✅ {symbol}: {signal} {quantity} shares @ ${price:.2f} (${trade['value']:.2f})")
                        
                        await self.log_event(
                            "trade_executed", symbol, "trade_execution", trade
                        )
                        
                    finally:
                        await conn.close()
                else:
                    logger.warning(f"⚠️ {symbol}: Quantity too small for trade")
            else:
                logger.info(f"🤔 {symbol}: Low confidence ({confidence:.2%}) - skipping trade")
        
        return executed_trades
    
    async def run_full_pipeline(self):
        """Run the complete end-to-end pipeline"""
        logger.info("🚀 STARTING COMPLETE END-TO-END TRADING PIPELINE")
        start_time = time.time()
        
        try:
            # Step 1: Data Collection
            raw_data = await self.step1_data_collection()
            if not raw_data:
                logger.error("❌ No data collected")
                return
            
            # Step 2: Data Cleaning
            cleaned_data = await self.step2_data_cleaning(raw_data)
            
            # Step 3: Feature Engineering
            featured_data = await self.step3_feature_engineering(cleaned_data)
            
            # Step 4: Model Training
            model = await self.step4_train_model(featured_data)
            if not model:
                logger.error("❌ Model training failed")
                return
            
            # Step 5: Signal Generation
            signals = await self.step5_generate_signals(featured_data)
            
            # Step 6: Trade Execution
            trades = await self.step6_execute_trades(signals)
            
            # Summary
            total_time = time.time() - start_time
            summary = {
                "pipeline_duration": f"{total_time:.1f}s",
                "symbols_processed": len(raw_data),
                "model_accuracy": f"{self.performance_metrics.get('accuracy', 0):.2%}",
                "signals_generated": len(signals),
                "trades_executed": len(trades),
                "total_trade_value": sum(trade['value'] for trade in trades)
            }
            
            logger.info("🎉 PIPELINE COMPLETE!")
            logger.info(f"📊 Summary: {json.dumps(summary, indent=2)}")
            
            await self.log_event(
                "pipeline_completed", "ALL", "full_pipeline", summary
            )
            
            return {
                "raw_data": raw_data,
                "signals": signals,
                "trades": trades,
                "performance": self.performance_metrics,
                "summary": summary
            }
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {str(e)}")
            await self.log_event(
                "pipeline_failed", "ALL", "full_pipeline", {"error": str(e)}
            )
            raise

def main():
    """Main execution"""
    pipeline = FullTradingPipeline()
    result = asyncio.run(pipeline.run_full_pipeline())
    
    print("\n" + "="*80)
    print("🎯 END-TO-END PIPELINE RESULTS")
    print("="*80)
    
    if result:
        print(f"✅ Symbols processed: {result['summary']['symbols_processed']}")
        print(f"✅ Model accuracy: {result['summary']['model_accuracy']}")
        print(f"✅ Signals generated: {result['summary']['signals_generated']}")
        print(f"✅ Trades executed: {result['summary']['trades_executed']}")
        print(f"✅ Total trade value: ${result['summary']['total_trade_value']:.2f}")
        print(f"✅ Pipeline duration: {result['summary']['pipeline_duration']}")
        
        if result['trades']:
            print("\n📈 EXECUTED TRADES:")
            for trade in result['trades']:
                print(f"  {trade['side'].upper()} {trade['quantity']} {trade['symbol']} @ ${trade['price']:.2f} (confidence: {trade['confidence']:.1%})")

if __name__ == "__main__":
    main()