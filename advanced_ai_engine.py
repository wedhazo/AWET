#!/usr/bin/env python3
"""
AWET Advanced AI Trading Engine
Multiple ML models + Sentiment Analysis + Real-time Predictions
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
import requests
from textblob import TextBlob
import feedparser
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.svm import SVC
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AdvancedAIEngine:
    def __init__(self):
        self.database_url = "postgresql://awet:awet@localhost:5433/awet"
        self.symbols = ['AAPL', 'MSFT', 'NVDA', 'TSLA', 'GOOGL', 'AMZN']
        self.models = {}
        self.ensemble_weights = {}
        self.scalers = {}
        self.sentiment_scores = {}
        
        # Initialize multiple models
        self.model_configs = {
            'random_forest': RandomForestClassifier(n_estimators=200, random_state=42),
            'gradient_boost': GradientBoostingClassifier(n_estimators=100, random_state=42),
            'neural_network': MLPClassifier(hidden_layer_sizes=(100, 50), max_iter=500, random_state=42),
            'svm': SVC(probability=True, random_state=42)
        }
        
    async def connect_db(self):
        return await asyncpg.connect(self.database_url)
    
    async def log_event(self, event_type, symbol, source, payload):
        """Enhanced logging with AI insights"""
        conn = await self.connect_db()
        try:
            await conn.execute("""
                INSERT INTO audit_events (event_id, correlation_id, idempotency_key, symbol, ts, 
                                        schema_version, source, event_type, payload) 
                VALUES ($1, $2, $3, $4, NOW(), 1, $5, $6, $7)
            """, 
            str(uuid.uuid4()), str(uuid.uuid4()), f"{event_type}-{int(time.time())}", 
            symbol, source, event_type, json.dumps(payload))
        finally:
            await conn.close()
    
    def get_enhanced_market_data(self, symbol, days=365):
        """Get comprehensive market data with additional features"""
        logger.info(f"🔍 Fetching enhanced data for {symbol}...")
        
        ticker = yf.Ticker(symbol)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Get historical data
        hist = ticker.history(start=start_date, end=end_date, interval="1d")
        if hist.empty:
            return None
            
        # Get additional info
        info = ticker.info
        
        # Add macro-economic features
        hist['Market_Cap'] = info.get('marketCap', 0)
        hist['PE_Ratio'] = info.get('trailingPE', 0)
        hist['Beta'] = info.get('beta', 1.0)
        
        logger.info(f"✅ {symbol}: {len(hist)} days with enhanced features")
        return hist
    
    def get_sentiment_analysis(self, symbol):
        """Advanced sentiment analysis from news"""
        logger.info(f"📰 Analyzing sentiment for {symbol}...")
        
        try:
            # Get news headlines (using RSS feeds as example)
            news_urls = [
                f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}",
                "https://feeds.bloomberg.com/markets/news.rss"
            ]
            
            sentiment_scores = []
            
            for url in news_urls:
                try:
                    feed = feedparser.parse(url)
                    for entry in feed.entries[:5]:  # Latest 5 articles
                        title = entry.get('title', '')
                        summary = entry.get('summary', '')
                        text = f"{title} {summary}"
                        
                        blob = TextBlob(text)
                        sentiment_scores.append(blob.sentiment.polarity)
                except:
                    continue
            
            if sentiment_scores:
                avg_sentiment = np.mean(sentiment_scores)
                sentiment_strength = abs(avg_sentiment)
                
                return {
                    'sentiment_score': avg_sentiment,
                    'sentiment_strength': sentiment_strength,
                    'news_count': len(sentiment_scores),
                    'sentiment_label': 'positive' if avg_sentiment > 0.1 else 'negative' if avg_sentiment < -0.1 else 'neutral'
                }
            
        except Exception as e:
            logger.warning(f"⚠️ Sentiment analysis failed for {symbol}: {e}")
        
        return {
            'sentiment_score': 0.0,
            'sentiment_strength': 0.0,
            'news_count': 0,
            'sentiment_label': 'neutral'
        }
    
    def create_advanced_features(self, data, sentiment_data):
        """Create comprehensive feature set"""
        df = data.copy()
        
        # Technical indicators (existing)
        df['SMA_5'] = df['Close'].rolling(window=5).mean()
        df['SMA_20'] = df['Close'].rolling(window=20).mean()
        df['SMA_50'] = df['Close'].rolling(window=50).mean()
        df['EMA_12'] = df['Close'].ewm(span=12).mean()
        df['EMA_26'] = df['Close'].ewm(span=26).mean()
        
        # Enhanced RSI
        df['RSI_14'] = self.calculate_rsi(df['Close'], 14)
        df['RSI_7'] = self.calculate_rsi(df['Close'], 7)
        
        # MACD family
        df['MACD'] = df['EMA_12'] - df['EMA_26']
        df['MACD_Signal'] = df['MACD'].ewm(span=9).mean()
        df['MACD_Histogram'] = df['MACD'] - df['MACD_Signal']
        
        # Bollinger Bands
        bb_period = 20
        df['BB_Middle'] = df['Close'].rolling(window=bb_period).mean()
        bb_std = df['Close'].rolling(window=bb_period).std()
        df['BB_Upper'] = df['BB_Middle'] + (bb_std * 2)
        df['BB_Lower'] = df['BB_Middle'] - (bb_std * 2)
        df['BB_Width'] = df['BB_Upper'] - df['BB_Lower']
        df['BB_Position'] = (df['Close'] - df['BB_Lower']) / df['BB_Width']
        
        # Price momentum
        df['Price_Change_1d'] = df['Close'].pct_change(1)
        df['Price_Change_3d'] = df['Close'].pct_change(3)
        df['Price_Change_7d'] = df['Close'].pct_change(7)
        df['Price_Change_30d'] = df['Close'].pct_change(30)
        
        # Volatility measures
        df['Volatility_10d'] = df['Close'].pct_change().rolling(window=10).std() * np.sqrt(252)
        df['Volatility_30d'] = df['Close'].pct_change().rolling(window=30).std() * np.sqrt(252)
        df['ATR'] = self.calculate_atr(df)
        
        # Volume indicators
        df['Volume_SMA_20'] = df['Volume'].rolling(window=20).mean()
        df['Volume_Ratio'] = df['Volume'] / df['Volume_SMA_20']
        df['Price_Volume'] = df['Close'] * df['Volume']
        df['VWAP'] = (df['Price_Volume'].rolling(window=20).sum() / 
                     df['Volume'].rolling(window=20).sum())
        
        # Market structure
        df['High_Low_Ratio'] = df['High'] / df['Low']
        df['Body_Size'] = abs(df['Close'] - df['Open']) / df['Open']
        df['Upper_Shadow'] = (df['High'] - np.maximum(df['Open'], df['Close'])) / df['Open']
        df['Lower_Shadow'] = (np.minimum(df['Open'], df['Close']) - df['Low']) / df['Open']
        
        # Sentiment features
        df['Sentiment_Score'] = sentiment_data['sentiment_score']
        df['Sentiment_Strength'] = sentiment_data['sentiment_strength']
        df['News_Count'] = sentiment_data['news_count']
        
        # Market timing features
        df['Day_of_Week'] = df.index.dayofweek
        df['Month'] = df.index.month
        df['Is_Month_End'] = (df.index.day > 25).astype(int)
        df['Is_Quarter_End'] = df.index.to_series().apply(
            lambda x: 1 if x.month in [3, 6, 9, 12] and x.day > 25 else 0
        ).values
        
        # Target variables (multiple horizons)
        df['Target_1d'] = (df['Close'].shift(-1) > df['Close']).astype(int)
        df['Target_3d'] = (df['Close'].shift(-3) > df['Close']).astype(int)
        df['Target_7d'] = (df['Close'].shift(-7) > df['Close']).astype(int)
        
        return df
    
    def calculate_rsi(self, prices, window=14):
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_atr(self, df, period=14):
        high_low = df['High'] - df['Low']
        high_close = np.abs(df['High'] - df['Close'].shift())
        low_close = np.abs(df['Low'] - df['Close'].shift())
        true_range = np.maximum(high_low, np.maximum(high_close, low_close))
        return true_range.rolling(window=period).mean()
    
    async def train_ensemble_models(self, symbol):
        """Train multiple models and create ensemble"""
        logger.info(f"🤖 Training ensemble models for {symbol}...")
        
        # Get data
        data = self.get_enhanced_market_data(symbol)
        if data is None:
            return None
        
        sentiment = self.get_sentiment_analysis(symbol)
        featured_data = self.create_advanced_features(data, sentiment)
        
        # Select features
        feature_columns = [
            'SMA_5', 'SMA_20', 'SMA_50', 'EMA_12', 'EMA_26',
            'RSI_14', 'RSI_7', 'MACD', 'MACD_Signal', 'MACD_Histogram',
            'BB_Width', 'BB_Position', 'Price_Change_1d', 'Price_Change_3d',
            'Price_Change_7d', 'Volatility_10d', 'Volatility_30d', 'ATR',
            'Volume_Ratio', 'VWAP', 'Body_Size', 'Sentiment_Score',
            'Sentiment_Strength', 'Day_of_Week', 'Month'
        ]
        
        # Prepare data
        df_clean = featured_data.dropna()
        available_features = [col for col in feature_columns if col in df_clean.columns]
        
        if len(df_clean) < 100 or len(available_features) < 10:
            logger.warning(f"⚠️ Insufficient data for {symbol}")
            return None
        
        X = df_clean[available_features]
        y = df_clean['Target_1d']
        
        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X_scaled, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Train models
        model_performance = {}
        trained_models = {}
        
        for name, model in self.model_configs.items():
            try:
                logger.info(f"  Training {name}...")
                model.fit(X_train, y_train)
                
                # Evaluate
                y_pred = model.predict(X_test)
                y_prob = model.predict_proba(X_test)[:, 1] if hasattr(model, 'predict_proba') else y_pred
                
                performance = {
                    'accuracy': accuracy_score(y_test, y_pred),
                    'precision': precision_score(y_test, y_pred, average='weighted', zero_division=0),
                    'recall': recall_score(y_test, y_pred, average='weighted', zero_division=0),
                    'f1': f1_score(y_test, y_pred, average='weighted', zero_division=0)
                }
                
                model_performance[name] = performance
                trained_models[name] = model
                
                logger.info(f"    {name}: Accuracy {performance['accuracy']:.3f}, F1 {performance['f1']:.3f}")
                
            except Exception as e:
                logger.error(f"    ❌ {name} training failed: {e}")
                continue
        
        if not trained_models:
            logger.error(f"❌ No models trained successfully for {symbol}")
            return None
        
        # Calculate ensemble weights based on F1 scores
        weights = {}
        total_f1 = sum(perf['f1'] for perf in model_performance.values())
        
        for name in trained_models:
            weights[name] = model_performance[name]['f1'] / total_f1 if total_f1 > 0 else 1.0 / len(trained_models)
        
        # Store everything
        self.models[symbol] = trained_models
        self.ensemble_weights[symbol] = weights
        self.scalers[symbol] = scaler
        self.sentiment_scores[symbol] = sentiment
        
        logger.info(f"✅ {symbol}: {len(trained_models)} models trained, ensemble ready")
        
        # Log training results
        await self.log_event(
            "ensemble_trained", symbol, "ai_engine",
            {
                "models_count": len(trained_models),
                "performance": model_performance,
                "weights": weights,
                "features_count": len(available_features),
                "training_samples": len(X_train)
            }
        )
        
        return {
            'models': trained_models,
            'performance': model_performance,
            'weights': weights,
            'feature_count': len(available_features)
        }
    
    async def generate_ensemble_prediction(self, symbol):
        """Generate prediction using ensemble of models"""
        if symbol not in self.models:
            logger.warning(f"⚠️ No trained models for {symbol}")
            return None
        
        logger.info(f"🎯 Generating ensemble prediction for {symbol}...")
        
        try:
            # Get latest data
            data = self.get_enhanced_market_data(symbol, days=100)
            if data is None:
                return None
            
            # Update sentiment
            sentiment = self.get_sentiment_analysis(symbol)
            featured_data = self.create_advanced_features(data, sentiment)
            
            # Get latest features
            latest_row = featured_data.iloc[-1:].copy()
            
            feature_columns = [
                'SMA_5', 'SMA_20', 'SMA_50', 'EMA_12', 'EMA_26',
                'RSI_14', 'RSI_7', 'MACD', 'MACD_Signal', 'MACD_Histogram',
                'BB_Width', 'BB_Position', 'Price_Change_1d', 'Price_Change_3d',
                'Price_Change_7d', 'Volatility_10d', 'Volatility_30d', 'ATR',
                'Volume_Ratio', 'VWAP', 'Body_Size', 'Sentiment_Score',
                'Sentiment_Strength', 'Day_of_Week', 'Month'
            ]
            
            available_features = [col for col in feature_columns if col in latest_row.columns]
            X_latest = latest_row[available_features].values.reshape(1, -1)
            
            if np.any(np.isnan(X_latest)):
                logger.warning(f"⚠️ NaN values in features for {symbol}")
                return None
            
            # Scale features
            X_scaled = self.scalers[symbol].transform(X_latest)
            
            # Get predictions from all models
            predictions = {}
            probabilities = {}
            
            for name, model in self.models[symbol].items():
                pred = model.predict(X_scaled)[0]
                prob = model.predict_proba(X_scaled)[0] if hasattr(model, 'predict_proba') else [1-pred, pred]
                
                predictions[name] = pred
                probabilities[name] = prob[1]  # Probability of positive class
            
            # Ensemble prediction
            weights = self.ensemble_weights[symbol]
            ensemble_prob = sum(probabilities[name] * weights[name] for name in predictions)
            ensemble_pred = 1 if ensemble_prob > 0.5 else 0
            
            # Signal strength based on agreement
            agreement = sum(1 for pred in predictions.values() if pred == ensemble_pred) / len(predictions)
            
            signal_strength = ensemble_prob if ensemble_pred == 1 else (1 - ensemble_prob)
            
            result = {
                'symbol': symbol,
                'prediction': ensemble_pred,
                'confidence': ensemble_prob,
                'signal_strength': signal_strength,
                'agreement': agreement,
                'individual_predictions': predictions,
                'individual_probabilities': probabilities,
                'current_price': float(data['Close'].iloc[-1]),
                'sentiment': sentiment,
                'recommendation': 'BUY' if ensemble_pred == 1 else 'SELL',
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"✅ {symbol}: {result['recommendation']} "
                       f"(confidence: {result['confidence']:.3f}, "
                       f"agreement: {result['agreement']:.3f})")
            
            # Log prediction
            await self.log_event(
                "ensemble_prediction", symbol, "ai_engine", result
            )
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Ensemble prediction failed for {symbol}: {e}")
            return None
    
    async def run_ai_analysis(self):
        """Run complete AI analysis for all symbols"""
        logger.info("🚀 Starting Advanced AI Analysis...")
        start_time = time.time()
        
        results = {
            'training_results': {},
            'predictions': {},
            'summary': {}
        }
        
        # Phase 1: Train ensemble models
        logger.info("🤖 Phase 1: Training Ensemble Models...")
        for symbol in self.symbols:
            training_result = await self.train_ensemble_models(symbol)
            if training_result:
                results['training_results'][symbol] = training_result
        
        # Phase 2: Generate predictions
        logger.info("🎯 Phase 2: Generating Ensemble Predictions...")
        for symbol in self.symbols:
            prediction = await self.generate_ensemble_prediction(symbol)
            if prediction:
                results['predictions'][symbol] = prediction
        
        # Phase 3: Summary
        total_time = time.time() - start_time
        successful_symbols = len(results['predictions'])
        
        # Calculate aggregate confidence
        all_confidences = [pred['confidence'] for pred in results['predictions'].values()]
        avg_confidence = np.mean(all_confidences) if all_confidences else 0
        
        # Count recommendations
        buy_signals = sum(1 for pred in results['predictions'].values() if pred['recommendation'] == 'BUY')
        sell_signals = len(results['predictions']) - buy_signals
        
        results['summary'] = {
            'total_time': f"{total_time:.1f}s",
            'symbols_analyzed': successful_symbols,
            'avg_confidence': avg_confidence,
            'buy_signals': buy_signals,
            'sell_signals': sell_signals,
            'models_per_symbol': len(self.model_configs)
        }
        
        logger.info("🎉 AI Analysis Complete!")
        logger.info(f"📊 Summary: {json.dumps(results['summary'], indent=2)}")
        
        return results

async def main():
    """Run the advanced AI engine"""
    engine = AdvancedAIEngine()
    results = await engine.run_ai_analysis()
    
    print("\n" + "="*80)
    print("🤖 ADVANCED AI TRADING ENGINE RESULTS")
    print("="*80)
    
    summary = results['summary']
    print(f"✅ Symbols analyzed: {summary['symbols_analyzed']}")
    print(f"✅ Average confidence: {summary['avg_confidence']:.1%}")
    print(f"✅ Buy signals: {summary['buy_signals']}")
    print(f"✅ Sell signals: {summary['sell_signals']}")
    print(f"✅ Analysis time: {summary['total_time']}")
    print(f"✅ Models per symbol: {summary['models_per_symbol']}")
    
    print("\n🎯 ENSEMBLE PREDICTIONS:")
    for symbol, pred in results['predictions'].items():
        print(f"  {symbol}: {pred['recommendation']} "
              f"(confidence: {pred['confidence']:.1%}, "
              f"agreement: {pred['agreement']:.1%}, "
              f"sentiment: {pred['sentiment']['sentiment_label']})")

if __name__ == "__main__":
    asyncio.run(main())