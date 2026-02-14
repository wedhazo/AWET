#!/usr/bin/env python3
"""
AWET Multi-Strategy Trading System
Multiple trading strategies with risk management
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
from dataclasses import dataclass
from typing import Dict, List, Optional
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StrategyType(Enum):
    MOMENTUM = "momentum"
    MEAN_REVERSION = "mean_reversion"
    BREAKOUT = "breakout"
    VOLUME_SPIKE = "volume_spike"
    SENTIMENT_MOMENTUM = "sentiment_momentum"

class SignalStrength(Enum):
    WEAK = "weak"
    MEDIUM = "medium"
    STRONG = "strong"

@dataclass
class TradingSignal:
    symbol: str
    strategy: StrategyType
    direction: str  # BUY/SELL
    strength: SignalStrength
    confidence: float
    entry_price: float
    stop_loss: float
    take_profit: float
    position_size: float
    reasoning: str
    timestamp: datetime

class MultiStrategyEngine:
    def __init__(self):
        self.database_url = "postgresql://awet:awet@localhost:5433/awet"
        self.symbols = ['AAPL', 'MSFT', 'NVDA', 'TSLA', 'GOOGL', 'AMZN', 'SPY', 'QQQ']
        self.portfolio_value = 100000  # $100k starting capital
        self.max_position_size = 0.15  # 15% max per position
        self.max_risk_per_trade = 0.02  # 2% max risk per trade
        self.strategies = {}
        self.active_signals = []
        
    async def connect_db(self):
        return await asyncpg.connect(self.database_url)
    
    async def log_signal(self, signal: TradingSignal):
        """Log trading signal to database"""
        conn = await self.connect_db()
        try:
            await conn.execute("""
                INSERT INTO audit_events (event_id, correlation_id, idempotency_key, symbol, ts, 
                                        schema_version, source, event_type, payload) 
                VALUES ($1, $2, $3, $4, NOW(), 1, $5, $6, $7)
            """, 
            str(uuid.uuid4()), str(uuid.uuid4()), f"signal-{int(time.time())}", 
            signal.symbol, "multi_strategy", "trading_signal", 
            json.dumps({
                "strategy": signal.strategy.value,
                "direction": signal.direction,
                "strength": signal.strength.value,
                "confidence": signal.confidence,
                "entry_price": signal.entry_price,
                "stop_loss": signal.stop_loss,
                "take_profit": signal.take_profit,
                "position_size": signal.position_size,
                "reasoning": signal.reasoning
            }))
        finally:
            await conn.close()
    
    def get_market_data(self, symbol: str, period: str = "6mo") -> pd.DataFrame:
        """Get comprehensive market data"""
        ticker = yf.Ticker(symbol)
        data = ticker.history(period=period, interval="1d")
        
        if data.empty:
            return None
            
        # Add technical indicators
        data = self.add_technical_indicators(data)
        return data
    
    def add_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add comprehensive technical indicators"""
        # Moving averages
        df['SMA_20'] = df['Close'].rolling(window=20).mean()
        df['SMA_50'] = df['Close'].rolling(window=50).mean()
        df['EMA_12'] = df['Close'].ewm(span=12).mean()
        df['EMA_26'] = df['Close'].ewm(span=26).mean()
        
        # RSI
        df['RSI'] = self.calculate_rsi(df['Close'])
        
        # MACD
        df['MACD'] = df['EMA_12'] - df['EMA_26']
        df['MACD_Signal'] = df['MACD'].ewm(span=9).mean()
        
        # Bollinger Bands
        bb_period = 20
        sma = df['Close'].rolling(window=bb_period).mean()
        bb_std = df['Close'].rolling(window=bb_period).std()
        df['BB_Upper'] = sma + (bb_std * 2)
        df['BB_Lower'] = sma - (bb_std * 2)
        df['BB_Width'] = df['BB_Upper'] - df['BB_Lower']
        df['BB_Position'] = (df['Close'] - df['BB_Lower']) / df['BB_Width']
        
        # Volatility
        df['ATR'] = self.calculate_atr(df)
        df['Volatility'] = df['Close'].pct_change().rolling(window=20).std() * np.sqrt(252)
        
        # Volume indicators
        df['Volume_SMA'] = df['Volume'].rolling(window=20).mean()
        df['Volume_Ratio'] = df['Volume'] / df['Volume_SMA']
        
        # Price patterns
        df['Price_Change'] = df['Close'].pct_change()
        df['High_Low_Ratio'] = df['High'] / df['Low']
        
        return df
    
    def calculate_rsi(self, prices: pd.Series, window: int = 14) -> pd.Series:
        """Calculate RSI"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
    
    def calculate_atr(self, df: pd.DataFrame, window: int = 14) -> pd.Series:
        """Calculate Average True Range"""
        high_low = df['High'] - df['Low']
        high_close = np.abs(df['High'] - df['Close'].shift())
        low_close = np.abs(df['Low'] - df['Close'].shift())
        true_range = np.maximum(high_low, np.maximum(high_close, low_close))
        return true_range.rolling(window=window).mean()
    
    def momentum_strategy(self, data: pd.DataFrame, symbol: str) -> Optional[TradingSignal]:
        """Momentum trading strategy"""
        if len(data) < 50:
            return None
            
        latest = data.iloc[-1]
        prev_20 = data.iloc[-20:]
        
        # Momentum conditions
        price_momentum = (latest['Close'] - data['Close'].iloc[-20]) / data['Close'].iloc[-20]
        volume_momentum = latest['Volume'] > latest['Volume_SMA'] * 1.5
        rsi_momentum = 50 < latest['RSI'] < 80
        macd_momentum = latest['MACD'] > latest['MACD_Signal']
        
        # Calculate signal strength
        momentum_score = 0
        if price_momentum > 0.05:  # 5% gain in 20 days
            momentum_score += 1
        if volume_momentum:
            momentum_score += 1
        if rsi_momentum:
            momentum_score += 1
        if macd_momentum:
            momentum_score += 1
        
        if momentum_score >= 3:
            # Calculate position sizing and risk management
            atr = latest['ATR']
            entry_price = latest['Close']
            stop_loss = entry_price - (2 * atr)
            take_profit = entry_price + (3 * atr)
            
            # Position size based on risk
            risk_per_share = entry_price - stop_loss
            max_shares = (self.portfolio_value * self.max_risk_per_trade) / risk_per_share
            max_position_value = self.portfolio_value * self.max_position_size
            position_size = min(max_shares, max_position_value / entry_price)
            
            strength = SignalStrength.STRONG if momentum_score == 4 else SignalStrength.MEDIUM
            
            return TradingSignal(
                symbol=symbol,
                strategy=StrategyType.MOMENTUM,
                direction="BUY",
                strength=strength,
                confidence=momentum_score / 4,
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                position_size=position_size,
                reasoning=f"Momentum: {momentum_score}/4 signals, {price_momentum:.2%} price gain",
                timestamp=datetime.now()
            )
        
        return None
    
    def mean_reversion_strategy(self, data: pd.DataFrame, symbol: str) -> Optional[TradingSignal]:
        """Mean reversion trading strategy"""
        if len(data) < 50:
            return None
            
        latest = data.iloc[-1]
        
        # Mean reversion conditions
        bb_oversold = latest['BB_Position'] < 0.1  # Near lower BB
        rsi_oversold = latest['RSI'] < 30
        price_below_sma = latest['Close'] < latest['SMA_20']
        volume_spike = latest['Volume'] > latest['Volume_SMA'] * 2
        
        reversion_score = 0
        if bb_oversold:
            reversion_score += 1
        if rsi_oversold:
            reversion_score += 1
        if price_below_sma:
            reversion_score += 1
        if volume_spike:
            reversion_score += 1
        
        if reversion_score >= 3:
            atr = latest['ATR']
            entry_price = latest['Close']
            stop_loss = entry_price - (1.5 * atr)
            take_profit = latest['SMA_20']  # Target mean reversion to SMA
            
            # Position sizing
            risk_per_share = entry_price - stop_loss
            max_shares = (self.portfolio_value * self.max_risk_per_trade) / risk_per_share
            max_position_value = self.portfolio_value * self.max_position_size
            position_size = min(max_shares, max_position_value / entry_price)
            
            strength = SignalStrength.STRONG if reversion_score == 4 else SignalStrength.MEDIUM
            
            return TradingSignal(
                symbol=symbol,
                strategy=StrategyType.MEAN_REVERSION,
                direction="BUY",
                strength=strength,
                confidence=reversion_score / 4,
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                position_size=position_size,
                reasoning=f"Mean reversion: {reversion_score}/4 signals, RSI={latest['RSI']:.1f}",
                timestamp=datetime.now()
            )
        
        return None
    
    def breakout_strategy(self, data: pd.DataFrame, symbol: str) -> Optional[TradingSignal]:
        """Breakout trading strategy"""
        if len(data) < 50:
            return None
            
        latest = data.iloc[-1]
        recent_20 = data.iloc[-20:]
        
        # Breakout conditions
        resistance_level = recent_20['High'].max()
        support_level = recent_20['Low'].min()
        
        price_breakout = latest['Close'] > resistance_level
        volume_confirmation = latest['Volume'] > latest['Volume_SMA'] * 1.8
        consolidation = (resistance_level - support_level) / support_level < 0.15  # Tight range
        bb_squeeze = latest['BB_Width'] < data['BB_Width'].rolling(window=20).mean()
        
        breakout_score = 0
        if price_breakout:
            breakout_score += 2  # Price breakout is most important
        if volume_confirmation:
            breakout_score += 1
        if consolidation:
            breakout_score += 1
        if bb_squeeze:
            breakout_score += 1
        
        if breakout_score >= 4:
            atr = latest['ATR']
            entry_price = latest['Close']
            stop_loss = resistance_level - (0.5 * atr)  # Just below breakout level
            take_profit = entry_price + (3 * atr)
            
            # Position sizing
            risk_per_share = entry_price - stop_loss
            if risk_per_share > 0:
                max_shares = (self.portfolio_value * self.max_risk_per_trade) / risk_per_share
                max_position_value = self.portfolio_value * self.max_position_size
                position_size = min(max_shares, max_position_value / entry_price)
                
                return TradingSignal(
                    symbol=symbol,
                    strategy=StrategyType.BREAKOUT,
                    direction="BUY",
                    strength=SignalStrength.STRONG,
                    confidence=breakout_score / 5,
                    entry_price=entry_price,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    position_size=position_size,
                    reasoning=f"Breakout: {breakout_score}/5 signals, breaking ${resistance_level:.2f}",
                    timestamp=datetime.now()
                )
        
        return None
    
    def volume_spike_strategy(self, data: pd.DataFrame, symbol: str) -> Optional[TradingSignal]:
        """Volume spike trading strategy"""
        if len(data) < 30:
            return None
            
        latest = data.iloc[-1]
        
        # Volume spike conditions
        extreme_volume = latest['Volume'] > latest['Volume_SMA'] * 3
        price_move = abs(latest['Price_Change']) > 0.03  # 3% price move
        rsi_not_extreme = 25 < latest['RSI'] < 75
        
        if extreme_volume and price_move and rsi_not_extreme:
            direction = "BUY" if latest['Price_Change'] > 0 else "SELL"
            
            atr = latest['ATR']
            entry_price = latest['Close']
            
            if direction == "BUY":
                stop_loss = entry_price - (2 * atr)
                take_profit = entry_price + (2 * atr)
            else:
                stop_loss = entry_price + (2 * atr)
                take_profit = entry_price - (2 * atr)
            
            # Position sizing
            risk_per_share = abs(entry_price - stop_loss)
            max_shares = (self.portfolio_value * self.max_risk_per_trade) / risk_per_share
            max_position_value = self.portfolio_value * self.max_position_size
            position_size = min(max_shares, max_position_value / entry_price)
            
            return TradingSignal(
                symbol=symbol,
                strategy=StrategyType.VOLUME_SPIKE,
                direction=direction,
                strength=SignalStrength.MEDIUM,
                confidence=0.7,
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                position_size=position_size,
                reasoning=f"Volume spike: {latest['Volume_Ratio']:.1f}x avg, {latest['Price_Change']:.2%} move",
                timestamp=datetime.now()
            )
        
        return None
    
    async def analyze_symbol(self, symbol: str) -> List[TradingSignal]:
        """Analyze symbol with all strategies"""
        logger.info(f"📊 Analyzing {symbol} with multi-strategy approach...")
        
        data = self.get_market_data(symbol)
        if data is None:
            return []
        
        signals = []
        
        # Run all strategies
        strategies = [
            self.momentum_strategy,
            self.mean_reversion_strategy,
            self.breakout_strategy,
            self.volume_spike_strategy
        ]
        
        for strategy_func in strategies:
            try:
                signal = strategy_func(data, symbol)
                if signal:
                    signals.append(signal)
                    await self.log_signal(signal)
                    logger.info(f"  ✅ {signal.strategy.value}: {signal.direction} "
                              f"({signal.strength.value}, {signal.confidence:.2%})")
            except Exception as e:
                logger.error(f"  ❌ Strategy {strategy_func.__name__} failed: {e}")
        
        return signals
    
    async def execute_signal(self, signal: TradingSignal) -> bool:
        """Execute trading signal with risk management"""
        logger.info(f"💰 Executing {signal.symbol} {signal.direction} signal...")
        
        # Validate signal
        if signal.position_size <= 0:
            logger.warning(f"⚠️ Invalid position size for {signal.symbol}")
            return False
        
        # Calculate trade value
        trade_value = signal.entry_price * signal.position_size
        
        # Risk checks
        max_trade_value = self.portfolio_value * self.max_position_size
        if trade_value > max_trade_value:
            logger.warning(f"⚠️ Position too large for {signal.symbol}: ${trade_value:.2f}")
            return False
        
        # Execute paper trade
        conn = await self.connect_db()
        try:
            await conn.execute("""
                INSERT INTO paper_trades (event_id, correlation_id, idempotency_key, 
                                        ticker, ts, side, qty, fill_price, status) 
                VALUES ($1, $2, $3, $4, NOW(), $5, $6, $7, 'filled')
            """, 
            str(uuid.uuid4()), str(uuid.uuid4()), 
            f"strategy-trade-{int(time.time())}", signal.symbol,
            signal.direction.lower(), signal.position_size, signal.entry_price)
            
            logger.info(f"✅ {signal.symbol}: {signal.direction} {signal.position_size:.2f} @ ${signal.entry_price:.2f}")
            
            # Log execution
            await conn.execute("""
                INSERT INTO audit_events (event_id, correlation_id, idempotency_key, symbol, ts, 
                                        schema_version, source, event_type, payload) 
                VALUES ($1, $2, $3, $4, NOW(), 1, $5, $6, $7)
            """, 
            str(uuid.uuid4()), str(uuid.uuid4()), f"execution-{int(time.time())}", 
            signal.symbol, "multi_strategy", "trade_executed", 
            json.dumps({
                "strategy": signal.strategy.value,
                "direction": signal.direction,
                "quantity": signal.position_size,
                "price": signal.entry_price,
                "stop_loss": signal.stop_loss,
                "take_profit": signal.take_profit,
                "trade_value": trade_value
            }))
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Execution failed for {signal.symbol}: {e}")
            return False
        finally:
            await conn.close()
    
    async def run_multi_strategy_analysis(self):
        """Run complete multi-strategy analysis"""
        logger.info("🚀 Starting Multi-Strategy Trading Analysis...")
        start_time = time.time()
        
        all_signals = []
        executed_trades = []
        
        # Analyze all symbols
        for symbol in self.symbols:
            signals = await self.analyze_symbol(symbol)
            all_signals.extend(signals)
        
        # Filter and prioritize signals
        # Priority: STRONG > MEDIUM > WEAK, higher confidence
        prioritized_signals = sorted(all_signals, 
                                   key=lambda s: (s.strength.value, s.confidence), 
                                   reverse=True)
        
        # Execute top signals (max 3 concurrent positions)
        max_positions = 3
        for signal in prioritized_signals[:max_positions]:
            if await self.execute_signal(signal):
                executed_trades.append(signal)
        
        total_time = time.time() - start_time
        
        # Summary
        summary = {
            'analysis_time': f"{total_time:.1f}s",
            'symbols_analyzed': len(self.symbols),
            'signals_generated': len(all_signals),
            'trades_executed': len(executed_trades),
            'strategies_used': len(set(s.strategy for s in all_signals)),
            'total_trade_value': sum(s.entry_price * s.position_size for s in executed_trades)
        }
        
        logger.info("🎉 Multi-Strategy Analysis Complete!")
        logger.info(f"📊 Summary: {json.dumps(summary, indent=2)}")
        
        return {
            'signals': all_signals,
            'executed_trades': executed_trades,
            'summary': summary
        }

async def main():
    """Run multi-strategy trading system"""
    engine = MultiStrategyEngine()
    results = await engine.run_multi_strategy_analysis()
    
    print("\n" + "="*80)
    print("⚡ MULTI-STRATEGY TRADING RESULTS")
    print("="*80)
    
    summary = results['summary']
    print(f"✅ Symbols analyzed: {summary['symbols_analyzed']}")
    print(f"✅ Signals generated: {summary['signals_generated']}")
    print(f"✅ Trades executed: {summary['trades_executed']}")
    print(f"✅ Strategies used: {summary['strategies_used']}")
    print(f"✅ Total trade value: ${summary['total_trade_value']:.2f}")
    print(f"✅ Analysis time: {summary['analysis_time']}")
    
    print("\n📈 EXECUTED TRADES:")
    for trade in results['executed_trades']:
        print(f"  {trade.symbol}: {trade.direction} {trade.position_size:.2f} @ ${trade.entry_price:.2f}")
        print(f"    Strategy: {trade.strategy.value} | Confidence: {trade.confidence:.1%}")
        print(f"    Stop Loss: ${trade.stop_loss:.2f} | Take Profit: ${trade.take_profit:.2f}")
        print(f"    Reasoning: {trade.reasoning}")
        print()

if __name__ == "__main__":
    asyncio.run(main())