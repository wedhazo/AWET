#!/usr/bin/env python3
"""
EXECUTE TRADING SESSION NOW - Manual override for cron
"""

import sys
import os
sys.path.append('/home/kironix/Awet')

from aggressive_day_trader import AggressiveDayTrader
from datetime import datetime

def execute_trading_session():
    """Force execute a trading session regardless of market hours"""
    
    trader = AggressiveDayTrader()
    
    print("🔥 AGGRESSIVE DAY TRADING - MANUAL EXECUTION")
    print("💰 Starting Capital: $30,000")
    print("🎯 Strategies: Day Trading + Options + Scalping")
    print("=" * 60)
    
    # Execute trading session
    portfolio_value, daily_pnl, total_trades = trader.trading_session()
    
    print("\n📋 FINAL EXECUTION SUMMARY:")
    print(f"💰 Portfolio Value: ${portfolio_value:,.2f}")
    print(f"📈 Session P&L: ${daily_pnl:+,.2f}")
    print(f"🎯 Trades Executed: {total_trades}")
    
    # Calculate metrics for the cron report
    roi_percent = (daily_pnl / 30000) * 100
    
    # Performance rating
    if roi_percent > 3:
        status = "🔥 EXCELLENT SESSION"
    elif roi_percent > 1:
        status = "✅ PROFITABLE"
    elif roi_percent > 0:
        status = "👍 POSITIVE"
    elif roi_percent > -1:
        status = "⚠️ MINOR LOSS"
    else:
        status = "🔴 LOSS"
    
    print(f"🏆 Status: {status}")
    print(f"📊 ROI: {roi_percent:+.2f}%")
    
    return portfolio_value, daily_pnl, total_trades, status

if __name__ == "__main__":
    execute_trading_session()