"""
Trading Safety Layer for AWET.

Provides risk limits and kill switch functionality for the execution layer.
All trades must pass through these checks before being submitted.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from src.integrations.trades_repository import TradesRepository

logger = structlog.get_logger("trading_safety")


@dataclass
class TradingLimits:
    """Trading risk limits - can be configured via env vars or config."""

    # Master kill switch
    trading_enabled: bool = False

    # Per-trade limits
    max_notional_per_trade: float = 1000.0  # USD
    max_qty_per_trade: int = 100

    # Daily limits
    max_trades_per_day: int = 20
    max_notional_per_day: float = 10000.0

    # Position limits
    max_position_pct: float = 0.05  # 5% of portfolio per symbol
    max_total_exposure: float = 50000.0  # Total portfolio exposure
    max_exposure_per_symbol: float = 10000.0

    # Cooldown
    cooldown_seconds: int = 300  # 5 min between trades for same symbol

    @classmethod
    def from_env(cls) -> "TradingLimits":
        """Load limits from environment variables."""
        return cls(
            trading_enabled=os.getenv("TRADING_ENABLED", "false").lower() == "true",
            max_notional_per_trade=float(os.getenv("MAX_NOTIONAL_PER_TRADE", "1000")),
            max_qty_per_trade=int(os.getenv("MAX_QTY_PER_TRADE", "100")),
            max_trades_per_day=int(os.getenv("MAX_TRADES_PER_DAY", "20")),
            max_notional_per_day=float(os.getenv("MAX_NOTIONAL_PER_DAY", "10000")),
            max_position_pct=float(os.getenv("MAX_POSITION_PCT", "0.05")),
            max_total_exposure=float(os.getenv("MAX_TOTAL_EXPOSURE", "50000")),
            max_exposure_per_symbol=float(os.getenv("MAX_EXPOSURE_PER_SYMBOL", "10000")),
            cooldown_seconds=int(os.getenv("COOLDOWN_SECONDS", "300")),
        )


@dataclass
class SafetyCheckResult:
    """Result of a safety check."""

    passed: bool
    reason: str | None = None
    check_name: str = ""

    def __bool__(self) -> bool:
        return self.passed


class TradingSafetyLayer:
    """
    Centralized safety layer for all trading operations.

    All trades must pass through check_trade() before execution.
    """

    def __init__(
        self,
        limits: TradingLimits | None = None,
        trades_repo: "TradesRepository | None" = None,
    ):
        self.limits = limits or TradingLimits.from_env()
        self.trades_repo = trades_repo
        self._daily_trade_count = 0
        self._daily_notional = 0.0
        self._last_reset_date: datetime | None = None

    def _reset_daily_counters_if_needed(self) -> None:
        """Reset daily counters at start of new trading day."""
        now = datetime.now(tz=timezone.utc)
        if self._last_reset_date is None or self._last_reset_date.date() != now.date():
            self._daily_trade_count = 0
            self._daily_notional = 0.0
            self._last_reset_date = now

    def check_kill_switch(self) -> SafetyCheckResult:
        """Check if trading is enabled via TRADING_ENABLED env var."""
        # Re-check env var each time (allows runtime toggling)
        enabled = os.getenv("TRADING_ENABLED", "false").lower() == "true"
        if not enabled:
            return SafetyCheckResult(
                passed=False,
                reason="TRADING_ENABLED=false (kill switch active)",
                check_name="kill_switch",
            )
        return SafetyCheckResult(passed=True, check_name="kill_switch")

    def check_notional_limit(
        self, notional: float, symbol: str
    ) -> SafetyCheckResult:
        """Check if trade notional is within limits."""
        if notional > self.limits.max_notional_per_trade:
            return SafetyCheckResult(
                passed=False,
                reason=f"Notional ${notional:.2f} > max ${self.limits.max_notional_per_trade:.2f}",
                check_name="notional_limit",
            )
        return SafetyCheckResult(passed=True, check_name="notional_limit")

    def check_quantity_limit(self, qty: int, symbol: str) -> SafetyCheckResult:
        """Check if trade quantity is within limits."""
        if qty > self.limits.max_qty_per_trade:
            return SafetyCheckResult(
                passed=False,
                reason=f"Qty {qty} > max {self.limits.max_qty_per_trade}",
                check_name="qty_limit",
            )
        return SafetyCheckResult(passed=True, check_name="qty_limit")

    def check_daily_trade_count(self) -> SafetyCheckResult:
        """Check if daily trade count is within limits."""
        self._reset_daily_counters_if_needed()
        if self._daily_trade_count >= self.limits.max_trades_per_day:
            return SafetyCheckResult(
                passed=False,
                reason=f"Daily trades {self._daily_trade_count} >= max {self.limits.max_trades_per_day}",
                check_name="daily_trade_count",
            )
        return SafetyCheckResult(passed=True, check_name="daily_trade_count")

    def check_daily_notional(self, notional: float) -> SafetyCheckResult:
        """Check if daily notional is within limits."""
        self._reset_daily_counters_if_needed()
        projected = self._daily_notional + notional
        if projected > self.limits.max_notional_per_day:
            return SafetyCheckResult(
                passed=False,
                reason=f"Daily notional ${projected:.2f} would exceed max ${self.limits.max_notional_per_day:.2f}",
                check_name="daily_notional",
            )
        return SafetyCheckResult(passed=True, check_name="daily_notional")

    async def check_position_limit(
        self,
        symbol: str,
        notional: float,
        portfolio_value: float = 100000.0,
    ) -> SafetyCheckResult:
        """Check if position size is within portfolio percentage limit."""
        max_allowed = portfolio_value * self.limits.max_position_pct
        if notional > max_allowed:
            return SafetyCheckResult(
                passed=False,
                reason=f"Position ${notional:.2f} > {self.limits.max_position_pct:.0%} of portfolio (${max_allowed:.2f})",
                check_name="position_limit",
            )
        return SafetyCheckResult(passed=True, check_name="position_limit")

    async def check_total_exposure(self, notional: float) -> SafetyCheckResult:
        """Check if total portfolio exposure is within limits."""
        current_exposure = 0.0
        if self.trades_repo:
            try:
                current_exposure = await self.trades_repo.get_total_exposure()
            except Exception:
                pass  # Continue with 0 if DB unavailable

        projected = current_exposure + notional
        if projected > self.limits.max_total_exposure:
            return SafetyCheckResult(
                passed=False,
                reason=f"Total exposure ${projected:.2f} would exceed max ${self.limits.max_total_exposure:.2f}",
                check_name="total_exposure",
            )
        return SafetyCheckResult(passed=True, check_name="total_exposure")

    async def check_symbol_exposure(
        self, symbol: str, notional: float
    ) -> SafetyCheckResult:
        """Check if per-symbol exposure is within limits."""
        current_exposure = 0.0
        if self.trades_repo:
            try:
                current_exposure = await self.trades_repo.get_symbol_exposure(symbol)
            except Exception:
                pass

        projected = current_exposure + notional
        if projected > self.limits.max_exposure_per_symbol:
            return SafetyCheckResult(
                passed=False,
                reason=f"{symbol} exposure ${projected:.2f} would exceed max ${self.limits.max_exposure_per_symbol:.2f}",
                check_name="symbol_exposure",
            )
        return SafetyCheckResult(passed=True, check_name="symbol_exposure")

    async def check_cooldown(self, symbol: str) -> SafetyCheckResult:
        """Check if symbol is in cooldown period."""
        if not self.trades_repo:
            return SafetyCheckResult(passed=True, check_name="cooldown")

        try:
            last_trade_time = await self.trades_repo.get_last_trade_time(symbol)
            if last_trade_time:
                elapsed = (datetime.now(tz=timezone.utc) - last_trade_time).total_seconds()
                if elapsed < self.limits.cooldown_seconds:
                    remaining = int(self.limits.cooldown_seconds - elapsed)
                    return SafetyCheckResult(
                        passed=False,
                        reason=f"{symbol} in cooldown: {remaining}s remaining",
                        check_name="cooldown",
                    )
        except Exception:
            pass

        return SafetyCheckResult(passed=True, check_name="cooldown")

    async def check_trade(
        self,
        symbol: str,
        side: str,
        qty: int,
        price: float,
        portfolio_value: float = 100000.0,
    ) -> tuple[bool, list[SafetyCheckResult]]:
        """
        Run all safety checks for a proposed trade.

        Returns:
            Tuple of (all_passed, list_of_check_results)
        """
        notional = qty * price
        results: list[SafetyCheckResult] = []

        # Synchronous checks
        results.append(self.check_kill_switch())
        results.append(self.check_notional_limit(notional, symbol))
        results.append(self.check_quantity_limit(qty, symbol))
        results.append(self.check_daily_trade_count())
        results.append(self.check_daily_notional(notional))

        # Async checks (only for BUY - sells reduce exposure)
        if side.lower() == "buy":
            results.append(await self.check_position_limit(symbol, notional, portfolio_value))
            results.append(await self.check_total_exposure(notional))
            results.append(await self.check_symbol_exposure(symbol, notional))
            results.append(await self.check_cooldown(symbol))

        all_passed = all(r.passed for r in results)
        failed = [r for r in results if not r.passed]

        if failed:
            logger.warning(
                "trade_rejected_by_safety",
                symbol=symbol,
                side=side,
                qty=qty,
                price=price,
                notional=notional,
                failed_checks=[r.check_name for r in failed],
                reasons=[r.reason for r in failed],
            )
        else:
            logger.debug(
                "trade_passed_safety_checks",
                symbol=symbol,
                side=side,
                qty=qty,
                price=price,
                notional=notional,
            )

        return all_passed, results

    def record_trade(self, notional: float) -> None:
        """Record a completed trade for daily limit tracking."""
        self._reset_daily_counters_if_needed()
        self._daily_trade_count += 1
        self._daily_notional += notional


# Convenience function for checking if trading is enabled
def is_trading_enabled() -> bool:
    """Quick check if TRADING_ENABLED=true."""
    return os.getenv("TRADING_ENABLED", "false").lower() == "true"
