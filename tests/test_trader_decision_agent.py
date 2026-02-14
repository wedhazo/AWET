from src.agents.trader_decision_agent import LruDedupSet, decide_trade


def test_long_high_confidence_buy() -> None:
    decision, reason = decide_trade("long", 0.9, 0.65)
    assert decision == "BUY"
    assert reason == "long_signal_confident"


def test_short_high_confidence_sell() -> None:
    decision, reason = decide_trade("short", 0.9, 0.65)
    assert decision == "SELL"
    assert reason == "short_signal_confident"


def test_low_confidence_hold() -> None:
    decision, reason = decide_trade("long", 0.4, 0.65)
    assert decision == "HOLD"
    assert reason == "insufficient_confidence_or_neutral"


def test_idempotency_dedup_drops_duplicate() -> None:
    dedup = LruDedupSet(max_size=3)
    assert dedup.add("k1") is True
    assert dedup.add("k1") is False
    assert len(dedup) == 1
