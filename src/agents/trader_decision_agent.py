from __future__ import annotations

import asyncio
from collections import OrderedDict
from datetime import datetime, timezone

from src.agents.base_agent import BaseAgent
from src.core.config import load_settings
from src.core.logging import set_correlation_id
from src.core.trade_decision import DecisionThresholds, decide_trade_v2
from src.models.events_prediction import PredictionEvent
from src.models.events_trade_decision import TradeDecisionEvent
from src.monitoring.metrics import EVENTS_PROCESSED, EVENT_LATENCY
from src.streaming.kafka_consumer import AvroConsumer
from src.streaming.kafka_producer import AvroProducer

PRED_SCHEMA = "src/schemas/prediction.avsc"
DECISION_SCHEMA = "src/schemas/trade_decision.avsc"


class LruDedupSet:
    def __init__(self, max_size: int = 10000) -> None:
        self._max_size = max_size
        self._data: OrderedDict[str, None] = OrderedDict()

    def add(self, key: str) -> bool:
        if key in self._data:
            self._data.move_to_end(key)
            return False
        self._data[key] = None
        if len(self._data) > self._max_size:
            self._data.popitem(last=False)
        return True

    def __len__(self) -> int:
        return len(self._data)


def _decision_reason(decision: str) -> str:
    if decision == "BUY":
        return "long_signal_confident"
    if decision == "SELL":
        return "short_signal_confident"
    return "insufficient_confidence_or_neutral"


class TraderDecisionAgent(BaseAgent):
    """Consumes predictions and emits BUY/SELL/HOLD decisions."""

    def __init__(self) -> None:
        settings = load_settings()
        super().__init__("TraderDecisionAgent", settings.trader_decision_agent.port)
        with open(PRED_SCHEMA, "r", encoding="utf-8") as handle:
            pred_schema = handle.read()
        self.consumer = AvroConsumer(
            self.settings.kafka,
            self.settings.kafka.group_ids.trader_decision,
            pred_schema,
            self.settings.kafka.topics.predictions_tft,
        )
        self.producer = AvroProducer(self.settings.kafka)
        with open(DECISION_SCHEMA, "r", encoding="utf-8") as handle:
            self._decision_schema = handle.read()
        self._dedup = LruDedupSet(max_size=10000)
        self._thresholds = DecisionThresholds(
            min_confidence=settings.trader_decision_agent.min_confidence,
            buy_return_threshold=settings.trader_decision_agent.buy_return_threshold,
            sell_return_threshold=settings.trader_decision_agent.sell_return_threshold,
        )
        self._out_topic = self.settings.kafka.topics.trade_decisions

    def _select_quantiles(self, event: PredictionEvent) -> tuple[float | None, float | None, float | None]:
        horizon = getattr(event, "horizon_minutes", 30)
        if horizon == 45:
            return (
                getattr(event, "horizon_45_q10", None),
                getattr(event, "horizon_45_q50", None),
                getattr(event, "horizon_45_q90", None),
            )
        if horizon == 60:
            return (
                getattr(event, "horizon_60_q10", None),
                getattr(event, "horizon_60_q50", None),
                getattr(event, "horizon_60_q90", None),
            )
        return (
            getattr(event, "horizon_30_q10", None),
            getattr(event, "horizon_30_q50", None),
            getattr(event, "horizon_30_q90", None),
        )

    async def start(self) -> None:
        self.app.add_event_handler("shutdown", self._shutdown)
        self.track_task(asyncio.create_task(self._consume_loop()))

    async def _shutdown(self) -> None:
        self.consumer.close()
        self.producer.close()

    async def _consume_loop(self) -> None:
        while not self.is_shutting_down:
          try:
            msg = self.consumer.poll(1.0)
            if msg is None:
                await asyncio.sleep(0.1)
                continue
            payload = msg.value()
            event = PredictionEvent.model_validate(payload)
            set_correlation_id(str(event.correlation_id))
            if not self._dedup.add(event.idempotency_key):
                self.logger.info(
                    "trade_decision_duplicate_dropped",
                    symbol=event.symbol,
                    idempotency_key=event.idempotency_key,
                )
                self.consumer.commit()
                continue

            start_ts = datetime.now(tz=timezone.utc)
            q10, q50, q90 = self._select_quantiles(event)
            decision = decide_trade_v2(
                {
                    "direction": event.direction,
                    "confidence": event.confidence,
                    "q10": q10,
                    "q50": q50,
                    "q90": q90,
                },
                self._thresholds,
            )
            reason = _decision_reason(decision)
            decision_event = TradeDecisionEvent(
                idempotency_key=event.idempotency_key,
                symbol=event.symbol,
                source=self.name,
                correlation_id=event.correlation_id,
                ts=event.ts,
                decision=decision,
                confidence=event.confidence,
                reason=reason,
                # Propagate price + quantiles for downstream risk sizing
                price=event.prediction,
                prediction=event.prediction,
                horizon_30_q10=getattr(event, "horizon_30_q10", 0.0),
                horizon_30_q50=getattr(event, "horizon_30_q50", 0.0),
                horizon_30_q90=getattr(event, "horizon_30_q90", 0.0),
                direction=getattr(event, "direction", "neutral"),
                created_at=datetime.now(tz=timezone.utc),
            )
            payload_out = decision_event.to_avro_dict()
            self.producer.produce(self._out_topic, self._decision_schema, payload_out, key=event.symbol)
            self.consumer.commit()

            duration = (datetime.now(tz=timezone.utc) - start_ts).total_seconds()
            EVENTS_PROCESSED.labels(agent=self.name, event_type=self._out_topic).inc()
            EVENT_LATENCY.labels(agent=self.name, event_type=self._out_topic).observe(duration)
            self.logger.info(
                "trade_decision_emitted",
                symbol=event.symbol,
                decision=decision,
                confidence=event.confidence,
                reason=reason,
            )
          except asyncio.CancelledError:
              self.logger.info("consume_loop_cancelled")
              break
          except Exception:
              self.logger.exception("consume_loop_error")
              await asyncio.sleep(1.0)

    async def health(self) -> dict[str, str]:
        return {"status": "ok", "agent": "TraderDecisionAgent"}


def main() -> None:
    TraderDecisionAgent().run()


if __name__ == "__main__":
    main()
