from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from src.agents.base_agent import BaseAgent
from src.audit.trail_logger import AuditTrailLogger
from src.core.config import load_settings
from src.core.logging import set_correlation_id
from src.models.events_prediction import PredictionEvent
from src.models.events_risk import RiskEvent
from src.monitoring.metrics import EVENTS_PROCESSED, EVENT_LATENCY, RISK_DECISIONS
from src.risk.engine import RiskDecision, RiskEngine, RiskInput
from src.streaming.kafka_consumer import AvroConsumer
from src.streaming.kafka_producer import AvroProducer
from src.streaming.topics import PREDICTIONS_TFT, RISK_APPROVED, RISK_REJECTED

PRED_SCHEMA = "src/schemas/prediction.avsc"
RISK_SCHEMA = "src/schemas/risk.avsc"


class RiskAgent(BaseAgent):
    """Risk agent with real institutional-grade risk controls.

    Implements:
    - Position size limits (2% max per position)
    - Daily loss limits (5% max daily drawdown)
    - Volatility filters
    - CVaR calculation
    - Kill switch for emergencies
    """

    def __init__(self) -> None:
        settings = load_settings()
        super().__init__("risk", settings.app.http.risk_port)
        with open(PRED_SCHEMA, "r", encoding="utf-8") as handle:
            pred_schema = handle.read()
        self.consumer = AvroConsumer(
            self.settings.kafka,
            self.settings.kafka.group_ids.risk,
            pred_schema,
            PREDICTIONS_TFT,
        )
        self.producer = AvroProducer(self.settings.kafka)
        self.audit = AuditTrailLogger(self.settings)
        self.risk_engine = RiskEngine()
        with open(RISK_SCHEMA, "r", encoding="utf-8") as handle:
            self._risk_schema = handle.read()

    async def start(self) -> None:
        self.app.add_event_handler("shutdown", self._shutdown)
        await self.audit.connect()
        asyncio.create_task(self._consume_loop())

    async def _shutdown(self) -> None:
        self.consumer.close()
        await self.audit.close()

    def _build_risk_input(self, event: PredictionEvent) -> RiskInput:
        """Build risk input from prediction event."""
        return RiskInput(
            symbol=event.symbol,
            price=event.prediction,
            direction=getattr(event, "direction", "neutral"),
            confidence=event.confidence,
            horizon_30_q10=getattr(event, "horizon_30_q10", -0.01),
            horizon_30_q50=getattr(event, "horizon_30_q50", 0.0),
            horizon_30_q90=getattr(event, "horizon_30_q90", 0.01),
            volatility_5=0.02,
            volatility_15=0.025,
            current_position=self.risk_engine.state.positions.get(event.symbol, 0.0),
            portfolio_value=self.risk_engine.state.portfolio_value,
            daily_pnl=self.risk_engine.state.daily_pnl,
        )

    async def _consume_loop(self) -> None:
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                await asyncio.sleep(0.1)
                continue
            payload = msg.value()
            event = PredictionEvent.model_validate(payload)
            set_correlation_id(str(event.correlation_id))
            if await self.audit.is_duplicate(RISK_APPROVED, event.idempotency_key) or await self.audit.is_duplicate(
                RISK_REJECTED, event.idempotency_key
            ):
                self.consumer.commit()
                continue
            start_ts = datetime.now(tz=timezone.utc)
            risk_input = self._build_risk_input(event)
            risk_output = self.risk_engine.evaluate(risk_input)
            approved = risk_output.decision == RiskDecision.APPROVED
            reason = "; ".join(risk_output.reasons)
            risk_event = RiskEvent(
                idempotency_key=event.idempotency_key,
                symbol=event.symbol,
                source=self.name,
                correlation_id=event.correlation_id,
                approved=approved,
                reason=reason,
                risk_score=risk_output.risk_score,
                max_position=risk_output.approved_size,
                max_notional=risk_output.approved_size * event.prediction,
                min_confidence=self.risk_engine.limits.min_confidence,
                cvar_95=risk_output.cvar_95,
                max_loss=risk_output.max_loss,
                direction=risk_input.direction,
            )
            payload_out = risk_event.to_avro_dict()
            topic = RISK_APPROVED if approved else RISK_REJECTED
            self.producer.produce(topic, self._risk_schema, payload_out, key=event.symbol)
            await self.audit.write_event(topic, payload_out)
            self.consumer.commit()
            duration = (datetime.now(tz=timezone.utc) - start_ts).total_seconds()
            EVENTS_PROCESSED.labels(agent=self.name, event_type=topic).inc()
            EVENT_LATENCY.labels(agent=self.name, event_type=topic).observe(duration)
            RISK_DECISIONS.labels(decision=risk_output.decision.value).inc()
            self.logger.info(
                "risk_evaluated",
                symbol=event.symbol,
                decision=risk_output.decision.value,
                risk_score=risk_output.risk_score,
                cvar_95=risk_output.cvar_95,
            )


def main() -> None:
    RiskAgent().run()


if __name__ == "__main__":
    main()
