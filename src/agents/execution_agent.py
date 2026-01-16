from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

from src.agents.base_agent import BaseAgent
from src.audit.trail_logger import AuditTrailLogger
from src.core.config import load_settings
from src.core.logging import set_correlation_id
from src.models.events_execution import ExecutionEvent
from src.models.events_risk import RiskEvent
from src.monitoring.metrics import EVENTS_PROCESSED, EVENT_LATENCY
from src.streaming.kafka_consumer import AvroConsumer
from src.streaming.kafka_producer import AvroProducer
from src.streaming.topics import EXECUTION_BLOCKED, EXECUTION_COMPLETED, RISK_APPROVED

RISK_SCHEMA = "src/schemas/risk.avsc"
EXEC_SCHEMA = "src/schemas/execution.avsc"


class ExecutionAgent(BaseAgent):
    def __init__(self) -> None:
        settings = load_settings()
        super().__init__("execution", settings.app.http.execution_port)
        with open(RISK_SCHEMA, "r", encoding="utf-8") as handle:
            risk_schema = handle.read()
        self.consumer = AvroConsumer(
            self.settings.kafka,
            self.settings.kafka.group_ids.execution,
            risk_schema,
            RISK_APPROVED,
        )
        self.producer = AvroProducer(self.settings.kafka)
        self.audit = AuditTrailLogger(self.settings)
        with open(EXEC_SCHEMA, "r", encoding="utf-8") as handle:
            self._exec_schema = handle.read()

    async def start(self) -> None:
        self.app.add_event_handler("shutdown", self._shutdown)
        await self.audit.connect()
        asyncio.create_task(self._consume_loop())

    async def _shutdown(self) -> None:
        self.consumer.close()
        await self.audit.close()

    async def _consume_loop(self) -> None:
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                await asyncio.sleep(0.1)
                continue
            payload = msg.value()
            event = RiskEvent.model_validate(payload)
            set_correlation_id(str(event.correlation_id))
            # Check idempotency for both possible output topics
            if await self.audit.is_duplicate(EXECUTION_COMPLETED, event.idempotency_key):
                self.consumer.commit()
                continue
            if await self.audit.is_duplicate(EXECUTION_BLOCKED, event.idempotency_key):
                self.consumer.commit()
                continue
            start_ts = datetime.now(tz=timezone.utc)
            approval_file = self.settings.app.execution_approval_file
            approved = os.path.exists(approval_file)
            status = "filled" if approved else "blocked"
            execution = ExecutionEvent(
                idempotency_key=event.idempotency_key,
                symbol=event.symbol,
                source=self.name,
                correlation_id=event.correlation_id,
                status=status,
                filled_qty=1 if approved else 0,
                avg_price=event.risk_score,
                paper_trade=True,
            )
            payload_out = execution.to_avro_dict()
            # GAP 6.1: Publish to separate topic based on approval status
            output_topic = EXECUTION_COMPLETED if approved else EXECUTION_BLOCKED
            self.producer.produce(
                output_topic, self._exec_schema, payload_out, key=event.symbol
            )
            await self.audit.write_event(output_topic, payload_out)
            self.consumer.commit()
            duration = (datetime.now(tz=timezone.utc) - start_ts).total_seconds()
            EVENTS_PROCESSED.labels(agent=self.name, event_type=output_topic).inc()
            EVENT_LATENCY.labels(agent=self.name, event_type=output_topic).observe(duration)
            self.logger.info(
                "execution_processed",
                symbol=event.symbol,
                status=status,
                topic=output_topic,
                approved=approved,
            )


def main() -> None:
    ExecutionAgent().run()


if __name__ == "__main__":
    main()
