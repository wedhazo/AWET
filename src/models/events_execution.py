from __future__ import annotations

from src.models.base import BaseEvent


class ExecutionEvent(BaseEvent):
    status: str
    filled_qty: int
    avg_price: float
    paper_trade: bool
