from datetime import datetime

from pydantic import BaseModel


class RawProductionEvent(BaseModel):
    source_system: str
    source_event_id: str
    event_ts: datetime
    plant_code: str
    source_item_id: str
    source_item_desc: str | None = None
    qty: float
    uom: str = "LB"
    scrap_qty: float = 0.0


class CanonicalProductionEvent(BaseModel):
    source_system: str
    source_event_id: str
    event_ts: datetime
    plant_code: str
    product_key: int
    produced_qty_lb: float
    scrap_qty_lb: float


class IngestResponse(BaseModel):
    status: str
    event: CanonicalProductionEvent
