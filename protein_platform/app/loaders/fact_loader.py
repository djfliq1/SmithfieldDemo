from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from app.contracts import CanonicalProductionEvent
from app.models import FactProduction


# Idempotency is enforced at the database layer via UNIQUE(source_system, source_event_id).
# This prevents race conditions and guarantees correctness even under concurrent ingestion.
class FactProductionLoader:
    def __init__(self, session: Session):
        self._session = session

    def insert_if_new(self, e: CanonicalProductionEvent) -> bool:
        row = FactProduction(
            event_ts=e.event_ts,
            plant_code=e.plant_code,
            product_key=e.product_key,
            produced_qty_lb=e.produced_qty_lb,
            scrap_qty_lb=e.scrap_qty_lb,
            source_system=e.source_system,
            source_event_id=e.source_event_id,
        )
        self._session.add(row)
        try:
            self._session.commit()
            return True
        except IntegrityError:
            self._session.rollback()
            return False
