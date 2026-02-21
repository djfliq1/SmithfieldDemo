from sqlalchemy.orm import Session
from sqlalchemy import select

from app.contracts import CanonicalProductionEvent
from app.models import FactProduction


class FactProductionLoader:
    def __init__(self, session: Session):
        self._session = session

    def insert_if_new(self, e: CanonicalProductionEvent) -> bool:
        stmt = select(FactProduction).where(
            FactProduction.source_system == e.source_system,
            FactProduction.source_event_id == e.source_event_id,
        )
        existing = self._session.execute(stmt).scalars().first()
        if existing is not None:
            return False

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
        self._session.commit()
        return True
