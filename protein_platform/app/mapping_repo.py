from sqlalchemy.orm import Session
from sqlalchemy import select

from app.models import MapProductSourceToCanonical


class MappingNotFoundError(Exception):
    def __init__(self, source_system: str, source_item_id: str, plant_code: str | None):
        super().__init__(
            f"No mapping found for source_system={source_system!r}, "
            f"source_item_id={source_item_id!r}, plant_code={plant_code!r}"
        )
        self.source_system = source_system
        self.source_item_id = source_item_id
        self.plant_code = plant_code


class ProductMappingRepo:
    def __init__(self, session: Session):
        self._session = session

    def resolve_product_key(
        self, source_system: str, source_item_id: str, plant_code: str | None
    ) -> int:
        # Exact match on plant_code first
        stmt = select(MapProductSourceToCanonical).where(
            MapProductSourceToCanonical.source_system == source_system,
            MapProductSourceToCanonical.source_item_id == source_item_id,
            MapProductSourceToCanonical.plant_code == plant_code,
            MapProductSourceToCanonical.is_current.is_(True),
        )
        row = self._session.execute(stmt).scalars().first()
        if row is not None:
            return row.product_key

        # Fallback: plant_code IS NULL
        stmt_null = select(MapProductSourceToCanonical).where(
            MapProductSourceToCanonical.source_system == source_system,
            MapProductSourceToCanonical.source_item_id == source_item_id,
            MapProductSourceToCanonical.plant_code.is_(None),
            MapProductSourceToCanonical.is_current.is_(True),
        )
        row_null = self._session.execute(stmt_null).scalars().first()
        if row_null is not None:
            return row_null.product_key

        raise MappingNotFoundError(source_system, source_item_id, plant_code)
