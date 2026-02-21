from sqlalchemy.orm import Session

from app.contracts import CanonicalProductionEvent, IngestResponse, RawProductionEvent
from app.loaders.fact_loader import FactProductionLoader
from app.mapping_repo import ProductMappingRepo
from app.registry import PluginRegistry

_KG_TO_LB = 2.2046226218


class NormalizationError(Exception):
    def __init__(self, uom: str):
        super().__init__(f"Unsupported UOM: {uom!r}. Supported UOMs: LB, KG")
        self.uom = uom


def to_lb(qty: float, uom: str) -> float:
    uom_upper = uom.upper()
    if uom_upper == "LB":
        return float(qty)
    if uom_upper == "KG":
        return float(qty) * _KG_TO_LB
    raise NormalizationError(uom)


class IngestOrchestrator:
    def __init__(self, registry: PluginRegistry):
        self._registry = registry

    def ingest_production(self, session: Session, payload: dict) -> IngestResponse:
        source_system = payload.get("source_system")
        if not source_system:
            raise ValueError("payload must include 'source_system'")

        plugin = self._registry.resolve(source_system)
        transformed = plugin.transform_payload(payload)
        raw = RawProductionEvent.model_validate(transformed)

        mapping_repo = ProductMappingRepo(session)
        product_key = mapping_repo.resolve_product_key(
            raw.source_system, raw.source_item_id, raw.plant_code
        )

        produced_lb = to_lb(raw.qty, raw.uom)
        scrap_lb = to_lb(raw.scrap_qty, raw.uom)

        canonical = CanonicalProductionEvent(
            source_system=raw.source_system,
            source_event_id=raw.source_event_id,
            event_ts=raw.event_ts,
            plant_code=raw.plant_code,
            product_key=product_key,
            produced_qty_lb=produced_lb,
            scrap_qty_lb=scrap_lb,
        )

        loader = FactProductionLoader(session)
        inserted = loader.insert_if_new(canonical)

        return IngestResponse(
            status="inserted" if inserted else "duplicate",
            event=canonical,
        )
