from typing import Generator

from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session

from app.contracts import IngestResponse
from app.db import build_engine, build_session_factory, load_db_config
from app.mapping_repo import MappingNotFoundError
from app.models import Base
from app.orchestration import IngestOrchestrator, NormalizationError
from app.plugins.beef_wms import BeefWmsPlugin
from app.plugins.pork_erp import PorkErpPlugin
from app.plugins.poultry_mes import PoultryMesPlugin
from app.registry import PluginNotFoundError, PluginRegistry
from app.models import Base, FactProduction, DimProduct



# --- DB setup ---
_config = load_db_config()
_engine = build_engine(_config)
_session_factory = build_session_factory(_engine)
Base.metadata.create_all(bind=_engine)

# --- Plugin registry ---
_registry = PluginRegistry()
_registry.register(PorkErpPlugin)
_registry.register(BeefWmsPlugin)
_registry.register(PoultryMesPlugin)

# --- Orchestrator ---
_orchestrator = IngestOrchestrator(_registry)

# --- FastAPI app ---
app = FastAPI(title="Protein Platform Ingestion API")


def get_session() -> Generator[Session, None, None]:
    with _session_factory() as session:
        yield session


@app.get("/health")
def health():
    return {"status": "ok", "sources": _registry.keys()}


@app.post("/ingest/production", response_model=IngestResponse)
def ingest_production(payload: dict, session: Session = Depends(get_session)):
    try:
        return _orchestrator.ingest_production(session, payload)
    except PluginNotFoundError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except MappingNotFoundError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except NormalizationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/production")
def get_production(
    session: Session = Depends(get_session),
    plant_code: str | None = None,
    source_system: str | None = None,
    limit: int = 100,
):
    # guardrails
    if limit < 1:
        limit = 1
    if limit > 500:
        limit = 500

    query = session.query(FactProduction)

    if plant_code:
        query = query.filter(FactProduction.plant_code == plant_code)

    if source_system:
        query = query.filter(FactProduction.source_system == source_system)

    rows = query.order_by(FactProduction.event_ts.desc()).limit(limit).all()

    return [
        {
            "event_ts": r.event_ts.isoformat(),
            "plant_code": r.plant_code,
            "product_key": r.product_key,
            "produced_qty_lb": float(r.produced_qty_lb),
            "scrap_qty_lb": float(r.scrap_qty_lb),
            "source_system": r.source_system,
            "source_event_id": r.source_event_id,
        }
        for r in rows
    ]


@app.get("/production/enriched")
def get_production_enriched(
    session: Session = Depends(get_session),
    plant_code: str | None = None,
    source_system: str | None = None,
    protein_type: str | None = None,
    limit: int = 100,
):
    if limit < 1:
        limit = 1
    if limit > 500:
        limit = 500

    q = (
        session.query(FactProduction, DimProduct)
        .join(DimProduct, FactProduction.product_key == DimProduct.product_key)
    )

    if plant_code:
        q = q.filter(FactProduction.plant_code == plant_code)

    if source_system:
        q = q.filter(FactProduction.source_system == source_system)

    if protein_type:
        q = q.filter(DimProduct.protein_type == protein_type)

    rows = q.order_by(FactProduction.event_ts.desc()).limit(limit).all()

    return [
        {
            # fact fields
            "event_ts": fact.event_ts.isoformat(),
            "plant_code": fact.plant_code,
            "source_system": fact.source_system,
            "source_event_id": fact.source_event_id,
            "produced_qty_lb": float(fact.produced_qty_lb),
            "scrap_qty_lb": float(fact.scrap_qty_lb),
            "event_date": fact.event_ts.date().isoformat(),
            "event_hour": fact.event_ts.hour,


            # dim fields (flattened)
            "product_key": prod.product_key,
            "canonical_sku": prod.canonical_sku,
            "product_name": prod.product_name,
            "protein_type": prod.protein_type,
            "cut_type": prod.cut_type,
            "product_uom": prod.uom,
            "product_is_active": prod.is_active,
        }
        for fact, prod in rows
    ]

