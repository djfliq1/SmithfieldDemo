from typing import Generator, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.contracts import IngestResponse
from app.db import build_engine, build_session_factory, load_db_config
from app.mapping_repo import MappingNotFoundError
from app.models import Base
from app.models import DimPlant, FactPriceByPlant, DimProduct
from app.views import ensure_protein_views
from app.orchestration import IngestOrchestrator, NormalizationError
from app.plugins.beef_wms import BeefWmsPlugin
from app.plugins.pork_erp import PorkErpPlugin
from app.plugins.poultry_mes import PoultryMesPlugin
from app.registry import PluginNotFoundError, PluginRegistry

# --- DB setup ---
_config = load_db_config()
_engine = build_engine(_config)
_session_factory = build_session_factory(_engine)
Base.metadata.create_all(bind=_engine)
ensure_protein_views(_engine)

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


@app.get("/dim/plants")
def get_dim_plants(
    region: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    is_active: Optional[bool] = Query(None),
    session: Session = Depends(get_session),
):
    q = session.query(DimPlant)
    if region:
        q = q.filter(DimPlant.region == region)
    if state:
        q = q.filter(DimPlant.state == state)
    if is_active is not None:
        q = q.filter(DimPlant.is_active == is_active)
    plants = q.order_by(DimPlant.plant_code).all()
    return [
        {
            "plant_code": p.plant_code,
            "plant_name": p.plant_name,
            "state": p.state,
            "region": p.region,
            "is_active": p.is_active,
        }
        for p in plants
    ]


@app.get("/fact/pricing")
def get_fact_pricing(
    plant_code: Optional[str] = Query(None),
    protein_type: Optional[str] = Query(None),
    is_current: Optional[bool] = Query(None),
    limit: int = Query(100),
    session: Session = Depends(get_session),
):
    limit = min(limit, 500)
    q = session.query(FactPriceByPlant, DimProduct, DimPlant).join(
        DimProduct, FactPriceByPlant.product_key == DimProduct.product_key
    ).join(DimPlant, FactPriceByPlant.plant_code == DimPlant.plant_code)
    if plant_code:
        q = q.filter(FactPriceByPlant.plant_code == plant_code)
    if protein_type:
        q = q.filter(DimProduct.protein_type == protein_type)
    if is_current is not None:
        q = q.filter(FactPriceByPlant.is_current == is_current)
    q = q.order_by(FactPriceByPlant.effective_start_dt.desc()).limit(limit)
    rows = q.all()
    results = []
    for price, prod, plant in rows:
        results.append(
            {
                "plant_code": plant.plant_code,
                "plant_name": plant.plant_name,
                "region": plant.region,
                "state": plant.state,
                "product_key": prod.product_key,
                "canonical_sku": prod.canonical_sku,
                "product_name": prod.product_name,
                "protein_type": prod.protein_type,
                "price_per_lb": float(price.price_per_lb),
                "currency": price.currency,
                "effective_start_dt": price.effective_start_dt.isoformat(),
                "effective_end_dt": price.effective_end_dt.isoformat()
                if price.effective_end_dt
                else None,
                "is_current": price.is_current,
            }
        )
    return results


def _query_view(view_name: str, session: Session, where_clause: str = "", params: dict | None = None, limit: int = 100):
    limit = min(limit, 500)
    sql = f"SELECT * FROM {view_name}"
    if where_clause:
        sql = sql + " WHERE " + where_clause
    sql = sql + " ORDER BY event_ts DESC LIMIT :limit"
    params = params or {}
    params["limit"] = limit
    res = session.execute(text(sql), params)
    cols = res.keys()
    return [dict(zip(cols, row)) for row in res.fetchall()]


@app.get("/vw/production/pork")
def vw_production_pork(
    plant_code: Optional[str] = Query(None),
    source_system: Optional[str] = Query(None),
    limit: int = Query(100),
    session: Session = Depends(get_session),
):
    where = []
    params = {}
    if plant_code:
        where.append("plant_code = :plant_code")
        params["plant_code"] = plant_code
    if source_system:
        where.append("source_system = :source_system")
        params["source_system"] = source_system
    where_clause = " AND ".join(where)
    return _query_view("vw_pork_production", session, where_clause, params, limit)


@app.get("/vw/production/beef")
def vw_production_beef(
    plant_code: Optional[str] = Query(None),
    source_system: Optional[str] = Query(None),
    limit: int = Query(100),
    session: Session = Depends(get_session),
):
    where = []
    params = {}
    if plant_code:
        where.append("plant_code = :plant_code")
        params["plant_code"] = plant_code
    if source_system:
        where.append("source_system = :source_system")
        params["source_system"] = source_system
    where_clause = " AND ".join(where)
    return _query_view("vw_beef_production", session, where_clause, params, limit)


@app.get("/vw/production/poultry")
def vw_production_poultry(
    plant_code: Optional[str] = Query(None),
    source_system: Optional[str] = Query(None),
    limit: int = Query(100),
    session: Session = Depends(get_session),
):
    where = []
    params = {}
    if plant_code:
        where.append("plant_code = :plant_code")
        params["plant_code"] = plant_code
    if source_system:
        where.append("source_system = :source_system")
        params["source_system"] = source_system
    where_clause = " AND ".join(where)
    return _query_view("vw_poultry_production", session, where_clause, params, limit)


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
