from typing import Generator, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.contracts import IngestResponse
from app.db import build_engine, build_session_factory, load_db_config
from app.mapping_repo import MappingNotFoundError
from app.models import Base, FactProduction, DimProduct, DimPlant, FactPriceByPlant
from app.views import ensure_protein_views
from app.orchestration import IngestOrchestrator, NormalizationError
from app.plugins.beef_wms import BeefWmsPlugin
from app.plugins.pork_erp import PorkErpPlugin
from app.plugins.poultry_mes import PoultryMesPlugin
from app.registry import PluginNotFoundError, PluginRegistry
import os
from fastapi import Header
from app import seed as seed_module

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


@app.post("/admin/seed")
def admin_seed(x_admin_token: str | None = Header(None)):
    admin_token = os.environ.get("ADMIN_TOKEN")
    if not admin_token or x_admin_token != admin_token:
        raise HTTPException(status_code=401, detail="unauthorized")
    counts = seed_module.run_seed(_engine)
    return {"status": "ok", "inserted": counts}


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

