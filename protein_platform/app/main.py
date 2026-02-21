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
