import pytest
from sqlalchemy import func, select

from app.models import FactProduction
from app.orchestration import IngestOrchestrator
from app.plugins.beef_wms import BeefWmsPlugin
from app.plugins.pork_erp import PorkErpPlugin
from app.plugins.poultry_mes import PoultryMesPlugin
from app.registry import PluginRegistry

PORK_PAYLOAD = {
    "source_system": "PORK_ERP",
    "source_event_id": "P-0001",
    "event_time": "2026-02-21T09:00:00",
    "plant_code": "VA01",
    "item_id": "ITM-100221",
    "item_desc": "LOIN BNLS",
    "qty": 100.0,
    "uom": "LB",
    "scrap_qty": 2.5,
}

BEEF_PAYLOAD = {
    "source_system": "BEEF_WMS",
    "source_event_id": "B-0001",
    "ts": "2026-02-21T09:05:00",
    "warehouse": "NC02",
    "sku": "SKU-88910",
    "sku_desc": "CHUCK ROAST",
    "produced": 80.0,
    "uom": "LB",
    "scrap": 1.0,
}

POULTRY_PAYLOAD = {
    "source_system": "POULTRY_MES",
    "source_event_id": "C-0001",
    "event_ts": "2026-02-21T09:10:00",
    "plant_code": "SC03",
    "material": {"id": "MAT-CHKBRS-77", "desc": "CHKN BRST BNLS"},
    "quantities": {"good": 120.0, "scrap": 3.0, "uom": "LB"},
}


@pytest.fixture
def registry():
    reg = PluginRegistry()
    reg.register(PorkErpPlugin)
    reg.register(BeefWmsPlugin)
    reg.register(PoultryMesPlugin)
    return reg


@pytest.fixture
def orchestrator(registry):
    return IngestOrchestrator(registry)


def test_ingest_pork_inserted(orchestrator, seeded_session):
    response = orchestrator.ingest_production(seeded_session, PORK_PAYLOAD)
    assert response.status == "inserted"
    assert response.event.source_event_id == "P-0001"
    assert response.event.produced_qty_lb == 100.0
    assert response.event.scrap_qty_lb == 2.5


def test_ingest_beef_inserted(orchestrator, seeded_session):
    response = orchestrator.ingest_production(seeded_session, BEEF_PAYLOAD)
    assert response.status == "inserted"
    assert response.event.source_event_id == "B-0001"
    assert response.event.produced_qty_lb == 80.0


def test_ingest_poultry_inserted(orchestrator, seeded_session):
    response = orchestrator.ingest_production(seeded_session, POULTRY_PAYLOAD)
    assert response.status == "inserted"
    assert response.event.source_event_id == "C-0001"
    assert response.event.produced_qty_lb == 120.0


def test_ingest_pork_duplicate(orchestrator, seeded_session):
    orchestrator.ingest_production(seeded_session, PORK_PAYLOAD)
    response2 = orchestrator.ingest_production(seeded_session, PORK_PAYLOAD)
    assert response2.status == "duplicate"


def test_fact_production_count_three(orchestrator, seeded_session):
    orchestrator.ingest_production(seeded_session, PORK_PAYLOAD)
    orchestrator.ingest_production(seeded_session, BEEF_PAYLOAD)
    orchestrator.ingest_production(seeded_session, POULTRY_PAYLOAD)
    # re-ingest pork - should remain 3
    orchestrator.ingest_production(seeded_session, PORK_PAYLOAD)

    count = seeded_session.execute(
        select(func.count()).select_from(FactProduction)
    ).scalar()
    assert count == 3
