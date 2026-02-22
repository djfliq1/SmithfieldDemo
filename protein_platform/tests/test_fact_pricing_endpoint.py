from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from sqlalchemy.orm import sessionmaker
from datetime import date
from decimal import Decimal

from app.main import app, get_session
from app.models import Base, DimPlant, DimProduct, FactPriceByPlant


def _create_session():
    eng = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    Base.metadata.create_all(bind=eng)
    Session = sessionmaker(bind=eng)
    return eng, Session


def test_fact_pricing_endpoint():
    eng, Session = _create_session()

    with Session() as sess:
        prod = DimProduct(
            canonical_sku="PORK-LOIN-001",
            product_name="Pork Loin",
            protein_type="PORK",
            cut_type="LOIN",
            uom="LB",
        )
        plant = DimPlant(plant_code="VA01", plant_name="VA Plant", state="VA", region="SOUTHEAST")
        sess.add_all([prod, plant])
        sess.flush()
        sess.add(
            FactPriceByPlant(
                product_key=prod.product_key,
                plant_code=plant.plant_code,
                price_per_lb=Decimal("1.50"),
                currency="USD",
                effective_start_dt=date(2026, 1, 1),
                is_current=True,
            )
        )
        sess.commit()

    def _get_test_session():
        with Session() as s:
            yield s

    app.dependency_overrides[get_session] = _get_test_session
    client = TestClient(app)
    r = client.get("/fact/pricing")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert len(data) == 1
    item = data[0]
    assert item["plant_code"] == "VA01"
    assert item["product_name"] == "Pork Loin"

    # filter by protein_type
    r2 = client.get("/fact/pricing?protein_type=PORK")
    assert r2.status_code == 200
    assert len(r2.json()) == 1

    app.dependency_overrides.clear()
