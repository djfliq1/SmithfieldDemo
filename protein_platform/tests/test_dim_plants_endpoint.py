from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from sqlalchemy.orm import sessionmaker

from app.main import app, get_session
from app.models import Base, DimPlant


def _create_session():
    eng = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    Base.metadata.create_all(bind=eng)
    Session = sessionmaker(bind=eng)
    return eng, Session


def test_dim_plants_endpoint():
    eng, Session = _create_session()

    with Session() as sess:
        sess.add_all(
            [
                DimPlant(plant_code="VA01", plant_name="VA Plant", state="VA", region="SOUTHEAST"),
                DimPlant(plant_code="NC02", plant_name="NC Plant", state="NC", region="SOUTHEAST", is_active=False),
            ]
        )
        sess.commit()

    def _get_test_session():
        with Session() as s:
            yield s

    app.dependency_overrides[get_session] = _get_test_session
    client = TestClient(app)
    r = client.get("/dim/plants")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert any(p["plant_code"] == "VA01" for p in data)
    # filter by is_active
    r2 = client.get("/dim/plants?is_active=false")
    assert r2.status_code == 200
    data2 = r2.json()
    assert any(p["plant_code"] == "NC02" for p in data2)

    app.dependency_overrides.clear()
