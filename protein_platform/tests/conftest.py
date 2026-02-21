import pytest
from datetime import date
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import Base, DimProduct, MapProductSourceToCanonical


@pytest.fixture(scope="function")
def engine():
    eng = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(bind=eng)
    yield eng
    Base.metadata.drop_all(bind=eng)


@pytest.fixture(scope="function")
def session(engine):
    SessionFactory = sessionmaker(bind=engine)
    with SessionFactory() as sess:
        yield sess


@pytest.fixture(scope="function")
def seeded_session(session):
    pork = DimProduct(
        canonical_sku="PORK-LOIN-001",
        product_name="Pork Loin Boneless",
        protein_type="PORK",
        cut_type="LOIN",
        uom="LB",
        is_active=True,
    )
    beef = DimProduct(
        canonical_sku="BEEF-CHUCK-001",
        product_name="Beef Chuck Roast",
        protein_type="BEEF",
        cut_type="CHUCK",
        uom="LB",
        is_active=True,
    )
    poultry = DimProduct(
        canonical_sku="POULTRY-BREAST-001",
        product_name="Chicken Breast Boneless",
        protein_type="POULTRY",
        cut_type="BREAST",
        uom="LB",
        is_active=True,
    )
    session.add_all([pork, beef, poultry])
    session.flush()

    session.add_all([
        MapProductSourceToCanonical(
            source_system="PORK_ERP",
            source_item_id="ITM-100221",
            plant_code="VA01",
            product_key=pork.product_key,
            source_item_desc="LOIN BNLS",
            effective_start_dt=date.today(),
            is_current=True,
        ),
        MapProductSourceToCanonical(
            source_system="BEEF_WMS",
            source_item_id="SKU-88910",
            plant_code="NC02",
            product_key=beef.product_key,
            source_item_desc="CHUCK ROAST",
            effective_start_dt=date.today(),
            is_current=True,
        ),
        MapProductSourceToCanonical(
            source_system="POULTRY_MES",
            source_item_id="MAT-CHKBRS-77",
            plant_code="SC03",
            product_key=poultry.product_key,
            source_item_desc="CHKN BRST BNLS",
            effective_start_dt=date.today(),
            is_current=True,
        ),
    ])
    session.commit()
    yield session
