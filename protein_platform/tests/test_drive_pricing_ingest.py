from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import Base, DimProduct, DimPlant, FactPriceByPlant
from app.crons.drive_pricing_ingest import (
    extract_drive_files,
    parse_pricing_csv,
    upsert_pricing_rows,
)

@pytest.fixture()
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as s:
        yield s


def test_extract_drive_files_pairs():
    html = """
    {"name":"pricing_by_plant_2026-03-01.csv","driveId":"FILE1234567890ABCDE"}
    {"name":"other.csv","driveId":"FILEZZZ"}
    """
    out = extract_drive_files(html, prefix="pricing_by_plant_", suffix=".csv")
    assert len(out) == 1
    assert out[0].file_id == "FILE1234567890ABCDE"
    assert out[0].file_name == "pricing_by_plant_2026-03-01.csv"


def test_parse_pricing_csv():
    csv_bytes = (
        "plant_code,canonical_sku,price_per_lb,currency,effective_start_dt,effective_end_dt,is_current\n"
        "VA01,PORK-LOIN-001,1.2345,USD,2026-03-01,,true\n"
    ).encode("utf-8")
    rows = parse_pricing_csv(csv_bytes)
    assert len(rows) == 1
    r = rows[0]
    assert r.plant_code == "VA01"
    assert r.canonical_sku == "PORK-LOIN-001"
    assert r.price_per_lb == Decimal("1.2345")
    assert r.currency == "USD"
    assert r.effective_start_dt == date(2026, 3, 1)
    assert r.effective_end_dt is None
    assert r.is_current is True


def test_upsert_pricing_rows_flips_current(db_session):
    # Seed product
    p = DimProduct(
        canonical_sku="PORK-LOIN-001",
        product_name="Pork Loin Boneless",
        protein_type="PORK",
        cut_type="LOIN",
        uom="LB",
        is_active=True,
    )
    db_session.add(p)
    db_session.commit()

    # First load
    csv1 = (
        "plant_code,canonical_sku,price_per_lb,currency,effective_start_dt,effective_end_dt,is_current\n"
        "VA01,PORK-LOIN-001,1.0000,USD,2026-03-01,,true\n"
    ).encode("utf-8")
    rows1 = parse_pricing_csv(csv1)
    loaded1 = upsert_pricing_rows(db_session, rows1)
    db_session.commit()

    assert loaded1 == 1
    cur = db_session.query(FactPriceByPlant).filter_by(plant_code="VA01").all()
    assert len(cur) == 1
    assert cur[0].is_current is True

    # Second load, later effective date also current => should flip prior to false
    csv2 = (
        "plant_code,canonical_sku,price_per_lb,currency,effective_start_dt,effective_end_dt,is_current\n"
        "VA01,PORK-LOIN-001,1.2500,USD,2026-04-15,,true\n"
    ).encode("utf-8")
    rows2 = parse_pricing_csv(csv2)
    loaded2 = upsert_pricing_rows(db_session, rows2)
    db_session.commit()

    assert loaded2 == 1

    rows = (
        db_session.query(FactPriceByPlant)
        .filter_by(plant_code="VA01")
        .order_by(FactPriceByPlant.effective_start_dt.asc())
        .all()
    )
    assert len(rows) == 2
    assert rows[0].effective_start_dt == date(2026, 3, 1)
    assert rows[0].is_current is False
    assert rows[1].effective_start_dt == date(2026, 4, 15)
    assert rows[1].is_current is True
