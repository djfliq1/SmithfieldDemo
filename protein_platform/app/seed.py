from datetime import date, datetime, timedelta
from typing import Dict

from sqlalchemy.orm import Session

from app.db import build_session_factory
from app.models import Base, DimProduct, MapProductSourceToCanonical
from app.models import DimPlant, FactPriceByPlant, FactProduction
from decimal import Decimal


def _get_or_create_product(session: Session, canonical_sku: str, **kwargs) -> DimProduct:
    existing = session.query(DimProduct).filter_by(canonical_sku=canonical_sku).first()
    if existing:
        return existing
    product = DimProduct(canonical_sku=canonical_sku, **kwargs)
    session.add(product)
    session.flush()
    return product


def _ensure_mapping(
    session: Session,
    source_system: str,
    source_item_id: str,
    plant_code: str,
    product_key: int,
    source_item_desc: str,
) -> None:
    existing = (
        session.query(MapProductSourceToCanonical)
        .filter_by(
            source_system=source_system,
            source_item_id=source_item_id,
            plant_code=plant_code,
        )
        .first()
    )
    if existing:
        return
    mapping = MapProductSourceToCanonical(
        source_system=source_system,
        source_item_id=source_item_id,
        source_item_desc=source_item_desc,
        product_key=product_key,
        plant_code=plant_code,
        effective_start_dt=date.today(),
        is_current=True,
    )
    session.add(mapping)


def seed(session: Session) -> None:
    pork = _get_or_create_product(
        session,
        canonical_sku="PORK-LOIN-001",
        product_name="Pork Loin Boneless",
        protein_type="PORK",
        cut_type="LOIN",
        uom="LB",
        is_active=True,
    )
    beef = _get_or_create_product(
        session,
        canonical_sku="BEEF-CHUCK-001",
        product_name="Beef Chuck Roast",
        protein_type="BEEF",
        cut_type="CHUCK",
        uom="LB",
        is_active=True,
    )
    poultry = _get_or_create_product(
        session,
        canonical_sku="POULTRY-BREAST-001",
        product_name="Chicken Breast Boneless",
        protein_type="POULTRY",
        cut_type="BREAST",
        uom="LB",
        is_active=True,
    )
    session.flush()

    _ensure_mapping(session, "PORK_ERP", "ITM-100221", "VA01", pork.product_key, "LOIN BNLS")
    _ensure_mapping(session, "BEEF_WMS", "SKU-88910", "NC02", beef.product_key, "CHUCK ROAST")
    _ensure_mapping(
        session, "POULTRY_MES", "MAT-CHKBRS-77", "SC03", poultry.product_key, "CHKN BRST BNLS"
    )
    session.commit()
    print("Seed complete.")


def seed_full(session: Session) -> None:
    # ensure products and mappings
    seed(session)

    # ensure plants
    plants = [
        ("VA01", "Smithfield VA Plant", "VA", "SOUTHEAST"),
        ("NC02", "Smithfield NC Plant", "NC", "SOUTHEAST"),
        ("SC03", "Smithfield SC Plant", "SC", "SOUTHEAST"),
        ("TX04", "Smithfield TX Plant", "TX", "SOUTH"),
        ("KS05", "Smithfield KS Plant", "KS", "MIDWEST"),
        ("IA06", "Smithfield IA Plant", "IA", "MIDWEST"),
        ("MN07", "Smithfield MN Plant", "MN", "NORTH"),
        ("NE08", "Smithfield NE Plant", "NE", "MIDWEST"),
    ]
    for code, name, state, region in plants:
        existing = session.query(DimPlant).filter_by(plant_code=code).first()
        if not existing:
            session.add(
                DimPlant(
                    plant_code=code,
                    plant_name=name,
                    state=state,
                    region=region,
                    is_active=True,
                )
            )
    session.flush()

    # pricing: for each product & plant, create two effective dated prices
    products = session.query(DimProduct).all()
    plant_codes = [p.plant_code for p in session.query(DimPlant).all()]
    start1 = date(2026, 1, 1)
    start2 = date(2026, 2, 15)
    for prod in products:
        for idx, pcode in enumerate(plant_codes[:6]):
            # first version
            exists1 = (
                session.query(FactPriceByPlant)
                .filter_by(product_key=prod.product_key, plant_code=pcode, effective_start_dt=start1)
                .first()
            )
            if not exists1:
                session.add(
                    FactPriceByPlant(
                        product_key=prod.product_key,
                        plant_code=pcode,
                        price_per_lb=Decimal("1.00") + Decimal(str(idx)) * Decimal("0.10"),
                        currency="USD",
                        effective_start_dt=start1,
                        is_current=False,
                    )
                )

            exists2 = (
                session.query(FactPriceByPlant)
                .filter_by(product_key=prod.product_key, plant_code=pcode, effective_start_dt=start2)
                .first()
            )
            if not exists2:
                session.add(
                    FactPriceByPlant(
                        product_key=prod.product_key,
                        plant_code=pcode,
                        price_per_lb=(Decimal("1.20") + Decimal(str(idx)) * Decimal("0.12")),
                        currency="USD",
                        effective_start_dt=start2,
                        is_current=True,
                    )
                )

    session.flush()

    # insert production facts (~60 rows) across plants and proteins
    base_ts = datetime(2026, 2, 1, 6, 0, 0)
    prod_keys = [p.product_key for p in products]
    i = 0
    for day in range(15):
        for plant in plant_codes[:6]:
            for pk in prod_keys:
                ts = base_ts + timedelta(days=day, hours=i % 6)
                src_event = f"SEED-{plant}-{pk}-{day}-{i}"
                exists = (
                    session.query(FactProduction)
                    .filter_by(source_system="SEED", source_event_id=src_event)
                    .first()
                )
                if not exists:
                    session.add(
                        FactProduction(
                            event_ts=ts,
                            plant_code=plant,
                            product_key=pk,
                            produced_qty_lb=Decimal("100.0") + Decimal(str(i % 5)),
                            scrap_qty_lb=Decimal("1.0"),
                            source_system="SEED",
                            source_event_id=src_event,
                        )
                    )
                i += 1

    session.commit()
    print("Full seed complete.")


def run_seed(engine) -> Dict[str, int]:
    """Run idempotent seed using given engine. Returns counts of inserted rows."""
    Base.metadata.create_all(bind=engine)
    SessionFactory = build_session_factory(engine)
    counts: Dict[str, int] = {
        "dim_plant": 0,
        "fact_price_by_plant": 0,
        "dim_product": 0,
        "map_product_source_to_canonical": 0,
        "fact_production": 0,
    }

    with SessionFactory() as session:
        # products
        products_to_ensure = [
            dict(
                canonical_sku="PORK-LOIN-001",
                product_name="Pork Loin Boneless",
                protein_type="PORK",
                cut_type="LOIN",
                uom="LB",
                is_active=True,
            ),
            dict(
                canonical_sku="BEEF-CHUCK-001",
                product_name="Beef Chuck Roast",
                protein_type="BEEF",
                cut_type="CHUCK",
                uom="LB",
                is_active=True,
            ),
            dict(
                canonical_sku="POULTRY-BREAST-001",
                product_name="Chicken Breast Boneless",
                protein_type="POULTRY",
                cut_type="BREAST",
                uom="LB",
                is_active=True,
            ),
        ]
        for p in products_to_ensure:
            existing = session.query(DimProduct).filter_by(canonical_sku=p["canonical_sku"]).first()
            if not existing:
                session.add(DimProduct(**p))
                session.flush()
                counts["dim_product"] += 1

        # mappings
        # ensure mappings for the three canonical products
        mappings = [
            ("PORK_ERP", "ITM-100221", "VA01", "LOIN BNLS", "PORK-LOIN-001"),
            ("BEEF_WMS", "SKU-88910", "NC02", "CHUCK ROAST", "BEEF-CHUCK-001"),
            ("POULTRY_MES", "MAT-CHKBRS-77", "SC03", "CHKN BRST BNLS", "POULTRY-BREAST-001"),
        ]
        session.flush()
        for src_sys, src_id, plant_code, desc, canonical in mappings:
            prod = session.query(DimProduct).filter_by(canonical_sku=canonical).first()
            if not prod:
                continue
            existing = (
                session.query(MapProductSourceToCanonical)
                .filter_by(source_system=src_sys, source_item_id=src_id, plant_code=plant_code)
                .first()
            )
            if not existing:
                session.add(
                    MapProductSourceToCanonical(
                        source_system=src_sys,
                        source_item_id=src_id,
                        source_item_desc=desc,
                        product_key=prod.product_key,
                        plant_code=plant_code,
                        effective_start_dt=date.today(),
                        is_current=True,
                    )
                )
                session.flush()
                counts["map_product_source_to_canonical"] += 1

        # plants
        plants = [
            ("VA01", "Smithfield VA Plant", "VA", "SOUTHEAST"),
            ("NC02", "Smithfield NC Plant", "NC", "SOUTHEAST"),
            ("SC03", "Smithfield SC Plant", "SC", "SOUTHEAST"),
            ("TX04", "Smithfield TX Plant", "TX", "SOUTH"),
            ("KS05", "Smithfield KS Plant", "KS", "MIDWEST"),
            ("IA06", "Smithfield IA Plant", "IA", "MIDWEST"),
            ("MN07", "Smithfield MN Plant", "MN", "NORTH"),
            ("NE08", "Smithfield NE Plant", "NE", "MIDWEST"),
        ]
        for code, name, state, region in plants:
            existing = session.query(DimPlant).filter_by(plant_code=code).first()
            if not existing:
                session.add(
                    DimPlant(
                        plant_code=code,
                        plant_name=name,
                        state=state,
                        region=region,
                        is_active=True,
                    )
                )
                session.flush()
                counts["dim_plant"] += 1

        # pricing: for each product & plant, create two effective dated prices
        products = session.query(DimProduct).all()
        plant_codes = [p.plant_code for p in session.query(DimPlant).all()]
        start1 = date(2026, 1, 1)
        start2 = date(2026, 2, 15)
        for prod in products:
            for idx, pcode in enumerate(plant_codes[:6]):
                exists1 = (
                    session.query(FactPriceByPlant)
                    .filter_by(product_key=prod.product_key, plant_code=pcode, effective_start_dt=start1)
                    .first()
                )
                if not exists1:
                    session.add(
                        FactPriceByPlant(
                            product_key=prod.product_key,
                            plant_code=pcode,
                            price_per_lb=Decimal("1.00") + Decimal(str(idx)) * Decimal("0.10"),
                            currency="USD",
                            effective_start_dt=start1,
                            is_current=False,
                        )
                    )
                    session.flush()
                    counts["fact_price_by_plant"] += 1

                exists2 = (
                    session.query(FactPriceByPlant)
                    .filter_by(product_key=prod.product_key, plant_code=pcode, effective_start_dt=start2)
                    .first()
                )
                if not exists2:
                    session.add(
                        FactPriceByPlant(
                            product_key=prod.product_key,
                            plant_code=pcode,
                            price_per_lb=(Decimal("1.20") + Decimal(str(idx)) * Decimal("0.12")),
                            currency="USD",
                            effective_start_dt=start2,
                            is_current=True,
                        )
                    )
                    session.flush()
                    counts["fact_price_by_plant"] += 1

        # production facts
        base_ts = datetime(2026, 2, 1, 6, 0, 0)
        prod_keys = [p.product_key for p in products]
        i = 0
        for day in range(15):
            for plant in plant_codes[:6]:
                for pk in prod_keys:
                    ts = base_ts + timedelta(days=day, hours=i % 6)
                    src_event = f"SEED-{plant}-{pk}-{day}-{i}"
                    exists = (
                        session.query(FactProduction)
                        .filter_by(source_system="SEED", source_event_id=src_event)
                        .first()
                    )
                    if not exists:
                        session.add(
                            FactProduction(
                                event_ts=ts,
                                plant_code=plant,
                                product_key=pk,
                                produced_qty_lb=Decimal("100.0") + Decimal(str(i % 5)),
                                scrap_qty_lb=Decimal("1.0"),
                                source_system="SEED",
                                source_event_id=src_event,
                            )
                        )
                        session.flush()
                        counts["fact_production"] += 1
                    i += 1

        session.commit()

    return counts


if __name__ == "__main__":
    # keep compatibility with previous entrypoint: create engine using env config
    from app.db import load_db_config, build_engine

    config = load_db_config()
    engine = build_engine(config)
    counts = run_seed(engine)
    print(counts)
