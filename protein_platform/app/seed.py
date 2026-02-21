from datetime import date

from sqlalchemy.orm import Session

from app.db import build_engine, build_session_factory, load_db_config
from app.models import Base, DimProduct, MapProductSourceToCanonical


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


if __name__ == "__main__":
    config = load_db_config()
    engine = build_engine(config)
    Base.metadata.create_all(bind=engine)
    SessionFactory = build_session_factory(engine)
    with SessionFactory() as session:
        seed(session)
