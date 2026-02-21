from datetime import datetime, date
from decimal import Decimal

from sqlalchemy import (
    Boolean,
    DateTime,
    Date,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class DimProduct(Base):
    __tablename__ = "dim_product"

    product_key: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    canonical_sku: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    product_name: Mapped[str] = mapped_column(String(200), nullable=False)
    protein_type: Mapped[str] = mapped_column(String(20), nullable=False)
    cut_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    uom: Mapped[str] = mapped_column(String(20), nullable=False, default="LB")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())

    source_mappings: Mapped[list["MapProductSourceToCanonical"]] = relationship(
        back_populates="product"
    )
    productions: Mapped[list["FactProduction"]] = relationship(back_populates="product")


class MapProductSourceToCanonical(Base):
    __tablename__ = "map_product_source_to_canonical"

    map_key: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_system: Mapped[str] = mapped_column(String(50), nullable=False)
    source_item_id: Mapped[str] = mapped_column(String(100), nullable=False)
    source_item_desc: Mapped[str | None] = mapped_column(String(250), nullable=True)
    product_key: Mapped[int] = mapped_column(
        Integer, ForeignKey("dim_product.product_key"), nullable=False
    )
    source_protein_type: Mapped[str | None] = mapped_column(String(20), nullable=True)
    source_uom: Mapped[str | None] = mapped_column(String(20), nullable=True)
    pack_size: Mapped[str | None] = mapped_column(String(30), nullable=True)
    plant_code: Mapped[str | None] = mapped_column(String(20), nullable=True)
    match_confidence: Mapped[Decimal] = mapped_column(
        Numeric(5, 2), nullable=False, default=Decimal("1.00")
    )
    mapping_method: Mapped[str] = mapped_column(String(30), nullable=False, default="MANUAL")
    effective_start_dt: Mapped[date] = mapped_column(Date, nullable=False)
    effective_end_dt: Mapped[date | None] = mapped_column(Date, nullable=True)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())

    product: Mapped["DimProduct"] = relationship(back_populates="source_mappings")

    __table_args__ = (
        UniqueConstraint("source_system", "source_item_id", "plant_code", name="uq_map_source_plant"),
        Index("ix_map_product_key", "product_key"),
        Index("ix_map_source_lookup", "source_system", "source_item_id"),
    )


class FactProduction(Base):
    __tablename__ = "fact_production"

    production_key: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_ts: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    plant_code: Mapped[str] = mapped_column(String(20), nullable=False)
    product_key: Mapped[int] = mapped_column(
        Integer, ForeignKey("dim_product.product_key"), nullable=False
    )
    produced_qty_lb: Mapped[Decimal] = mapped_column(Numeric(18, 3), nullable=False)
    scrap_qty_lb: Mapped[Decimal] = mapped_column(Numeric(18, 3), nullable=False, default=Decimal("0"))
    source_system: Mapped[str] = mapped_column(String(50), nullable=False)
    source_event_id: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    product: Mapped["DimProduct"] = relationship(back_populates="productions")

    __table_args__ = (
        UniqueConstraint("source_system", "source_event_id", name="uq_fact_source_event"),
        Index("ix_fact_event_ts", "event_ts"),
        Index("ix_fact_plant", "plant_code"),
    )
