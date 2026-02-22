from sqlalchemy import text


def ensure_protein_views(engine) -> None:
    """Create or replace protein-specific production views.

    The function executes CREATE OR REPLACE VIEW statements for pork, beef, and poultry.
    It is idempotent and safe to run on each startup.
    """
    view_tpl = """
    CREATE OR REPLACE VIEW {view_name} AS
    SELECT
        fp.event_ts,
        fp.plant_code,
        fp.produced_qty_lb,
        fp.scrap_qty_lb,
        fp.source_system,
        fp.source_event_id,
        fp.product_key,
        dp.canonical_sku,
        dp.product_name,
        dp.protein_type,
        dp.cut_type,
        dp.uom AS product_uom
    FROM fact_production fp
    JOIN dim_product dp ON fp.product_key = dp.product_key
    WHERE dp.protein_type = '{protein}'
    """

    views = [
        ("vw_pork_production", "PORK"),
        ("vw_beef_production", "BEEF"),
        ("vw_poultry_production", "POULTRY"),
    ]

    # Use a transaction/connection context to run statements
    with engine.begin() as conn:
        for view_name, protein in views:
            sql = view_tpl.format(view_name=view_name, protein=protein)
            # SQLite doesn't support CREATE OR REPLACE VIEW; use DROP/CREATE instead
            dialect = getattr(engine, "dialect", None)
            dialect_name = getattr(dialect, "name", None)
            if dialect_name == "sqlite":
                conn.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
                # create view without OR REPLACE
                create_sql = sql.replace("CREATE OR REPLACE VIEW", "CREATE VIEW")
                conn.execute(text(create_sql))
            else:
                conn.execute(text(sql))


__all__ = ["ensure_protein_views"]
