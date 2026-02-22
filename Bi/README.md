
# Smithfield Demo — Enterprise BI Implementation Strategy

## Overview

This repository simulates a Smithfield-style protein production data platform focused on data integrity first, with a thin BI layer (PowerBI) layered on top. The backend implements enterprise-ready patterns so analytics can trust the source of truth.

## Table of Contents

- [Design Philosophy](#design-philosophy)
- [Backend Architecture Summary](#backend-architecture-summary)
- [Enterprise Modeling Decisions](#enterprise-modeling-decisions)
    - [Canonical SKU Registry](#canonical-sku-registry)
    - [Dimensional Model (Star Schema)](#dimensional-model-star-schema)
    - [SCD Type 2 Pricing](#scd-type-2-pricing)
    - [Idempotent ETL](#idempotent-etl)
- [PowerBI Implementation Strategy](#powerbi-implementation-strategy)
- [Scaling Considerations](#scaling-considerations)
- [Failure Scenarios & Operational Handling](#failure-scenarios--operational-handling)
- [System Flow Diagram](#system-flow-diagram)
- [Why This Approach](#why-this-approach)
- [If Expanded to Production](#if-expanded-to-production)

## Design Philosophy

- Prioritize data reliability over visualization aesthetics
- Dimensional consistency across plants
- Idempotent ingestion and historical accuracy of pricing
- Extensibility without schema redesign
- Clear separation of operational and analytical layers

This backend-first approach makes the BI layer simple: PowerBI reads a trustworthy star schema.

## Backend Architecture Summary

**Technology stack**

- FastAPI — API layer
- SQLAlchemy 2.0 — ORM
- PostgreSQL — primary datastore (hosted on Render in the demo)
- Render Cron Jobs — automated ingestion
- Public Google Drive — external source simulation
- PowerBI — analytics consumer

**Example Postgres connection (PowerBI / psql)**

```bash
# Connection string example
export DATABASE_URL="postgresql://<user>:<pass>@<host>:5432/<db>"

# Quick psql example
psql "$DATABASE_URL"
```

## Enterprise Modeling Decisions

### Canonical SKU Registry

Real enterprises have multiple ERPs, plant-specific SKUs, and historical product drift. We include:

- `dim_product`
- `map_product_source_to_canonical`

Every external SKU resolves to a canonical `product_key` to ensure consistent cross-plant reporting and aggregation.

### Dimensional Model (Star Schema)

- Dimensions: `dim_product`, `dim_plant`, (create `dim_date` in BI)
- Facts: `fact_production`, `fact_price_by_plant`

Star schemas scale well for facts and keep dimensional change logic separate from analytics.

### SCD Type 2 Pricing

Pricing uses columns:

- `effective_start_dt`
- `effective_end_dt`
- `is_current`

Prices are inserted and previous records are closed out (no overwrites). Example SQL pattern for switching the current price:

```sql
-- Close previous current record
UPDATE fact_price_by_plant
SET is_current = FALSE,
        effective_end_dt = '2026-02-22'
WHERE product_key = 'PRODUCT_X'
    AND plant_code = 'PLANT_A'
    AND is_current = TRUE;

-- Insert new price row
INSERT INTO fact_price_by_plant (
    product_key, plant_code, price, effective_start_dt, effective_end_dt, is_current
) VALUES (
    'PRODUCT_X', 'PLANT_A', 123.45, '2026-02-23', NULL, TRUE
);
```

This preserves history for auditability, trend analysis, and regulatory needs.

### Idempotent ETL

Files are:

- Scraped from Google Drive
- Downloaded via file ID
- Hashed (SHA256) and logged in `etl_file_ingestion_state`
- Skipped if the hash already exists

Example bash snippet to hash a file and check existence:

```bash
sha=$(sha256sum pricing.csv | awk '{print $1}')

# Query ingestion state (psql example)
psql "$DATABASE_URL" -c "SELECT 1 FROM etl_file_ingestion_state WHERE file_hash = '$sha' LIMIT 1;"
```

If the hash is present, the file is skipped — preventing duplicates and silent drift.

## PowerBI Implementation Strategy

The BI layer is intentionally thin: consume the star schema directly, avoid heavy reshaping in PowerBI.

Implementation steps:

1. Connect PowerBI to Postgres using the connector and import:
     - `dim_product`
     - `dim_plant`
     - `fact_price_by_plant`
     - `fact_production`
2. Create `dim_date` table in the database or within PowerBI
3. Define one-to-many relationships (single direction)
4. Use DAX measures to resolve price-by-date (price ranges remain inactive relationships)

Example DAX (simplified) to resolve price for a given date context:

```dax
PriceAtDate =
VAR selDate = SELECTEDVALUE(dim_date[date])
RETURN
CALCULATE(
    MAX(fact_price_by_plant[price]),
    FILTER(
        fact_price_by_plant,
        fact_price_by_plant[product_key] = SELECTEDVALUE(dim_product[product_key])
            && fact_price_by_plant[effective_start_dt] <= selDate
            && (
                 fact_price_by_plant[effective_end_dt] >= selDate
                 || ISBLANK(fact_price_by_plant[effective_end_dt])
            )
    )
)
```

Keep relationships single-direction and let DAX perform range-based lookup logic for correctness.

## Scaling Considerations

If scaling to enterprise volume, recommended changes:

- Add indexes on `product_key`, `plant_code`, `effective_start_dt`
- Partition fact tables by date
- Introduce connection pooling
- Adopt dbt for transformations and versioning
- Move orchestration to Airflow/Prefect
- Use materialized views for BI-heavy workloads
- Add row-level security for plant isolation

These changes are additive and do not require schema redesign.

## Failure Scenarios & Operational Handling

- Duplicate file upload: handled by hashing and ingestion-state logging (skip logic).
- File schema change: validation fails, status logged as FAILED, no partial insert.
- Missing plant/product: auto-create `dim_plant` rows where safe; fail gracefully if canonical mapping cannot resolve.
- Overlapping price ranges: current ETL closes prior `is_current` before inserting; future enhancement would be a DB constraint enforcing non-overlap.

## System Flow Diagram

[ External Source (Google Drive) ]
                                ↓
[ Render Cron ETL Job ]
                                ↓
[ PostgreSQL (Star Schema) ]
                                ↓
[ PowerBI (Read-only Analytics Layer) ]

Each layer has a single responsibility and does not leak logic across boundaries.

## Why This Approach

Enterprise BI should be predictable, auditable, historically accurate, and extensible without fragile dashboard transformations. This demo emphasizes reliability first and visualization second.

## If Expanded to Production

Next steps for production readiness:

- Data validation staging tables
- CI/CD for ETL logic
- dbt model layer
- Airflow orchestration
- Row-level security (RLS)
- Snapshot inventory fact and yield variance fact
- Materialized executive KPI views
- Automated anomaly detection on pricing shifts

## Closing

This demo simulates a multi-source ingestion platform with canonical SKU mapping, preserved pricing history, idempotent ETL, dimensional modeling, cloud-native deployment, and a BI layer that reads the validated star schema.

(If you want, I can commit/push these changes and open a PR for review.)

