Smithfield Demo – Enterprise BI Implementation Strategy
Overview

This demo simulates a Smithfield-style protein production data platform.

It is not a dashboard-first project.

It is a data integrity-first architecture with BI layered on top.

The goal was to demonstrate how I would design, scale, and operationalize a production-grade analytics environment for a multi-plant protein enterprise.

The BI layer exists because the backend is trustworthy.

1. Design Philosophy

When implementing BI in an enterprise protein environment, I prioritize:

Data reliability over visualization aesthetics

Dimensional consistency across plants

Idempotent ingestion

Historical accuracy of pricing

Extensibility without schema redesign

Clear separation of operational and analytical layers

This demo backend was designed first to behave like an enterprise system.

PowerBI simply consumes it.

2. Backend Architecture Summary
Technology Stack

FastAPI (API layer)

SQLAlchemy 2.0 (ORM)

PostgreSQL (Render)

Render Cron Jobs (automated ingestion)

Public Google Drive (external source simulation)

PowerBI (analytics consumer)

The system is fully cloud-deployed and production-ready in structure.

3. Enterprise Modeling Decisions
3.1 Canonical SKU Registry

Real protein companies operate with:

Multiple ERP systems

Plant-specific SKU naming

Historical product drift

To address this, the system includes:

dim_product

map_product_source_to_canonical

Every external SKU resolves to a canonical product_key.

Why this matters:

Reporting consistency across plants

Reliable cross-system aggregation

Enterprise-ready master data modeling behavior

This mimics how real MDM systems behave.

3.2 Dimensional Model (Star Schema)

The schema separates:

Dimensions

dim_product

dim_plant

Facts

fact_production

fact_price_by_plant

Why a star schema?

Facts scale rapidly

Dimensions change slowly

BI engines optimize for this structure

Relationships remain predictable

This ensures scalable performance and clean modeling in PowerBI.

3.3 Slowly Changing Pricing (SCD Type 2 Behavior)

Pricing is modeled using:

effective_start_dt

effective_end_dt

is_current

Prices are never overwritten.

Instead:

Previous price is closed out

New price is inserted

History is preserved

Why this matters:

Accurate price trend analysis

Regulatory traceability

Historical margin analysis

Executive-level auditability

3.4 Idempotent ETL

External pricing files are:

Scraped from Google Drive

Downloaded via file ID

Hashed (SHA256)

Logged in etl_file_ingestion_state

Skipped if already processed

Inserted only if new

This prevents:

Duplicate loads

Manual reprocessing errors

Silent data drift

In enterprise systems, repeatable ingestion is critical.

4. PowerBI Implementation Strategy

The BI layer is intentionally thin.

It does not reshape data.

It consumes the star schema directly.

Implementation Steps

Connect via PostgreSQL connector

Import:

dim_product

dim_plant

fact_price_by_plant

fact_production

Create a dim_date table

Define one-to-many relationships

Use DAX for price range logic

Keep relationships single-direction

Price date relationships are intentionally inactive.

Why?

Because price is range-based, not event-based.

DAX resolves pricing based on selected date context.

This avoids incorrect joins and preserves modeling correctness.

5. Scaling Considerations

The backend was designed with growth in mind.

If scaled to enterprise volume, I would:

Add indexing on:

product_key

plant_code

effective_start_dt

Partition fact tables by date

Introduce connection pooling

Introduce dbt for transformation versioning

Move orchestration to Airflow or Prefect

Introduce materialized views for BI-heavy workloads

Add row-level security for plant isolation

The schema does not require redesign to scale.

It is intentionally future-proofed.

6. Failure Scenarios & Operational Handling

This is where systems prove themselves.

Scenario 1: Duplicate File Upload

Handled by:

File hashing

Ingestion state logging

Skip logic

Result:
No duplicate pricing loads.

Scenario 2: File Schema Change

If CSV headers change:

Validation fails

Status logged as FAILED

No partial insert

Result:
BI never sees corrupted price records.

Scenario 3: Missing Plant or Product

System behavior:

Creates missing plant dimension row if needed

Fails gracefully if canonical mapping cannot resolve

Future improvement:

Introduce staging validation layer before dimension auto-creation

Scenario 4: Overlapping Price Ranges

Current behavior:

Prior is_current record is closed before inserting new record

Prevents multiple active price rows

Future enhancement:

Add database constraint to enforce non-overlapping effective date windows

7. BI Layer Discussion Points (For Interview)

This implementation enables discussion around:

Dimensional modeling strategy

MDM simulation

SCD handling

Idempotent ETL

Cloud-native ingestion

Cost-efficient deployment

Separation of concerns (API vs BI)

Enterprise scaling roadmap

The BI layer is intentionally not flashy.

It is structurally correct.

8. System Flow Diagram
[ External Source (Google Drive) ]
                ↓
[ Render Cron ETL Job ]
                ↓
[ PostgreSQL (Star Schema) ]
                ↓
[ PowerBI (Read-only Analytics Layer) ]

Each layer has a clear responsibility.

No layer leaks logic into another.

9. Why This Approach

I believe enterprise BI should:

Be predictable

Be auditable

Be historically accurate

Be extensible without refactoring

Avoid fragile transformations inside dashboards

This demo reflects how I would build a production-grade protein analytics platform.

Not just how I would build a report.

10. If Expanded to Production

Next evolution steps would include:

Data validation staging tables

CI/CD for ETL logic

dbt model layer

Airflow orchestration

RLS for plant-level security

Snapshot inventory fact

Yield variance fact

Materialized executive KPI views

Automated anomaly detection on pricing shifts

The foundation supports all of this.

Closing Statement

This demo simulates a Smithfield-style protein data platform with:

Multi-source ingestion

Canonical SKU mapping

Historical pricing preservation

Idempotent ETL

Dimensional modeling

Cloud-native deployment

Enterprise-ready BI integration

The emphasis is reliability first, visualization second. (See <attachments> above for file contents. You may not need to search or read the file again.)
