# Protein Platform — Scalable Plugin-Based Ingestion System

A complete, runnable Python project implementing a scalable, config-friendly, plugin-based ingestion platform for 3 protein sources: **PORK_ERP**, **BEEF_WMS**, and **POULTRY_MES**.

## Quick Start

### 1. Set up virtual environment

```bash
cd protein_platform
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Seed the database

```bash
python -m app.seed
```

This creates `protein_dw.sqlite` at the repo root with canonical products and source mappings.

### 4. Start the API server

```bash
uvicorn app.main:app --reload
```

Server runs at `http://localhost:8000`. API docs at `http://localhost:8000/docs`.

### 5. Test the endpoints

**Health check:**
```bash
curl http://localhost:8000/health
```

**Ingest PORK_ERP event (uses `event_time` and `item_id` aliases):**
```bash
curl -s -X POST http://localhost:8000/ingest/production \
  -H "Content-Type: application/json" \
  -d '{
    "source_system": "PORK_ERP",
    "source_event_id": "P-0001",
    "event_time": "2026-02-21T09:00:00",
    "plant_code": "VA01",
    "item_id": "ITM-100221",
    "item_desc": "LOIN BNLS",
    "qty": 100.0,
    "uom": "LB",
    "scrap_qty": 2.5
  }'
```

**Ingest BEEF_WMS event (uses `ts`, `warehouse`, `sku`, `produced`, `scrap` aliases):**
```bash
curl -s -X POST http://localhost:8000/ingest/production \
  -H "Content-Type: application/json" \
  -d '{
    "source_system": "BEEF_WMS",
    "source_event_id": "B-0001",
    "ts": "2026-02-21T09:05:00",
    "warehouse": "NC02",
    "sku": "SKU-88910",
    "sku_desc": "CHUCK ROAST",
    "produced": 80.0,
    "uom": "LB",
    "scrap": 1.0
  }'
```

**Ingest POULTRY_MES event (uses nested `material` and `quantities`):**
```bash
curl -s -X POST http://localhost:8000/ingest/production \
  -H "Content-Type: application/json" \
  -d '{
    "source_system": "POULTRY_MES",
    "source_event_id": "C-0001",
    "event_ts": "2026-02-21T09:10:00",
    "plant_code": "SC03",
    "material": {"id": "MAT-CHKBRS-77", "desc": "CHKN BRST BNLS"},
    "quantities": {"good": 120.0, "scrap": 3.0, "uom": "LB"}
  }'
```

### 6. Run tests

```bash
pytest -q
```

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `sqlite:///./protein_dw.sqlite` | SQLAlchemy connection URL |

Example using PostgreSQL:
```bash
export DATABASE_URL="postgresql+psycopg2://user:pass@localhost/protein_db"
uvicorn app.main:app --reload
```

---

## Project Structure

```
protein_platform/
  app/
    __init__.py
    db.py             # DB config, engine, session factory
    models.py         # SQLAlchemy 2.0 ORM models
    contracts.py      # Pydantic schemas (RawProductionEvent, CanonicalProductionEvent, IngestResponse)
    registry.py       # Plugin registry
    mapping_repo.py   # Product mapping resolution
    orchestration.py  # Ingest orchestrator + UOM normalization
    main.py           # FastAPI application
    seed.py           # Database seeder (idempotent)
    loaders/
      __init__.py
      fact_loader.py  # Idempotent fact table writer
    plugins/
      __init__.py
      base.py         # Abstract SourcePlugin
      pork_erp.py     # PORK_ERP plugin
      beef_wms.py     # BEEF_WMS plugin
      poultry_mes.py  # POULTRY_MES plugin
  tests/
    __init__.py
    conftest.py       # Shared fixtures (in-memory SQLite engine/session/seed)
    test_registry.py  # Registry unit tests
    test_units.py     # UOM conversion unit tests
    test_ingest_e2e.py # End-to-end ingestion tests
  requirements.txt
  pyproject.toml
  README.md
```

---

## App internals (`app/`)

This project is organized so the `app/` package contains the runtime logic for ingestion, mapping, and persistence. The following explains each major module, the runtime ingestion flow, and small developer snippets to interact with the code.

### Key modules

- `app/db.py` — SQLAlchemy engine, async/sync session factory and helpers. Use `DATABASE_URL` env var to change the backend. The default demo uses SQLite (`sqlite:///./protein_dw.sqlite`).
- `app/models.py` — SQLAlchemy 2.0 ORM models for `dim_product`, `dim_plant`, `map_product_source_to_canonical`, `fact_production`, and `fact_price_by_plant`.
- `app/contracts.py` — Pydantic schemas used by FastAPI and the orchestrator (`RawProductionEvent`, `CanonicalProductionEvent`, `IngestResponse`).
- `app/registry.py` — Plugin registry that tracks available `SourcePlugin` implementations and provides lookup by `source_system`.
- `app/mapping_repo.py` — Canonical SKU resolution logic. Returns `product_key` for source SKUs and optionally auto-creates `dim_product`/`dim_plant` rows when safe.
- `app/orchestration.py` — Central orchestrator that:
  1. Validates incoming payloads
  2. Resolves correct plugin from `registry`
  3. Invokes plugin's `transform_payload` to canonicalize fields
  4. Normalizes UOM to LB via UOM helpers
  5. Writes idempotent facts using `loaders.fact_loader`
- `app/seed.py` — Idempotent seeder used to populate canonical products and mappings for local demos and tests.
- `app/loaders/fact_loader.py` — Idempotent fact writer that hashes canonical events to prevent duplicate inserts; records ingestion state in `etl_file_ingestion_state`-like table.
- `app/plugins/*` — Source plugin implementations. Each plugin implements the `SourcePlugin` abstract base in `app/plugins/base.py`.

### Ingest flow (high-level)

1. FastAPI `POST /ingest/production` receives any JSON payload containing `source_system`.
2. The `orchestrator` asks `registry` for the plugin class matching `source_system`.
3. Plugin's `transform_payload(payload: dict) -> dict` returns a normalized dict with canonical keys:

```py
{
  "source_system": "PORK_ERP",
  "source_event_id": "P-0001",
  "event_ts": datetime(...),
  "plant_code": "VA01",
  "source_item_id": "ITM-100221",
  "source_item_desc": "LOIN BNLS",
  "qty": 100.0,
  "uom": "LB",
  "scrap_qty": 2.5,
}
```

4. Orchestrator resolves `product_key` via `mapping_repo` and converts quantities to LB.
5. `loaders.fact_loader` checks the event hash against previous ingestions; inserts `fact_production` if new, else returns `duplicate` response.
6. The API returns an `IngestResponse` indicating `inserted` or `duplicate`.

### Developer snippets

Registering a plugin (runtime dynamic registration):

```py
from app.registry import registry
from app.plugins.pork_erp import PorkErpPlugin

registry.register(PorkErpPlugin())
```

Resolving a canonical product (mapping_repo example):

```py
from app.mapping_repo import MappingRepo
from app.db import Session

with Session() as session:
    repo = MappingRepo(session)
    product_key = repo.resolve(source_system='PORK_ERP', source_item_id='ITM-100221')
```

Writing a canonical event via the orchestrator:

```py
from app.orchestration import Orchestrator
from app.db import Session

payload = {...}  # raw plugin input
with Session() as session:
    orch = Orchestrator(session)
    resp = orch.ingest(payload)
    print(resp)
```

### Tests and fixtures

The `tests/conftest.py` exposes an in-memory SQLite engine/session and idempotent seeding used by unit tests and end-to-end tests. Tests exercise plugin registration, UOM conversion, mapping resolution, and end-to-end ingestion (`test_ingest_e2e.py`).

### Operational notes

- Idempotency: events are hashed (SHA256) before insertion; duplicates are detected and safely skipped.
- Pricing: `fact_price_by_plant` models SCD Type 2 using `effective_start_dt`, `effective_end_dt`, and `is_current` flags.
- Scaling: for production, swap `DATABASE_URL` to Postgres, add indexing and partitioning, and move orchestration to a task scheduler (Airflow/Prefect).

---


## How to Add a New Source Plugin

1. **Create a new plugin file** in `app/plugins/`, e.g. `app/plugins/lamb_tms.py`:

```python
from datetime import datetime
from app.plugins.base import SourcePlugin

class LambTmsPlugin(SourcePlugin):
    @property
    def source_system(self) -> str:
        return "LAMB_TMS"

    def transform_payload(self, payload: dict) -> dict:
        return {
            "source_system": self.source_system,
            "source_event_id": payload["source_event_id"],
            "event_ts": datetime.fromisoformat(payload["event_ts"]),
            "plant_code": payload["plant_code"],
            "source_item_id": payload["item_id"],
            "source_item_desc": payload.get("item_desc"),
            "qty": payload.get("qty", 0.0),
            "uom": payload.get("uom", "LB"),
            "scrap_qty": payload.get("scrap_qty", 0.0),
        }
```

2. **Register the plugin** in `app/main.py`:

```python
from app.plugins.lamb_tms import LambTmsPlugin
# ...
_registry.register(LambTmsPlugin)
```

3. **Add a seed mapping** for the new source in `app/seed.py` (or insert directly into `map_product_source_to_canonical`).

4. **Restart the server** — the new plugin is immediately available via `POST /ingest/production`.

---

## API Reference

### `GET /health`
Returns the platform status and list of registered source systems.

```json
{"status": "ok", "sources": ["BEEF_WMS", "PORK_ERP", "POULTRY_MES"]}
```

### `POST /ingest/production`
Accepts any JSON payload. Uses `source_system` field to route to the correct plugin.

**Response (success):**
```json
{
  "status": "inserted",
  "event": {
    "source_system": "PORK_ERP",
    "source_event_id": "P-0001",
    "event_ts": "2026-02-21T09:00:00",
    "plant_code": "VA01",
    "product_key": 1,
    "produced_qty_lb": 100.0,
    "scrap_qty_lb": 2.5
  }
}
```

**Response (duplicate):**
```json
{"status": "duplicate", "event": {...}}
```

**Error codes:**
- `400` — Unknown source system or unsupported UOM
- `422` — SKU mapping not found
- `500` — Unexpected error
