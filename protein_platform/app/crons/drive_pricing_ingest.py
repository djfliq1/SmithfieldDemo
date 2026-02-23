from __future__ import annotations

import csv
import hashlib
import os
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from io import StringIO
from typing import Iterable, Optional

import requests
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db import build_engine, build_session_factory, load_db_config
from app.models import DimPlant, DimProduct, FactPriceByPlant

# ----------------------------
# Config / Constants
# ----------------------------

# ✅ Your folder to watch (default). Env var can override if you want.
DEFAULT_FOLDER_URL = "https://drive.google.com/drive/folders/1h4lc-yx70G21Nnmo6CSOAMdawmLg8dfZ?usp=drive_link"

DEFAULT_PREFIX = "pricing_by_plant_"
DEFAULT_SUFFIX = ".csv"
STATE_SOURCE_SYSTEM = "GDRIVE_PUBLIC"


# ----------------------------
# Data models (local)
# ----------------------------

@dataclass(frozen=True)
class DriveFile:
    file_id: str
    file_name: str


@dataclass(frozen=True)
class PricingRow:
    plant_code: str
    canonical_sku: str
    price_per_lb: Decimal
    currency: str
    effective_start_dt: date
    effective_end_dt: Optional[date]
    is_current: bool


# ----------------------------
# Ingestion state table
# ----------------------------

def ensure_ingestion_state_table(engine) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS etl_file_ingestion_state (
        ingestion_key SERIAL PRIMARY KEY,
        source_system VARCHAR(50) NOT NULL,
        source_location TEXT NOT NULL,
        file_id VARCHAR(128) NOT NULL,
        file_name TEXT NOT NULL,
        file_hash VARCHAR(64),
        status VARCHAR(20) NOT NULL,
        rows_loaded INTEGER NOT NULL DEFAULT 0,
        error_message TEXT,
        ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_etl_file UNIQUE (source_system, file_id, source_location)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def already_ingested_success(
    session: Session,
    folder_url: str,
    file_id: str,
    file_hash: Optional[str],
) -> bool:
    q = text(
        """
        SELECT status, file_hash
        FROM etl_file_ingestion_state
        WHERE source_system = :source_system
          AND source_location = :source_location
          AND file_id = :file_id
        ORDER BY ingested_at DESC
        LIMIT 1
        """
    )
    row = session.execute(
        q,
        {
            "source_system": STATE_SOURCE_SYSTEM,
            "source_location": folder_url,
            "file_id": file_id,
        },
    ).mappings().first()

    if not row:
        return False

    if row["status"] != "SUCCESS":
        return False

    prev_hash = row.get("file_hash")
    if file_hash and prev_hash and file_hash != prev_hash:
        # file changed => reprocess
        return False

    return True


def write_ingestion_state(
    session: Session,
    folder_url: str,
    file_id: str,
    file_name: str,
    status: str,
    rows_loaded: int = 0,
    file_hash: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    ins = text(
        """
        INSERT INTO etl_file_ingestion_state
            (source_system, source_location, file_id, file_name, file_hash, status, rows_loaded, error_message)
        VALUES
            (:source_system, :source_location, :file_id, :file_name, :file_hash, :status, :rows_loaded, :error_message)
        """
    )
    session.execute(
        ins,
        {
            "source_system": STATE_SOURCE_SYSTEM,
            "source_location": folder_url,
            "file_id": file_id,
            "file_name": file_name,
            "file_hash": file_hash,
            "status": status,
            "rows_loaded": rows_loaded,
            "error_message": error_message,
        },
    )


# ----------------------------
# Google Drive (public folder scrape + download)
# ----------------------------

def normalize_folder_url(url: str) -> str:
    """
    Normalize for consistent state keys. Keep folder id, drop extra tracking params if present.
    """
    url = (url or "").strip()
    if not url:
        return url
    # Keep base + folder id portion
    m = re.search(r"(https?://drive\.google\.com/drive/folders/[a-zA-Z0-9_-]+)", url)
    if m:
        return m.group(1)
    return url


    # Fallback: zip ids with target_names (deterministic, but weak pairing)
    if not target_names or not ids:
        return []

    ids_list = sorted(ids)  # deterministic
    return [DriveFile(file_id=fid, file_name=nm) for fid, nm in zip(ids_list, target_names)]


def _download_uc(file_id: str, session: requests.Session, timeout_sec: int = 60) -> bytes:
    """
    Downloads via uc endpoint. Handles Google "confirm download" interstitial.
    """
    url = "https://drive.google.com/uc"
    params = {"export": "download", "id": file_id}

    r = session.get(url, params=params, stream=True, timeout=timeout_sec)
    r.raise_for_status()

    content_type = (r.headers.get("content-type") or "").lower()
    if "text/html" in content_type:
        text_html = r.text

        m = re.search(r"confirm=([0-9A-Za-z_]+)", text_html)
        if m:
            confirm = m.group(1)
            r2 = session.get(
                url,
                params={"export": "download", "id": file_id, "confirm": confirm},
                stream=True,
                timeout=timeout_sec,
            )
            r2.raise_for_status()
            return r2.content

        m2 = re.search(r'name="confirm"\s+value="([^"]+)"', text_html)
        if m2:
            confirm = m2.group(1)
            r3 = session.get(
                url,
                params={"export": "download", "id": file_id, "confirm": confirm},
                stream=True,
                timeout=timeout_sec,
            )
            r3.raise_for_status()
            return r3.content

        # If it's HTML and no confirm found, it's likely a permissions/auth wall
        raise RuntimeError("Drive download returned HTML (likely not public or blocked).")

    return r.content


def download_drive_file(file_id: str, timeout_sec: int = 60) -> bytes:
    with requests.Session() as s:
        return _download_uc(file_id=file_id, session=s, timeout_sec=timeout_sec)


def sha256_hex(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


# ----------------------------
# CSV parsing
# ----------------------------

def _parse_date(s: str) -> date:
    s = s.strip()
    return date.fromisoformat(s)


def _parse_bool(s: str) -> bool:
    s = (s or "").strip().lower()
    return s in {"1", "true", "t", "yes", "y"}


def parse_pricing_csv(csv_bytes: bytes) -> list[PricingRow]:
    text_data = csv_bytes.decode("utf-8-sig")
    reader = csv.DictReader(StringIO(text_data))

    required = {
        "plant_code",
        "canonical_sku",
        "price_per_lb",
        "currency",
        "effective_start_dt",
        "effective_end_dt",
        "is_current",
    }
    missing = required - set(reader.fieldnames or [])
    if missing:
        raise ValueError(f"CSV missing required headers: {sorted(missing)}")

    rows: list[PricingRow] = []
    for i, r in enumerate(reader, start=2):
        try:
            plant_code = (r["plant_code"] or "").strip()
            canonical_sku = (r["canonical_sku"] or "").strip()
            if not plant_code or not canonical_sku:
                raise ValueError("plant_code and canonical_sku must be non-empty")

            price = Decimal((r["price_per_lb"] or "").strip())
            currency = (r["currency"] or "USD").strip().upper()

            start_dt = _parse_date(r["effective_start_dt"])
            end_raw = (r["effective_end_dt"] or "").strip()
            end_dt = _parse_date(end_raw) if end_raw else None

            is_current = _parse_bool(r["is_current"])

            rows.append(
                PricingRow(
                    plant_code=plant_code,
                    canonical_sku=canonical_sku,
                    price_per_lb=price,
                    currency=currency,
                    effective_start_dt=start_dt,
                    effective_end_dt=end_dt,
                    is_current=is_current,
                )
            )
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"Bad row at line {i}: {exc}") from exc

    return rows


# ----------------------------
# DB upserts
# ----------------------------

def ensure_plants(session: Session, plant_codes: Iterable[str]) -> None:
    """
    Create plants if missing (simple demo defaults).
    """
    for code in sorted(set(pc.strip() for pc in plant_codes if pc and pc.strip())):
        existing = session.get(DimPlant, code)
        if existing:
            continue
        session.add(
            DimPlant(
                plant_code=code,
                plant_name=f"Plant {code}",
                state=None,
                region=None,
                is_active=True,
            )
        )


def resolve_products(session: Session, skus: Iterable[str]) -> dict[str, DimProduct]:
    """
    Requires products already exist in dim_product (seeded by your app seed).
    """
    sku_list = sorted(set(s.strip() for s in skus if s and s.strip()))
    if not sku_list:
        return {}

    q = (
        session.query(DimProduct)
        .filter(DimProduct.canonical_sku.in_(sku_list))
        .all()
    )
    by_sku = {p.canonical_sku: p for p in q}

    missing = [s for s in sku_list if s not in by_sku]
    if missing:
        raise ValueError(f"Unknown canonical_sku(s) not found in dim_product: {missing}")

    return by_sku


def upsert_pricing_rows(session: Session, rows: list[PricingRow]) -> int:
    if not rows:
        return 0

    ensure_plants(session, [r.plant_code for r in rows])
    products_by_sku = resolve_products(session, [r.canonical_sku for r in rows])

    inserted = 0
    for r in rows:
        product = products_by_sku[r.canonical_sku]

        # If new record is_current=True, turn off existing current rows for same product+plant
        if r.is_current:
            session.query(FactPriceByPlant).filter(
                FactPriceByPlant.product_key == product.product_key,
                FactPriceByPlant.plant_code == r.plant_code,
                FactPriceByPlant.is_current.is_(True),
            ).update({"is_current": False}, synchronize_session=False)

        # Check if this exact (product_key, plant_code, effective_start_dt) already exists
        existing = (
            session.query(FactPriceByPlant)
            .filter(
                FactPriceByPlant.product_key == product.product_key,
                FactPriceByPlant.plant_code == r.plant_code,
                FactPriceByPlant.effective_start_dt == r.effective_start_dt,
            )
            .one_or_none()
        )

        if existing:
            # Update fields (idempotent)
            existing.price_per_lb = r.price_per_lb
            existing.currency = r.currency
            existing.effective_end_dt = r.effective_end_dt
            existing.is_current = r.is_current
        else:
            session.add(
                FactPriceByPlant(
                    product_key=product.product_key,
                    plant_code=r.plant_code,
                    price_per_lb=r.price_per_lb,
                    currency=r.currency,
                    effective_start_dt=r.effective_start_dt,
                    effective_end_dt=r.effective_end_dt,
                    is_current=r.is_current,
                )
            )
            inserted += 1

    return inserted


# ----------------------------
# Orchestration
# ----------------------------

def fetch_manifest_files() -> list[DriveFile]:
    """
    Manifest-driven file discovery.
    Reads pricing_manifest.csv from Drive and returns DriveFile objects.
    """
    manifest_id = os.getenv("GDRIVE_MANIFEST_FILE_ID")
    if not manifest_id:
        raise SystemExit("Missing GDRIVE_MANIFEST_FILE_ID (required for manifest-driven ingestion)")

    url = f"https://drive.google.com/uc?export=download&id={manifest_id}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    reader = csv.DictReader(StringIO(resp.text))
    files: list[DriveFile] = []

    for row in reader:
        file_name = (row.get("file_name") or "").strip()
        file_id = (row.get("file_id") or "").strip()
        if not file_name or not file_id:
            continue
        files.append(DriveFile(file_id=file_id, file_name=file_name))

    return files


def ingest_folder_once(session: Session, folder_url: str, prefix: str, suffix: str) -> dict:
    files = fetch_manifest_files()

    summary = {
        "folder_url": folder_url,
        "files_found": len(files),
        "files_processed": 0,
        "files_skipped": 0,
        "rows_loaded": 0,
        "failures": [],
    }

    for f in files:
        try:
            data = download_drive_file(f.file_id)
            file_hash = sha256_hex(data)

            if already_ingested_success(session, folder_url, f.file_id, file_hash):
                summary["files_skipped"] += 1
                continue

            rows = parse_pricing_csv(data)
            _ = upsert_pricing_rows(session, rows)

            write_ingestion_state(
                session=session,
                folder_url=folder_url,
                file_id=f.file_id,
                file_name=f.file_name,
                status="SUCCESS",
                rows_loaded=len(rows),
                file_hash=file_hash,
                error_message=None,
            )
            session.commit()

            summary["files_processed"] += 1
            summary["rows_loaded"] += len(rows)

        except Exception as exc:
            session.rollback()
            # record failure state
            try:
                write_ingestion_state(
                    session=session,
                    folder_url=folder_url,
                    file_id=f.file_id,
                    file_name=f.file_name,
                    status="FAILED",
                    rows_loaded=0,
                    file_hash=None,
                    error_message=str(exc)[:2000],
                )
                session.commit()
            except Exception:
                session.rollback()

            summary["failures"].append({"file_name": f.file_name, "file_id": f.file_id, "error": str(exc)})

    return summary


def main() -> None:
    # ✅ Uses your folder by default (no env var required), but env var still overrides if set.
    folder_url = normalize_folder_url(os.getenv("GDRIVE_FOLDER_URL", DEFAULT_FOLDER_URL))
    if not folder_url:
        raise SystemExit("Missing folder URL (GDRIVE_FOLDER_URL or DEFAULT_FOLDER_URL)")

    prefix = os.getenv("GDRIVE_FILE_PREFIX", DEFAULT_PREFIX)
    suffix = os.getenv("GDRIVE_FILE_SUFFIX", DEFAULT_SUFFIX)

    # DB setup (same style as your API)
    config = load_db_config()
    engine = build_engine(config)
    ensure_ingestion_state_table(engine)
    session_factory = build_session_factory(engine)

    with session_factory() as session:
        summary = ingest_folder_once(session, folder_url, prefix, suffix)

    print("CRON SUMMARY:", summary)
    return


if __name__ == "__main__":
    main()
