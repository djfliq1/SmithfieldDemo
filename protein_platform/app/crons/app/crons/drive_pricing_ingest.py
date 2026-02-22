from __future__ import annotations

import csv
import hashlib
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from io import BytesIO, StringIO
from typing import Iterable, Optional

import requests
from sqlalchemy import text, select, update
from sqlalchemy.orm import Session

from app.db import build_engine, build_session_factory, load_db_config
from app.models import Base, DimPlant, DimProduct, FactPriceByPlant


# ----------------------------
# Config / Constants
# ----------------------------

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
# Small helper: ingestion state table
# (Created via SQL so you don't need to touch app/models.py for this demo.)
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
        CONSTRAINT uq_etl_file UNIQUE (source_system, file_id)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def already_ingested_success(session: Session, folder_url: str, file_id: str, file_hash: Optional[str]) -> bool:
    """
    If we have a SUCCESS record for this file_id, skip.
    If file_hash is provided and differs, we will reprocess (treat as updated file).
    """
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
# Google Drive (public folder scraping + download)
# ----------------------------

def fetch_folder_html(folder_url: str, timeout_sec: int = 30) -> str:
    resp = requests.get(folder_url, timeout=timeout_sec)
    resp.raise_for_status()
    return resp.text


def extract_drive_files(html: str, prefix: str, suffix: str) -> list[DriveFile]:
    """
    Hacky extraction:
    - pulls file IDs from patterns like /file/d/<id> and id=<id>
    - pulls file names from occurrences like "name":"<filename>" in embedded JSON
    - pairs them by best-effort proximity; then filters by prefix/suffix
    """
    # Find candidate file IDs
    ids = set()

    # /file/d/<id>
    for m in re.finditer(r"/file/d/([a-zA-Z0-9_-]{10,})", html):
        ids.add(m.group(1))

    # id=<id>
    for m in re.finditer(r"[?&]id=([a-zA-Z0-9_-]{10,})", html):
        ids.add(m.group(1))

    # Names often appear in JSON: "name":"pricing_by_plant_2026-03-01.csv"
    # We'll collect all matching names.
    names = []
    for m in re.finditer(r'"name"\s*:\s*"([^"]+)"', html):
        names.append(m.group(1))

    # Filter names by prefix/suffix first
    target_names = [n for n in names if n.startswith(prefix) and n.endswith(suffix)]

    # If we can't reliably pair names->ids, we still can ingest by ID with unknown name,
    # BUT for demo we want names too. We'll do a best-effort pairing:
    # Many pages embed "driveId":"<id>" near "name":"<filename>".
    # We'll attempt to find (name, id) pairs via proximity patterns.
    pairs = []
    pair_pattern = re.compile(
        r'"name"\s*:\s*"(?P<name>[^"]+)"[^{}]{0,600}?"(driveId|id)"\s*:\s*"(?P<id>[a-zA-Z0-9_-]{10,})"',
        re.DOTALL,
    )
    for m in pair_pattern.finditer(html):
        nm = m.group("name")
        fid = m.group("id")
        if nm.startswith(prefix) and nm.endswith(suffix):
            pairs.append(DriveFile(file_id=fid, file_name=nm))

    # If pairs found, prefer them
    if pairs:
        # Deduplicate by file_id
        seen = set()
        out = []
        for p in pairs:
            if p.file_id in seen:
                continue
            seen.add(p.file_id)
            out.append(p)
        return out

    # Fallback: if no pairs, create entries by IDs and use a synthetic name
    # (still filtered by prefix/suffix is impossible without name).
    # We'll use any discovered target_names to build placeholder name list,
    # otherwise ingest nothing.
    out = []
    ids_list = list(ids)
    if not target_names:
        return out

    # Map first N names to first N ids (best effort) — still deterministic
    for fid, nm in zip(ids_list, target_names):
        out.append(DriveFile(file_id=fid, file_name=nm))
    return out


def _download_uc(file_id: str, session: requests.Session, timeout_sec: int = 60) -> bytes:
    """
    Downloads via uc endpoint. Handles Google "confirm download" interstitial.
    """
    url = "https://drive.google.com/uc"
    params = {"export": "download", "id": file_id}

    r = session.get(url, params=params, stream=True, timeout=timeout_sec)
    r.raise_for_status()

    # If it's a confirmation page, extract confirm token and retry.
    content_type = (r.headers.get("content-type") or "").lower()
    if "text/html" in content_type:
        text_html = r.text
        # token often like confirm=t or confirm=XXXX
        m = re.search(r"confirm=([0-9A-Za-z_]+)", text_html)
        if m:
            confirm = m.group(1)
            r2 = session.get(url, params={"export": "download", "id": file_id, "confirm": confirm}, stream=True, timeout=timeout_sec)
            r2.raise_for_status()
            return r2.content

        # Alternate: look for form input name="confirm"
        m2 = re.search(r'name="confirm"\s+value="([^"]+)"', text_html)
        if m2:
            confirm = m2.group(1)
            r3 = session.get(url, params={"export": "download", "id": file_id, "confirm": confirm}, stream=True, timeout=timeout_sec)
            r3.raise_for_status()
            return r3
