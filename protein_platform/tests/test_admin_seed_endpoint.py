import os
from fastapi.testclient import TestClient
import importlib
import sys


def _import_app_after_env():
    # ensure app.main is imported after env vars are set
    if "app.main" in sys.modules:
        del sys.modules["app.main"]
    mod = importlib.import_module("app.main")
    return mod.app


def test_admin_seed_ok(monkeypatch):
    token = "test-admin-token"
    monkeypatch.setenv("ADMIN_TOKEN", token)
    # use isolated in-memory DB for this test
    monkeypatch.setenv("DATABASE_URL", "sqlite:///:memory:")
    app = _import_app_after_env()
    client = TestClient(app)
    headers = {"X-Admin-Token": token}
    resp = client.post("/admin/seed", headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("status") == "ok"
    inserted = data.get("inserted")
    assert isinstance(inserted, dict)
    # counts should be non-negative and at least one category should have >0
    assert any(v > 0 for v in inserted.values())


def test_admin_seed_unauthorized(monkeypatch):
    # ensure ADMIN_TOKEN is set but header omitted
    monkeypatch.setenv("ADMIN_TOKEN", "abc")
    # ensure app imported after env set
    app = _import_app_after_env()
    client = TestClient(app)
    resp = client.post("/admin/seed")
    assert resp.status_code == 401
