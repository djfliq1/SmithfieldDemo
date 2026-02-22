import os
from fastapi.testclient import TestClient
from app.main import app


def test_admin_seed_ok(monkeypatch):
    token = "test-admin-token"
    monkeypatch.setenv("ADMIN_TOKEN", token)
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
    client = TestClient(app)
    resp = client.post("/admin/seed")
    assert resp.status_code == 401
