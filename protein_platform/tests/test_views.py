from app.views import ensure_protein_views


class _FakeConn:
    def __init__(self, recorder):
        self.recorder = recorder

    def execute(self, stmt):
        # record the SQL text
        try:
            text = str(stmt)
        except Exception:
            text = stmt
        self.recorder.append(text)


class _FakeEngine:
    def __init__(self, recorder):
        self.recorder = recorder

    def begin(self):
        class Ctx:
            def __enter__(inner_self):
                return _FakeConn(self.recorder)

            def __exit__(inner_self, exc_type, exc, tb):
                return False

        return Ctx()


def test_ensure_protein_views_executes_create_statements():
    rec = []
    eng = _FakeEngine(rec)
    ensure_protein_views(eng)
    # should have executed 3 statements
    assert any("vw_pork_production" in r for r in rec)
    assert any("vw_beef_production" in r for r in rec)
    assert any("vw_poultry_production" in r for r in rec)
    # ensure CREATE OR REPLACE VIEW appears
    assert any("CREATE OR REPLACE VIEW" in r.upper() for r in rec)
