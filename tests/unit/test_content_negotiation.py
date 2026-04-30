"""
Unit tests for ``ContentNegotiationMiddleware``.

Covers: format resolution (query > Accept > default JSON), in-scope vs.
out-of-scope routes, CSV tabularization rules (single dict, list of
dicts, nested-value rejection with 406), XML serialization (lists,
dicts, scalars, name sanitization), error and non-JSON pass-through.
"""

from xml.etree import ElementTree as ET

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from tsigma.middleware import ContentNegotiationMiddleware


def _app(routes: dict | None = None) -> FastAPI:
    """Build a minimal FastAPI app under /api/ with the negotiation middleware."""
    app = FastAPI()
    app.add_middleware(ContentNegotiationMiddleware)
    routes = routes or {}

    @app.get("/api/v1/list")
    async def _list():
        return [
            {"signal_id": "SIG-001", "primary_street": "Main",
             "latitude": 33.7490, "longitude": -84.3880},
            {"signal_id": "SIG-002", "primary_street": "Peach",
             "latitude": 33.7550, "longitude": -84.3900},
        ]

    @app.get("/api/v1/single")
    async def _single():
        return {"signal_id": "SIG-001", "enabled": True, "latitude": 33.749}

    @app.get("/api/v1/nested")
    async def _nested():
        return {"signal_id": "SIG-001", "metadata": {"key": "value"}}

    @app.get("/api/v1/empty")
    async def _empty():
        return []

    @app.get("/api/v1/scalar")
    async def _scalar():
        return 42

    @app.get("/api/v1/error")
    async def _error():
        raise HTTPException(status_code=404, detail="not found")

    @app.get("/api/v1/reports/foo/export")
    async def _export():
        return {"this": "should pass through unchanged"}

    @app.get("/api/graphql")
    async def _graphql():
        return {"data": {"events": []}}

    @app.get("/health")
    async def _health():
        return {"status": "ok"}

    return app


def _client() -> TestClient:
    return TestClient(_app())


# ---------------------------------------------------------------------------
# Format resolution
# ---------------------------------------------------------------------------


class TestFormatResolution:
    def test_default_is_json(self):
        resp = _client().get("/api/v1/list")
        assert resp.status_code == 200
        assert "application/json" in resp.headers["content-type"]
        assert isinstance(resp.json(), list)

    def test_query_param_csv(self):
        resp = _client().get("/api/v1/list?format=csv")
        assert resp.status_code == 200
        assert "text/csv" in resp.headers["content-type"]
        assert "signal_id" in resp.text  # header row

    def test_query_param_xml(self):
        resp = _client().get("/api/v1/list?format=xml")
        assert resp.status_code == 200
        assert "application/xml" in resp.headers["content-type"]
        assert resp.text.startswith("<?xml")

    def test_accept_header_csv(self):
        resp = _client().get("/api/v1/list", headers={"Accept": "text/csv"})
        assert "text/csv" in resp.headers["content-type"]

    def test_accept_header_xml(self):
        resp = _client().get(
            "/api/v1/list", headers={"Accept": "application/xml"},
        )
        assert "application/xml" in resp.headers["content-type"]

    def test_accept_header_text_xml(self):
        resp = _client().get(
            "/api/v1/list", headers={"Accept": "text/xml"},
        )
        assert "application/xml" in resp.headers["content-type"]

    def test_query_wins_over_accept(self):
        # Conflict: ?format=json but Accept: text/csv → JSON wins.
        resp = _client().get(
            "/api/v1/list?format=json", headers={"Accept": "text/csv"},
        )
        assert "application/json" in resp.headers["content-type"]

    def test_unknown_format_falls_back_to_accept(self):
        resp = _client().get(
            "/api/v1/list?format=yaml", headers={"Accept": "text/csv"},
        )
        assert "text/csv" in resp.headers["content-type"]

    def test_format_param_is_case_insensitive(self):
        resp = _client().get("/api/v1/list?format=CSV")
        assert "text/csv" in resp.headers["content-type"]


# ---------------------------------------------------------------------------
# Scope: out-of-scope paths pass through unchanged
# ---------------------------------------------------------------------------


class TestScope:
    def test_health_path_unchanged(self):
        resp = _client().get("/health?format=csv")
        assert "application/json" in resp.headers["content-type"]

    def test_graphql_path_unchanged(self):
        resp = _client().get("/api/graphql?format=csv")
        assert "application/json" in resp.headers["content-type"]

    def test_export_path_unchanged(self):
        resp = _client().get("/api/v1/reports/foo/export?format=csv")
        assert "application/json" in resp.headers["content-type"]

    def test_post_request_unchanged(self):
        # Negotiation only applies to GET.  Make a POST and verify the
        # middleware doesn't interfere (synthesise a POST route inline).
        app = _app()

        @app.post("/api/v1/echo")
        async def _echo(body: dict):
            return body

        client = TestClient(app)
        resp = client.post(
            "/api/v1/echo?format=csv",
            json={"hello": "world"},
        )
        # POST returns JSON regardless.
        assert "application/json" in resp.headers["content-type"]


# ---------------------------------------------------------------------------
# Errors and non-JSON responses
# ---------------------------------------------------------------------------


class TestErrorPassThrough:
    def test_404_keeps_json(self):
        resp = _client().get("/api/v1/error?format=csv")
        assert resp.status_code == 404
        assert "application/json" in resp.headers["content-type"]


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------


class TestCSV:
    def test_list_of_dicts_renders_with_header_row(self):
        resp = _client().get("/api/v1/list?format=csv")
        assert resp.status_code == 200
        lines = resp.text.strip().splitlines()
        # Header + 2 data rows.
        assert len(lines) == 3
        # Header has every key from the first row.
        for col in ("signal_id", "primary_street", "latitude", "longitude"):
            assert col in lines[0]
        assert "SIG-001" in lines[1]
        assert "SIG-002" in lines[2]

    def test_single_dict_renders_as_one_row(self):
        resp = _client().get("/api/v1/single?format=csv")
        assert resp.status_code == 200
        lines = resp.text.strip().splitlines()
        assert len(lines) == 2
        assert "signal_id" in lines[0]
        assert "SIG-001" in lines[1]

    def test_empty_list_renders_as_empty_body(self):
        resp = _client().get("/api/v1/empty?format=csv")
        assert resp.status_code == 200
        assert resp.text == ""
        assert "text/csv" in resp.headers["content-type"]

    def test_nested_returns_406(self):
        resp = _client().get("/api/v1/nested?format=csv")
        assert resp.status_code == 406
        body = resp.json()
        assert "tabularize" in body["detail"]
        assert "text/csv" in body["detail"]

    def test_scalar_returns_406(self):
        resp = _client().get("/api/v1/scalar?format=csv")
        assert resp.status_code == 406
        body = resp.json()
        assert "object or array" in body["detail"]


# ---------------------------------------------------------------------------
# XML
# ---------------------------------------------------------------------------


class TestXML:
    def test_list_of_dicts_renders_as_items(self):
        resp = _client().get("/api/v1/list?format=xml")
        assert resp.status_code == 200
        root = ET.fromstring(resp.text)
        assert root.tag == "items"
        items = list(root)
        assert len(items) == 2
        for item in items:
            assert item.tag == "item"
        first = items[0]
        signal_id = first.find("signal_id")
        assert signal_id is not None
        assert signal_id.text == "SIG-001"

    def test_single_dict_renders_as_item(self):
        resp = _client().get("/api/v1/single?format=xml")
        root = ET.fromstring(resp.text)
        assert root.tag == "item"
        sid = root.find("signal_id")
        assert sid is not None and sid.text == "SIG-001"

    def test_nested_dict_serializes_recursively(self):
        resp = _client().get("/api/v1/nested?format=xml")
        assert resp.status_code == 200
        root = ET.fromstring(resp.text)
        meta = root.find("metadata")
        assert meta is not None
        key = meta.find("key")
        assert key is not None and key.text == "value"

    def test_scalar_renders_as_value(self):
        resp = _client().get("/api/v1/scalar?format=xml")
        root = ET.fromstring(resp.text)
        assert root.tag == "value"
        assert root.text == "42"

    def test_bool_serializes_as_lowercase(self):
        resp = _client().get("/api/v1/single?format=xml")
        root = ET.fromstring(resp.text)
        enabled = root.find("enabled")
        assert enabled is not None
        assert enabled.text == "true"

    def test_safe_tag_replaces_illegal_chars(self):
        # Build an inline app whose response uses tricky keys.
        app = FastAPI()
        app.add_middleware(ContentNegotiationMiddleware)

        @app.get("/api/v1/dirty")
        async def _dirty():
            return {"123leading": 1, "with space": 2, "ok-key": 3}

        client = TestClient(app)
        resp = client.get("/api/v1/dirty?format=xml")
        assert resp.status_code == 200
        root = ET.fromstring(resp.text)
        # leading-digit key gets an underscore prefix
        assert root.find("_123leading") is not None
        # space gets replaced
        assert root.find("with_space") is not None
        # safe characters preserved
        assert root.find("ok-key") is not None
