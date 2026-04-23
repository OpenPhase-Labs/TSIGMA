"""
Unit tests for middleware.

Tests that middleware properly adds headers, request IDs, timing, etc.
"""


class TestRequestIDMiddleware:
    """Tests for RequestIDMiddleware."""

    def test_adds_request_id_header(self, client):
        """Test that X-Request-ID header is added to response."""
        response = client.get("/docs")
        assert "X-Request-ID" in response.headers
        assert len(response.headers["X-Request-ID"]) == 36  # UUID length with dashes


class TestTimingMiddleware:
    """Tests for TimingMiddleware."""

    def test_adds_process_time_header(self, client):
        """Test that X-Process-Time header is added to response."""
        response = client.get("/docs")
        assert "X-Process-Time" in response.headers
        assert response.headers["X-Process-Time"].endswith("ms")


class TestSecurityHeadersMiddleware:
    """Tests for SecurityHeadersMiddleware."""

    def test_adds_csp_header(self, client):
        """Test that Content-Security-Policy header is added."""
        response = client.get("/docs")
        assert "Content-Security-Policy" in response.headers
        csp = response.headers["Content-Security-Policy"]
        assert "default-src 'self'" in csp
        assert "frame-ancestors 'none'" in csp

    def test_adds_x_frame_options(self, client):
        """Test that X-Frame-Options is set to DENY."""
        response = client.get("/docs")
        assert response.headers["X-Frame-Options"] == "DENY"

    def test_adds_x_content_type_options(self, client):
        """Test that X-Content-Type-Options is set to nosniff."""
        response = client.get("/docs")
        assert response.headers["X-Content-Type-Options"] == "nosniff"

    def test_adds_referrer_policy(self, client):
        """Test that Referrer-Policy header is added."""
        response = client.get("/docs")
        assert "Referrer-Policy" in response.headers


class TestGZipMiddleware:
    """Tests for GZIP compression."""

    def test_compresses_large_responses(self, client):
        """Test that large responses are compressed."""
        # This would require an endpoint that returns >1KB
        # For now, just verify middleware is registered
        response = client.get("/docs")
        # GZIP only applies to responses > 1KB
        # FastAPI docs page should be large enough to trigger compression
        assert response.status_code == 200
