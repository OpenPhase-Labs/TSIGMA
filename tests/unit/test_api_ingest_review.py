"""
Unit tests for the IngestReview worklist API endpoints (inc-3 Task 4).

Covers the two endpoints added to ``tsigma/api/v1/collection.py``:

* ``GET /reviews`` — list IngestReview rows (newest first), optionally
  filtered by ``status`` and/or ``signal_id``. Read access via
  ``require_access("management")`` (same dependency as the checkpoint-read
  endpoints).
* ``POST /reviews/{review_id}/resolve`` — admin only
  (``require_admin``); marks a review resolved, stamping ``resolved_at``
  and ``resolved_by``.

These endpoints do not exist yet, so every test here is expected to FAIL
(import error / 404) until the implementation lands. This file is the RED
side of the TDD cycle and must not be made to pass here.

Mirrors the existing collection API test idiom in
``tests/unit/test_collection_api.py``: a minimal FastAPI app with the
collection ``router`` mounted under ``/api/v1``, auth dependencies replaced
via ``app.dependency_overrides``, ``get_session`` overridden with an async
generator yielding a mock session, and ``session.execute(...).scalars()
.all()`` faked through ``_mock_scalars_result``.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tests._helpers import make_mock_session
from tsigma.api.v1.collection import router
from tsigma.auth.sessions import SessionData

_ADMIN_USERNAME = "review-admin"


def _create_test_app():
    """Create a minimal FastAPI app with the collection router."""
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return app


def _admin_session():
    """Build a SessionData for an admin user with a known username."""
    return SessionData(
        user_id=uuid4(),
        username=_ADMIN_USERNAME,
        role="admin",
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
    )


def _override_admin(app):
    """Override require_admin to inject an admin session."""
    from tsigma.auth.dependencies import require_admin

    app.dependency_overrides[require_admin] = _admin_session


def _override_management_access(app):
    """Override require_access('management') for the list endpoint.

    require_access is a factory; the read endpoints call it at import time,
    so the produced dependency callable is what FastAPI keys on. Mirror the
    checkpoint-read tests' approach by allowing the request through. We
    register an override keyed on the factory-produced dependency by
    clearing auth: the simplest reliable parity is to override the
    underlying current-user dependency to a no-op admin.
    """
    from tsigma.auth.dependencies import get_current_user_optional

    app.dependency_overrides[get_current_user_optional] = _admin_session


def _override_session(app, mock_session):
    """Override get_session with an async generator yielding mock_session."""
    from tsigma.dependencies import get_session as dep_get_session

    async def _gen():
        yield mock_session

    app.dependency_overrides[dep_get_session] = _gen


def _mock_scalars_result(items):
    """Build a mock execute result whose .scalars().all() returns items."""
    result = MagicMock()
    scalars = MagicMock()
    scalars.all.return_value = items
    result.scalars.return_value = scalars
    return result


def _fake_review(
    review_id=None,
    signal_id="SIG-001",
    reason="decode_anomaly",
    status="open",
    severity="warning",
    summary="Something looks off",
    detail=None,
    source_filename="data_001.dat",
    provenance_id=None,
    detected_at=None,
    resolved_at=None,
    resolved_by=None,
):
    """Build a MagicMock standing in for an IngestReview ORM row."""
    review = MagicMock()
    review.review_id = review_id or uuid4()
    review.signal_id = signal_id
    review.reason = reason
    review.source_filename = source_filename
    review.provenance_id = provenance_id
    review.severity = severity
    review.summary = summary
    review.detail = detail if detail is not None else {"k": "v"}
    review.status = status
    review.detected_at = detected_at or datetime(
        2024, 1, 15, 12, 0, tzinfo=timezone.utc
    )
    review.resolved_at = resolved_at
    review.resolved_by = resolved_by
    return review


# ---------------------------------------------------------------------------
# GET /reviews
# ---------------------------------------------------------------------------


class TestListReviews:
    """Tests for GET /api/v1/reviews."""

    def test_list_reviews_returns_rows(self):
        """GET /reviews returns the review rows with the expected shape."""
        app = _create_test_app()
        _override_management_access(app)

        rid = uuid4()
        fake = _fake_review(
            review_id=rid,
            signal_id="SIG-001",
            reason="decode_anomaly",
            status="open",
            detail={"line": 42},
        )

        mock_session = make_mock_session()
        mock_session.execute = AsyncMock(
            return_value=_mock_scalars_result([fake]),
        )
        _override_session(app, mock_session)

        client = TestClient(app)
        resp = client.get("/api/v1/reviews")

        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 1
        row = data[0]
        assert row["review_id"] == str(rid)
        assert row["signal_id"] == "SIG-001"
        assert row["reason"] == "decode_anomaly"
        assert row["status"] == "open"
        assert row["detail"] == {"line": 42}

    def test_list_reviews_filter_by_status(self):
        """GET /reviews?status=open passes through and returns open rows."""
        app = _create_test_app()
        _override_management_access(app)

        fake = _fake_review(status="open")

        mock_session = make_mock_session()
        mock_session.execute = AsyncMock(
            return_value=_mock_scalars_result([fake]),
        )
        _override_session(app, mock_session)

        client = TestClient(app)
        resp = client.get("/api/v1/reviews", params={"status": "open"})

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["status"] == "open"
        mock_session.execute.assert_awaited_once()

    def test_list_reviews_filter_by_signal_id(self):
        """GET /reviews?signal_id=SIG-001 filters by signal."""
        app = _create_test_app()
        _override_management_access(app)

        fake = _fake_review(signal_id="SIG-001")

        mock_session = make_mock_session()
        mock_session.execute = AsyncMock(
            return_value=_mock_scalars_result([fake]),
        )
        _override_session(app, mock_session)

        client = TestClient(app)
        resp = client.get("/api/v1/reviews", params={"signal_id": "SIG-001"})

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["signal_id"] == "SIG-001"
        mock_session.execute.assert_awaited_once()


# ---------------------------------------------------------------------------
# POST /reviews/{review_id}/resolve
# ---------------------------------------------------------------------------


class TestResolveReview:
    """Tests for POST /api/v1/reviews/{review_id}/resolve."""

    def test_resolve_existing_review(self):
        """Resolving an existing review marks it resolved and stamps fields."""
        app = _create_test_app()
        _override_admin(app)

        rid = uuid4()
        fake = _fake_review(review_id=rid, status="open")

        mock_session = make_mock_session()
        # Endpoint is expected to fetch the row by id.
        mock_session.get = AsyncMock(return_value=fake)
        # Fallback path if implementation uses execute().scalar_one_or_none().
        exec_result = MagicMock()
        exec_result.scalar_one_or_none.return_value = fake
        mock_session.execute = AsyncMock(return_value=exec_result)
        mock_session.commit = AsyncMock()
        _override_session(app, mock_session)

        client = TestClient(app)
        resp = client.post(
            f"/api/v1/reviews/{rid}/resolve",
            json={"note": "checked and corrected"},
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["review_id"] == str(rid)
        assert data["status"] == "resolved"
        assert data["resolved_at"] is not None
        assert data["resolved_by"]
        # resolved_by must be derived from the admin session identity.
        assert data["resolved_by"] == _ADMIN_USERNAME
        mock_session.commit.assert_awaited_once()

    def test_resolve_without_body(self):
        """Resolve works with no JSON body (note is optional)."""
        app = _create_test_app()
        _override_admin(app)

        rid = uuid4()
        fake = _fake_review(review_id=rid, status="open")

        mock_session = make_mock_session()
        mock_session.get = AsyncMock(return_value=fake)
        exec_result = MagicMock()
        exec_result.scalar_one_or_none.return_value = fake
        mock_session.execute = AsyncMock(return_value=exec_result)
        mock_session.commit = AsyncMock()
        _override_session(app, mock_session)

        client = TestClient(app)
        resp = client.post(f"/api/v1/reviews/{rid}/resolve")

        assert resp.status_code == 200
        assert resp.json()["status"] == "resolved"

    def test_resolve_unknown_review_returns_404(self):
        """Resolving an unknown review id returns 404."""
        app = _create_test_app()
        _override_admin(app)

        mock_session = make_mock_session()
        mock_session.get = AsyncMock(return_value=None)
        exec_result = MagicMock()
        exec_result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=exec_result)
        mock_session.commit = AsyncMock()
        _override_session(app, mock_session)

        rid = uuid4()
        client = TestClient(app)
        resp = client.post(f"/api/v1/reviews/{rid}/resolve")

        assert resp.status_code == 404

    def test_resolve_requires_admin(self):
        """The resolve endpoint is gated by require_admin (non-admin -> 403)."""
        from fastapi import HTTPException

        from tsigma.auth.dependencies import require_admin

        app = _create_test_app()

        def _deny():
            raise HTTPException(status_code=403, detail="Admin required")

        app.dependency_overrides[require_admin] = _deny

        mock_session = make_mock_session()
        mock_session.get = AsyncMock(return_value=_fake_review())
        mock_session.commit = AsyncMock()
        _override_session(app, mock_session)

        rid = uuid4()
        client = TestClient(app)
        resp = client.post(f"/api/v1/reviews/{rid}/resolve")

        assert resp.status_code == 403
