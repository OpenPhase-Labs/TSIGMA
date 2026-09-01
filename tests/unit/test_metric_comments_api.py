"""Unit tests for the metric comments API (Phase B).

Covers the hand-rolled author-scoped CRUD routes and the metric-type
membership sub-routes for ``/api/v1/metric-comments``.

Reads are gated by ``require_access("comments")``; create/update/delete
require an authenticated caller (``get_current_user``) and are additionally
restricted to the comment's author or an admin.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import FastAPI, status
from fastapi.testclient import TestClient
from pydantic import ValidationError

from tests._helpers import make_mock_session
from tsigma.api.v1.metric_comments import (
    MetricCommentCreate,
    MetricCommentResponse,
    MetricCommentUpdate,
)
from tsigma.api.v1.metric_comments import router as metric_comments_router
from tsigma.api.v1.schemas import UPDATE_REQUIRED_MSG
from tsigma.auth.dependencies import _get_db_session, get_current_user_optional
from tsigma.auth.sessions import SessionData
from tsigma.dependencies import get_audited_session, get_session
from tsigma.settings_service import settings_cache

PREFIX = '/api/v1/metric-comments'

AUTHOR_ID = uuid4()
OTHER_ID = uuid4()
ADMIN_ID = uuid4()


def _user(user_id, username, role='viewer'):
    return SessionData(
        user_id=user_id, username=username, role=role,
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
    )


def _author():
    return _user(AUTHOR_ID, 'alice')


def _other():
    return _user(OTHER_ID, 'bob')


def _admin():
    return _user(ADMIN_ID, 'admin', role='admin')


def _mk_app():
    app = FastAPI()
    app.include_router(metric_comments_router, prefix=PREFIX)
    return app


comments_app = _mk_app()


def _overrides(app, mock_session, user):
    """Wire auth + session dependency overrides. ``user`` of None means unauthenticated."""
    app.dependency_overrides[get_current_user_optional] = (lambda: user)

    async def _access_db():
        m = AsyncMock()
        r = MagicMock()
        r.scalars.return_value.all.return_value = []
        m.execute = AsyncMock(return_value=r)
        yield m

    app.dependency_overrides[_get_db_session] = _access_db

    async def _sess():
        yield mock_session

    app.dependency_overrides[get_session] = _sess
    app.dependency_overrides[get_audited_session] = _sess

    # Force require_access to re-read the (empty) policy table from the mock
    # session so the 'comments' category resolves to authenticated-required.
    settings_cache.invalidate()


def _result(scalar=None, items=None, rows=None):
    """A result mock that answers scalar / scalars / row-tuple access shapes."""
    r = MagicMock()
    items = list(items or [])
    rows = list(rows or [])
    r.scalar_one_or_none.return_value = scalar
    r.scalar.return_value = scalar
    r.scalars.return_value.all.return_value = items
    r.scalars.return_value.first.return_value = items[0] if items else None
    r.all.return_value = rows
    r.first.return_value = rows[0] if rows else None
    r.one_or_none.return_value = rows[0] if rows else None
    return r


def _by_table(pairs, default=None):
    """Dispatch session.execute by the table named in the rendered statement.

    ``pairs`` is an ordered list of (table_name_substring, result); longest /
    most specific names must come first. Order-independent with respect to the
    implementation's query sequence.
    """

    def _execute(statement, *args, **kwargs):
        rendered = str(statement)
        for needle, result in pairs:
            if needle in rendered:
                return result
        return default if default is not None else _result()

    return AsyncMock(side_effect=_execute)


def _comment_obj(author_uuid=AUTHOR_ID, comment_id=None, anchor_start=None, anchor_end=None):
    now = datetime.now(timezone.utc)
    return MagicMock(
        id=comment_id or uuid4(),
        signal_id='SIG-1',
        text='Construction here 2026-05',
        author_uuid=author_uuid,
        author_username='alice',
        anchor_start=anchor_start,
        anchor_end=anchor_end,
        created_at=now,
        updated_at=now,
    )


def _comment_result(comment, username='alice'):
    """One comment, reachable either as a scalar or as a (comment, username) row."""
    return _result(scalar=comment, items=[comment], rows=[(comment, username)])


def _missing_result():
    return _result(scalar=None, items=[], rows=[])


def _capture_add(mock_session):
    created = []
    mock_session.add = MagicMock(side_effect=created.append)

    async def _flush():
        now = datetime.now(timezone.utc)
        for obj in created:
            if getattr(obj, 'id', None) is None:
                obj.id = uuid4()
            if getattr(obj, 'created_at', None) is None:
                obj.created_at = now
            if getattr(obj, 'updated_at', None) is None:
                obj.updated_at = now

    mock_session.flush = _flush
    return created


def _client(mock_session, user):
    app = comments_app
    _overrides(app, mock_session, user)
    return TestClient(app)


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


def test_response_schema_carries_author_username():
    """MetricCommentResponse exposes the author's username, not just author_uuid."""
    assert 'author_username' in MetricCommentResponse.model_fields
    assert 'author_uuid' in MetricCommentResponse.model_fields


def test_create_schema_anchors_are_optional():
    """Decision 1 - three valid anchor states; both anchor fields default to None."""
    body = MetricCommentCreate(signal_id='SIG-1', text='note')
    assert body.anchor_start is None
    assert body.anchor_end is None


def test_update_schema_requires_at_least_one_field():
    """Empty update bodies reuse the shared UPDATE_REQUIRED_MSG constant."""
    with pytest.raises(ValidationError) as exc:
        MetricCommentUpdate()
    assert UPDATE_REQUIRED_MSG in str(exc.value)


# ---------------------------------------------------------------------------
# Read - require_access("comments")
# ---------------------------------------------------------------------------


def test_list_metric_comments_joins_author_username():
    mock_session = make_mock_session()
    comment = _comment_obj()
    mock_session.execute = _by_table([('metric_comment', _comment_result(comment, 'alice'))])
    res = _client(mock_session, _other()).get(f'{PREFIX}/')
    assert res.status_code == status.HTTP_200_OK
    body = res.json()
    assert len(body) == 1
    assert body[0]['author_username'] == 'alice'
    assert body[0]['author_uuid'] == str(AUTHOR_ID)
    assert body[0]['signal_id'] == 'SIG-1'


def test_list_metric_comments_401_when_unauthenticated():
    mock_session = make_mock_session()
    mock_session.execute = _by_table([('metric_comment', _comment_result(_comment_obj()))])
    res = _client(mock_session, None).get(f'{PREFIX}/')
    assert res.status_code == status.HTTP_401_UNAUTHORIZED


def test_get_metric_comment_200():
    mock_session = make_mock_session()
    comment = _comment_obj()
    mock_session.execute = _by_table([('metric_comment', _comment_result(comment, 'alice'))])
    res = _client(mock_session, _other()).get(f'{PREFIX}/{comment.id}')
    assert res.status_code == status.HTTP_200_OK
    assert res.json()['author_username'] == 'alice'
    assert res.json()['id'] == str(comment.id)


def test_get_metric_comment_404():
    mock_session = make_mock_session()
    mock_session.execute = _by_table([('metric_comment', _missing_result())])
    res = _client(mock_session, _other()).get(f'{PREFIX}/{uuid4()}')
    assert res.status_code == status.HTTP_404_NOT_FOUND


def test_get_metric_comment_401_when_unauthenticated():
    mock_session = make_mock_session()
    comment = _comment_obj()
    mock_session.execute = _by_table([('metric_comment', _comment_result(comment))])
    res = _client(mock_session, None).get(f'{PREFIX}/{comment.id}')
    assert res.status_code == status.HTTP_401_UNAUTHORIZED


# ---------------------------------------------------------------------------
# Create - get_current_user, author taken from the session
# ---------------------------------------------------------------------------


def test_create_metric_comment_201_sets_author_from_session():
    mock_session = make_mock_session()
    created = _capture_add(mock_session)
    mock_session.execute = _by_table([], default=_result(scalar=MagicMock(signal_id='SIG-1')))
    res = _client(mock_session, _author()).post(
        f'{PREFIX}/', json={'signal_id': 'SIG-1', 'text': 'Construction here'}
    )
    assert res.status_code == status.HTTP_201_CREATED
    body = res.json()
    assert body['author_uuid'] == str(AUTHOR_ID)
    assert body['author_username'] == 'alice'
    assert body['text'] == 'Construction here'
    assert len(created) == 1
    assert created[0].author_uuid == AUTHOR_ID


def test_create_metric_comment_ignores_client_supplied_author():
    """author_uuid comes from the session; a body value must not override it."""
    mock_session = make_mock_session()
    _capture_add(mock_session)
    mock_session.execute = _by_table([], default=_result(scalar=MagicMock(signal_id='SIG-1')))
    res = _client(mock_session, _author()).post(
        f'{PREFIX}/',
        json={'signal_id': 'SIG-1', 'text': 'note', 'author_uuid': str(OTHER_ID)},
    )
    assert res.status_code == status.HTTP_201_CREATED
    assert res.json()['author_uuid'] == str(AUTHOR_ID)


def test_create_metric_comment_with_range_anchor():
    mock_session = make_mock_session()
    _capture_add(mock_session)
    mock_session.execute = _by_table([], default=_result(scalar=MagicMock(signal_id='SIG-1')))
    res = _client(mock_session, _author()).post(
        f'{PREFIX}/',
        json={
            'signal_id': 'SIG-1',
            'text': 'Construction window',
            'anchor_start': '2026-05-01T00:00:00+00:00',
            'anchor_end': '2026-05-31T00:00:00+00:00',
        },
    )
    assert res.status_code == status.HTTP_201_CREATED
    assert res.json()['anchor_start'] is not None
    assert res.json()['anchor_end'] is not None


def test_create_metric_comment_401_when_unauthenticated():
    mock_session = make_mock_session()
    _capture_add(mock_session)
    mock_session.execute = _by_table([], default=_result(scalar=MagicMock(signal_id='SIG-1')))
    res = _client(mock_session, None).post(
        f'{PREFIX}/', json={'signal_id': 'SIG-1', 'text': 'note'}
    )
    assert res.status_code == status.HTTP_401_UNAUTHORIZED


def test_create_metric_comment_422_when_text_blank():
    mock_session = make_mock_session()
    _capture_add(mock_session)
    mock_session.execute = _by_table([], default=_result(scalar=MagicMock(signal_id='SIG-1')))
    res = _client(mock_session, _author()).post(
        f'{PREFIX}/', json={'signal_id': 'SIG-1', 'text': ''}
    )
    assert res.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


# ---------------------------------------------------------------------------
# Update - author or admin
# ---------------------------------------------------------------------------


def test_update_metric_comment_as_author_200():
    mock_session = make_mock_session()
    mock_session.flush = AsyncMock()
    comment = _comment_obj()
    mock_session.execute = _by_table([('metric_comment', _comment_result(comment, 'alice'))])
    res = _client(mock_session, _author()).put(f'{PREFIX}/{comment.id}', json={'text': 'Revised'})
    assert res.status_code == status.HTTP_200_OK
    assert res.json()['text'] == 'Revised'


def test_update_metric_comment_as_admin_returns_author_username():
    """An admin may edit another user's comment; the response still names the AUTHOR."""
    mock_session = make_mock_session()
    mock_session.flush = AsyncMock()
    comment = _comment_obj()
    mock_session.execute = _by_table([('metric_comment', _comment_result(comment, 'alice'))])
    res = _client(mock_session, _admin()).put(f'{PREFIX}/{comment.id}', json={'text': 'Revised'})
    assert res.status_code == status.HTTP_200_OK
    assert res.json()['author_username'] == 'alice'
    assert res.json()['author_uuid'] == str(AUTHOR_ID)


def test_update_metric_comment_403_for_non_author_non_admin():
    mock_session = make_mock_session()
    mock_session.flush = AsyncMock()
    comment = _comment_obj()
    mock_session.execute = _by_table([('metric_comment', _comment_result(comment, 'alice'))])
    res = _client(mock_session, _other()).put(f'{PREFIX}/{comment.id}', json={'text': 'Revised'})
    assert res.status_code == status.HTTP_403_FORBIDDEN


def test_update_metric_comment_404():
    mock_session = make_mock_session()
    mock_session.flush = AsyncMock()
    mock_session.execute = _by_table([('metric_comment', _missing_result())])
    res = _client(mock_session, _author()).put(f'{PREFIX}/{uuid4()}', json={'text': 'Revised'})
    assert res.status_code == status.HTTP_404_NOT_FOUND


def test_update_metric_comment_401_when_unauthenticated():
    mock_session = make_mock_session()
    mock_session.flush = AsyncMock()
    comment = _comment_obj()
    mock_session.execute = _by_table([('metric_comment', _comment_result(comment))])
    res = _client(mock_session, None).put(f'{PREFIX}/{comment.id}', json={'text': 'Revised'})
    assert res.status_code == status.HTTP_401_UNAUTHORIZED


def test_update_metric_comment_422_when_body_empty():
    mock_session = make_mock_session()
    mock_session.flush = AsyncMock()
    comment = _comment_obj()
    mock_session.execute = _by_table([('metric_comment', _comment_result(comment))])
    res = _client(mock_session, _author()).put(f'{PREFIX}/{comment.id}', json={})
    assert res.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert UPDATE_REQUIRED_MSG in res.text


def test_update_metric_comment_422_when_clearing_start_leaves_a_dangling_end():
    """A partial update must not be able to reach the fourth anchor state.

    Decision 1 allows three states only. Clearing ``anchor_start`` on a row that
    already carries an ``anchor_end`` produces end-without-start, and neither
    anchor field need appear in the payload for it to happen - so the schema
    validator cannot see it and the merged row must be re-checked.
    """
    mock_session = make_mock_session()
    mock_session.flush = AsyncMock()
    comment = _comment_obj(
        anchor_start=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
        anchor_end=datetime(2026, 6, 1, 13, 0, tzinfo=timezone.utc),
    )
    mock_session.execute = _by_table([('metric_comment', _comment_result(comment))])
    res = _client(mock_session, _author()).put(
        f'{PREFIX}/{comment.id}', json={'anchor_start': None},
    )
    assert res.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert 'anchor_end requires anchor_start' in res.text


def test_update_metric_comment_422_when_inverting_the_range():
    """An update may not leave ``anchor_end`` earlier than ``anchor_start``."""
    mock_session = make_mock_session()
    mock_session.flush = AsyncMock()
    comment = _comment_obj(
        anchor_start=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
        anchor_end=datetime(2026, 6, 1, 13, 0, tzinfo=timezone.utc),
    )
    mock_session.execute = _by_table([('metric_comment', _comment_result(comment))])
    res = _client(mock_session, _author()).put(
        f'{PREFIX}/{comment.id}',
        json={'anchor_start': '2026-06-01T14:00:00+00:00'},
    )
    assert res.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert 'anchor_end must not precede anchor_start' in res.text


# ---------------------------------------------------------------------------
# Delete - author or admin
# ---------------------------------------------------------------------------


def test_delete_metric_comment_as_author_204():
    mock_session = make_mock_session()
    mock_session.delete = AsyncMock()
    comment = _comment_obj()
    mock_session.execute = _by_table([('metric_comment', _comment_result(comment))])
    res = _client(mock_session, _author()).delete(f'{PREFIX}/{comment.id}')
    assert res.status_code == status.HTTP_204_NO_CONTENT
    mock_session.delete.assert_awaited_once()


def test_delete_metric_comment_as_admin_204():
    mock_session = make_mock_session()
    mock_session.delete = AsyncMock()
    comment = _comment_obj()
    mock_session.execute = _by_table([('metric_comment', _comment_result(comment))])
    res = _client(mock_session, _admin()).delete(f'{PREFIX}/{comment.id}')
    assert res.status_code == status.HTTP_204_NO_CONTENT


def test_delete_metric_comment_403_for_non_author_non_admin():
    mock_session = make_mock_session()
    mock_session.delete = AsyncMock()
    comment = _comment_obj()
    mock_session.execute = _by_table([('metric_comment', _comment_result(comment))])
    res = _client(mock_session, _other()).delete(f'{PREFIX}/{comment.id}')
    assert res.status_code == status.HTTP_403_FORBIDDEN
    mock_session.delete.assert_not_awaited()


def test_delete_metric_comment_404():
    mock_session = make_mock_session()
    mock_session.delete = AsyncMock()
    mock_session.execute = _by_table([('metric_comment', _missing_result())])
    res = _client(mock_session, _author()).delete(f'{PREFIX}/{uuid4()}')
    assert res.status_code == status.HTTP_404_NOT_FOUND


def test_delete_metric_comment_401_when_unauthenticated():
    mock_session = make_mock_session()
    mock_session.delete = AsyncMock()
    comment = _comment_obj()
    mock_session.execute = _by_table([('metric_comment', _comment_result(comment))])
    res = _client(mock_session, None).delete(f'{PREFIX}/{comment.id}')
    assert res.status_code == status.HTTP_401_UNAUTHORIZED


# ---------------------------------------------------------------------------
# Metric-type membership (m2m) - mirrors the area/signal membership routes
# ---------------------------------------------------------------------------


def test_list_comment_metric_types_200():
    mock_session = make_mock_session()
    comment = _comment_obj()
    assoc_a = MagicMock(comment_id=comment.id, metric_type_key='ApproachDelay')
    assoc_b = MagicMock(comment_id=comment.id, metric_type_key='SplitMonitor')
    mock_session.execute = _by_table([
        ('metric_comment_metric_type', _result(items=[assoc_a, assoc_b])),
        ('metric_comment', _comment_result(comment)),
    ])
    res = _client(mock_session, _other()).get(f'{PREFIX}/{comment.id}/metric-types')
    assert res.status_code == status.HTTP_200_OK
    assert res.json() == [
        {'metric_type_key': 'ApproachDelay'},
        {'metric_type_key': 'SplitMonitor'},
    ]


def test_list_comment_metric_types_404_when_comment_missing():
    mock_session = make_mock_session()
    mock_session.execute = _by_table([
        ('metric_comment_metric_type', _result(items=[])),
        ('metric_comment', _missing_result()),
    ])
    res = _client(mock_session, _other()).get(f'{PREFIX}/{uuid4()}/metric-types')
    assert res.status_code == status.HTTP_404_NOT_FOUND


def test_list_comment_metric_types_401_when_unauthenticated():
    mock_session = make_mock_session()
    comment = _comment_obj()
    mock_session.execute = _by_table([
        ('metric_comment_metric_type', _result(items=[])),
        ('metric_comment', _comment_result(comment)),
    ])
    res = _client(mock_session, None).get(f'{PREFIX}/{comment.id}/metric-types')
    assert res.status_code == status.HTTP_401_UNAUTHORIZED


def test_add_comment_metric_type_201_as_author():
    mock_session = make_mock_session()
    created = _capture_add(mock_session)
    comment = _comment_obj()
    mock_session.execute = _by_table([
        ('metric_comment_metric_type', _result(scalar=None)),
        ('metric_type', _result(scalar=MagicMock(key='ApproachDelay'))),
        ('metric_comment', _comment_result(comment)),
    ])
    res = _client(mock_session, _author()).post(
        f'{PREFIX}/{comment.id}/metric-types', json={'metric_type_key': 'ApproachDelay'}
    )
    assert res.status_code == status.HTTP_201_CREATED
    assert res.json() == {'metric_type_key': 'ApproachDelay'}
    assert len(created) == 1
    assert created[0].metric_type_key == 'ApproachDelay'
    assert created[0].comment_id == comment.id


def test_add_comment_metric_type_403_for_non_author_non_admin():
    mock_session = make_mock_session()
    created = _capture_add(mock_session)
    comment = _comment_obj()
    mock_session.execute = _by_table([
        ('metric_comment_metric_type', _result(scalar=None)),
        ('metric_type', _result(scalar=MagicMock(key='ApproachDelay'))),
        ('metric_comment', _comment_result(comment)),
    ])
    res = _client(mock_session, _other()).post(
        f'{PREFIX}/{comment.id}/metric-types', json={'metric_type_key': 'ApproachDelay'}
    )
    assert res.status_code == status.HTTP_403_FORBIDDEN
    assert created == []


def test_add_comment_metric_type_201_as_admin():
    mock_session = make_mock_session()
    _capture_add(mock_session)
    comment = _comment_obj()
    mock_session.execute = _by_table([
        ('metric_comment_metric_type', _result(scalar=None)),
        ('metric_type', _result(scalar=MagicMock(key='ApproachDelay'))),
        ('metric_comment', _comment_result(comment)),
    ])
    res = _client(mock_session, _admin()).post(
        f'{PREFIX}/{comment.id}/metric-types', json={'metric_type_key': 'ApproachDelay'}
    )
    assert res.status_code == status.HTTP_201_CREATED


def test_add_comment_metric_type_404_when_comment_missing():
    mock_session = make_mock_session()
    mock_session.execute = _by_table([
        ('metric_comment_metric_type', _result(scalar=None)),
        ('metric_type', _result(scalar=MagicMock(key='ApproachDelay'))),
        ('metric_comment', _missing_result()),
    ])
    res = _client(mock_session, _author()).post(
        f'{PREFIX}/{uuid4()}/metric-types', json={'metric_type_key': 'ApproachDelay'}
    )
    assert res.status_code == status.HTTP_404_NOT_FOUND


def test_add_comment_metric_type_404_when_metric_type_missing():
    mock_session = make_mock_session()
    comment = _comment_obj()
    mock_session.execute = _by_table([
        ('metric_comment_metric_type', _result(scalar=None)),
        ('metric_type', _result(scalar=None)),
        ('metric_comment', _comment_result(comment)),
    ])
    res = _client(mock_session, _author()).post(
        f'{PREFIX}/{comment.id}/metric-types', json={'metric_type_key': 'NoSuchMetric'}
    )
    assert res.status_code == status.HTTP_404_NOT_FOUND


def test_add_comment_metric_type_401_when_unauthenticated():
    mock_session = make_mock_session()
    comment = _comment_obj()
    mock_session.execute = _by_table([
        ('metric_comment_metric_type', _result(scalar=None)),
        ('metric_type', _result(scalar=MagicMock(key='ApproachDelay'))),
        ('metric_comment', _comment_result(comment)),
    ])
    res = _client(mock_session, None).post(
        f'{PREFIX}/{comment.id}/metric-types', json={'metric_type_key': 'ApproachDelay'}
    )
    assert res.status_code == status.HTTP_401_UNAUTHORIZED


def test_delete_comment_metric_type_204_as_author():
    mock_session = make_mock_session()
    mock_session.delete = AsyncMock()
    comment = _comment_obj()
    assoc = MagicMock(comment_id=comment.id, metric_type_key='ApproachDelay')
    mock_session.execute = _by_table([
        ('metric_comment_metric_type', _result(scalar=assoc, items=[assoc])),
        ('metric_comment', _comment_result(comment)),
    ])
    res = _client(mock_session, _author()).delete(
        f'{PREFIX}/{comment.id}/metric-types/ApproachDelay'
    )
    assert res.status_code == status.HTTP_204_NO_CONTENT
    mock_session.delete.assert_awaited_once()


def test_delete_comment_metric_type_403_for_non_author_non_admin():
    mock_session = make_mock_session()
    mock_session.delete = AsyncMock()
    comment = _comment_obj()
    assoc = MagicMock(comment_id=comment.id, metric_type_key='ApproachDelay')
    mock_session.execute = _by_table([
        ('metric_comment_metric_type', _result(scalar=assoc, items=[assoc])),
        ('metric_comment', _comment_result(comment)),
    ])
    res = _client(mock_session, _other()).delete(
        f'{PREFIX}/{comment.id}/metric-types/ApproachDelay'
    )
    assert res.status_code == status.HTTP_403_FORBIDDEN
    mock_session.delete.assert_not_awaited()


def test_delete_comment_metric_type_404_when_association_missing():
    mock_session = make_mock_session()
    mock_session.delete = AsyncMock()
    comment = _comment_obj()
    mock_session.execute = _by_table([
        ('metric_comment_metric_type', _result(scalar=None, items=[])),
        ('metric_comment', _comment_result(comment)),
    ])
    res = _client(mock_session, _author()).delete(
        f'{PREFIX}/{comment.id}/metric-types/ApproachDelay'
    )
    assert res.status_code == status.HTTP_404_NOT_FOUND


def test_delete_comment_metric_type_401_when_unauthenticated():
    mock_session = make_mock_session()
    mock_session.delete = AsyncMock()
    comment = _comment_obj()
    assoc = MagicMock(comment_id=comment.id, metric_type_key='ApproachDelay')
    mock_session.execute = _by_table([
        ('metric_comment_metric_type', _result(scalar=assoc, items=[assoc])),
        ('metric_comment', _comment_result(comment)),
    ])
    res = _client(mock_session, None).delete(
        f'{PREFIX}/{comment.id}/metric-types/ApproachDelay'
    )
    assert res.status_code == status.HTTP_401_UNAUTHORIZED
